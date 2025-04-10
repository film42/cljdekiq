(ns cljdekiq.core
  (:require [taoensso.carmine :as car :refer [wcar]]
            [cljdekiq.queue :refer :all]
            [cljdekiq.redis :as redis]))

;; Re-export the time helper functions here
(doseq [[sym v] (ns-publics 'cljdekiq.time)]
  (intern *ns* sym v))

;; String heleprs
(defn capitalized? [s]
  (let [s1 (str (first s))
        s1-cap (clojure.string/capitalize s1)]
    (= s1 s1-cap)))

(defn constantize [s]
  (->> (clojure.string/split (str s) #"-|_")
       (map (fn [s1]
              ;; Java classes might already be titleized. We should
              ;; check to see if formatting is required.
              (if (capitalized? s1)
                s1
                (clojure.string/capitalize s1))))
       clojure.string/join))

(defn class->ruby-constant [c]
  (let [name (.getName c)
        var-like (clojure.string/replace name "$" "/")]
    (when (clojure.string/includes? name "$fn__")
      (throw (IllegalArgumentException.
              (str "Cannot convert an anonymous function to a ruby constant name: " name))))

    (let [parts (clojure.string/split var-like #"/")

          ; Constantize the fn name to look like ruby.
          ; We can skip this operation if the parts vec size is one.
          ; Java classes don't include a $ or / in the name, but it
          ; still looks like a valid clojure ns, so skip this part.
          ruby-name (if (> (count parts) 1) (constantize (last parts)))

          ; Transform namespace segments (split by dots, capitalize each)
          ns-name (first parts)
          ns-parts (vec (map constantize (clojure.string/split ns-name #"\.")))]

      ; Combine namespace and name
      (clojure.string/join
       "::"
       (remove nil? (conj ns-parts ruby-name))))))

(defn conn
  ;; Automatically create a redis queue using localhost.
  ([]
   (conn (redis/->RedisQueueWithDefaults)))

  ([queue]
   {:workers [] :queue queue}))

(defn queue [conn]
  (:queue conn))

(defn worker [name-or-fn & {:as options}]
  (let [options (or options {})
        class-name (or
                     ;; Check for a user provided class name
                    (:as options)
                     ;; Check for a string constant (not callable name)
                    (if (string? name-or-fn) name-or-fn)
                     ;; Generate a ruby-like constant from the fn's name.
                    (class->ruby-constant (class name-or-fn)))
        retries (if  (nil? (:retry options))
                  ;; Default retry count is 25.
                  25
                  ;; Any number or false is also valid.
                  (:retry options))
        queue (or (:queue options) :default)
        job-fn (if (fn? name-or-fn) name-or-fn)]

    {:class-name class-name
     :retries retries
     :queue queue
     :job-fn job-fn}))

(defn merge-worker [worker-or-fn & {:as options}]
  (let [options (or options {})
        worker (or
                 ;; See if we need to derive some worker opts
                (if (fn? worker-or-fn) (worker worker-or-fn options))

                 ;; Use the provided worker map.
                worker-or-fn)

        ;; Let the caller pass in some overrides. We'll normalize
        ;; and then merge them into the worker map.
        new-opts (->
                  options
                  (clojure.set/rename-keys {:as :class-name})
                  (select-keys [:class-name :retries :queue]))
        new-worker (merge worker new-opts)]

    new-worker))

(defn register [conn worker-or-fn & {:as options}]
  (let [options (or options {})
        worker (merge-worker worker-or-fn options)
        workers (:workers conn)]

    ;; Make sure someone isn't doing something silly like trying to register
    ;; a worker without a fn.
    (if (nil? (:job-fn worker))
      (throw (IllegalArgumentException.
              (str "Cannot register worker because it's missing a job-fn. Is this a ref-only worker?"))))

    ;; Update the conn state.
    (assoc conn :workers (conj workers worker))))

(defn -retry-delay-secs [n]
  (+ (Math/pow n 4)
     15
     (* (+ (rand-int 30) 0)
        (+ n 1))))

;; Assumed retries are allowed to happen if called.
(defn -retry [conn job err]
  (let [retry-value (:retry job)
        max-retries (or
                      ;; Check for a specified number
                     (if (int? retry-value) retry-value)
                      ;; Check for a "true" value so we can use default.
                     (if (boolean? retry-value) 25))

        ;; Check number of times job has retries so far.
        retry-count (or (:retry_count job) 0)

        ;; Extract error info
        err-class (.getName (.getClass err))
        err-msg (.getMessage err)
        err-stacktrace  (map str (.getStackTrace err))

        ;; Push changes into the job map
        new-job (assoc job
                       :retry_count (inc retry-count)
                       ;; Failed at should only be set on the first failure.
                       :failed_at (or (:failed_at job) (now))
                       :retried_at (now)
                       :error_message err-msg
                       :error_class err-class
                       :error_backtrace err-stacktrace)

        ;; Compute the time we'll retry the job.
        retry-at (+ (now)
                    (-retry-delay-secs (inc retry-count)))]

    ;; Insert the job if the max retries have not exceeded.
    (if (< retry-count max-retries)
      (retry (:queue conn) new-job retry-at))))

(defn -invoke [conn worker job]
  (let [job-fn (:job-fn worker)
        args (:args job)]

    (try
      (apply job-fn args)

      (catch Exception e
        (do
          (println "Something bad happened: " e)

          ;; Retry according to the job state, not the worker
          (if (:retry job)
            (-retry conn job e)))))))

(defn -poll-once [conn queues class-to-worker]
  (let [[queue job] (poll (:queue conn) queues)
        worker (get class-to-worker (:class job))]

    (comment
      (clojure.pprint/pprint queue)
      (clojure.pprint/pprint job)
      (clojure.pprint/pprint worker))

    (if (not (nil? worker))
      (-invoke conn worker job)

      ;; Only log about unknown jobs if the job itself is nil.
      (if (not (nil? job))
        (println "Skipping because there is no worker defined with class name: " (:class job))))))

(defn -spawn-fn [f]
  (let [running (atom true)
        ;; Spawn future to compute work until asked to stop.
        ;; Maybe we should make the f decide on while?
        task (future
               (while @running (f)))]

    ;; Return a stopping function. The stopping function returns the underlying
    ;; future so you can serially wait or broadcast "stop" before waiting.
    (fn []
      (reset! running false)
      task)))

(defn -poll-queues [conn]
  (let [queues (->> (:workers conn) (map :queue) set (into []))
        class-to-worker (->>
                         (:workers conn)
                         (map (fn [w] {(:class-name w) w}))
                         (into {}))]
    (fn []
      (-poll-once conn queues class-to-worker))))

(defn run [conn & {:as options}]
  (let [options (or options {})
        num-workers (or (:workers options) 4)

        ;; Build a fn for polling the retry and schedule sets.
        poll-sets (fn []
                    (Thread/sleep
                     (* 1000 (tick (:queue conn)))))
        ;; Build a fn to poll queues. Memo conn state for fast lookups.
        poll-queues (-poll-queues conn)

        ;; Spawn workers
        workers (conj
                 (repeatedly num-workers #(-spawn-fn poll-queues))
                 (-spawn-fn poll-sets))]

    ;; Return a new (stop) function that stops all other stop functions.
    (fn []
      (->> workers
           ;; We _must_ use (mapv) here because it is non-lazy.
           (mapv #(%))
           (mapv deref)))))

(defn -jid []
  (let [random-bytes (byte-array 12)
        secure-random (java.security.SecureRandom.)]
    (.nextBytes secure-random random-bytes)
    (apply str
           (for [b random-bytes]
             (format "%02x" (bit-and b 0xff))))))

(defn new-job [worker-or-fn & args]
  (let [worker (merge-worker worker-or-fn {})
        job-map {:class (:class-name worker)
                 :queue (or (:queue worker) :default)
                 :jid (-jid)
                 :args (or args [])
                 :retry (or (:retries worker) true)
                 :retry_count 0
                 :created_at (now)
                 :enqueued_at (now)}]
    job-map))

(defn perform-async [conn worker-or-fn & args]
  (push (:queue conn) (apply new-job worker-or-fn args)))

(defn perform-in [conn worker-or-fn seconds & args]
  (schedule (:queue conn)
            (apply new-job worker-or-fn args)
            (+ (now) seconds)))

(comment
  ;; Server

  (defn send-email [user-id]
    (do
      (println [:email user-id])

      (+ user-id + 123 + "FAIL")))

  (defn some-ruby-fn [id]
    (println [:email id]))

  (def create-subscription-worker
    (worker "CreateSubscriptionWorker" :queue :web))

  (def demo-proc
    (->
     (conn)

      ;; Register your number function to jobs in the mailers queue.
     (register send-email :queue :mailers :not-used true)

      ;; Register _any_ function!
     (register println)

      ;(register create-subscription-worker :retries false)

      ;(register create-subscription-worker :queue :test )

      ;; You can even pick up work from a legacy ruby app.
     (register some-ruby-fn :as "Legacy::V3::SomeWorker")))

  (dotimes [i 1000]

    (perform-async demo-proc send-email 123)

    ;; I don't love this interface. The duration is kind of hard to see.
    (perform-in demo-proc send-email (seconds 5) 999))

  (->
   (ck/processor)

    ;; Register your number function to jobs in the mailers queue.
   (ck/register send-email :queue :mailers)

    ;; Register _any_ function!
   (ck/register println)

    ;; You can even pick up work from a legacy ruby app.
   (ck/register some-ruby-fn :as "Legacy::V3::SomeWorker")

    ;; Start your server.
   (ck/run)

    ;; Do something with the future returned by the server.
   (deref))

  ;; Client

  (ck/perform-async
   (ck/processor)
   (ck/job some-ruby-fn :queue :mailers) "user-1234")

  (def report-sync-job
    (ck/job "V2::ReportWorker" :queue :mailer))

  (ck/perform-async conn report-sync-job "usr-123")

  (ck/perform-async
   (ck/processor)
   "V2:UserReportWorker"
   (ck/job some-ruby-fn :queue :mailers) "user-1234")

  (ck/perform-async
   (ck/processor)
   println
   "user-1234" "whatever you want")

  (deftype UserMailerJob [db]
    Job
    (perform [this user-id email-type]
      (println [user-id email-typ])))

  (def server
    (->
     (kq/processor)

     (->
      (kq/queue :nasty-girl)
      (kq/register send-email)
      (kq/register sync-stripe :as "ScottLeune::StripeSyncJob"))

     (kq/register send-email)
     (kq/register send-email :as "MyDeep::AppWorker")
     (kq/register send-email :as "MyClass" :queue :nasty-girl)))

  (kq/perform "UserMailerWorker")
  (kq/perform "UserMailerWorker" 1)
  ; (kq/perform '(Test::UserMailerWorker 1))

  (kq/perform send-email)

  (kq/perform send-email "usr-123")

  (kq/perform send-email ["usr-123", 2]
              (kq/opts {:queue :rabbit :retries false}))

  (kq/perform '(send-email "usr-123")
              (kq/opts {:queue :rabbit :retries false})))
