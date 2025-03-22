(ns cljdekiq.core
  (:require [taoensso.carmine :as car :refer [wcar]]
            [clojure.data.json :as json]))

;;(defn var->ruby-constant [v]
;;  (let [var-meta (meta v)
;;        ns-name (str (:ns var-meta))  ; Get namespace as string
;;        var-name (name (:name var-meta))  ; Get var name
;;
;;        ; Transform namespace segments (split by dots, capitalize each)
;;        ns-parts (map #(str (Character/toUpperCase (first %)) (subs % 1))
;;                       (clojure.string/split ns-name #"\."))
;;
;;        ; Transform var name (replace hyphens with underscores, capitalize each part)
;;        var-parts (map #(str (Character/toUpperCase (first %)) (subs % 1))
;;                        (clojure.string/split var-name #"-"))
;;
;;        ; Join namespace with double colons, join var parts with nothing
;;        ruby-ns (clojure.string/join "::" ns-parts)
;;        ruby-name (clojure.string/join "" var-parts)]
;;
;;    ; Combine namespace and name
;;    (str ruby-ns "::" ruby-name)))

(defn -now []
  (long (/ (System/currentTimeMillis) 1000)))

;; (defn constant-to-kebab [ruby-constant]
;;   (let [kebab-case (->> ruby-constant
;;                         (re-seq #"[A-Z][a-z0-9]*")
;;                         (map clojure.string/lower-case)
;;                         (clojure.string/join "-"))]
;;     kebab-case))
;;
;; (defn ruby-constant->var [rb]
;;   (let [parts (clojure.string/split rb #"::")
;;         ns-parts (map constant-to-kebab (drop-last parts))
;;         fn-parts (list (constant-to-kebab (last parts)))
;;         var-str (clojure.string/join "/"
;;                                      (list (clojure.string/join "." ns-parts)
;;                                            (clojure.string/join "" fn-parts)))]
;;     (resolve (symbol var-str))))
;;

;; (defprotocol Job
;;   (perform [this & args ] "Perform some work"))
;;
;; (defn into-job [f]
;;   (if (satisfies? Job f)
;;     f
;;     (reify Job
;;       (perform [this & args]
;;         (apply f args)))))
;;
;; (defn perform
;;   ([f] (perform f []))
;;
;;   ([f & args]
;;    (-> f into-job (apply args))))
;;
;;
;; (defn clj->rb [n]
;;   (if (string? n)
;;     n
;;     (symbol :idk )))
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
  ;; Automatically create a redis pool using localhost.
  ([]
   (let [conn-pool (car/connection-pool {})
         conn-spec {:uri "redis://localhost:6379/"}
         wcar-opts {:pool conn-pool, :spec conn-spec}]
     (conn wcar-opts)))

  ([redis]
   {:workers [] :redis redis}))

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

(defn poll-for-work [conn queues]
  (wcar (:redis conn)
        ;; This looks like (brpop :q1 :q2 :q3 5) when eval'd
        (apply car/brpop (conj queues 5))))

(defn push-job [conn job]
  (wcar (:reds conn)
        (car/lpush
          ;; Set the work queue from job or use default.
         (or (:queue job) :default)
         (json/write-str job))))

;; Assumed retries are allowed to happen if called.
(defn -retry [conn job err]
  (let [retry (:retry job)
        max-retries (or
                      ;; Check for a specified number
                     (if (int? retry) retry)
                      ;; Check for a "true" value so we can use default.
                     (if (boolean? retry) 25))

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
                       :failed_at (or (:failed_at job) (-now))
                       :retried_at (-now)
                       :error_message err-msg
                       :error_class err-class
                       :error_backtrace err-stacktrace)]

    ;; Insert the job if the max retries have not exceeded.
    (if (< retry-count max-retries)
      (push-job conn new-job))))

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
  (let [[queue job-data] (poll-for-work conn queues)
        job (if (string? job-data)
              ;; TODO: Reject bad json strings so as to not raise.
              (json/read-str job-data :key-fn keyword))
        worker (get class-to-worker (:class job))]

    (comment
      (clojure.pprint/pprint job-data)
      (clojure.pprint/pprint queue)
      (clojure.pprint/pprint job)
      (clojure.pprint/pprint worker))

    (if (not (nil? worker))
      (-invoke conn worker job)
      (println "Skipping because there is no worker defined with class name: " (:class job)))))

(defn -spawn-worker [conn]
  (let [queues (->> (:workers conn) (map :queue) set (into []))
        class-to-worker (->>
                         (:workers conn)
                         (map (fn [w] {(:class-name w) w}))
                         (into {}))

        ;; Spawn future to compute work until asked to stop.
        running (atom true)
        task (future
               (while @running
                 (-poll-once conn queues class-to-worker)))]

    ;; Return a stopping function.
    (fn []
      (reset! running false)
      @task)))

(defn run [conn]
  (let [workers [(-spawn-worker conn)]]
    (fn []
      (doseq [stop-fn workers]
        (stop-fn)))))

(defn -jid []
  (let [random-bytes (byte-array 12)
        secure-random (java.security.SecureRandom.)]
    (.nextBytes secure-random random-bytes)
    (apply str
           (for [b random-bytes]
             (format "%02x" (bit-and b 0xff))))))

(defn perform-async [conn worker-or-fn & args]
  (let [worker (merge-worker worker-or-fn {})
        job-map {:class (:class-name worker)
                 :queue (or (:queue worker) :default)
                 :jid (-jid)
                 :args args
                 :retry (or (:retries worker) true)
                 :retry_count 0
                 :created_at (-now)
                 :enqueued_at (-now)}]

    (push-job conn job-map)))

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
    (perform-async demo-proc send-email 123))

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
