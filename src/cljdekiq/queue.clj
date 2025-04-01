(ns cljdekiq.queue
  (:require [cljdekiq.time :as ct]))

(defprotocol Queue
  "A generic interface over the stateful queue semantics."

  (tick [this] "Called every N seconds to allow the queue to advance internal machinery. Returns the next ticket delay in seconds.")
  (poll [this queues] "Poll for new work. Called repeatedly. Can return [] or nil to indicate no work available.")
  (push [this job] "Insert a new job. Use the job map to infer all routing logic.")
  (retry [this job retry-at] "Retry a job")
  (schedule [this job enqueue-at] "Similar to push but for scheduling a job in the future.")
  (close [this] "Clean up any of the queue components."))

;; Essentially ruby's partition method.
(defn split-by [fn coll]
  (let [split (group-by fn coll)
        hits (or (get split true) [])
        misses (or (get split false) [])]
    [hits, misses]))

;; For retries and scheduled jobs, we use a map to create a redis sorted set.
;; This lets us use swap! to rotate data, but also capture the value that was
;; removed.
(defn swap-pop-sorted-set! [atm at]
  (let [res (atom nil)]
    ;; Atomically split the collection to find the next value <= at.
    (swap! atm (fn [coll]
                 (let [[match rest] (split-by #(<= (:at %) at) coll)]
                   ;; Store the match in a temporary atom.
                   (reset! res match)

                   rest)))

    ;; Return the (maybe) found element.
    @res))

;; Take the main in-mem job queue, look to see if a job matches one of the
;; provided queue names. The set of queues should be an actual set of strings.
;; If a job is found, it will be captured and returned. Uses swap! to be thread
;; safe.
(defn swap-find-and-pop! [atm queues-str-set]
  (let [res (atom nil)
        job-not-in-queue-set (fn [j] (not (contains? queues-str-set
                                                     (name (:queue j)))))]

    ;; Atomically pop the first item matching one of the provided queues.
    (swap! atm (fn [coll]
                 ;; Sometimes times we get a vec out. Make sure we're always using a list.
                 (let [coll-as-list (apply list coll)
                       [skips [next & after]] (split-with job-not-in-queue-set coll-as-list)]

                   ;; Capture the next job return before returning the remaining items to swap!.
                   (reset! res next)

                   (concat skips after))))

    ;; Return the popped item.
    @res))

(defrecord TestQueue [jobs scheduled retry]
  Queue

  (tick [this]
    ;; Atomically pop jobs from sorted sets.
    (let [to-enqueue1 (swap-pop-sorted-set! (:retry this) (ct/now))
          to-enqueue2 (swap-pop-sorted-set! (:scheduled this) (ct/now))]
      ;; Push the job if not nil.
      (doseq [{job :job} (concat to-enqueue1 to-enqueue2)]
        (push this job)))

    ;; Sleep 1s
    1)

  (poll [this queues]
    (let [queue-str-set (set (map name queues))
          ;; Pop the next job that matches one of the provided queues.
          next-job (swap-find-and-pop! (:jobs this) queue-str-set)
          ;; Make sure we convert the queue to a str.
          queue-name (if (map? next-job) (name (:queue next-job)))]

      ;; If we don't have a job, simulate the redis brpop timeout.
      (if (nil? next-job)
        (Thread/sleep 500))

      [queue-name next-job]))

  (push [this job]
    (swap! (:jobs this) concat [job])

    job)

  (retry [this job retry-at]
    (swap! (:retry this) (fn [coll]
                           (->>
                            (conj coll {:at retry-at :job job})
                            (sort-by :at))))

    job)

  (schedule [this job enqueue-at]
    (swap! (:scheduled this) (fn [coll]
                               (->>
                                (conj coll {:at enqueue-at :job job})
                                (sort-by :at))))

    job)

  (close [this]))

(defn test-queue []
  ;; A bunch of atoms of empt lists for queue-like behavior.
  (->TestQueue (atom ()) (atom ()) (atom ())))
