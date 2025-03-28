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

(defrecord TestQueue [jobs scheduled retry]
  Queue

  ;; Not used.
  (tick [this]
    (let [[to-enqueue1 retries] (split-by #(<= (:at %) (ct/now)) (deref (:retry this)))
          [to-enqueue2 scheduled] (split-by #(<= (:at %) (ct/now)) (deref (:scheduled this)))]

      ;; Update state
      (reset! (:retry this) (into () retries))
      (reset! (:scheduled this) (into () scheduled))

      (doseq [{job :job} (concat to-enqueue1 to-enqueue2)]
        (push this job)))

     ;; Sleep 1s
    1)

  ;; TODO: Make this ignore queues too.
  (poll [this queues]
    ;; The list can turn lazy, so always transform back to list.
    (let [queue-str-set (set (map name queues))
          jobs (apply list (deref (:jobs this)))
          ;; Find the first job that matches one of the specified queues.
          [skips [next-job & after]] (split-with
                                      (fn [j] (not (contains? queue-str-set
                                                              (name (:queue j)))))
                                      jobs)
          ;; Make sure we convert the queue to a str.
          queue-name (if (map? next-job) (name (:queue next-job)))]

      (if (some? next-job)
        ;; We found a job. Pop the queue and return the rest.
        (reset! (:jobs this) (concat skips after))

        ;; Otherwise we'll just sleep to simulate brpop. Don't sleep long.
        (Thread/sleep 500))

      ;; Return a [queue job] vec to match the spec.
      [queue-name next-job]))

  (push [this job]
    (swap! (:jobs this) concat [job])

    job)

  (retry [this job retry-at]
    (let [retries (deref (:retry this))]
      (reset! (:retry this)
              (->>
               (conj retries
                     {:at retry-at :job job})
               (sort-by :at))))

    job)

  (schedule [this job enqueue-at]
    (let [scheduled (deref (:scheduled this))]
      (reset! (:scheduled this)
              (->>
               (conj scheduled
                     {:at enqueue-at :job job})
               (sort-by :at))))

    job)

  (close [this]))

(defn test-queue []
  ;; A bunch of atoms of empt lists for queue-like behavior.
  (->TestQueue (atom ()) (atom ()) (atom ())))


