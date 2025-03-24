(ns cljdekiq.queue)

(defprotocol Queue
  "A generic interface over the stateful queue semantics."

  (tick [this] "Called every N seconds to allow the queue to advance internal machinery. Returns the next ticket delay in seconds.")
  (poll [this queues] "Poll for new work. Called repeatedly. Can return [] or nil to indicate no work available.")
  (push [this job] "Insert a new job. Use the job map to infer all routing logic.")
  (retry [this job retry-at] "Retry a job")
  (schedule [this job enqueue-at] "Similar to push but for scheduling a job in the future."))
