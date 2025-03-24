(ns cljdekiq.redis
  (:require [taoensso.carmine :as car :refer [wcar]]
            [clojure.data.json :as json]
            [cljdekiq.queue :as cq]
            [cljdekiq.time :refer :all]))

;;
;; Redis Impl
;;

(defn poll-for-work [conn queues]
  (wcar conn
        ;; This looks like (brpop :q1 :q2 :q3 5) when eval'd
        (apply car/brpop (conj queues 5))))

(defn push-job [conn job]
  (wcar conn
        (car/lpush
          ;; Set the work queue from job or use default.
         (or (:queue job) :default)
         (json/write-str job))))

(defn push-schedule [conn job enqueue-at]
  (wcar conn
        (car/zadd
         :schedule
         enqueue-at
         (json/write-str job))))

(defn push-retry [conn job retry-at]
  (wcar conn
        (car/zadd
         :retry
         retry-at
         (json/write-str job))))

(defn enqueue-scheduled [conn key]
  (let [items (wcar (:redis conn)
                    (car/zrangebyscore key "-inf" (now) "limit" 0 10))]

    ;; We have a list of items that we need to delete from the range and
    ;; re-enqueue to run immediately.
    (doseq [item items]
      (let [n-removed (wcar (:redis conn) (car/zrem key item))
            job (json/read-str item :key-fn keyword)]

        ;; Rely on ZREM to tell us if we're the lucky proc that actually
        ;; removed it from redis. If so, then add. Else, skip.
        (if (not (zero? n-removed))
          (push-job conn job))))))

(defrecord RedisQueue [conn]
  cq/Queue

  (tick [this]
    ;; Move jobs in the retry set.
    (enqueue-scheduled (:conn this) :retry)
    ;; Move jobs in the schedule set.
    (enqueue-scheduled (:conn this) :schedule)

    ;; Ask to be called after 5 seconds
    5)

  (poll [this queues]
    (poll-for-work (:conn this) queues))

  (push [this job]
    (push-job (:conn this) job))

  (retry [this job retry-at]
    (push-retry (:conn this) job retry-at))

  (schedule [this job enqueue-at]
    (push-schedule (:conn this) job enqueue-at)))

(defn ->RedisQueueWithDefaults []
  (let [conn-pool (car/connection-pool {})
        conn-spec {:uri "redis://localhost:6379/"}
        wcar-opts {:pool conn-pool, :spec conn-spec}]
    (->RedisQueue wcar-opts)))
