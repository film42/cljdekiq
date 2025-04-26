(ns cljdekiq.sqlite
  (:require [next.jdbc :as jdbc]
            [hikari-cp.core :as hikari]
            [clojure.data.json :as json]
            [cljdekiq.queue :as cq]
            [cljdekiq.time :refer :all]))

(def spec
  {:adapter "sqlite"
   :url "jdbc:sqlite::memory:"
   ;; Only allow one connection to the in-memory db
   :idle-timeout 0
   :maximum-pool-size 1})

(def db (hikari/make-datasource spec))

(def schema ["CREATE TABLE IF NOT EXISTS jobs (
             		id INTEGER PRIMARY KEY AUTOINCREMENT,
                 perform_at INTEGER,
                 queue TEXT,
                 data TEXT
             )"

             "CREATE TABLE IF NOT EXISTS retries (
             		id INTEGER PRIMARY KEY AUTOINCREMENT,
                 retry_at INTEGER,
                 queue TEXT,
                 data TEXT
             )"

             "CREATE TABLE IF NOT EXISTS scheduled (
             		id INTEGER PRIMARY KEY AUTOINCREMENT,
                 enqueue_at INTEGER,
                 queue TEXT,
                 data TEXT
             )"])

(defn init-db [db]
  (jdbc/with-transaction [tx db]
    (doseq [s schema]
      (jdbc/execute! tx [s])))

  db)


(defn insert-job [db queue job-json]
  (jdbc/execute! db ["insert into jobs (data, queue, perform_at) values (?, ?, ?)"
                     job-json
                     queue
                     (now)]))

(defn schedule-job [db queue job-json at]
  (jdbc/execute! db ["insert into scheduled_jobs (data, queue, enqueue_at) values (?, ?, ?)"
                     job-json
                     queue
                     at]))

(defn retry-job [db queue job-json at]
  (jdbc/execute! db ["insert into retries_jobs (data, queue, retry_at) values (?, ?, ?)"
                     job-json
                     queue
                     at]))

(defn pop-job [db queues]
  (let [queues-? (clojure.string/join ", "
                                      (repeat (count queues) "?"))
        bindings (conj queues (now))
        query (str
               "delete from jobs where id in (
                  select id from jobs where queue in (" queues-? ") and perform_at < ? order by id asc limit 1
                ) returning *")

    job (jdbc/execute-one! db (concat [query] bindings))]

    (println job)

    (if (some? job)
      [(:jobs/queue job) (:jobs/data job)]

      [nil nil])))

(defn enqueue-retries [db]
  (jdbc/with-transaction [tx db]
    (let [right-now (now)
          retried-ids (jdbc/execute! tx [(str
                                           "with to_retry as (select id, queue, data from retries where retry_at <= ? order by id asc)"
                                           "insert into jobs (queue, data) select queue, data from to_retry")
                                         right-now])]

      (jdbc/execute! tx ["delete from retries where retry_at <= ?" right-now]))))

(defn enqueue-scheduled [db]
  (jdbc/with-transaction [tx db]
    (let [right-now (now)
          retried-ids (jdbc/execute! tx [(str
                                           "with to_sched as (select id, queue, data from scheduled where enqueue_at <= ? order by id asc)"
                                           "insert into jobs (queue, data) select queue, data from to_sched")
                                         right-now])]

      (jdbc/execute! tx ["delete from scheduled where enqueue_at <= ?" right-now]))))


(enqueue-retries db)
(enqueue-scheduled db)

(jdbc/execute! db ["insert into retries (queue, data, retry_at) values (?, ?, ?)"
                   "default"
                   "{}"
                  (- (now) 1000)])

(jdbc/execute! db ["select * from retries"])
(jdbc/execute! db ["select * from jobs"])


(insert-job db "default" "{\"test\": \"ok\"")
(insert-job db "other" "{\"test\": \"ok\"")

(jdbc/with-transaction [tx db]
  (jdbc/execute! tx [(nth schema 0)]))

(jdbc/with-transaction [tx db]
  (jdbc/execute! tx ["select * from jobs where queue in (?, ?) order by id desc limit 1"
                     "default"
                     "other"]))


(defrecord SqliteQueue [db]
  cq/Queue

  (tick [this]
    (enqueue-retries (:db this))
    (enqueue-scheduled (:db this))

    1)

  (poll [this queues]
    (pop-job (:db this) (vec queues)))

  (push [this job]
    (insert-job (:db this) (:queue job) job)

    job)

  (retry [this job retry-at]
    (retry-job (:db this) job retry-at)

    job)

  (schedule [this job enqueue-at]
    (schedule-job (:db this) job enqueue-at)

    job)

  (close [this]))

(comment

  (let [q (->SqliteQueue (init-db db))]
    ;(cq/push q {:queue "default"})
    ;(cq/push q {:queue "default"})
    (cq/push q {:queue "default"})

    (println (cq/poll q ["testing"]))
    (println (cq/poll q ["default"]))

    )

  (jdbc/execute! db ["select * from jobs"])



)
