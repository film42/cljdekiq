(ns cljdekiq.sqlite
  (:require [next.jdbc :as jdbc]
            [hikari-cp.core :as hikari]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [cljdekiq.queue :as cq]
            [cljdekiq.time :refer [now]]))

(def schema
  [;; Main job queue. The composite index on (queue, perform_at) covers
   ;; the pop query: WHERE queue IN (...) AND perform_at <= ? ORDER BY id.
   ;; Since id is the rowid (INTEGER PRIMARY KEY), it's implicitly part of
   ;; every index, so the subquery in pop-job is an index-only scan.
   "CREATE TABLE IF NOT EXISTS jobs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      perform_at INTEGER NOT NULL,
      queue TEXT NOT NULL,
      data TEXT NOT NULL
    )"
   "CREATE INDEX IF NOT EXISTS idx_jobs_queue_perform_at ON jobs (queue, perform_at)"

   ;; Scheduled and retry jobs share one table, distinguished by label.
   ;; Ticked every second — index on enqueue_at so the range scan is fast.
   "CREATE TABLE IF NOT EXISTS scheduled (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      enqueue_at INTEGER NOT NULL,
      queue TEXT NOT NULL,
      label TEXT NOT NULL,
      data TEXT NOT NULL
    )"
   "CREATE INDEX IF NOT EXISTS idx_scheduled_enqueue_at ON scheduled (enqueue_at)"])

(defn init-db
  "Initialize the queue schema on an existing datasource. Safe to call
   repeatedly — uses IF NOT EXISTS. Returns the datasource."
  [db]
  (jdbc/with-transaction [tx db]
    (doseq [s schema]
      (jdbc/execute! tx [s])))

  db)

(defn insert-job [db queue job-json]
  (jdbc/execute! db ["INSERT INTO jobs (data, queue, perform_at) VALUES (?, ?, ?)"
                     job-json
                     queue
                     (now)]))

(defn schedule-job
  ([db queue job-json enqueue-at]
   (schedule-job db queue job-json enqueue-at "scheduled"))

  ([db queue job-json enqueue-at label]
   (jdbc/execute! db ["INSERT INTO scheduled (data, queue, enqueue_at, label) VALUES (?, ?, ?, ?)"
                      job-json
                      queue
                      enqueue-at
                      label])))

(defn retry-job [db queue job-json at]
  (schedule-job db queue job-json at "retry"))

(defn pop-job [db queues]
  (let [queue-names (mapv name queues)
        placeholders (str/join ", " (repeat (count queue-names) "?"))
        ;; Uses the idx_jobs_queue_perform_at index to find the oldest
        ;; eligible job, then deletes by rowid (fast point delete).
        query (str "DELETE FROM jobs WHERE id = ("
                   "SELECT id FROM jobs WHERE queue IN (" placeholders ") "
                   "AND perform_at <= ? ORDER BY perform_at ASC, id ASC LIMIT 1"
                   ") RETURNING *")
        bindings (conj queue-names (now))
        job (jdbc/execute-one! db (concat [query] bindings))]

    (if (some? job)
      [(:jobs/queue job)
       (json/read-str (:jobs/data job) :key-fn keyword)]

      [nil nil])))

(defn enqueue-scheduled [db]
  (jdbc/with-transaction [tx db]
    (let [right-now (now)]
      ;; Move ready scheduled/retry jobs into the main queue.
      ;; Uses idx_scheduled_enqueue_at for the range scan.
      (jdbc/execute! tx ["INSERT INTO jobs (queue, data, perform_at)
                          SELECT queue, data, enqueue_at
                          FROM scheduled WHERE enqueue_at <= ?
                          ORDER BY id ASC"
                         right-now])

      (jdbc/execute! tx ["DELETE FROM scheduled WHERE enqueue_at <= ?" right-now]))))

(defrecord SqliteQueue [db]
  cq/Queue

  (tick [this]
    (enqueue-scheduled (:db this))
    1)

  (poll [this queues]
    (pop-job (:db this) (vec queues)))

  (push [this job]
    (insert-job (:db this) (name (:queue job)) (json/write-str job))
    job)

  (retry [this job retry-at]
    (retry-job (:db this) (name (:queue job)) (json/write-str job) retry-at)
    job)

  (schedule [this job enqueue-at]
    (schedule-job (:db this) (name (:queue job)) (json/write-str job) enqueue-at)
    job)

  (close [this]
    (when (instance? java.io.Closeable (:db this))
      (.close (:db this)))))

(defn- ->datasource [path]
  (let [in-mem (= path ":memory:")]
    (hikari/make-datasource
     (cond-> {:adapter "sqlite"
              :url (str "jdbc:sqlite:" path)}
       in-mem (assoc :idle-timeout 0 :maximum-pool-size 1)))))

(defn sqlite-queue
  "Pass a path string for simple mode, or a datasource for full control.
   (sqlite-queue \":memory:\")
   (sqlite-queue \"./jobs.db\")
   (sqlite-queue my-hikari-datasource)"
  [path-or-datasource]
  (let [db (if (string? path-or-datasource)
             (->datasource path-or-datasource)
             path-or-datasource)]
    (->SqliteQueue (init-db db))))

(comment

  ;; Simple — in-memory
  (def q (sqlite-queue ":memory:"))

  ;; Simple — on-disk
  (def q (sqlite-queue "./my-jobs.db"))

  ;; Full control — bring your own datasource
  (def ds (hikari/make-datasource
           {:adapter "sqlite"
            :url "jdbc:sqlite:./my-jobs.db"
            :maximum-pool-size 4}))
  (jdbc/execute! ds ["PRAGMA journal_mode=WAL"])
  (def q (sqlite-queue ds))

  (cq/push q {:queue :default :class "TestWorker" :args [1 2 3]})
  (cq/poll q ["default"])
  (cq/close q))
