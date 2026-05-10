(ns cljdekiq.sqlite-test
  (:require [clojure.test :refer [deftest is testing]]
            [cljdekiq.sqlite :as sq]
            [cljdekiq.queue :as cq]
            [cljdekiq.core :as ck]
            [cljdekiq.time :as ct]))

(defn example-worker1 [])
(defn example-worker2 [])

(deftest it-can-enqueue-and-process-a-job
  (testing "end to end job processing with sqlite"
    (let [q (sq/sqlite-queue ":memory:")
          app (-> (ck/conn q)
                  (ck/register example-worker1 :queue :one)
                  (ck/register example-worker2 :queue :two))]

      (ck/perform-async app (ck/worker example-worker1 :queue :one))

      (let [[queue job] (cq/poll q [:one :two])
            static-keys (select-keys job [:class :queue :args :retry :retry_count])]

        (is (= queue "one"))

        (is (= {:class "Cljdekiq::SqliteTest::ExampleWorker1"
                :queue "one"
                :args []
                :retry 25
                :retry_count 0} static-keys))

        (is (string? (:jid job)))
        (is (int? (:created_at job)))
        (is (int? (:enqueued_at job)))))))

(deftest it-can-schedule-a-job
  (testing "added to scheduled before being moved to jobs queue"
    (let [q (sq/sqlite-queue ":memory:")
          app (-> (ck/conn q)
                  (ck/register example-worker1 :queue :one))
          _job (ck/perform-in app (ck/worker example-worker1 :queue :one) (ct/seconds 1))]

      ;; Job should not be in the main queue yet
      (let [[queue polled-job] (cq/poll q ["one"])]
        (is (nil? queue))
        (is (nil? polled-job)))

      (Thread/sleep 1100)
      (cq/tick q)

      ;; Now it should be available
      (let [[queue polled-job] (cq/poll q ["one"])]
        (is (= queue "one"))
        (is (= (:class polled-job) "Cljdekiq::SqliteTest::ExampleWorker1"))))))

(deftest it-can-retry-a-job
  (testing "retried jobs are moved to the main queue after their retry_at time"
    (let [q (sq/sqlite-queue ":memory:")
          _app (-> (ck/conn q)
                   (ck/register example-worker1 :queue :one))
          worker (ck/worker example-worker1 :queue :one)
          job (ck/new-job worker)
          retry-at (+ (ct/now) (ct/seconds 1))]

      (cq/retry q job retry-at)

      ;; Should not be available yet
      (let [[queue _polled-job] (cq/poll q ["one"])]
        (is (nil? queue)))

      (Thread/sleep 1100)
      (cq/tick q)

      ;; Now it should be available
      (let [[queue polled-job] (cq/poll q ["one"])]
        (is (= queue "one"))
        (is (= (:class polled-job) "Cljdekiq::SqliteTest::ExampleWorker1"))))))

(deftest it-only-polls-specified-queues
  (testing "gets a job when a job was enqueued for that queue"
    (let [q (sq/sqlite-queue ":memory:")
          app (-> (ck/conn q)
                  (ck/register example-worker1 :queue :one))]

      (ck/perform-async app (ck/worker example-worker1 :queue :one))

      (let [[queue job] (cq/poll q ["one"])]
        (is (= queue "one"))
        (is (= (:class job) "Cljdekiq::SqliteTest::ExampleWorker1")))))

  (testing "ignores enqueued jobs if queue not specified on poll"
    (let [q (sq/sqlite-queue ":memory:")
          app (-> (ck/conn q))]

      (ck/perform-async app (ck/worker example-worker1 :queue :some-other-queue))

      (let [[queue job] (cq/poll q ["one"])]
        (is (nil? queue))
        (is (nil? job))))))
