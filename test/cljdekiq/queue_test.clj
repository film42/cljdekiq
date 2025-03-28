(ns cljdekiq.queue-test
  (:require [clojure.test :refer :all]
            [cljdekiq.queue :refer :all]
            [cljdekiq.core :as ck]))

(defn example-worker1 [])
(defn example-worker2 [])

(deftest it-can-enqueue-and-process-a-job
  (testing "end to end job processing"
    (def q (test-queue))
    (def app (->
              (ck/conn q)
              (ck/register example-worker1 :queue :one)
              (ck/register example-worker2 :queue :two)))

    (ck/perform-async app (ck/worker example-worker1 :queue :one))

    (let [[queue job] (poll q [:one :two])
          static-keys (select-keys job [:class :queue :args :retry :retry_count])]

      (is (= queue "one"))

      (is (= {:class "Cljdekiq::QueueTest::ExampleWorker1"
              :queue :one
              :args ()
              :retry 25
              :retry_count 0} static-keys))

      (is (string? (:jid job)))
      (is (int? (:created_at job)))
      (is (int? (:enqueued_at job))))))

(deftest it-can-scheduled-a-job
  (testing "added to schedueld before being moved to jobs queue"
    (def q (test-queue))
    (def app (->
              (ck/conn q)
              (ck/register example-worker1 :queue :one)
              (ck/register example-worker2 :queue :two)))

    (let [at (ck/seconds 1)
          to-be-enqueued-at (+ (ck/now) at)
          job (ck/perform-in app (ck/worker example-worker1 :queue :one) at)
          scheduled @(:scheduled q)]

      (is (= (list {:at to-be-enqueued-at :job job}) scheduled))

      (Thread/sleep 1100)
      (tick q)

      (is (= () @(:scheduled q)) "After a tick the scheduled items were moved to the main queue")
      (is (= (list job) @(:jobs q)) "The jobs queue now has the job")

      ;; Ensure polling now fetches the job.
      (is (= (poll q ["one"]) ["one" job])))))

(deftest it-can-retry-a-job
  (testing "added to schedueld before being moved to jobs queue"
    (def q (test-queue))
    (def app (->
              (ck/conn q)
              (ck/register example-worker1 :queue :one)
              (ck/register example-worker2 :queue :two)))

    (let [at (ck/seconds 1)
          retry-at (+ (ck/now) at)
          worker (ck/worker example-worker1 :queue :one)
          job (ck/new-job worker)]

      (retry q job retry-at)

      (is (= (list {:at retry-at :job job}) @(:retry q)))

      (Thread/sleep 1100)
      (tick q)

      (is (= () @(:retry q)) "After a tick the retried items were moved to the main queue")
      (is (= (list job) @(:jobs q)) "The jobs queue now has the job")

      ;; Ensure polling now fetches the job.
      (is (= (poll q ["one"]) ["one" job])))))

(deftest it-only-polls-specified-queues
  (testing "gets a job when a job was enqueued for that queue"
    (def q (test-queue))
    (def app (->
              (ck/conn q)
              (ck/register example-worker1 :queue :one)))

    (ck/perform-async app (ck/worker example-worker1 :queue :one))

    (let [[queue job] (poll q ["one"])]
      (is (= queue "one"))
      (is (= (:class job) "Cljdekiq::QueueTest::ExampleWorker1"))))

  (testing "ignores enqueued jobs if queue not specified on poll"
    (def q (test-queue))
    (def app (-> (ck/conn q)))

    (ck/perform-async app (ck/worker example-worker1 :queue :some-other-queue-we-dont-watch))

    (let [[queue job] (poll q ["one"])]
      (is (= queue nil))
      (is (= job nil)))))
