(ns cljdekiq.core-test
  (:require [clojure.test :refer :all]
            [cljdekiq.core :refer :all]
            [cljdekiq.core :as ck]))

(defn whatever-dude [])

(deftest it-can-convert-class-to-ruby-style-constant
  (testing "a defined function"
    (is (= (ck/class->ruby-constant
            (class whatever-dude))
           "Cljdekiq::CoreTest::WhateverDude"))

    (is (= (ck/class->ruby-constant
            (class clojure.string/split))
           "Clojure::String::Split"))

    (is (= (ck/class->ruby-constant
            (class test))
           "Clojure::Core::Test")))

  (testing "a java class"
    (is (= (ck/class->ruby-constant IllegalArgumentException)
           "Java::Lang::IllegalArgumentException")))

  (testing "that an anonymous function throws an exception"
    (is (thrown-with-msg?
         IllegalArgumentException
         #"Cannot convert an anonymous function to a ruby constant name: cljdekiq\.core_test\$fn__"
         (ck/class->ruby-constant
          (class (fn [] :not-used)))))))

(deftest it-can-build-a-worker
  (testing "function with default arguments"
    (is (= (ck/worker whatever-dude)
           {:class-name "Cljdekiq::CoreTest::WhateverDude"
            :retries 25
            :queue :default
            :job-fn whatever-dude})))

  (testing "function with custom class name"
    (is (= (ck/worker whatever-dude :as "IKnowBetterWorker")
           {:class-name "IKnowBetterWorker"
            :retries 25
            :queue :default
            :job-fn whatever-dude})))

  (testing "function with custom queue"
    (is (= (ck/worker whatever-dude :queue :whatever)
           {:class-name "Cljdekiq::CoreTest::WhateverDude"
            :retries 25
            :queue :whatever
            :job-fn whatever-dude})))

  (testing "function with custom retry"
    (is (= (ck/worker whatever-dude :retry false)
           {:class-name "Cljdekiq::CoreTest::WhateverDude"
            :retries false
            :queue :default
            :job-fn whatever-dude})))

  (testing "anonymous function with custom class name"
    (let [anon-fn (fn [])]
      (is (= (ck/worker anon-fn :as "IKnowBetterWorker")
             {:class-name "IKnowBetterWorker"
              :retries 25
              :queue :default
              :job-fn anon-fn}))))

  (testing "string only class name without a job fn"
    (is (= (ck/worker "Some::OtherWorker")
           {:class-name "Some::OtherWorker"
            :retries 25
            :queue :default
            :job-fn nil})))

  (testing "anonymous function without a custom class name"
    (is (thrown-with-msg?
         IllegalArgumentException
         #"Cannot convert an anonymous function to a ruby constant name: cljdekiq\.core_test\$fn__"
         (ck/worker (fn []))))))

(deftest it-can-merge-new-worker-options
  (def whatever-worker
    (ck/worker whatever-dude))

  (is (= (:queue whatever-worker) :default))
  (is (= (:class-name whatever-worker) "Cljdekiq::CoreTest::WhateverDude"))

  (testing "can update a queue name"
    (let [new-worker (ck/merge-worker whatever-worker :queue :frontend)]
      (is (= (:queue new-worker) :frontend))))

  (testing "can update a class name"
    (let [new-worker (ck/merge-worker whatever-worker :as "Some::NewThing")]
      (is (= (:class-name new-worker) "Some::NewThing")))))

(deftest it-can-regiser-to-a-ck-conn
  ;; Mock out redis conn
  (def a-conn (ck/conn nil))
  (def whatever-worker (ck/worker whatever-dude))

  (testing "can register a worker map"
    (let [new-conn (ck/register a-conn whatever-worker)]
      (is (= (:workers new-conn) [whatever-worker]))))

  (testing "can register a worker with new overrides"
    (let [new-conn (ck/register a-conn whatever-worker :as "NewName" :queue :test)
          new-worker (assoc whatever-worker
                            :class-name "NewName"
                            :queue :test)]
      (is (= (:workers new-conn) [new-worker]))))

  (testing "can register a fn"
    (let [new-conn (ck/register a-conn whatever-dude)]
      (is (= (:workers new-conn) [whatever-worker]))))

  (testing "missing job-fn from a worker map"
    (is (thrown-with-msg?
         IllegalArgumentException
         #"Cannot register worker because it's missing a job-fn\. Is this a ref-only worker\?"
         (ck/register a-conn (ck/worker "NoJobFnWorker"))))))
