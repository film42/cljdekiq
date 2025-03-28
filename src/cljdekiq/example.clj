(ns cljdekiq.example
  (:require [cljdekiq.core :as ck]
            [cljdekiq.queue :as cq]
            [cljdekiq.redis :as cr]))

(defn scan-for-phi []
  (Thread/sleep 1000)
  (println "Done scanning for PHI"))

(defn welcome-email [user-id]
  (println "Welcome user:" user-id))

(defn error-worker []
  (println "I'm the error worker. I'm about to raise an error so this retries.")
  (+ "I'm bad" 1337))

(def ruby-worker
  (ck/worker "SomeRailsApp::RandomWorker"))

(def anon-worker
  (ck/worker
   (fn [a b c] (println "You called me with: " [a b c]))
   :as "SomeNiceNameWorker"
   :queue :other
   :retry false))

(defn run-with-queue [queue]
  (println "Building app")
  (def app
    (-> (ck/conn queue)
        (ck/register scan-for-phi :queue :tasks)
        (ck/register welcome-email :retry false)
        (ck/register error-worker)
        (ck/register anon-worker)))

  (println "Starting server")
  (def stop (ck/run app))

  (println "Adding work...")
  (ck/perform-async app scan-for-phi)
  (ck/perform-async app welcome-email "user-1")
  (ck/perform-async app error-worker)
  (println "Scheduling work...")
  (ck/perform-in app anon-worker (ck/seconds 6) 1 2 3)

  (println "Running server for 5 seconds.")
  (Thread/sleep 5000)

  (println "Shutting down...")
  (stop)

  ;; Optional: Clean up queue (in this case, close the redis pool)
  (cq/close (ck/queue app))

  (println "Done"))

(defn run []
  (println "Running with the redis backed queue (sidekiq mode)")
  (run-with-queue (cr/redis-queue))

  (println "Running with an in-mempory test queue (pluggable backends)")
  (run-with-queue (cq/test-queue))

  ;; Extra for a demo: shutdown agents so lein run exits cleanly.
  (shutdown-agents))
