(ns cljdekiq.time)

;; Time helpers
(defn seconds [n] (long n))
(defn minutes [n] (long (* n 60)))
(defn hours [n] (long (* (minutes n) 60)))
(defn days [n] (long (* (hours n) 24)))

(defn now []
  (long (/ (System/currentTimeMillis) 1000)))

