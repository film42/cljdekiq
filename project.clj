(defproject com.github.comfysoft/cljdekiq "0.3.0-SNAPSHOT"
  :description "A sidekiq client/server implementation in clojure"
  :url "https://github.com/film42/cljdekiq"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [com.taoensso/carmine "3.4.1"]
                 [org.clojure/data.json "2.5.1"]]
  :repl-options {:init-ns cljdekiq.core})
