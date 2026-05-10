(defproject com.github.comfysoft/cljdekiq "0.3.0-SNAPSHOT"
  :description "A sidekiq client/server implementation in clojure"
  :url "https://github.com/film42/cljdekiq"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [com.taoensso/carmine "3.4.1"]
                 [datascript "1.7.4"]
                 [org.clojure/data.json "2.5.1"]
                 ;; TODO make this optional
                 [hikari-cp "3.2.0"]
                 [com.github.seancorfield/next.jdbc "1.3.1002"]
                 [org.xerial/sqlite-jdbc "3.49.1.0"]]
  :repl-options {:init-ns cljdekiq.core})
