(defproject dvlopt/milena
            "0.0.0-alpha0"
  :description  "A straightforward clojure client for Kafka"
  :url          "https://github.com/dvlopt/milena"
  :license      {:name "Eclipse Public License"
                 :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins      [[lein-codox "0.10.3"]]
  :codox        {:output-path "doc/auto"
                 :namespaces  [#"^milena"]}
  :main         milena.core
  :dependencies [[org.clojure/clojure "1.9.0-alpha10"]
                 [org.apache.kafka/kafka-clients "0.10.2.0"]
                 ]
  :profiles     {:dev     {:source-paths ["dev"]
                           :main         user
                           :plugins      [[venantius/ultra "0.4.1"]                ;; colorful repl
                                          ]
                           :dependencies [[org.clojure/tools.namespace "0.2.11"]
                                          [criterium "0.4.4"]
                                          ;[org.clojure/test.check "0.9.0"]]
                                          ]}
                 :uberjar {:aot :all}})
