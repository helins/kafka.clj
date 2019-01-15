(defproject dvlopt/kafka
            "1.2.0-beta0"

  :description  "Clojure client for Kafka"
  :url          "https://github.com/dvlopt/kafka.clj"
  :license      {:name "Eclipse Public License"
                 :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[dvlopt/void                    "0.0.1"]
                 [org.apache.kafka/kafka-clients "2.1.0"]
                 [org.apache.kafka/kafka-streams "2.1.0"]]
  :profiles     {:dev {:source-paths ["dev"]
                       :main         user
                       :dependencies [[com.taoensso/nippy     "2.13.0"]
                                      [criterium              "0.4.4"]
                                      [org.clojure/clojure    "1.10.0"]
                                      [org.clojure/test.check "0.10.0-alpha2"]]
                       :plugins      [[lein-codox      "0.10.5"]
                                      [venantius/ultra "0.5.1"]]
                       :codox        {:namespaces   [dvlopt.kafka
                                                     dvlopt.kafka.admin
                                                     dvlopt.kafka.in
                                                     dvlopt.kafka.in.mock
                                                     dvlopt.kafka.out
                                                     dvlopt.kafka.out.mock
                                                     dvlopt.kstreams
                                                     dvlopt.kstreams.builder
                                                     dvlopt.kstreams.ctx
                                                     dvlopt.kstreams.store
                                                     dvlopt.kstreams.stream
                                                     dvlopt.kstreams.table
                                                     dvlopt.kstreams.topology]
                                      :output-path  "doc/auto"
                                      :source-paths ["src"]}
                       :global-vars  {*warn-on-reflection* true}}})
