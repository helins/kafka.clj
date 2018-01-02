(defproject dvlopt/milena
            "0.0.0-alpha11"

  :description  "Clojure client for Kafka"
  :url          "https://github.com/dvlopt/milena"
  :license      {:name "Eclipse Public License"
                 :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.apache.kafka/kafka-clients "0.11.0.1"]]
  :profiles     {:dev {:source-paths ["dev"]
                       :main         user
                       :dependencies [[org.clojure/clojure            "1.9.0"]
                                      [org.clojure/test.check         "0.10.0-alpha2"]
                                      [criterium                      "0.4.4"]
                                      [org.apache.kafka/kafka-streams "0.11.0.1"]]
                       :plugins      [[venantius/ultra "0.5.1"]
                                      [lein-midje      "3.0.0"]
                                      [lein-codox      "0.10.3"]]
                       :codox        {:output-path  "doc/auto"
                                      :source-paths ["src"]}
                       :global-vars  {*warn-on-reflection* true}}})
