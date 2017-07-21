(defproject dvlopt/milena
            "0.0.0-alpha7"
  :description  "A straightforward clojure client for Kafka"
  :url          "https://github.com/dvlopt/milena"
  :license      {:name "Eclipse Public License"
                 :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins      [[lein-codox "0.10.3"]]
  :codox        {:output-path "doc/auto"
                 :namespaces  [#"^milena"]}
  :dependencies [[org.apache.kafka/kafka-clients "0.10.2.0"]]
  :profiles     {:dev     {:source-paths ["dev"]
                           :main         user
                           :plugins      [[mvxcvi/whidbey "1.3.1"]]
                           :whidbey      {:color-scheme {:keyword         [:green]
                                                         :boolean         [:bold :yellow]
                                                         :character       [:white]
                                                         :string          [:white]
                                                         :symbol          [:bold :magenta]
                                                         :nil             [:bold :cyan]}}
                           :dependencies [[org.clojure/clojure         "1.9.0-alpha17"]
                                          [org.clojure/tools.namespace "0.2.11"]
                                          [criterium                   "0.4.4"]
                                          [org.clojure/test.check      "0.9.0"]]
                           :global-vars  {*warn-on-reflection* true}}})
