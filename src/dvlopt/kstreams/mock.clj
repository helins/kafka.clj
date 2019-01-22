(ns dvlopt.kstreams.mock

  "Building a fake Kafka Streams app."

  {:author "Adam Helinski"}

  (:require [dvlopt.kafka               :as K]
            [dvlopt.kafka.-interop.clj  :as K.-interop.clj]
            [dvlopt.kafka.-interop.java :as K.-interop.java]
            [dvlopt.void                :as void])
  (:import (org.apache.kafka.streams Topology
                                     TopologyTestDriver)))




;;;;;;;;;; Creating and handling a mock Kafka Streams application


(defn mock-app

  ""

  (^TopologyTestDriver

   [application-id topology]

   (mock-app application-id
             topology
             nil))


  (^TopologyTestDriver

   [application-id ^Topology topology options]

   (TopologyTestDriver. topology
                        (K.-interop.java/streams-config application-id
                                                        (::configuration options))
                        (or (some-> (::clock options)
                                    K.-interop.java/to-milliseconds)
                            (get K/defaults
                                 ::clock)))))




(defn disband

  ""

  [^TopologyTestDriver mock-app]

  (.close mock-app))




(defn metrics

  ""

  [^TopologyTestDriver mock-app]

  (K.-interop.clj/metrics (.metrics mock-app)))




(defn pipe-record

  ""

  ^TopologyTestDriver

  [^TopologyTestDriver mock-app record]
  
  (.pipeInput mock-app
              (K.-interop.java/consumer-record record))
  mock-app)




(defn read-record

  ""

  ([mock-app topic]

   (read-record mock-app
                     topic
                     nil))


  ([^TopologyTestDriver mock-app topic options]

   (some-> (.readOutput mock-app
                        topic
                        (K.-interop.java/extended-deserializer (void/obtain ::K/deserializer.key
                                                                            options
                                                                            K/defaults))
                        (K.-interop.java/extended-deserializer (void/obtain ::K/deserializer.value
                                                                            options
                                                                            K/defaults)))
           K.-interop.clj/producer-record)))
