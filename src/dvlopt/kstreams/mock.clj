(ns dvlopt.kstreams.mock

  "Building a fake Kafka Streams app."

  {:author "Adam Helinski"}

  (:require [dvlopt.kafka               :as K]
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
