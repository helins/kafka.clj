(ns dvlopt.kstreams.mock

  "Building a mock Kafka Streams application which do not need a Kafka cluster."

  {:author "Adam Helinski"}

  (:require [dvlopt.kafka               :as K]
            [dvlopt.kafka.-interop.clj  :as K.-interop.clj]
            [dvlopt.kafka.-interop.java :as K.-interop.java]
            [dvlopt.void                :as void])
  (:import (org.apache.kafka.streams Topology
                                     TopologyTestDriver)
           (org.apache.kafka.streams.state KeyValueStore
                                           SessionStore
                                           WindowStore)))




;;;;;;;;;; Creating and handling a mock Kafka Streams application


(defn mock-app

  "Given a topology, creates a mock Kafka Streams application which can simulate a cluster. It is
   very useful for testing and fiddling at the REPL.

   It is mainly operated by using `pipe-record` and `read-record`.

  
   A map of options may be given :

     ::wall-clock (defaults to 0)
      Timestamp for initiating the mock application.
      Cf. `dvlopt.kstreams.ctx/schedule` for a description of the difference between :wall-clock-time
          and :stream-time  

     :dvlopt.kstreams/configuration
      Cf. `dvlopt.kstreams/app`, many properties are ignored but it is useful to supply something
          realistic."

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
                        (or (some-> (::wall-clock options)
                                    K.-interop.java/to-milliseconds)
                            (get K/defaults
                                 ::wall-clock)))))




(defn disband

  "Closes the mock application and releases all associated resources."

  [^TopologyTestDriver mock-app]

  (.close mock-app))




(defn metrics

  "Cf. `dvlopt.kstreams/metrics`"

  [^TopologyTestDriver mock-app]

  (K.-interop.clj/metrics (.metrics mock-app)))




(defn pipe-record

  "Processes the given records by the mock application. 

   Cf. `dvlopt.kafka.in/poll` for the structure of a record ready to be consumed"

  ^TopologyTestDriver

  [^TopologyTestDriver mock-app record]
  
  (.pipeInput mock-app
              (K.-interop.java/consumer-record record))
  mock-app)




(defn read-record

  "Records processed after using `pipe-record` may produce an output depending on the topology
   of the mock application.

   Cf. `dvlopt.kafka.out/send` for the structure of a record produced by the mock application"

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




(defn advance

  "Advances the wall-clock time of the mock application."

  ^TopologyTestDriver

  [^TopologyTestDriver mock-app interval]

  (.advanceWallClockTime mock-app
                         (K.-interop.java/to-milliseconds interval))
  mock-app)




(defn kv-store

  "Retrieves a writable key-value store used by the mock application."

  ^KeyValueStore

  [^TopologyTestDriver mock-app store-name]

  (.getKeyValueStore mock-app
                     store-name))




(defn session-store

  "Retrieves a writable session store used by the application."

  ^SessionStore

  [^TopologyTestDriver mock-app store-name]

  (.getSessionStore mock-app
                    store-name))




(defn window-store

  "Retrieves a read-only window store used by the application."

  ^WindowStore

  [^TopologyTestDriver mock-app store-name]

  (.getWindowStore mock-app
                   store-name))
