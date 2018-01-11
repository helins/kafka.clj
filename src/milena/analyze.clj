(ns milena.analyze

  ""

  ;; TODO docstring

  {:author "Adam Helinski"}

  (:require [milena.interop.java :as M.interop.java]
            [milena.interop.clj  :as M.interop.clj])
  (:import java.util.concurrent.TimeUnit
           (org.apache.kafka.streams KafkaStreams
                                     KafkaStreams$State
                                     StreamsBuilder
                                     StreamsConfig
                                     Topology)
           (org.apache.kafka.streams.state QueryableStoreTypes
                                           ReadOnlyKeyValueStore
                                           ReadOnlyWindowStore
                                           ReadOnlySessionStore)))




;;;;;;;;;;


(defn app 

  ""

  ;; TODO docstring

  (^KafkaStreams

   [application-name topology]

   (app application-name
        topology
        nil))


  (^KafkaStreams

   [application-name ^Topology topology opts]

   (KafkaStreams. topology
                  (M.interop.java/streams-config application-name
                                                 opts))))




(defn start

  ""

  ^KafkaStreams

  [^KafkaStreams app]

  (.start app))




(defn close

  ""

  (^KafkaStreams

   [^KafkaStreams app]

   (.close app)
   app)


  (^KafkaStreams

   [^KafkaStreams app timeout-ms]

   (.close app
           timeout-ms
           TimeUnit/MILLISECONDS)
   app))




(defn clean-up

  ""

  ^KafkaStreams

  [^KafkaStreams app]

  (.cleanUp app))




(defn state

  ""

  [^KafkaStreams app]

  (M.interop.clj/kafka-streams$state (.state app)))




(defn state-listen

  ""

  ^KafkaStreams

  [^KafkaStreams app f]

  (.setStateListener app
                     (M.interop.java/kafka-streams$state-listener f))
  app)




(defn handle-exception

  ""

  ^KafkaStreams

  [^KafkaStreams app f]

  (.setUncaughtExceptionHandler app
                                (M.interop.java/thread$uncaught-exception-handler f))
  app)




(defn store-kv

  ""

  ^ReadOnlyKeyValueStore

  [^KafkaStreams app store-name]

  (.store app
          store-name
          (QueryableStoreTypes/keyValueStore)))




(defn store-windows

  ""

  ^ReadOnlyWindowStore

  [^KafkaStreams app store-name]

  (.store app
          store-name
          (QueryableStoreTypes/windowStore)))




(defn store-sessions

  ""

  ^ReadOnlySessionStore

  [^KafkaStreams app store-name]

  (.store app
          store-name
          (QueryableStoreTypes/sessionStore)))




(defn metrics

  ""

  [^KafkaStreams streams]

  (M.interop.clj/metrics (.metrics streams)))





;; TODO docstrings
;; TODO possible config description
