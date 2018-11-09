(ns dvlopt.kstreams

  "Creation and handling of Kafka Streams applications.

   The user can either write a low-level implementation by building himself a topology of processing nodes (cf. dvlopt.kstreams.topology) or
   use the high-level API presenting a more functional approach (cf. dvlopt.kstreams.high)."

  {:author "Adam Helinski"}

  (:require [dvlopt.kafka               :as K]
            [dvlopt.kafka.-interop.clj  :as K.-interop.clj]
            [dvlopt.kafka.-interop.java :as K.-interop.java])
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




;;;;;;;;;; Create and handle a Kafka Streams application


(defn app 

  "Given a low-level topology or a high-level builder, creates a Kafka Streams application.
  
   In order to release the ressource associated with an application, always call `disband`, even if the
   application has not been started.

   Works with clojure's `with-open` which behaves like calling `disband` without options.


   A map of options may be given :

     :dvlopt.kafka/nodes
      List of [Host Port].

     :dvlopt.kafka.admin/configuration.topics
      Map of topic properties applying to any topic created by the application.
      Cf. https://kafka.apache.org/documentation/#topicconfigs

     :dvlopt.kafka.consume/configuration
      Map of consumer properties applying to any consumer needed by the app.
      Cf. https://kafka.apache.org/documentation/#newconsumerconfigs

     :dvlopt.kafka.produce/configuration
      Map of producer properties applying to any producer needed by the app.
      Cf. https://kafka.apache.org/documentation/#producerconfigs

     ::configuration
      Map of Kafka Streams properties.
      Cf. https://kafka.apache.org/documentation/#streamsconfigs

     ::on-exception
      Callback taking a Thread and an Exception, called when an error occurs during runtime.

     ::on-state-change
      Callback accepting an old state and a new one, called everytime the state changes.
      Cf. `state`"

  (^KafkaStreams

   [application-id topology]

   (app application-id
        topology
        nil))


  (^KafkaStreams

   [application-id ^Topology topology options]

   (let [app' (KafkaStreams. topology
                             (K.-interop.java/streams-config application-id
                                                             options))]
     (some->> (::on-exception options)
              (.setUncaughtExceptionHandler app'))
     (some->> (::on-state-change options)
              K.-interop.java/kafka-streams$state-listener
              (.setStateListener app'))
     app')))




(defn start

  "Starts a Kafka Streams application."

  ^KafkaStreams

  [^KafkaStreams app]

  (.start app)
  app)




(defn disband

  "Releases resources acquired for a Kafka Streams application (after stopping it if it was running).

   A map of options may be given :

     :timeout
      Cf. `dvlopt.kafka` for description of time intervals"

  (^KafkaStreams

   [^KafkaStreams app]

   (.close app)
   app)


  (^KafkaStreams

   [^KafkaStreams app options]

   (if-let [[timeout-duration unit] (::K/timeout options)]
     (.close app
             timeout-duration
             (K.-interop.java/time-unit unit))
     (.close app))
   app))




(defn clean-up

  "Does a clean-up of the local state store directory by deleting all data with regard to the application ID."

  ^KafkaStreams

  [^KafkaStreams app]

  (.cleanUp app)
  app)




(defn state

  "Returns the current state of the application, one of :
  
     :created
     :error
     :not-running
     :pending-shutdown
     :rebalancing
     :running"

  [^KafkaStreams app]

  (K.-interop.clj/kafka-streams$state (.state app)))




(defn kv-store

  "Retrieves a read-only key-value store used by the application."

  ^ReadOnlyKeyValueStore

  [^KafkaStreams app store-name]

  (.store app
          store-name
          (QueryableStoreTypes/keyValueStore)))




(defn window-store

  "Retrieves a read-only window store used by the application."

  ^ReadOnlyWindowStore

  [^KafkaStreams app store-name]

  (.store app
          store-name
          (QueryableStoreTypes/windowStore)))




(defn session-store

  "Retrieves a read-only session store used by the application."

  ^ReadOnlySessionStore

  [^KafkaStreams app store-name]

  (.store app
          store-name
          (QueryableStoreTypes/sessionStore)))




(defn metrics

  "Requests metrics about this application.
  
   Returned valued presented in the same form as `dvlopt.kafka.produce/metrics`."

  [^KafkaStreams app]

  (K.-interop.clj/metrics (.metrics app)))




(defn remote-instances

  "Returns a vector of maps about instances of this application running on other hosts.
  
   Each map represents a point in time and contains :

     :dvlopt.kafka/host
      Host of the remote instance.

     :dvlopt.kafka/port
      Port associated with the remote instance.

     :dvlopt.kafka/topic-partitions
      List of [topic partition]'s the remote instance is handling.

     :dvlopt.kstreams.store/names
      List of the store names the instance is handling."

  [^KafkaStreams app]

  (into []
        (map K.-interop.clj/streams-metadata)
        (.allMetadata app)))
