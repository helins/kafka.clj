(ns dvlopt.kstreams

  "Kafka Streams applications.


   An app is a topology of nodes sourcing, processing, and sending records. Such a topology can be built in a rather imperative fashion
   by using the `dvlopt.kstreams.topology` namespace, the so-called \"low-level API\". The `dvlopt.kstreams.builder` namespace is the
   entry point of the so-called \"high-level API\". It provides a \"builder\" which will build a topology for the user while he/she enjoys
   the rather functional API. Both APIs are actually complementary. Once a builder produces a topology, it can be augmented using the
   low-level API if needed.
  
   The high-level API is usually prefered."

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




;;;;;;;;;; Creating and handling a Kafka Streams application


(defn app 

  "Given a topology, creates a Kafka Streams application.
  
   In order to release the ressource associated with an application, `disband` must be always called when done, even if the
   application has not been started.

   Works with clojure's `with-open` which behaves like calling `disband` without options.


   A map of options may be given :

     :dvlopt.kafka/nodes
      List of [Host Port].

     :dvlopt.kafka.admin/configuration.topics
      Map of topic properties applying to any topic created by the application.
      Cf. https://kafka.apache.org/documentation/#topicconfigs

     :dvlopt.kafka.in/configuration
      Map of consumer properties applying to any consumer needed by the app.
      Cf. https://kafka.apache.org/documentation/#newconsumerconfigs

     :dvlopt.kafka.out/configuration
      Map of producer properties applying to any producer needed by the app.
      Cf. https://kafka.apache.org/documentation/#producerconfigs

     ::configuration
      Map of Kafka Streams properties.
      Cf. https://kafka.apache.org/documentation/#streamsconfigs

     ::on-exception
      Callback taking an Exception and a Thread, called when an error occurs during runtime.

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
              K.-interop.java/thread$uncaught-exception-handler
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

  "Releases resources acquired for the Kafka Streams application (after stopping it if it was running).

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




(defn session-store

  "Retrieves a read-only session store used by the application."

  ^ReadOnlySessionStore

  [^KafkaStreams app store-name]

  (.store app
          store-name
          (QueryableStoreTypes/sessionStore)))




(defn window-store

  "Retrieves a read-only window store used by the application."

  ^ReadOnlyWindowStore

  [^KafkaStreams app store-name]

  (.store app
          store-name
          (QueryableStoreTypes/windowStore)))




(defn metrics

  "Requests metrics about this application.
  
   Returned valued presented in the same form as `dvlopt.kafka.out/metrics`."

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
