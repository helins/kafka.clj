(ns milena.produce

  "Everything related to Kafka producers."

  {:author "Adam Helinski"}

  (:refer-clojure :exclude [flush])
  (:require [milena.interop      :as $.interop]
            [milena.interop.clj  :as $.interop.clj]
            [milena.interop.java :as $.interop.java]
            [milena.serialize    :as $.serialize])
  (:import java.util.concurrent.TimeUnit
           org.apache.kafka.clients.producer.KafkaProducer))




;;;;;;;;;; API


(defn producer?

  "Is `x` a producer ?"

  [x]

  (instance? KafkaProducer
             x))




(defn partitions
  
  "Gets a list of partitions for a given topic.

   <!> Blocks forever is the topic does not exist and dynamic creation has been disabled.


   @ producer
     Kafka producer.

   @ topic
     Topic name.

   => List of partitions.
      Cf. `interop.clj/partition-info`


   Throws

     org.apache.kafka.common.errors

       WakeupException
         When `unblock` is called before or while this fn is called.

       InterruptException
         When the calling thread is interrupted."

  [^KafkaProducer producer topic]

  (map $.interop.clj/partition-info
       (.partitionsFor producer
                       topic)))




(defn commit

  "Asynchronously commits a message to Kafka via a producer and calls the optional callback
   on acknowledgment.

   @ producer
     Kafka producer.

   @ record
     Cf. `milena.interop.java/producer-record`

   @ callback
     Cf. `milena.interop.java/callback`

   => Future resolving to the metadata or throwing if an error occured.
      Cf. `milena.interop.clj/record-metadata`


   Possible exceptions are :

     java.lang

       IllegalStateException
         When a 'transactional.id' has been configured and no transaction has been started.

     org.apache.kafka.common.errors
   
       InterruptException
         When the thread is interrupted while blocked.

       SerializationException
         When the key or value are not valid objects given the configured serializers.

       TimeoutException
         When the time take for fetching metadata or allocating memory for the record has surpassed 'max.block.ms'.

       KafkaException
         Any other unrecoverable error.


  Ex. (commit producer
              {:topic \"my-topic\"
               :key   \"some-key\"
               :value 42}
              (fn callback [exception meta]
                (when-not exception
                  (println :committed meta))))


  Cf. `milena.produce/deref`"

  ([^KafkaProducer producer record]

   ($.interop/future-proxy (.send producer
                                  ($.interop.java/producer-record record))
                           $.interop.clj/record-metadata))


  ([^KafkaProducer producer record callback]

   ($.interop/future-proxy (.send producer
                                  ($.interop.java/producer-record record)
                                  ($.interop.java/callback callback))
                           $.interop.clj/record-metadata)))




(defn flush

  "Flushes the producer.
  
   Sends all buffered messages immediately, even is 'linger.ms' is greater than 0, and blocks until
   completion. Other thread can continue sending messages but no garantee is made they will be part
   of the flush.

   
   @ producer
     Kafka producer.

   => nil


   Throws
  
     org.apache.kafka.common.errors

       InterruptException
         When the thread is interrupted while flushing."

  [^KafkaProducer producer]

  (.flush producer))




(defn metrics

  "Gets metrics about this producer.

   @ producer
     Kafka producer.
  
   => Cf. `milena.interop.clj/metrics`"

  [^KafkaProducer producer]

  ($.interop.clj/metrics (.metrics producer)))




(defn close

  "Tries to close the producer cleanly.

   Blocks until all sends are done or the optional timeout is elapsed.


   @ producer
     Kafka producer.

   @ timeout-ms
     Timeout in milliseconds.

   => nil
  

   Throws

     org.apache.kafka.common.errors

       InterruptException
       Whenf the thread is interrupted while blocked."

  ([^KafkaProducer producer]

   (.close producer))


  ([^KafkaProducer producer timeout-ms]

   (.close producer
           (max timeout-ms
                0)
           TimeUnit/MILLISECONDS)))




(defn make

  "Builds a Kafka producer.

   Producers are thread-safe and it is efficient to share one between multiple threads.


   @ opts (nilable)
     {:nodes (nilable)
       List of [host port].

      :config (nilable)
       Kafka configuration map.
       Cf. https://kafka.apache.org/documentation/#producerconfigs
     
      :serializer (nilable)
       Kafka serializer or fn eligable for becoming one.
       Cf. `milena.serialize`
           `milena.serialize/make`

      :serializer-key (nilable)
       Defaulting to `?serializer`.

      :serializer-value (nilable)
       Defaulting to `?deserializer`.}

   => org.apache.kafka.clients.producer.KafkaProducer


   Ex. (make {:nodes            [[\"some_host\" 9092]]
              :config           {:client.id \"my_id\"}
              :serializer-key   milena.serialize/string
              :serializer-value (fn [_ data]
                                  (nippy/freeze data))})"

  ^KafkaProducer

  ([]

   (make nil))


  ([{:as   opts
     :keys [nodes
            config
            serializer
            serializer-key
            serializer-value]
     :or   {nodes            [["localhost" 9092]]
            serializer       $.serialize/byte-array
            serializer-key   serializer
            serializer-value serializer}}]

   (KafkaProducer. ($.interop/config config
                                     nodes)
                   ($.serialize/make serializer-key)
                   ($.serialize/make serializer-value))))
