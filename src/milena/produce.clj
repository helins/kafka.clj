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




(defn trx-init

  "When 'transactional.id' is set, needs to be called exactly once before doing any transaction.
  
   Based on this id, ensures any previous transaction committed by a previous instance is completed or any pending
   transaction is aborted. Further more, it prepares the producer for future ones.


   @ producer
     Kafka producer.

   => `producer`


   Throws

     java.lang
    
       IllegalStateException
         When no 'transactional.id' has been configured.

     org.apache.kafka.common.errors
  
       UnsupportedVersionException
         When broker does not support transactions.

       AuthorizationException
         When the 'transactional.id' is not authorized.

       KafkaException
         Any other unrecoverable error."

  ^KafkaProducer

  [^KafkaProducer producer]

  (.initTransactions producer)
  producer)




(defn trx-begin

  "Must be called exactly once at the start of each new transaction.

   <!> `trx-init` must be called before any transaction.


   @ producer
     Kafka producer.

   => `producer`


   Throws

     java.lang
    
       IllegalStateException
         When no 'transactional.id' has been configured.

     org.apache.kafka.common.errors

       ProducerFencedException
         When another producer with the same transaction id is already active.
  
       UnsupportedVersionException
         When the broker does not support transactions.

       AuthorizationException
         When 'transactional.id' is not authorized.

       KafkaException
         Any other unrecoverable error."

  ^KafkaProducer

  [^KafkaProducer producer]

  (.beginTransaction producer)
  producer)




(defn trx-commit

  "Commits the ongoing transaction.

   A transaction succeeds only if every step succeeds.

   If any record commit hit an irrecoverable error, this function will rethrow that exception and the transaction
   will not be committed. 


   @ producer
     Kafka producer.

   => `producer`


   Throws

     java.lang
    
       IllegalStateException
         When no 'transactional.id' has been configured.

     org.apache.kafka.common.errors

       ProducerFencedException
         When another producer with the same transaction id is already active.
  
       UnsupportedVersionException
         When the broker does not support transactions.

       AuthorizationException
         When the 'transactional.id' is not authorized.

       KafkaException
         Any other unrecoverable error."

  ^KafkaProducer

  [^KafkaProducer producer]

  (.commitTransaction producer)
  producer)




(defn trx-offsets

  "Commits offsets for a consumer group id as part of the ongoing transaction.

   Very useful for a consume-transform-produce use case.
   
   The consumer polls records without committing its offsets ('enable.auto.commit' must be set to false). After some computation,
   while in a transaction, the producer publishes results as well as the offsets (ie. for each topic partition, the offset of the
   last processed record + 1). This garantees exacly once semantics.
  

   @ producer
     Kafka producer.

   => `producer`


   Throws

     java.lang
    
       IllegalStateException
         When no 'transactional.id' has been configured.

     org.apache.kafka.common.errors

       ProducerFencedException
         When another producer with the same transaction id is already active.
  
       UnsupportedVersionException
         When the broker does not support transactions.

       AuthorizationException
         When the 'transactional.id' is not authorized.

       KafkaException
         Any other unrecoverable error."

  ^KafkaProducer

  [^KafkaProducer producer group-id offsets]

  (.sendOffsetsToTransaction producer
                             ($.interop.java/topic-partition-to-offset offsets)
                             group-id)
  producer)




(defn trx-abort

  "Aborts the ongoing transaction.


   @ producer
     Kafka producer.

   => `producer`


   Throws

     java.lang
    
       IllegalStateException
         When no 'transactional.id' has been configured.

     org.apache.kafka.common.errors

       ProducerFencedException
         When another producer with the same transaction id is already active.
  
       UnsupportedVersionException
         When the broker does not support transactions.

       AuthorizationException
         When the 'transactional.id' is not authorized.

       KafkaException
         Any other unrecoverable error."

  ^KafkaProducer

  [^KafkaProducer producer]

  (.abortTransaction producer)
  producer)




(defn commit

  "In normal mode, asynchronously commits a message to Kafka via a producer and calls the optional callback
   on acknowledgment or error.

   In transactional mode, behaves synchronously and throws right away in case of failure. When a recoverable error occurs,
   the user should call `trx-abort`. Otherwise, the producer should be closed as it enters a defunct state.

   <!> Callbacks will be executed on the IO thread of the producer, so they should be fast.


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

       AuthenticationException
         When authentication fails.
   
       InterruptException
         When the thread is interrupted while blocked.

       SerializationException
         When the key or value are not valid objects given the configured serializers.

       TimeoutException
         When the time take for fetching metadata or allocating memory for the record has surpassed 'max.block.ms'.

       KafkaException
         Any other unrecoverable error.


    Possibles exceptions in transactions :

      org.apache.kafka.common.errors

       ProducerFencedException (unrecoverable)
         When another producer with the same transaction id is already active.

       OutOfOrderSequenceException (unrecorable)
         When the broker receives an unexpected sequence number from the producer which means that data may have been lost.
         
       UnsupportedVersionException (unrecovarable)
         When the transaction API is not supported by the broker.

       UnsupportedForMessageFormatException (recoverable)
         When the message format of the desination topic is not upgraded to 0.11.0.0.
         


  Ex. (commit producer
              {:topic \"my-topic\"
               :key   \"some-key\"
               :value 42}
              (fn callback [exception meta]
                (when-not exception
                  (println :committed meta))))"


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
