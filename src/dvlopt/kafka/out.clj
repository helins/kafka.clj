(ns dvlopt.kafka.out

  "Handling of Kafka producers."

  {:author "Adam Helinski"}

  (:refer-clojure :exclude [flush
                            send])
  (:require [dvlopt.kafka               :as K]
            [dvlopt.kafka.-interop      :as K.-interop]
            [dvlopt.kafka.-interop.clj  :as K.-interop.clj]
            [dvlopt.kafka.-interop.java :as K.-interop.java]
            [dvlopt.void                :as void])
  (:import java.util.concurrent.TimeUnit
           org.apache.kafka.clients.producer.KafkaProducer))




;;;;;;;;;; Starting and stopping a producer


(defn producer

  "Builds a Kafka producer.

   Producers are thread-safe and it is efficient to share one between multiple threads.

   A map of options may be given :

     ::configuration
       Map of Kafka producer properties.
       Cf. https://kafka.apache.org/documentation/#producerconfigs

     :dvlopt.kafka/nodes
       List of [host port].

     :dvlopt.kafka/serializer.key
     :dvlopt.kafka/serializer.value
      Cf. `dvlopt.kafka` for description of serializers.


   Ex. (producer {::configuration                {\"client.id\" \"my_id\"}
                  :dvlopt.kafka/nodes            [[\"some_host\" 9092]]
                  :dvlopt.kafka/serializer.key   :string
                  :dvlopt.kafka/serializer.value (fn [data _metadata]
                                                   (some-> data
                                                           nippy/freeze))})"

  (^KafkaProducer
    
   []

   (producer nil))


  (^KafkaProducer
  
   [options]

   (KafkaProducer. (K.-interop/resource-configuration (::configuration options)
                                                      (void/obtain ::K/nodes
                                                                   options
                                                                   K/defaults))
                   (K.-interop.java/extended-serializer (void/obtain ::K/serializer.key
                                                                     options
                                                                     K/defaults))
                   (K.-interop.java/extended-serializer (void/obtain ::K/serializer.value
                                                                     options
                                                                     K/defaults)))))




(defn close

  "Closes the producer and releases all associated resources.

   A map of options may be given :

     :dvlopt.kafka/timeout
      Blocks until all requests finish or the given timeout is reached, failing unsent and unacked records immediately.
      Cf. `dvlopt.kafka` for description of time intervals"

  ([^KafkaProducer producer]

   (.close producer)
   nil)


  ([^KafkaProducer producer options]

   (if-let [[timeout-duration
             unit]            (:dvlopt.kafka/timeout options)]
     (.close producer
             timeout-duration
             (K.-interop.java/time-unit unit))
     (.close producer))
   nil))




;;;;;;;;;; Handling producers


(defn partitions
  
  "Requests a list of partitions for a given topic.

   <!> Blocks forever if the topic does not exist and dynamic creation has been disabled.
  

   Returns a list of maps containing :
  
     :dvlopt.kafka/leader-node (may be absent)
      Cf. `dvlopt.kafka` for descriptions of Kafka nodes.
  
     :dvlopt.kafka/partition
      Partition number.

     :dvlopt.kafka/replica-nodes
      Cf. `dvlopt.kafka` for descriptions of Kafka nodes.

     :dvlopt.kafka/topic
      Topic name."

  [^KafkaProducer producer topic]

  (map K.-interop.clj/partition-info
       (.partitionsFor producer
                       topic)))




(defn trx-init

  "The producer configuration \"transactional.id\" needs to be set.

   Based on this id, ensures any previous transaction committed by a previous instance is completed or any pending
   transaction is aborted. Further more, it prepares the producer for future ones.

   This function needs to be called exactly once prior to doing any new transactions.

   Of course, brokers need to support transactions."

  ^KafkaProducer

  [^KafkaProducer producer]

  (.initTransactions producer)
  producer)




(defn trx-begin

  "Must be called exactly once at the start of each new transaction.

   <!> `trx-init` must be called before any transaction."

  ^KafkaProducer

  [^KafkaProducer producer]

  (.beginTransaction producer)
  producer)




(defn trx-commit

  "Requests a Commit of the ongoing transaction.

   A transaction succeeds only if every step succeeds.

   If any record commit hit an irrecoverable error, this function will rethrow that exception and the transaction
   will not be committed."

  ^KafkaProducer

  [^KafkaProducer producer]

  (.commitTransaction producer)
  producer)




(defn trx-offsets

  "Adds offsets for a consumer group id as part of the ongoing transaction.

   Very useful for a consume-transform-produce use case.
   
   The consumer polls records without committing its offsets (\"enable.auto.commit\" must be set to false). After some computation,
   while in a transaction, the producer publishes results as well as the offsets (ie. for each topic partition, the offset of the
   last processed record + 1). This garantees exacly once semantics.
  

   Ex. ;; commits offset 65 for \"some-topic\" partition 0
  
       (trx-offsets producer
                    \"my-consumer-group\"
                    {[\"some-topic\" 0] 65})"

  ^KafkaProducer

  [^KafkaProducer producer consumer-group topic-partition->offset]

  (.sendOffsetsToTransaction producer
                             (K.-interop.java/topic-partition->offset-and-metadata topic-partition->offset)
                             consumer-group)
  producer)




(defn trx-abort

  "Aborts the ongoing transaction."

  ^KafkaProducer

  [^KafkaProducer producer]

  (.abortTransaction producer)
  producer)




(defn send

  "In normal mode, asynchronously sends a record to Kafka via a producer and calls the optional callback
   on acknowledgment or error. The callback needs to accept 2 arguments : an exception in case of failure and
   metadata in case of success. It will be executed on the IO thread of the producer, so it should be fast or
   pass data to another thread.
  
   This function returns a future when synchronous behaviour is needed. Derefing the future will throw if a failure
   occured or it will provide the same metadata passed to the optional callback.

   A record is a map containing at least the :dvlopt.kafka/topic and can also hold :

     :dvlopt.kafka/key
     :dvlopt.kafka/partition
     :dvlopt.kafka/timestamp
     :dvlopt.kafka/value

   If the partition is missing, it is automatically selected based on the hash of the key.

   Metadata is a map containing :dvlopt.kafka/topic and when available :

     :dvlopt.kafka/offset
     :dvlopt.kafka/partition
     :dvlopt.kafka/timestamp

   In transactional mode, there is no need for callbacks or checking the futures because a call to `trx-commit` will
   throw the exception from the last failed send. When such a failure occurs, the easiest way to deal with it is simply
   to restart the transactional producer.


   Ex. (send producer
             {:dvlopt.kafka/key   \"some-key\"
              :dvlopt.kafka/topic \"my-topic\"
              :dvlopt.kafka/value 42}
             (fn callback [exception meta]
               (when-not exception
                 (println :committed meta))))"


  ([producer record]

   (send producer
         record
         nil))


  ([^KafkaProducer producer record callback]

   (K.-interop/future-proxy (.send producer
                                   (K.-interop.java/producer-record record)
                                   (some-> callback
                                           K.-interop.java/callback))
                            K.-interop.clj/record-metadata)))




(defn flush

  "Flushes the producer.
  
   Sends all buffered messages immediately, even if \"'linger.ms\" is greater than 0, and blocks until
   completion. Other threads can continue sending messages but no garantee is made they will be part
   of the current flush."

  [^KafkaProducer producer]

  (.flush producer)
  producer)




(defn metrics

  "Requests metrics about this producer.

   Returns a map of metric group name -> map of metric name -> map containing

     :dvlopt.kafka/description (when available)
      String description for human consumption.

     :dvlopt.kafka/properties
      Map of keywords to strings, additional arbitrary key-values.

     :dvlopt.kafka/floating-value
      Numerical value."

  [^KafkaProducer producer]

  (K.-interop.clj/metrics (.metrics producer)))
