(ns dvlopt.kafka.out.mock

  "Mocking Kafka producers."

  {:author "Adam Helinski"}

  (:require [dvlopt.kafka               :as K]
            [dvlopt.kafka.-interop.clj  :as K.-interop.clj]
            [dvlopt.kafka.-interop.java :as K.-interop.java]
            [dvlopt.void                :as void])
  (:import org.apache.kafka.clients.consumer.OffsetAndMetadata
           org.apache.kafka.clients.producer.MockProducer))




;;;;;;;;;; Creating a mock producer


(defn mock-producer

  "Builds a mock Kafka producer for testing purposes. It can use the `dvlopt.kafka.out` API.
  
   A map of options may be given :

     :dvlopt.kafka/serializer.key
     :dvlopt.kafka/serializer.value
      Cf. `dvlopt.kafka` for description of serializers.

     ::auto-complete?
      By default, each send operation is completed synchronously. Set this option to false in order to keep control.
      cf. `advance`"

  (^MockProducer

   []

   (mock-producer nil))


  (^MockProducer

   [options]

   (MockProducer. (void/obtain ::auto-complete?
                               options
                               K/defaults)
                  (K.-interop.java/extended-serializer (void/obtain ::K/serializer.key
                                                                    options
                                                                    K/defaults))
                  (K.-interop.java/extended-serializer (void/obtain ::K/serializer.value
                                                                    options
                                                                    K/defaults)))))




;;;;;;;;;; Querying the state of mock producers


(defn closed?

  "Is this mock producer closed ?"

  [^MockProducer mock-producer]

  (.closed mock-producer))




(defn flushed?

  "Is this mock producer flushed ?"

  [^MockProducer mock-producer]

  (.flushed mock-producer))




(defn records

  "Returns a list of records this mock producer received.

   Can be cleared with `clear-history`."

  [^MockProducer mock-producer]

  (map K.-interop.clj/producer-record
       (.history mock-producer)))




(defn trx-aborted?

  "Is the current transaction of this mock producer aborted ?"

  [^MockProducer mock-producer]

  (.transactionAborted mock-producer))




(defn trx-committed?

  "Is the current transaction of this mock producer committed ?"

  [^MockProducer mock-producer]

  (.transactionCommitted mock-producer))




(defn trx-committed-offsets

  "Returns a list of consumer offsets transaction commits (ie. maps of consumer-group -> (map of [topic partition] -> offset)).
  
   Can be cleared with `clear-history`."

  [^MockProducer mock-producer]

  (map (fn by-commit [consumer-group->offsets]
         (reduce (fn consumer-groups [consumer-group-offsets' [consumer-group offsets]]
                   (assoc consumer-group-offsets'
                          consumer-group
                          (reduce (fn by-topic-partition [offsets' [tp ^OffsetAndMetadata oam]]
                                    (assoc offsets'
                                           (K.-interop.clj/topic-partition tp)
                                           (.offset oam)))
                                  {}
                                  offsets)))
                 {}
                 consumer-group->offsets))
       (.consumerGroupOffsetsHistory mock-producer)))



(defn trx-count-commits

  "Returns the number of transactions committed by this mock producer."

  [^MockProducer mock-producer]

  (.commitCount mock-producer))



(defn trx-in-flight?

  "Is there a transaction in flight for this mock producer ?"

  [^MockProducer mock-producer]

  (.transactionInFlight mock-producer))




(defn trx-init?

  "Is there a transaction initialized for this mock producer ?"

  [^MockProducer mock-producer]

  (.transactionInitialized mock-producer))




;;;;;;;;;; Stateful operations


(defn clear-history

  "Clears the history underlying these functions :

     `records`
     `trx-committed-offsets`"

  ^MockProducer

  [^MockProducer mock-producer]

  (.clear mock-producer)
  mock-producer)




(defn advance

  "When the ::autocomplete? options is set to false, this function is used to complete the next pending send
   operation.
  
   Returns true if there was a pending record, false otherwise."

  [^MockProducer mock-producer]

  (.completeNext mock-producer))




(defn fail

  "Like `advance` but fails the next pending send operation with the given exception."

  [^MockProducer mock-producer exception]

  (.errorNext mock-producer
              exception))
