(ns dvlopt.kafka.in.mock

  "Mocking Kafka consumers."

  {:author "Adam Helinski"}

  (:require [dvlopt.kafka               :as K]
            [dvlopt.kafka.-interop.java :as K.-interop.java]
            [dvlopt.void                :as void])
  (:import (org.apache.kafka.clients.consumer Consumer
                                              MockConsumer)))




;;;;;;;;;; Creating a mock consumer


(defn mock-consumer

  "Builds a mock Kafka consumer for testing purposes. It can use the `dvlopt.kafka.in` API like a regular consumer.

   Like regular consumers, mock ones are not thread-safe.

  
   A map of options may be provided :

     ::ofset-reset-strategy
       One of :

         :ealiest
          Consumption of records will start from the beginning (ie. what has been set by a subsequent call to `set-beginning-offsets`).

         :latest (default)
          Consumption of records will start from the known end (ie. what has been set by a subsequent call to `set-end-offsets`)."

  (^MockConsumer
    
   []

   (mock-consumer nil))


  (^MockConsumer

   [options]

   (MockConsumer. (K.-interop.java/offset-reset-strategy (void/obtain ::offset-reset-strategy
                                                                      options
                                                                      K/defaults)))))




;;;;;;;;;; Stateful operations


(defn add-record

  "Adds a record for the next poll operation.
  
   Polling subsequently deletes all records that have been added up to that point. Hence, those records cannot be consumed more than
   once.
  
  
   Cf. `dvlopt.kafka.in/poll` for the structure of a record (headers not supported)"

  ^MockConsumer

  [^MockConsumer mock-consumer record]

  (.addRecord mock-consumer
              (K.-interop.java/consumer-record record))
  mock-consumer)




(defn during-next-poll

  "Next time this mock consumer polls records, no-op `f` will be executed.
  
   `f` can safely be executed in a multithreaded situtation."

  [^MockConsumer mock-consumer f]

  (.schedulePollTask mock-consumer
                     f)
  mock-consumer)




(defn rebalance

  "Simulates a rebalance event just like when consumers join or leave the relevant consumer group."

  [^MockConsumer mock-consumer topic-partitions]

  (.rebalance mock-consumer
              (map K.-interop.java/topic-partition
                   topic-partitions))
  mock-consumer)




(defn set-beginning-offsets

  "Sets beginning offsets by following a map of [topic partition] -> offset."

  ^MockConsumer

  [^MockConsumer mock-consumer topic-partition->offset]

  (.updateBeginningOffsets mock-consumer
                           (K.-interop.java/topic-partition->offset topic-partition->offset))
  mock-consumer)




(defn set-end-offsets

  "Sets end offsets by following a map of [topic partition] -> offset."

  ^MockConsumer

  [^MockConsumer mock-consumer topic-partition->offset]

  (.updateEndOffsets mock-consumer
                  (K.-interop.java/topic-partition->offset topic-partition->offset))
  mock-consumer)
