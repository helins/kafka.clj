(ns milena.converters

  "Converting between Kafka java objects and clojure
   data structures.

   The conversions are straightforward and haven't been
   fully optimized."

  (:require [clojure.set :as sets])
  (:import (org.apache.kafka.common Metric
                                    MetricName
                                    Node
                                    PartitionInfo
                                    TopicPartition)
           (org.apache.kafka.clients.producer ProducerRecord
                                              RecordMetadata
                                              Callback)
           (org.apache.kafka.clients.consumer 
                                              ConsumerRebalanceListener
                                              ConsumerRecord
                                              OffsetAndMetadata
                                              OffsetAndTimestamp
                                              OffsetCommitCallback)))





(defn to-TopicPartition

  "Convert [topic partition] to a TopicPartition, commonly used
   in the library"

  ([[ktopic kpart]]

   (to-TopicPartition ktopic
                      kpart))


  ([ktopic kpart]

   (TopicPartition. ktopic
                    (int kpart))))




(defn TopicPartition->vec

  "Convert a TopicPartition to [topic partition]"

  [^TopicPartition t-p]

  [(.topic t-p) (.partition t-p)])




(defn hmap->ProducerRecord

  "Mappify a ProducerRecord for sending data to Kafka.

   :partition, :timestamp and :key are optional."

  [{ktopic     :topic
    kpart      :partition
    timestamp  :timestamp
    kkey       :key
    kvalue     :value
    :as        message}]

  (ProducerRecord. ktopic
                   kpart
                   timestamp
                   kkey
                   kvalue))




(defn ConsumerRecord->hmap

  "Mappify a Kafka record returned by polling.

   Contains :topic
            :partition
            :offset
            :timestamp
            :key
            :value
            :checksum"

  [^ConsumerRecord c-r]

  {:topic     (.topic     c-r)
   :partition (.partition c-r)
   :offset    (.offset    c-r)
   :timestamp (.timestamp c-r)
   :key       (.key       c-r)
   :value     (.value     c-r)
   :checksum  (.checksum  c-r)
   })




(defn RecordMetadata->hmap

  "Mappify data returned by derefing futures produced on sends.

   Contains :topic
            :partition
            :timestamp
            :offset
            :checksum"

  [^RecordMetadata r-m]

  {:topic     (.topic     r-m)
   :partition (let [kpart (.partition r-m)] (when (not= kpart
                                                        RecordMetadata/UNKNOWN_PARTITION)
                                              kpart))
   :timestamp (.timestamp r-m)
   :offset    (.offset    r-m)
   :checksum  (.checksum  r-m)})




(defn OffsetAndMetadata->hmap

  "Mappify offsets for offset commits.

   Contains :offset
            :meta"

  [^OffsetAndMetadata o+m]

  (let [ret {:offset (.offset o+m)}
        mta (.metadata o+m)]
    (if mta
        (assoc ret :meta mta)
        ret)))




(defn hmap->OffsetAndMetadata

  "Produce an OffsetAndMetadata object"

  [{mta    :meta
    offset :offset}]

  (OffsetAndMetadata. offset
                      mta))




(defn OffsetAndTimestamp->hmap

  "Mainly used for searching offsets by timestamp.

   Contains :timestamp
            :offset"

  [^OffsetAndTimestamp o+t]

  {:timestamp (.timestamp o+t)
   :offset    (.offset    o+t)
   })




(defn Node->hmap

  "Mappify a Kafka node.

   Contains :host
            :port
            :rack
            :id"

  [^Node n]

  {:host (.host n)
   :port (.port n)
   :rack (.rack n)
   :id   (.id   n)
   })




(defn PartitionInfo->hmap

  "Mappify data about a partition.

   Contains :leader
            :replicas
            :topic
            :partition"

  [^PartitionInfo p-i]
  
  {:leader    (Node->hmap (.leader p-i))
   :replicas  (let [all    (into #{}
                                 (map Node->hmap
                                      (.replicas p-i)))
                    synced (into #{}
                                 (map Node->hmap
                                      (.inSyncReplicas p-i)))]
                {:synced     synced
                 :not-synced (sets/difference all
                                              synced)})
   :topic     (.topic     p-i)
   :partition (.partition p-i)
   })




(defn MetricName->hmap

  "Mappify metadata about a metric.

   Contains :group
            :name
            :description
            :tags"

  [^MetricName m-n]

  {:group       (.group       m-n)
   :name        (.name        m-n)
   :description (.description m-n)
   :tags        (.tags        m-n)
   })




(defn f->Callback

  "For producer callbacks.
   
   'f' takes an exception and a map representing metadata about the record
   that has been sent. Only one of them will be non-nil.

   cf. milena.converters/RecordMetadata->hmap"

  [f]

  (reify Callback

    (onCompletion [_ r-m exception]
      (f exception
         (some-> r-m
                 RecordMetadata->hmap)))))




(defn f->ConsumerRebalanceListener

  "For registering callbacks when a consumer has been assigned or revoked partitions.

   'f' takes an arg specifying if the event is an :assignment or a :revokation
   and a list of the affected [topic partition]."

  [f]

  (reify ConsumerRebalanceListener

    (onPartitionsAssigned [_ t-ps] (f :assigned (map TopicPartition->vec
                                                     t-ps)))

    (onPartitionsRevoked  [_ t-ps] (f :revoked  (map TopicPartition->vec
                                                     t-ps)))))




(defn f->OffsetCommitCallback

  "For commiting offsets asynchronously.

   'f' takes an exception as well as a map of [topic partition] -> offset.
   Only one arg will be non-nil."

  [f]

  (reify OffsetCommitCallback
    
    (onComplete [_ offsets exception]
      (f exception
         (reduce (fn [offsets' [t-p ^OffsetAndMetadata o+m]]
                   (assoc offsets'
                          (TopicPartition->vec t-p)
                          (.offset o+m)))
                 {}
                 offsets)))))
