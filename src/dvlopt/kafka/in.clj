(ns dvlopt.kafka.in

  "Kafka consumers."

  {:author "Adam Helinski"}

  (:require [dvlopt.kafka               :as K]
            [dvlopt.kafka.-interop      :as K.-interop]
            [dvlopt.kafka.-interop.clj  :as K.-interop.clj]
            [dvlopt.kafka.-interop.java :as K.-interop.java]
            [dvlopt.void                :as void])
  (:import java.util.Collection
           java.util.concurrent.TimeUnit
           java.util.regex.Pattern
           (org.apache.kafka.clients.consumer ConsumerRecords
                                              KafkaConsumer
                                              OffsetAndMetadata)
           org.apache.kafka.common.TopicPartition
           (org.apache.kafka.common.errors AuthorizationException
                                           InterruptException
                                           WakeupException)))




;;;;;;;;;; Starting and stopping a consumer


(defn consumer 

  "Builds a Kafka consumer.

   <!> Consumers are NOT thread safe !
       1 consumer / thread or a queueing policy must be implemented.

   A map of options may be given :

     ::configuration
       Map of Kafka consumer properties.
       Cf. https://kafka.apache.org/documentation/#newconsumerconfigs

     :dvlopt.kafka/nodes
       List of [host port].

     :dvlopt.kafka/deserializer.key
       Prepared deserialize or a function coercing a byte-array to a key (cf. `deserializer`).

     :dvlopt.kafka/deserializer.value
       Idem, but for values.


   Ex. (consumer {::configuration                  {\"group.id\"           \"m-_group\"
                                                    \"enable.auto.commit\" false}
                  :dvlopt.kafka/nodes              [[\"some_host\"] 9092]
                  :dvlopt.kafka/deserializer.key   :string
                  :dvlopt.kafka/deserializer.value (fn [data _metadata]
                                                     (some-> data
                                                             nippy/thaw))})"

  (^KafkaConsumer
    
   []

   (consumer nil))


  (^KafkaConsumer

   [options]

   (KafkaConsumer. (K.-interop/resource-configuration (::configuration options)
                                                      (void/obtain ::K/nodes
                                                                   options
                                                                   K/defaults))
                   (K.-interop.java/extended-deserializer (void/obtain ::K/deserializer.key
                                                                       options
                                                                       K/defaults))
                   (K.-interop.java/extended-deserializer (void/obtain ::K/deserializer.value
                                                                       options
                                                                       K/defaults)))))






(defn close

  "Closes the consumer and releases all associated resources.

   A map of options may be given :

     :dvlopt.kafka/timeout
      Default one is of 30 seconds.
      The consumer will try to complete pending commits and leave its consumer group. If auto-commit
      is enabled, the current offsets will be committed. If operations are not completed before the
      timeout, the consumer will be force closed.
      Cf. `dvlopt.kafka` for description of time intervals"

  ([^KafkaConsumer consumer]

   (.close consumer)
   nil)


  ([^KafkaConsumer consumer options]

   (if-let [timeout (::K/timeout options)]
     (.close consumer
             (K.-interop.java/duration timeout))
     (.close consumer))
   nil))




;;;;;;;;;; Handling consumers


(defn topics

  "Requests a list of metadata about partitions for all topics the consumer is authorized to consume.

   Returns a map of topic -> list of metadata exactly like in `partitions`.
  
   A map of options may be given :

      :dvlopt.kafka/timeout
      Cf. `dvlopt.kafka` for description of time intervals"

  ([consumer]

   (topics consumer
           nil))


  ([^KafkaConsumer consumer options]

   (reduce (fn reduce-topics [topic->partition-infos [topic pis]]
             (assoc topic->partition-infos
                    topic
                    (map K.-interop.clj/partition-info
                         pis)))
           {}
           (if-let [timeout (::K/timeout options)]
             (.listTopics consumer
                          (K.-interop.java/duration timeout))
             (.listTopics consumer)))))




(defn partitions
  
  "Requests a list of partitions for a given topic.

   For the returned value, cf. `dvlopt.kafka.out/partitions`.
  
   A map of options may be given :

      :dvlopt.kafka/timeout
      Cf. `dvlopt.kafka` for description of time intervals"

  ([consumer topic]

   (partitions consumer
               topic
               nil))


  ([^KafkaConsumer consumer topic options]

   (map K.-interop.clj/partition-info
        (if-let [timeout (::K/timeout options)]
          (.partitionsFor consumer
                          topic
                          (K.-interop.java/duration timeout))
          (.partitionsFor consumer
                          topic)))))




(defn register-for

  "A consumer can either be assigned to a list of specific [topic partition]'s by the user or subscribe
   to a list of topics or a regular expression describing topics. When subscribing, the [topic partition]'s
   are dynamically assigned by the broker. Assignments change over time. For instance, when another consumer
   from the same consumer group subscribes to the same topics, [topic partition]'s are rebalanced between
   consumers so that one of them does not overwork or underwork.

   Everytime this function is called, the consumer is unregistered from previous sources if there are any.

   A map of options may be given :

     ::on-rebalance
      Callback in order to be informed in case of dynamic :assignment or :revocation (first argument) regarding
      [topic partition]'s (second argument).


   Ex. ;; dynamic subscription by regular expression
  
       (register-for consumer
                     #\"topic-.+\"
                     {::on-rebalance (fn [operation topic-partitions]
                                       (when (= operation
                                                :assignment)
                                         ...))})

       ;; dynamic subscription to given topics

       (register-for consumer
                     [\"my-topic\"
                      \"another-topic\"])

       ;; static assignment to specific [topic partition]'s

       (register-for consumer
                     [[\"my-topic\"      0]
                      [\"another-topic\" 3]])"

  (^KafkaConsumer

   [^KafkaConsumer consumer source]

   (register-for consumer
                 source
                 nil))


  (^KafkaConsumer
    
   [^KafkaConsumer consumer source options]
   
   (.unsubscribe consumer)
   (let [on-rebalance (some-> (::on-rebalance options)
                              K.-interop.java/consumer-rebalance-listener)]
     (if (sequential? source)
       (if (sequential? (first source))
         (.assign consumer
                  (map K.-interop.java/topic-partition
                       source))
         (if on-rebalance
           (.subscribe consumer
                       ^Collection source
                       on-rebalance)
           (.subscribe consumer
                       ^Collection source)))
       (if on-rebalance
         (.subscribe consumer
                     ^Pattern source
                     on-rebalance)
         (.subscribe consumer
                     ^Pattern source))))
   consumer))




(defn unregister

  "Unregisters a consumer from all the sources it is consuming."

  ^KafkaConsumer
   
  [^KafkaConsumer consumer]

  (.unsubscribe consumer)
  consumer)




(defn registered-for

  "Returns a map containing :

     ::assignments
      Set of all [topic partition]'s the consumer is currently assigned to, either by the broker or by the user.

     ::subscriptions
      Set of all topics the consumer is subscribed to, if any."


  [^KafkaConsumer consumer]

  {::assignments   (into #{}
                         (map K.-interop.clj/topic-partition
                              (.assignment consumer)))
   ::subscriptions (into #{}
                         (.subscription consumer))})




(defn registered-for?

  "Is the consumer listening to the given topic / [topic partition] ?"

  [consumer source]

  (contains? (get (registered-for consumer)
                  (if (string? source)
                    ::subscriptions
                    ::assignments))
             source))




(defn pause

  "Suspends fetching records from the given [topic partition]'s until `resume` is called.

   This function does not affect subscriptions nor does it trigger a consumer group rebalance.


   Ex. (pause consumer
              [[\"my-topic\"      0]
               [\"another-topic\" 3]])"

  ^KafkaConsumer

  [^KafkaConsumer consumer topic-partitions]

  (.pause consumer
          (map K.-interop.java/topic-partition
               topic-partitions))
  consumer)




(defn paused

  "Returns a set of the currently paused [topic partition]'s."

  [^KafkaConsumer consumer]

  (into #{}
        (map K.-interop.clj/topic-partition
             (.paused consumer))))




(defn resume

  "Undoes `pause`.
   

   Ex. ;; resumes everything that has been paused
  
       (resume consumer)

       ;; resumes only specific [topic partition]'s
    
       (resume [[\"my-topic\"      0]
                [\"another-topic\" 3]])"

  ^KafkaConsumer

  (^KafkaConsumer
    
   [consumer]

   (resume consumer
           (paused consumer)))


  (^KafkaConsumer
    
   [^KafkaConsumer consumer topic-partitions]

   (.resume consumer
            (map K.-interop.java/topic-partition
                 topic-partitions))
   consumer))




(defn beginning-offsets

  "Requests a map of [topic partition] -> first available offset for consumption.

   This function does not change the current position of the consumer.

   <!> Blocks forever if the topic doesn't exist and dynamic creation has been disabled server-side.


   A map of options may be given :

      :dvlopt.kafka/timeout
      Cf. `dvlopt.kafka` for description of time intervals


   Ex. (beginning-offsets consumer
                          [[\"my-topic\"      0]
                           [\"another-topic\" 3]]
                          {:dvlopt.kafka/timeout [5 :seconds]})"

  ([consumer topic-partitions]

   (beginning-offsets consumer
                      topic-partitions
                      nil))


  ([^KafkaConsumer consumer topic-partitions options]

   (let [topic-partitions' (map K.-interop.java/topic-partition
                                topic-partitions)]
     (K.-interop.clj/topic-partition->offset (if-let [timeout (::K/timeout options)]
                                               (.beginningOffsets consumer
                                                                  topic-partitions'
                                                                  (K.-interop.java/duration timeout))
                                               (.beginningOffsets consumer
                                                                  topic-partitions'))))))




(defn end-offsets

  "Requests a map of [topic partition] -> end offset (ie. offset of the last message + 1).

   Works like `beginning-offsets`."

  ([^KafkaConsumer consumer topic-partitions]

   (end-offsets consumer
                topic-partitions
                nil))


  ([^KafkaConsumer consumer topic-partitions options]

   (let [topic-partitions' (map K.-interop.java/topic-partition
                                topic-partitions)]
     (K.-interop.clj/topic-partition->offset (if-let [timeout (::K/timeout options)]
                                               (.endOffsets consumer
                                                            topic-partitions'
                                                            (K.-interop.java/duration timeout))
                                               (.endOffsets consumer
                                                            topic-partitions'))))))




(defn offsets-for-timestamps

  "Requests a map of [topic partition] -> map containing :

     :dvlopt.kafka/offset 
      Earliest offset whose timestamp is greather than or equal to the given one for that [topic partition].

     :dvlopt.kafka/timestamp
      Timestamp of the record at that offset.


   <!> Blocks forever if the topic doesn't exist and dynamic creation has been disabled server-side.
  

   A map of options may be given :

      :dvlopt.kafka/timeout
      Cf. `dvlopt.kafka` for description of time intervals


   Ex. (offsets-for-timestamps consumer
                               {[\"my-topic\"      0] 1507812268270
                                [\"another-topic\" 3] 1507812338294}
                               {:dvlopt.kafka/timeout [5 :seconds]})"

  ([consumer topic-partition->timestamp]

   (offsets-for-timestamps consumer
                           topic-partition->timestamp
                           nil))


  ([^KafkaConsumer consumer topic-partition->timestamp options]
  
   (reduce (fn reduce-offsets [offsets [topic-partition oat]]
             (assoc offsets
                    (K.-interop.clj/topic-partition topic-partition)
                    (K.-interop.clj/offset-and-timestamp oat)))
           {}
           (let [topic-partition->timestamp' (reduce-kv (fn reduce-timestamps [hmap topic-partition timestamp]
                                                          (assoc hmap
                                                                 (K.-interop.java/topic-partition topic-partition)
                                                                 timestamp))
                                                        {}
                                                        topic-partition->timestamp)]
             (if-let [timeout (::K/timeout options)]
               (.offsetsForTimes consumer
                                 topic-partition->timestamp'
                                 (K.-interop.java/duration timeout))
               (.offsetsForTimes consumer
                                 topic-partition->timestamp'))))))




(defn next-offset

  "Given a [topic partition], requests the offset of the next record this consumer can consume.

   Issues a remote call only if there is no current position for the requested [topic partition].


   A map of options may be given :

      :dvlopt.kafka/timeout
      Cf. `dvlopt.kafka` for description of time intervals


   Ex. (next-offset consumer
                    [\"my-topic\" 0]
                    {:dvlopt.kafka/timeout [5 :seconds]})"

  ([consumer [topic partition :as topic-partition]]

   (next-offset consumer
                topic-partition
                nil))


  ([^KafkaConsumer consumer [topic partition] options]

   (let [tp      (K.-interop.java/topic-partition topic
                                                  partition)
         timeout (::K/timeout options)]
     (if timeout
       (.position consumer
                  tp
                  (K.-interop.java/duration timeout))
       (.position consumer
                  tp)))))




(defn seek

   "Moves the consumer to the new offsets.

    Actually happens lazily on the next call to `poll` and variations.


    Ex. (seek consumer
              {[\"my-topic\"      0] 42
               [\"another-topic\" 3] 84})"

  ^KafkaConsumer

  [^KafkaConsumer consumer topic-partition->offset]

  (doseq [[topic-partition
           offset]         topic-partition->offset]
    (.seek consumer
           (K.-interop.java/topic-partition topic-partition)
           offset))
  consumer)




(defn rewind

  "Rewinds a consumer to the first available offset for the given [topic partition]'s.

   Actually happens lazily on the next call to `poll` or `next-offset`.


   Ex. ;; rewind all currently registered [topic partition]'s
       
       (rewind consumer)

       ;; rewind only specific currently registered [topic partition]'s
  
       (rewind consumer
               [[\"my-topic\"      0]
                [\"another-topic\" 3]])"

  (^KafkaConsumer
    
   [consumer]

   (rewind consumer
           []))


  (^KafkaConsumer
    
   [^KafkaConsumer consumer topic-partitions]

   (.seekToBeginning consumer
                     (map K.-interop.java/topic-partition
                          topic-partitions))
   consumer))




(defn fast-forward

  "Fast forwards a consumer to the end of the given [topic partition]'s.
   
   Actually happens lazily on the next call to `poll` or `next-offset`.
  
   If no [topic partition] is supplied, applies to all currently assigned partitions.

   If the consumer was configured with :isolation.level = \"read_committed\", the latest offsets will be
   the \"last stable\" ones.
  

   Ex. ;; fast-forward all currently registered [topic partition]'s

       (fast-forward consumer)

       ;; fast-forward specific currently registered [topic partition]'s
  
       (fast-forward consumer
                     [[\"my-topic\"      0]
                      [\"another-topic\" 3]])"

  (^KafkaConsumer
    
   [consumer]

   (fast-forward consumer
                 []))


  (^KafkaConsumer
    
   [^KafkaConsumer consumer topic-partitions]

   (.seekToEnd consumer
               (map K.-interop.java/topic-partition
                    topic-partitions))
   consumer))




(defn- -poll-raw-records

  ;; Helper for record polling functions.

  ^ConsumerRecords
   
  [^KafkaConsumer consumer options]

  (let [^ConsumerRecords raw-records (try
                                       (if-let [timeout (::K/timeout options)]
                                         (.poll consumer
                                                (K.-interop.java/duration timeout))
                                         (.poll consumer
                                                Long/MAX_VALUE))
                                       (catch IllegalStateException _
                                         ;; When the consumer is not subscribed nor assigned.
                                         nil))]
    (when (and raw-records
               (not (.isEmpty raw-records)))
      raw-records)))




(defn poll

  "Synchronously polls records from the registered sources.

  
   A map of options may be given :

      :dvlopt.kafka/timeout
      An emtpy interval such as [0 :milliseconds] returns what is available in the consumer buffer without blocking.
      Cf. `dvlopt.kafka` for description of time intervals


   A record is a map containing :
    
     :dvlopt.kafka/key
      Deserialized key.

     :dvlopt.kafka/offset
      Record offset.

     :dvlopt.kafka/partition
      Partition number.

     :dvlopt.kafka/timestamp
      Unix timestamp.

     :dvlopt.kafka/topic
      Topic name.

     :dvlopt.kafka/value
      Deserialized value."

  ([consumer]

   (poll consumer
         nil))


  ([consumer options]

   (some->> (-poll-raw-records consumer
                               options)
            (map K.-interop.clj/consumer-record))))




(defn poll-topic-partitions

  "Behaves exactly like `poll` but returns a map of [topic partition] -> list of record.

   More efficient when records are dispatched to workers by [topic partition]."

  ([consumer]

   (poll-topic-partitions consumer
                          nil))


  ([consumer options]

   (when-let [raw-records (-poll-raw-records consumer
                                             options)]
     (reduce (fn reduce-partitions [partitions ^TopicPartition tp]
               (assoc partitions
                      (K.-interop.clj/topic-partition tp)
                      (map K.-interop.clj/consumer-record
                           (.records raw-records
                                     tp))))
             {}
             (.partitions raw-records)))))




(defn- -poll-seq

  ;; Helper for `poll-seq`.

  [consumer options records]

  (lazy-seq
    (when-let [records (or records
                           (poll consumer
                                 options))]
      (cons (first records)
            (-poll-seq consumer
                       options
                       (next records))))))




(defn poll-seq

  "Returns a sequence of records obtained by lazily and continuously calling `poll` under the hood.


   Ex. (take 5
             (poll-seq consumer
                       {:dvlopt.kafka/timeout [5 :seconds]}))"

  ([consumer]

   (poll-seq consumer
             nil))


  ([consumer options]

   (-poll-seq consumer
              options
              nil)))




(defn commit-offsets

  "Manually and synchronously commits offsets to Kafka.

   The committed offsets will be used on the first fetch after every assignment and also on startup.
   As such, if the user need to store offsets anywhere else, this function should not be used.


   A map of options may be given :

     :dvlopt.kafka/timeout
      Cf. `dvlopt.kafka` for description of time intervals

     ::topic-partition->offset
      Map of [topic partition] -> offset to commit. If none is provided, offsets from the last call to `poll` and variations
      are used.


   Ex. ;; commits specific offsets

       (commit-offsets consumer
                       {:dvlopt.kafka/timeout     [5 :seconds]
                        ::topic-partition->offset {[\"my-topic\"      0] 24
                                                   [\"another-topic\" 3] 84})"

  (^KafkaConsumer
    
   [^KafkaConsumer consumer]

   (.commitSync consumer)
   consumer)


  (^KafkaConsumer
    
   [^KafkaConsumer consumer options]

   (let [timeout                 (some-> (::K/timeout options)
                                         K.-interop.java/duration)
         topic-partition->offset (some-> (::topic-partition->offset options)
                                         K.-interop.java/topic-partition->offset-and-metadata)]
     (if timeout
       (if topic-partition->offset
         (.commitSync consumer
                      topic-partition->offset
                      timeout)
         (.commitSync consumer
                      timeout))
       (if topic-partition->offset
         (.commitSync consumer
                      topic-partition->offset)
         (.commitSync consumer))))
   consumer))




(defn commit-offsets-async

  "Manually and asynchronously commits offsets to Kafka.

   The end result is the same as `commit-offsets-sync` but the offsets will be committed on the next
   trip to the server, such as calling `poll`.

   Multiple calls to this function are garanteed to be sent in order and any pending async commit will happen before a new sync one.


   A map of options may be given :

     ::on-committed
      Callback called when offsets are actually committed. Acceps 2 arguments : an exception in case of failure and a map of
      [topic partition] -> committed offset in case of success.

     ::topic-partition->offset
      Map of [topic partition] -> offset to commit. If none is provided, offsets from the last call to `poll` and variations
      are used.


   Ex. (commit-offsets-async consumer
                             {::on-committed            (fn callback [exception topic-partition->offset]
                                                          (when exception
                                                            ...))
                              ::topic-partition->offset {[\"some_topic\" 0]} 42})"

  (^KafkaConsumer
    
   [^KafkaConsumer consumer]

   (.commitAsync consumer)
   consumer)


  (^KafkaConsumer
    
   [^KafkaConsumer consumer options]

   ;; `on-committed` may be nil without NPE later.

   (let [on-committed            (some-> (::on-committed options)
                                         K.-interop.java/offset-commit-callback)
         topic-partition->offset (some-> (::topic-partition->offset options)
                                         K.-interop.java/topic-partition->offset-and-metadata)]
     (if topic-partition->offset
       (.commitAsync consumer
                     topic-partition->offset
                     on-committed)
       (.commitAsync consumer
                     on-committed)))))




(defn committed-offset

  "Requests the last committed offset for a [topic partition] (nil if nothing has been committed).

   May block if the partition is not assigned to this consumer or if the consumer hasn't yet initialized its cache of committed
   offsets.

   A map of options may be given :

      :dvlopt.kafka/timeout
      Cf. `dvlopt.kafka` for description of time intervals"

  ([consumer [topic partition :as topic-partition]]

   (committed-offset consumer
                     topic-partition
                     nil))


  ([^KafkaConsumer consumer [topic partition] options]

   (let [tp (K.-interop.java/topic-partition topic
                                             partition)]
     (when-let [^OffsetAndMetadata om (if-let [timeout (::K/timeout options)]
                                        (.committed consumer
                                                    tp
                                                    (K.-interop.java/duration timeout))
                                        (.committed consumer
                                                    tp))]
         (.offset om)))))




(defn metrics

  "Requests metrics about this consumer, exactly like `dvlopt.kafka.out/metrics`."

  [^KafkaConsumer consumer]

  (K.-interop.clj/metrics (.metrics consumer)))
