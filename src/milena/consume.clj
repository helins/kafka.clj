(ns milena.consume

  "Everything related to Kafka consumers"

  {:author "Adam Helinski"}

  (:require [milena.shared     :as shared]
            [milena.converters :as convert])
  (:import milena.shared.Wrapper
           (org.apache.kafka.common.errors WakeupException
                                           InterruptException
                                           AuthorizationException)
           (org.apache.kafka.clients.consumer KafkaConsumer
                                              ConsumerRecords
                                              OffsetAndMetadata)
           (org.apache.kafka.common.serialization Deserializer
                                                  ByteArrayDeserializer
                                                  ByteBufferDeserializer
                                                  DoubleDeserializer
                                                  IntegerDeserializer
                                                  LongDeserializer
                                                  StringDeserializer)))





(def deserializers

  "Basic deserializers provided by Kafka"

  {:byte-array  (ByteArrayDeserializer.)
   :byte-buffer (ByteBufferDeserializer.)
   :double      (DoubleDeserializer.)
   :int         (IntegerDeserializer.)
   :long        (LongDeserializer.)
   :string      (StringDeserializer.)
   })




(defn make-deserializer

  "Given a fn that takes a topic and data to deserialize,
   make a Kafka deserializer for consumers"

  [f]

  (reify Deserializer
    
    (deserialize [_ ktopic data] (f ktopic
                                    data))

    (close [_] nil)

    (configure [_ _ _] nil)))




(defn deserialize

  "Deserialize data using a Kafka deserializer"

  [^Deserializer deserializer ktopic data]

  (.deserialize deserializer
                ktopic
                data))




(declare listen)


(defn make

  "Build a Kafka consumer.

   config :
     config : a configuration map for the consumer as described in
              Kafka documentation (keys can be keywords)
     nodes : a connection string to Kafka nodes
           | a list of [host port]
     serializer... : cf. deserializers
                         (make-deserializer) 

   <!> Consumers are NOT thread safe !
       1 consumer / thread or a queueing policy must be
       implemented."

  [& [{:as     opts
       :keys   [config
                nodes
                deserializer
                deserializer-key
                deserializer-value
                listening]
       listen' :listen
       :or     {nodes              [["localhost" 9092]]
                deserializer       (deserializers :byte-array)
                deserializer-key   deserializer
                deserializer-value deserializer}}]]
  
  (let [consumer (shared/wrap (KafkaConsumer. (assoc (shared/stringify-keys config)
                                              "bootstrap.servers"
                                              (shared/nodes-string nodes))
                                       deserializer-key
                                       deserializer-value))]
    (when listen'
      (try (listen consumer
                   listen')
           (catch Throwable e
             (shared/close consumer)
             (throw e))))
    consumer))




(defn closed?

  "Is this consumer closed ?"

  [consumer]

  (shared/closed? consumer))




(defn close

  "Try to close the consumer cleanly within the given timeout or a default one of
   30 seconds.
  
   It'll try to complete pending commits and leave the group. If auto-commit is enabled,
   the current offsets will be committed. If those operations aren't completed when the
   timeout is reacehd, the consumer will be force closed.

   Note that (unblock) cannot be used to interrupt this fn."

  ([consumer]

   (shared/close consumer))


  ([consumer timeout-ms]

   (shared/close consumer
                 timeout-ms)))




(defn raw

  "Unwrap this consumer and get the raw Kafka object.
  
   At your own risks."

  [consumer]

  (shared/raw consumer))




(defn consumer?

  "Is this a consumer ?"

  [x]

  (instance? KafkaConsumer
             (shared/raw x)))




(defn topics

  "Get a list of metadata about partitions for all the topics the consumer
   is authorized to consume.

   metadata : a map containing :leader
                               :replicas 
                               :topic
                               :partition

   cf. milena.converters/PartitionInfo->hmap"

  [consumer]

  (shared/try-nil
    (reduce (fn [ktopics [ktopic p-i]]
              (assoc ktopics
                     ktopic
                     (map convert/PartitionInfo->hmap
                         p-i)))
            {}
            (.listTopics ^KafkaConsumer (raw consumer)))))




(defn partitions
  
  "Get a list of partitions for a given topic"

  [consumer ktopic]

  (shared/partitions consumer
                     ktopic))




(defn- -subscribe

  "Subscribe a consumer.
  
   Helper for (listen)"

  [^KafkaConsumer consumer topic+ f-rebalance]

  (when topic+ (if-let [f-rebalance' (some-> f-rebalance
                                             convert/f->ConsumerRebalanceListener)]
                 (.subscribe consumer
                             topic+
                             f-rebalance')
                 (.subscribe consumer
                             topic+)))
  consumer)




(defn listen

  "Subscribe a consumer to topics or assign it to specific partitions.

   An assignement precisely refers to a [topic partition] whereas
   a subscription only ask for a topic, the partition being assigned
   dynamically.

   source : a regular expression to subscribe to
          | a list of topics as strings to subscribe to
          | a list of [topic partition] to be assigned to
          | nil for explicitly stopping listening
   f-rebalance : an optional fn for subscriptions, will be called with the
                 event type (:assigned or :revoked) and a list of the affected
                 [topic partition]
                 (cf. milena.converters/f->ConsumerRebalanceListener)

   <!> A consumer can only consume from one type of source. This fn will throw if
       the user try to mix, for instance, subscriptions and assignments, or regexes
       and strings."

  [consumer source & [f-rebalance]]

  (when-let [^KafkaConsumer consumer' (and (not (closed? consumer))
                                           (raw consumer))]
    (if source
        (if (sequential? source)
            (if (sequential? (first source))
                (.assign consumer'
                         (map convert/to-TopicPartition
                              source))
                (-subscribe consumer'
                            source
                            f-rebalance))
              (-subscribe consumer'
                          source
                          (or f-rebalance
                              (fn [_ _]))))
        (.unsubscribe consumer')))
  consumer)




(defn listening

  "Get all the partitions the consumer is assigned to as well as the
   subscriptions.

   cf. milena.core/listen"

  [consumer]

  (shared/try-nil
    (let [^KafkaConsumer consumer' (raw consumer)]
      {:partitions    (into #{}
                            (map convert/TopicPartition->vec
                                 (.assignment consumer')))
       :subscriptions (into #{}
                            (.subscription consumer'))})))




(defn listening?

  "Is the consumer listening to the given topic / [topic partition] ?"

  [consumer source]

  (contains? (get (listening consumer)
                  (if (string? source)
                      :subscriptions
                      :partitions))
             source))




(defn pause

  "Temporarely pause consumption for a partition or a list of [topic partition]"

  ([consumer kpartitions]

   (shared/try-nil
     (.pause ^KafkaConsumer (raw consumer)
             (map convert/to-TopicPartition
                  kpartitions)))
   consumer)


  ([consumer ktopic kpart]

   (pause consumer
          [[ktopic kpart]])))




(defn paused

  "Get a list of [topic partition] currently paused.

   cf. (pause)"

  [consumer]

  (into #{}
        (shared/try-nil (map convert/TopicPartition->vec
                             (.paused ^KafkaConsumer (raw consumer))))))




(defn paused?

  "Is a [topic partition] currenly paused ?

   cf. (pause)"

  ([consumer kpartition]

   (boolean (some #(= %
                      kpartition)
                  (paused consumer))))


  ([consumer ktopic kpart]

   (paused? consumer
            [ktopic kpart])))




(defn resume

  "Resume a partition or a list of [topic partition] or
   everything is nothing is provided.

   cf. (pause)"

  ([consumer kpartitions]

   (shared/try-nil
     (.resume ^KafkaConsumer (raw consumer)
              (map convert/to-TopicPartition
                   kpartitions)))
   consumer)


  ([consumer]

   (resume consumer
           (paused consumer)))


  ([consumer ktopic kpart]

   (resume consumer
           [[ktopic kpart]])))




(defn earliest

  "Get the earliest offsets exiting for the given partitions.

   kpartitions : a list of [topic partition]

   Returns a map of [topic partition] -> offset.

   ----------

   ktopic : topic
   kpart : partition

   Returns the offset.

   <!> Will block forever if the topic doesn't exist and dynamic
       creation has been disabled server-side."

  ([consumer kpartitions]

   (shared/try-nil
     (convert/TP+offset->tp+offset (.beginningOffsets ^KafkaConsumer (raw consumer)
                                                      (map convert/to-TopicPartition
                                                           kpartitions)))))


  ([consumer ktopic kpart]

   (shared/try-nil
     (let [t-p (convert/to-TopicPartition ktopic
                                          kpart)]
       (get (.beginningOffsets ^KafkaConsumer (raw consumer)
                               [t-p])
            t-p)))))




(defn latest

  "Get the latest offsets existing for the required partitions.

   cf. (earliest)" 

  ([consumer kpartitions]

   (shared/try-nil
     (convert/TP+offset->tp+offset (.endOffsets ^KafkaConsumer (raw consumer)
                                                (map convert/to-TopicPartition
                                                     kpartitions)))))


  ([consumer ktopic kpart]

   (shared/try-nil
     (let [t-p (convert/to-TopicPartition ktopic
                                          kpart)]
       (get (.endOffsets ^KafkaConsumer (raw consumer)
                         [t-p])
            t-p)))))




(defn at

  "Search for offsets by timestamp.

   kpartitions+ts : a map of [topic partition] -> timestamp in unix time

   Returns a map of [topic partition] -> offset where offset is refering
   to the first message published at or after the corresponding timestamp.

   ----------

   ktopic : topic
   kpart : partition number
   ts : timestamp in unix time
   
   Returns the offset.

   <!> Will block forever if the topic doesn't exist and dynamic creation
       has been disabled server-side."

  ([consumer kpartitions+ts]
   
   (shared/try-nil
     (reduce (fn [offsets [t-p o+ts]]
               (assoc offsets
                      (convert/TopicPartition->vec      t-p)
                      (convert/OffsetAndTimestamp->hmap o+ts)))
             {}
             (.offsetsForTimes ^KafkaConsumer (raw consumer)
                              (reduce-kv (fn [t-p+ts kpartition ts]
                                             (assoc t-p+ts
                                                   (convert/to-TopicPartition kpartition)
                                                   (max 0 ts)))
                                          {}
                                          kpartitions+ts)))))


  ([consumer ktopic kpart ts]

   (shared/try-nil
     (let [tp (convert/to-TopicPartition ktopic
                                         kpart)]
       (some-> (get (.offsetsForTimes ^KafkaConsumer (raw consumer)
                                      {tp ts})
                    tp)
               convert/OffsetAndTimestamp->hmap)))))




(defn position

  "Returns the current position of the consumer, or nil
   if something is wrong/unavailable.

   ktopic : topic
   kpart : partition number

   Returns the current position.

   ----------

   kpartitions : a list of [topic partition]

   Returns a map of [topic partition] -> current position."

  ([consumer ktopic kpart]

   (shared/try-nil
     (.position ^KafkaConsumer (raw consumer)
                (convert/to-TopicPartition ktopic
                                           kpart))))


  ([consumer kpartitions]

   (reduce (fn [positions [ktopic kpart :as kpartition]]
             (assoc positions
                    kpartition
                    (position consumer
                              ktopic
                              kpart)))
           {}
           kpartitions)))




(defn seek

  "Seek a partition or a list of [topic partition] to a new position.

   ktopic : topic
   kpart : partition number
   position : new position

   -----------

   kpartitions : a list of [topic partition]
   position : new position"

  ([consumer ktopic kpart position]

   (shared/try-nil
     (.seek ^KafkaConsumer (raw consumer)
            (convert/to-TopicPartition ktopic
                                       kpart)
            (max 0 position)))
   consumer)


  ([consumer kpartitions position]

   (doseq [[ktopic kpart] kpartitions] (seek consumer
                                             position
                                             ktopic
                                             kpart))
   consumer))




(defn rewind

  "Rewind a consumer for the given partitions or all of them
   if none are provided.

   kpartitions : a list of [topic partition]
   
   ----------

   ktopic : topic
   kpart : partition"

  ([consumer kpartitions]

   (shared/try-nil
     (.seekToBeginning ^KafkaConsumer (raw consumer)
                       (map convert/to-TopicPartition
                            kpartitions)))
   consumer)


  ([consumer]

   (rewind consumer
           []))


  ([consumer ktopic kpart]

   (rewind consumer
           [[ktopic kpart]])))




(defn forward

  "Fast forward a consumer for the given partitions or all of them
   if none are provided.

   cf. (rewind)"

  ([consumer kpartitions]

   (shared/try-nil
     (.seekToEnd ^KafkaConsumer (raw consumer)
                 (map convert/to-TopicPartition
                      kpartitions)))
   consumer)


  ([consumer]

   (forward consumer
            []))


  ([consumer ktopic kpart]

   (forward consumer
            [[ktopic kpart]])))




(defn poll

  "Poll messages. If there aren't any, block until there are some or
   until 'timeout-ms' is elapsed.
  
   A timeout of 0 returns what is available in the consumer buffer
   without blocking."

  [consumer & [timeout-ms]]

  (shared/try-nil
    (let [^ConsumerRecords records (try (.poll ^KafkaConsumer (raw consumer)
                                               (if timeout-ms
                                                   (max 0
                                                        timeout-ms)
                                                   Long/MAX_VALUE))
                                        (catch WakeupException _
                                          nil)
                                        (catch InterruptException _
                                          nil)
                                        (catch AuthorizationException _
                                          ;; not authorized, hence there is nothing
                                          nil)
                                        (catch IllegalStateException _
                                          ;; when the consumer is not subscribed nor assigned
                                          nil))]
      (when-not (.isEmpty records)
          (map convert/ConsumerRecord->hmap
               (iterator-seq (.iterator records)))))))




(defn poll-do

  "Poll messages and perform side-effects.

   f : a fn accepting Kafka messages and returning true
       or false whether polling should continue or not

   cf. (poll)"

  [f consumer & [timeout-ms]]

  (while (when-let [kmsgs (poll consumer
                                timeout-ms)]
           (f kmsgs))))




(defn poll-reduce

  "Poll and reduce messages.

   f : a classic 2-args reducing fn 
   seed : the initial value for the reducing fn

   Will stop when there are no more messages or 'f' returns
   a (reduced) value, just like in (reduce).

   cf. (poll)"

  [f seed consumer timeout-ms]

  (loop [acc seed]
    (if-let [msgs (poll consumer
                        timeout-ms)]
      (let [acc' (reduce f
                         acc
                         msgs)]
        (if (reduced? acc')
            acc'
            (recur acc')))
      acc)))




(defn- -commit-sync

  "Helper for (commit).

   Commit offsetes synchronously"

  [consumer & [offsets]]

  (shared/try-bool
    (let [consumer' ^KafkaConsumer (raw consumer)]
      (if offsets
          (.commitSync consumer'
                       (convert/tpart+offset->TP+O&M offsets))
          (.commitSync consumer')))))




(defn- -commit-async

  "Helper for (commit).

   Commit offfsets asynchronously."

  [consumer & [callback offsets]]

  (shared/try-bool
    (let [consumer' ^KafkaConsumer (raw consumer)
          callback' (some-> callback
                            convert/f->OffsetCommitCallback)]
      (if callback'
          (try (if offsets
                   (.commitAsync consumer'
                                 (convert/tpart+offset->TP+O&M offsets)
                                 callback')
                   (.commitAsync consumer'
                                 callback'))
               (catch Throwable e
                 (callback e
                           nil)
                 true))
          (.commitAsync consumer')))))
              
          


(defn commit

  "Commit offsets to Kafka itself.

   The committed offsets will be used on the first fetch after every
   assignment and also on startup. As such, if the user need to store offsets
   anywhere else, this fn should not be used.

   opts :
     async? : send asynchronously ? (false by default)
     offsets : an optional map of [topic partition] -> offset
     callback : a fn invoked on async completion and accepting
                an exception and a map of [topic-partition] -> offset

   If offsets are not provided, offsets resulting from the last call to
   (poll) with this consumer will be committed.

   Async commits will be performed on the next call to a Kafka node such as
   (poll).

   Returns true or false whether the operation succeeded or not. A sync call
   returns false when an exception is thrown. It could be because the commit
   failed, the consumer is not authorized, or any other unrecoverable error. If
   error granularity is needed, use the async call."

  [consumer & [{:as   opts
                :keys [async?
                       offsets
                       callback]
                :or   {async? false}}]]
  (if async?
      (-commit-async consumer
                     callback
                     offsets)
      (-commit-sync consumer
                    offsets)))




(defn committed

  "Get the last committed offset for the given partition.

   May block if the partition isn't assigned to this consumer or
   if the consumer hasn't yet initialized its cache of committed
   offsets.

   ktopic : topic
   kpart : partition number

   Returns the last committed offset.

   -----------

   kpartitions : a list of [topic partition]

   Returns a map of [topic partition] -> last comitted offset"

  ([consumer ktopic kpart]

   (shared/try-nil
     (when-let [^OffsetAndMetadata o&m (.committed ^KafkaConsumer (raw consumer)
                                                   (convert/to-TopicPartition ktopic
                                                                              kpart))]
       (.offset o&m))))


  ([consumer kpartitions]

   (reduce (fn [commits [ktopic kpart :as kpartition]]
             (assoc commits
                    kpartition
                    (committed consumer
                               ktopic
                               kpart)))
           {}
           kpartitions)))




(defn unblock

  "From another thread, unblock the consumer. The blocking thread
   will throw an org.apache.kafka.common.errors.WakeupException against
   which fns in this library are protected (eg. (poll) returns nil).

   If the thread isn't blocking on a fn which can throw such an
   exception, the next call to such a fn will raise it instead.

   Must be used sparingly, not to compensate for a bad design."

  [^KafkaConsumer consumer]

  (.wakeup ^KafkaConsumer (raw consumer))
  consumer)




(defmacro safe-consume

  "If the body doesn't compute before the required timeout,
   milena.consume/unblock will be called on the consumer."

  [consumer timeout-ms & body]

  `(locking consumer
     (let [consumer# ~consumer
           timeout#  ~timeout-ms
           p#        (promise)
           ft#       (future (Thread/sleep timeout#)
                             (when-not (realized? p#)
                               (unblock consumer#)))
           ret#      (try ~@body
                          (catch Throwable e#
                            (future-cancel ft#)
                            (throw e#)))]
       (deliver p#
                ret#)
       (future-cancel ft#)
       ret#)))




(defn metrics

  "Get metrics about this consumer"

  [consumer]

  (shared/metrics consumer))
