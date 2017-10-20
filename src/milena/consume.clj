(ns milena.consume

  "Everything related to Kafka consumers"

  {:author "Adam Helinski"}

  (:require [milena.interop      :as $.interop]
            [milena.interop.clj  :as $.interop.clj]
            [milena.interop.java :as $.interop.java]
            [milena.deserialize  :as $.deserialize])
  (:import java.util.Collection
           java.util.regex.Pattern
           java.util.concurrent.TimeUnit
           (org.apache.kafka.common.errors WakeupException
                                           InterruptException
                                           AuthorizationException)
           (org.apache.kafka.clients.consumer KafkaConsumer
                                              ConsumerRecords
                                              OffsetAndMetadata)
           org.apache.kafka.common.serialization.Deserializer))




;;;;;;;;;; Private helpers


(defn- -to-deserializer

  "Given a fn, creates a deserializer. Otherwise, returns the arg."

  ^Deserializer

  [arg]

  (if (fn? arg)
    ($.deserialize/make arg)
    arg))




;;;;;;;;;; API


(defn consumer?

  "Is this a consumer ?"

  [x]

  (instance? KafkaConsumer
             x))




(defn topics

  "Gets a list of metadata about partitions for all the topics the consumer is authorized to consume.


   @ consumer
     Kafka consumer.

   => Map of topic names to partition infos.
      Cf. `$.interop.clj/partition-info`


   Throws
  
     org.apache.kafka.common.errors

       WakeupException
       When `unblock` is called before or while this fn is called.

       InterruptException
       When the calling thread is interrupted before of while this fn is called.

       TimeoutException
       When the topic metadata could not be fetched before expiration of the configured request timeout.

       KafkaException
       Any other unrecoverable errors."

  [^KafkaConsumer consumer]

  (reduce (fn reduce-topics [ktopics [ktopic partition-info]]
            (assoc ktopics
                   ktopic
                   (map $.interop.clj/partition-info
                        partition-info)))
          {}
          (.listTopics consumer)))




(defn partitions
  
  "Gets a list of partitions for a given topic.
  

   @ consumer
     Kafka consumer.

   @ topic
     Topic name.

   => List of partitions.
      Cf. `interop.clj/partition-info`


   Throws

     org.apache.kafka.common.errors

       WakeupException
       When `unblock` is called before or while this fn is called.

       InterruptException
       When the calling thread is interrupted.

       AuthorizationException
       When not authorized to the specified topic.

       TimeoutException
       When the topic metadata could not be fetched before expiration of the configured request timeout.

       KafkaException
       Any other unrecoverable errors."

  [^KafkaConsumer consumer topic]

  (map $.interop.clj/partition-info
       (.partitionsFor consumer
                       topic)))




(defn listen

  "Subscribes a consumer to topics or assigns it to specific partitions.

   An assignement precisely refers to a [topic partition] whereas a subscription only ask for a topic,
   the partition being assigned dynamically.

   <!> A consumer can only consume from one type of source. This fn will throw if
       the user try to mix, for instance, subscriptions and assignments, or regexes
       and strings.


   @ consumer
     Kafka consumer.

   @ source
     One of : regular expression representing topics to subscribe to
            | list of topics as strings to subscribe to
            | list of [topic partition] to be assigned to
            | nil

   @ f-rebalance
     Optional fn for subscriptions.
     Cf. `milena.interop.java/consumer-rebalance-listener`

  

   Ex. (listen consumer
               #\"topic-.+\"
               (fn [assigned? topic-partitions]
                 (when assigned?
                   ...)))

       (listen consumer
               [\"my-topic\"
                \"another-topic\"])

       (listen consumer
               [[\"my-topic\"      0]
                [\"another-topic\" 3]])

       (listen consumer
               nil)"

  ^KafkaConsumer

  ([^KafkaConsumer consumer source]

   (if source
     (if (sequential? source)
       (if (sequential? (first source))
         (.assign consumer
                  (map $.interop.java/topic-partition
                       source))
         (.subscribe consumer
                     ^Collection source))
       (.subscribe consumer
                   ^Pattern source
                   ($.interop.java/consumer-rebalance-listener (fn no-op-rebalance [_ _]
                                                                 nil))))
     (.unsubscribe consumer))
   consumer)


  ([^KafkaConsumer consumer source f-rebalance]

   (let [f-rebalance' ($.interop.java/consumer-rebalance-listener f-rebalance)]
     (if (sequential? source)
       (if (sequential? (first source))
         (.assign consumer
                  (map $.interop.java/topic-partition
                       source))
         (.subscribe consumer
                     ^Collection source
                     f-rebalance'))
       (.subscribe consumer
                   ^Pattern source
                   f-rebalance')))
   consumer))




(defn listening

  "Gets all the partitions the consumer is assigned to or its subscriptions.


   @ consumer
     Kafka consumer.

   => {:partitions
        Set of topic-partitions.
        Cf. `milena.interop.clj/topic-partition`

       :subscriptions
        Set of topic names.}


   Cf. `listen`"

  [^KafkaConsumer consumer]

  {:partitions    (into #{}
                        (map $.interop.clj/topic-partition
                             (.assignment consumer)))
   :subscriptions (into #{}
                        (.subscription consumer))})




(defn listening?

  "Is the consumer listening to the given topic / [topic partition] ?"

  [consumer source]

  (contains? (get (listening consumer)
                  (if (string? source)
                    :subscriptions
                    :partitions))
             source))




(defn pause

  "Temporarely pauses consumption.


   @ consumer
     Kafka consumer.

   ---

   @ topic-partitions
     List of [topic partition].
     Cf. `milena.interop.java/topic-partition`

   ---

   @ topic
     Topic name.

   @ partition
     Partition number.

   ---

    => `consumer`
  

    Ex. (pause consumer
               \"my-topic\"
               0)
 
        (pause consumer
               [[\"my-topic\"      0]
                [\"another-topic\" 3]])"

  ^KafkaConsumer

  ([^KafkaConsumer consumer topic-partitions]

   (.pause consumer
           (map $.interop.java/topic-partition
                topic-partitions))
   consumer)


  ([consumer topic partition]

   (pause consumer
          [[topic partition]])))




(defn paused

  "@ consumer
     Kafka consumer.

   => Set of [topic partition] currently paused.
      Cf. `pause`"


  [^KafkaConsumer consumer]

  (into #{}
        (map $.interop.clj/topic-partition
             (.paused consumer))))




(defn paused?

  "Is a [topic partition] currenly paused ?

   Cf. `pause`
       `paused`"

  ([consumer topic-partition]

   (boolean (some (fn equal-partition [x]
                    (= x
                       partition))
                  (paused consumer))))


  ([consumer topic partition]

   (paused? consumer
            [topic partition])))




(defn resume

  "Resumes consumptions.

   Resumes everything if no other arg than the consumer is provided.


   @ consumer
     Kafka consumer.

   ---

   @ topic-partitions
     List of [topic partition].
     Cf. `milena.interop.java/topic-partition`

   ---

   @ topic
     Topic name.

   @ partition
     Partition number.

   ---

   => `consumer`

   
   Ex. (resume consumer)
    
       (resume \"my-topic\"
               0)

       (resume [[\"my-topic\"      0]
                [\"another-topic\" 3]])


   Cf. `pause`"

  ^KafkaConsumer

  ([consumer]

   (resume consumer
           (paused consumer)))


  ([^KafkaConsumer consumer topic-partitions]

   (.resume consumer
            (map $.interop.java/topic-partition
                 topic-partitions))
   consumer)


  ([consumer topic partition]

   (resume consumer
           [[topic partition]])))




(defn find-first

  "Finds the first available offset of the given topic-partition(s).

   <!> Blocks forever if the topic doesn't exist and dynamic creation has been disabled server-side.


   @ consumer
     Kafka consumer.

   ---

   @ topic-partitions
     List of [topic partition].
     Cf. `milena.interop.java/topic-partition`

   => Map of [topic partition] to offset.

   ---

   @ topic
     Topic name.

   @ partition
     Partition number.

   => Offset.


   Ex. (find-first consumer
                   \"my-topic\"
                   0)
       => 421

       (find-first consumer
                   [[\"my-topic\"      0]
                    [\"another-topic\" 3]])
       => {[\"my-topic\"      0] 421
           [\"another-topic\" 3] 842}"


  ([^KafkaConsumer consumer topic-partitions]

   ($.interop.clj/topic-partition-to-offset (.beginningOffsets consumer
                                                               (map $.interop.java/topic-partition
                                                                    topic-partitions))))


  ([^KafkaConsumer consumer topic partition]

   (let [topic-partition ($.interop.java/topic-partition topic
                                                         partition)]
     (get (.beginningOffsets consumer
                             [topic-partition])
          topic-partition))))




(defn find-last

  "Finds the latest offset of the given partition(s), ie. the offset of the last message + 1.

   @ consumer
     Kafka consumer.

   ---

   @ topic-partitions
     List of [topic partition].
     Cf. `milena.interop.java/topic-partition`

   => Map of [topic partition] to offset.

   ---

   @ topic
     Topic name.

   @ partition
     Partition number.

   => Offset.


   Cf. `find-first`"

  ([^KafkaConsumer consumer topic-partitions]

   ($.interop.clj/topic-partition-to-offset (.endOffsets consumer
                                                         (map $.interop.java/topic-partition
                                                              topic-partitions))))


  ([^KafkaConsumer consumer topic partition]

   (let [topic-partition ($.interop.java/topic-partition topic
                                                         partition)]
     (get (.endOffsets consumer
                       [topic-partition])
          topic-partition))))




(defn find-ts

  "Finds offsets for the given partition(s) by timestamp, ie. the earliest offsets whose timestamp
   is greater than or equal to the corresponding ones.

   <!> Blocks forever if the topic doesn't exist and dynamic creation has been disabled server-side.
  

   @ consumer
     Kafka consumer.

   ---

   @ topic-partitions
     List of [topic partition].
     Cf. `milena.interop.java/topic-partition`

   => Map of [topic partition] to results.

      + results
        {:timestamp
          Unix timestamp of the record.

         :offset
          Offset in the partition.}

   ---

   @ topic
     Topic name.

   @ partition
     Partition number.

   => Offset.
  

   Ex. (find-ts consumer
                \"my-topic\"
                0
                1507812268270)
       => 42

       (find-ts consumer
                {[\"my-topic\"      0] 1507812268270
                 [\"another-topic\" 3] 1507812338294})
       => {[\"my-topic\"      0] {:timestamp 1507812268270
                                  :offset    42
           [\"another-topic\" 3] {:timestamp 1507812338294
                                  :offset    84}"

  ([^KafkaConsumer consumer timestamps]
   
   (reduce (fn reduce-offsets [offsets [topic-partition oat]]
             (assoc offsets
                    ($.interop.clj/topic-partition topic-partition)
                    ($.interop.clj/offset-and-timestamp oat)))
           {}
           (.offsetsForTimes consumer
                             (reduce-kv (fn reduce-timestamps [hmap topic-partition ts]
                                          (assoc hmap
                                                 ($.interop.java/topic-partition topic-partition)
                                                 (max ts
                                                      0)))
                                         {}
                                         timestamps))))


  ([^KafkaConsumer consumer topic partition ts]

   (let [topic-partition ($.interop.java/topic-partition topic
                                                         partition)]
     (some-> (get (.offsetsForTimes consumer
                                    {topic-partition ts})
                  topic-partition)
             $.interop.clj/offset-and-timestamp))))




(defn position 

  "Gets the current offset of a consumer on one or several partitions.


   @ consumer
     Kafka consumer.

   ---

   @ topic-partitions
     List of [topic partition].
     Cf. `milena.interop.java/topic-partition`

   => Map of [topic partition] to positions.

   ---

   @ topic
     Topic name.

   @ partition
     Partition number.

   => Position. 


   Ex. (position consumer
                 \"my-topic\"
                 0)
       => 42

       (position consumer
                 [[\"my-topic\"      0]
                  [\"another-topic\" 3]])
       => {[\"my-topic\"      0] 42
           [\"another-topic\" 3] 84}


   Throws

     org.apache.kafka.common.errors

       WakeupException
       When `unblock` is called before or while this fn is called.

       InterruptException
       When the calling thread is interrupted.

       AuthorizationException
       When not authorized to the specified topic.

       TimeoutException
       When the topic metadata could not be fetched before expiration of the configured request timeout.

       KafkaException
       Any other unrecoverable errors."

  ([consumer topic-partitions]

   (reduce (fn reduce-topic-partitions [positions [topic partition :as topic-partition]]
             (assoc positions
                    topic-partition
                    (position consumer
                              topic
                              partition)))
           {}
           topic-partitions))


  ([^KafkaConsumer consumer topic partition]

   (.position consumer
              ($.interop.java/topic-partition topic 
                                              partition))))




(defn seek

   "Seeks one or several partitions to a new offset.

    Happens lazily on the next call to `poll`.

    Note that you may loose data if this fn is arbitrarily called in the middle of consumption.


    @ consumer
      Kafka consumer.

    ---

    @ positions
      Map of [topic partition] to new positions.
      Cf. `milena.interop.java/topic-partition`

    ---

    @ topic
      Topic name.

    @ partition
      Partition number.

    @ position
      New position.

    ---

    => `consumer`


    Ex. (seek consumer
              \"my-topic\"
              0
              42)

        (seek consumer
              {[\"my-topic\"      0] 42
               [\"another-topic\" 3] 84})"

  ^KafkaConsumer

  ([consumer positions]

   (doseq [[[topic
             partition] position] positions]
     (seek consumer
           topic
           partition
           position))
   consumer)


  ([^KafkaConsumer consumer topic partition position]

   (.seek consumer
          ($.interop.java/topic-partition topic
                                          partition)
          (max position
               0))
   consumer))




(defn rewind

  "Rewinds a consumer to the first offset (ie. offset of the first available message) for one or
   several partitions.

   Happens lazily on the next call to `poll` or `position`.

   If no partition is given, applies to all currently assigned partitions.
  

   @ consumer
     Kafka consumer.

   ---

   @ topic-partitions
     List of [topic partition].
     Cf. `milena.interop.java/topic-partition`

   ---

   @ topic
     Topic name.

   @ partition
     Partition number.

   ---

   => `consumer`


   Ex. (rewind consumer
               \"my-topic\"
               0)
  
       (rewind consumer
               [[\"my-topic\"      0]
                [\"another-topic\" 3]])"

  ^KafkaConsumer

  ([consumer]

   (rewind consumer
           []))


  ([^KafkaConsumer consumer topic-partitions]

   (.seekToBeginning consumer
                     (map $.interop.java/topic-partition
                          topic-partitions))
   consumer)


  ([consumer topic partition]

   (rewind consumer
           [[topic partition]])))




(defn fast-forward

  "Fast forwards a consumer to the last offset (ie. offset of the last message + 1) for one or
   several partitions.
   
   Happens lazily on the next call to `poll` or `position`.
  
   If no partition is given, applies to all currently assigned partitions.
  

   @ consumer
     Kafka consumer.

   ---

   @ topic-partitions
     List of [topic partition].
     Cf. `milena.interop.java/topic-partition`

   ---

   @ topic
     Topic name.

   @ partition
     Partition number.

   ---

   => `consumer`


   Ex. (fast-forward consumer
                     \"my-topic\"
                     0)

       (fast-forward consumer
                     [[\"my-topic\"      0]
                      [\"another-topic\" 3]])"

  ^KafkaConsumer

  ([^KafkaConsumer consumer topic-partitions]

   (.seekToEnd consumer
               (map $.interop.java/topic-partition
                    topic-partitions))
   consumer)


  ([consumer]

   (fast-forward consumer
                 []))


  ([consumer topic partition]

   (fast-forward consumer
                 [[topic partition]])))




(defn unblock

  "From another thread, unblocks the consumer.
   
   The blocking thread will throw an org.apache.kafka.common.errors.WakeupException.

   If the thread isn't blocking on a fn which can throw such an exception, the next call
   to such a fn will raise it instead.

   Must be used sparingly, not to compensate for a bad design.
  
   
   @ consumer
     Kafka consumer.
  
   => `consumer`"

  ^KafkaConsumer

  [^KafkaConsumer consumer]

  (.wakeup consumer)
  consumer)




(defmacro safely

  "If the body doesn't compute before the required timeout, `unblock` will be called on the consumer."

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




(defn close

  "Tries to close the consumer cleanly within the given timeout or a default one of 30 seconds.
  
   It will try to complete pending commits and leave the group. If auto-commit is enabled,
   the current offsets will be committed. If those operations aren't completed when the timeout
   is reached, the consumer will be force closed.

   Note that `unblock` cannot be used to interrupt this fn.


   @ consumer
     Kafka consumer.

   @ timeout-ms
     Timeout in milliseconds.

   => nil


   Throws
  
     org.apache.kafka.common.errors
  
       InterruptException
       When the thread is interrupted while blocked."

  ([^KafkaConsumer consumer]

   (.close consumer))


  ([^KafkaConsumer consumer timeout-ms]

   (.close consumer
           (max timeout-ms
                0)
           TimeUnit/MILLISECONDS)))




(defn- -poll-raw

  "Polls records. 

   Blocks until records are received or `timeout-ms` is elapsed (resulting in nil). 
  
   A timeout of 0 returns what is available in the consumer buffer without blocking.

   If no timeout is given, polls for ever.
  
   
   Throws
  
     org.apache.kafka.clients.consumer

       InvalidOffsetException
       When the offset for a partition or set of partitions is undefined or out of range and no offset reset
       policy has been configured.

     org.apache.kafka.common.errors

       WakeupException
       When `unblock` is called while blocking.

       InterruptException
       The calling thread is interrupted while blocking.

       AuthorizationException
       When not authorized to any of the assigned topics or to the configured groupId.

       KafkaException
       Any other unrecoverable errors (eg. deserializing key/value)."

  ^ConsumerRecords

  ([consumer]

   (-poll-raw consumer
              nil))


  ([^KafkaConsumer consumer ?timeout-ms]

   (let [^ConsumerRecords records (try
                                    (.poll consumer
                                           (if ?timeout-ms
                                             (max ?timeout-ms
                                                  0)
                                             Long/MAX_VALUE))
                                    #_(catch WakeupException _
                                      nil)
                                    #_(catch InterruptException _
                                      nil)
                                    (catch IllegalStateException _
                                      ;; when the consumer is not subscribed nor assigned
                                      nil))]
     (when (and records
                (not (.isEmpty records)))
       records))))




(defn poll

  "Synchronously polls records.


   @ consumer
     Kafka consumer.

   @ ?timeout-ms
     Optional timeout in milliseconds.
       Nil will wait forever.
       0   returns what is available in the consumer buffer without blocking.

   => Sequence of individual records.
      Cf. `milena.interop.clj/consumer-record`
  
   
   Throws
  
     org.apache.kafka.clients.consumer

       InvalidOffsetException
         When the offset for a partition or set of partitions is undefined or out of range and no offset
         reset policy has been configured.

     org.apache.kafka.common.errors

       WakeupException
         When `unblock` is called while blocking.

       InterruptException
         The calling thread is interrupted while blocking.

       AuthorizationException
         When not authorized to any of the assigned topics or to the configured groupId.

       KafkaException
         Any other unrecoverable errors (eg. deserializing key/value)."

  ([consumer]

   (poll consumer
         nil))


  ([consumer ?timeout-ms]

   (some->> (-poll-raw consumer
                    ?timeout-ms)
            (map $.interop.clj/consumer-record))))




(defn poll-partitions

  "Synchronously polls records by partitions.

   Behaves exactly like `poll` but returns a map of [topic partition] to sequence of individual records.

   More efficient when the consumer polls several partitions in order to dispatch the results to workers.


   Cf. `poll`"

  ([consumer]

   (poll-partitions consumer
                    nil))


  ([consumer ?timeout-ms]

   (some-> (-poll-raw consumer
                      ?timeout-ms)
           $.interop.clj/consumer-records-by-partitions )))




(defn- -poll-seq

  "Helper for `poll-seq`."

  [consumer ?timeout-ms ?records]

  (lazy-seq
    (when-let [records (or ?records
                           (poll consumer
                                 ?timeout-ms))]
      (cons (first records)
            (-poll-seq consumer
                       ?timeout-ms
                       (next records))))))




(defn poll-seq 

  "Convert a consumer to a sequence of individual records by lazily and continuously calling `poll`.


   Ex. (take 5
             (to-seq consumer))
  

   Cf. `poll` for arguments and exceptions."

  ([consumer]

   (poll-seq consumer
             nil))


  ([consumer ?timeout-ms]

   (-poll-seq consumer
             ?timeout-ms
             nil)))




(defn commit-sync

  "Synchronously commits offsets to Kafka.

   If none are given, commits offsets from the last call to `poll`.

   The committed offsets will be used on the first fetch after every assignment and also on startup.
   As such, if the user need to store offsets anywhere else, this fn should not be used.


   @ consumer
     Kafka consumer.

   @ offsets
     Map of [topic partition] to offsets.

   => `consumer`


   Ex. (commit-sync consumer)

       (commit-sync consumer
                    {[\"my-topic\"      0] 24
                     [\"another-topic\" 3] 84})
  
  
   Throws

     org.apache.kafka.clients.consumer

       CommitFailedException
       When the commit failed and cannot be retried (only occurs when using subscriptions or if there is an active
       groupe with the same groupId).
  
     org.apache.kafka.common.errors

       WakeupException
       When `unblock` is called before or while this fn is called.

       InterruptException
       When the calling thread is interrupted.

       AuthorizationException
       When not authorized to the specified topic.

       KafkaException
       Any other unrecoverable errors."

  ^KafkaConsumer

  ([^KafkaConsumer consumer]

   (.commitSync consumer)
   consumer)


  ([^KafkaConsumer consumer offsets]

   (.commitSync consumer
                ($.interop.java/topic-partition-to-offset offsets))
   consumer))




(defn commit-async

  "Asynchronously commits offsets to Kafka.

   If none are given, commits offsets from the last call to `poll`.

   The committed offsets will be used on the first fetch after every assignment and also on startup.
   As such, if the user need to store offsets anywhere else, this fn should not be used.

   Actually commits on the next trip to the server, such as calling `poll`.

   The callback must accept as arguments a possible exception and a map of [topic partition] -> committed offset.
  

   @ consumer
     Kafka consumer.

   @ callback / ?callback
     Cf.

   => `consumer`


   Ex. (commit-sync consumer)

       (commit-sync consumer
                    (fn [exception offsets]
                       ...))

       (commit-sync consumer
                    (fn [exception offsets]
                      (when-not exception
                        (println \"Should be true :\"
                                 (= (get offsets
                                         [\"my-topic\" 0])
                                    24))))
                    {[\"my-topic\"      0] 24
                     [\"another-topic\" 3] 84})
  

   Throws

     Cf. `commit-sync` for exceptions"

  ^KafkaConsumer

  ([^KafkaConsumer consumer]

   (.commitAsync consumer)
   consumer)


  ([^KafkaConsumer consumer callback]

   (.commitAsync consumer
                 ($.interop.java/offset-commit-callback callback))
   consumer)


  ([^KafkaConsumer consumer ?callback offsets]

   (.commitAsync consumer
                 ($.interop.java/topic-partition-to-offset offsets)
                 (some-> ?callback
                         $.interop.java/offset-commit-callback))
   consumer))




(defn committed

  "Gets the last committed offset for one or several partitions.

   May block if the partition is not assigned to this consumer or if the consumer hasn't yet initialized its cache of committed
   offsets.


   @ consumer
     Kafka consumer.

   ---

   @ topic-partitions
     List of [topic partition].
     Cf. `milena.interop.java/topic-partition`

   => Map of [topic partition] to offsets.

   ---

   @ topic
     Topic name.

   @ partition
     Partition number.

   => Offset.


   Ex. (committed consumer
                  \"my-topic\"
                  0)
       => 42

       (committed consumer
                  [[\"my-topic\"      0]
                   [\"another-topic\" 3]])
       => {[\"my-topic\"      0] 42
           [\"another-topic\" 3] 84}"
  

  ([consumer topic-partitions]

   (reduce (fn reduce-topic-partitions [commits [topic partition :as topic-partition]]
             (assoc commits
                    topic-partition
                    (committed consumer
                               topic
                               partition)))
           {}
           topic-partitions))


  ([^KafkaConsumer consumer topic partition]

   (when-let [^OffsetAndMetadata om (.committed consumer
                                                ($.interop.java/topic-partition topic
                                                                                partition))]
       (.offset om))))




(defn metrics

  "Gets metrics about this consumer.

   @ consumer
     Kafka consumer.
  
   => Cf. `milena.interop.clj/metrics`"

  [^KafkaConsumer consumer]

  ($.interop.clj/metrics (.metrics consumer)))




(defn make

  "Builds a Kafka consumer.

   <!> Consumers are NOT thread safe !
       1 consumer / thread or a queueing policy must be implemented.


   @ ?opts
     {:?nodes
       List of [host port].

      :?config
       Kafka configuration map.
       Cf. https://kafka.apache.org/documentation/#newconsumerconfigs

      :?deserializer
       Kafka deserializer or fn eligable for becoming one.
       Cf. `milena.deserialize`
           `milena.deserialize/make`

      :?deserializer-key
       Defaulting to `?deserializer`.

      :?deserializer-value
       Defaulting to `?deserializer`.

      :?listen
       Subscribes or assigns this new consumer.
       Cf. `listen`}

   => org.apache.kafka.clients.consumer.KafkaConsumer


   Ex. (make {:?nodes              [[\"some_host\" 9092]]
              :?config             {:group.id           \"my_group\"
                                    :enable.auto.commit false}
              :?deserializer-key   milena.deserialize/string
              :?deserializer-value (fn [_ data]
                                     (nippy/thaw data))
              :?listen             [[\"my-topic\" 3]]})"

  ^KafkaConsumer


  ([]

   (make nil))


  ([{:as     ?opts
     :keys   [?nodes
              ?config
              ?deserializer
              ?deserializer-key
              ?deserializer-value
              ?listen]
     :or     {?nodes              [["localhost" 9092]]
              ?deserializer       $.deserialize/byte-array
              ?deserializer-key   ?deserializer
              ?deserializer-value ?deserializer}}]
   
   (let [consumer (KafkaConsumer. ($.interop/config ?config
                                                    ?nodes)
                                  (-to-deserializer ?deserializer-key)
                                  (-to-deserializer ?deserializer-value))]
     (when ?listen
       (try
         (listen consumer
                 ?listen)
         (catch Throwable e
           (close consumer)
           (throw e))))
     consumer)))
