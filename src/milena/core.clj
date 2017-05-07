(ns milena.core

  "Fns for handling everything that is useful in the kafka client library.
   This work doesn't try to be too clever or opiniated. Rather, it allows
   one to think in term of clojure data structures instead of java interop.
   Better semantics + data-driven.

   <!> Sometimes, fns try and catch certain exceptions when it is meaningful.
       This behavior is subject to changes for the time being."

  {:author "Adam Helinski"}

  (:require [clojure.string    :as string]
            [milena.converters :as convert])
  (:import java.util.concurrent.TimeUnit
           (org.apache.kafka.common Metric
                                    MetricName
                                    PartitionInfo)
           (org.apache.kafka.common.serialization Serializer
                                                  Deserializer
                                                  ByteArraySerializer
                                                  ByteArrayDeserializer
                                                  ByteBufferSerializer
                                                  ByteBufferDeserializer
                                                  DoubleSerializer
                                                  DoubleDeserializer
                                                  IntegerSerializer
                                                  IntegerDeserializer
                                                  LongSerializer
                                                  LongDeserializer
                                                  StringSerializer
                                                  StringDeserializer)
           (org.apache.kafka.common.errors WakeupException
                                           InterruptException)
           org.apache.kafka.clients.producer.KafkaProducer
           (org.apache.kafka.clients.consumer KafkaConsumer
                                              ConsumerRecords
                                              OffsetAndMetadata)))



;;; Misc


(defn now

  "Get the unix time in milliseconds"

  []

  (System/currentTimeMillis))




(defn nodes-string

  "Produce a string of Kafka nodes for producers and consumers.

   nodes : a list of [host port] representing Kafka nodes
         | an already prepared connection string"

  [nodes]

  (if (string? nodes)
      nodes
      (string/join ","
                   (map (fn [[host port]] (str host ":" port))
                        nodes))))




(defn- -stringify-keys

  "Simply stringify symbol or keyword keys in a map"

  [hmap]

  (reduce-kv (fn [hmap' k v]
               (assoc hmap'
                      (name k)
                      v))
             {}
             hmap))




;;; Serialization


(defn make-serializer
  
  "Given a fn that takes a topic and data to serialize,
   make a Kafka serializer for producers"

  [f]

  (reify Serializer
    
    (serialize [_ ktopic data] (f ktopic
                                  data))

    (close [_] nil)

    (configure [_ _ _] nil)))




(defn make-deserializer

  "Given a fn that takes a topic and data to deserialize,
   make a Kafka deserializer for consumers"

  [f]

  (reify Deserializer
    
    (deserialize [_ ktopic data] (f ktopic
                                    data))

    (close [_] nil)

    (configure [_ _ _] nil)))




(def serializers

  "Basic serializers provided by Kafka"

  {:byte-array  (ByteArraySerializer.)
   :byte-buffer (ByteBufferSerializer.)
   :double      (DoubleSerializer.)
   :int         (IntegerSerializer.)
   :long        (LongSerializer.)
   :string      (StringSerializer.)
   })




(def deserializers

  "Basic deserializers provided by Kafka"

  {:byte-array  (ByteArrayDeserializer.)
   :byte-buffer (ByteBufferDeserializer.)
   :double      (DoubleDeserializer.)
   :int         (IntegerDeserializer.)
   :long        (LongDeserializer.)
   :string      (StringDeserializer.)
   })




(defn serialize

  "Serialize data using a Kafka serializer"

  [^Serializer serializer ktopic data]

  (.serialize serializer
              ktopic
              data))




(defn deserialize

  "Deserialize data using a Kafka deserializer"

  [^Deserializer deserializer ktopic data]

  (.deserialize deserializer
                ktopic
                data))




;;; Producers & Consumers


(defn producer

  "Build a Kafka producer.

   config : read the signature
            (cf. milena.core/nodes-string)
   kopts : optional producer options (cf. Kafka documentation)

   Producer are thread-safe and it is more efficient to share
   one amongst multiple threads."

  [& [{:as   config
       :keys [nodes
              serializer
              serializer-key
              serializer-value]
       :or   {nodes            [["localhost" 9092]]
              serializer       (serializers :byte-array)
              serializer-key   serializer
              serializer-value serializer}}
      kopts]]

  (KafkaProducer. (assoc (-stringify-keys kopts)
                         "bootstrap.servers"
                         (nodes-string nodes))
                  serializer-key
                  serializer-value))




(declare listen)

(declare close)


(defn consumer

  "Build a Kafka consumer.

   config :
     nodes : cf. milena.core/nodes-string
     deserializer : a common deserializer for the key and the value
                    defaulting to (serializers :byte-array)
     deserializer-key : a deserializer only for the key defaulting
                        to 'deserializer'
     deserializer-value : a deserializer only for the value defaulting
                          to 'deserializer'
     listen : cf. milena.core/listen
   kopts : optional consumer options (cf. Kafka documentation)

   <!> Consumers are NOT thread safe !
       1 consumer / thread or a queueing policy must be
       implemented."

  [& [{:keys   [nodes
                deserializer
                deserializer-key
                deserializer-value
                listening]
       listen' :listen
       :or     {nodes              [["localhost" 9092]]
                deserializer       (deserializers :byte-array)
                deserializer-key   deserializer
                deserializer-value deserializer}}
      kopts]]
  
  (let [consumer (KafkaConsumer. (assoc (-stringify-keys kopts)
                                        "bootstrap.servers"
                                        (nodes-string nodes))
                                 deserializer-key
                                 deserializer-value)]
    (when listen'
      (try (listen consumer
                   listen')
           (catch Throwable e
             (close consumer)
             (throw e))))
    consumer))




(defn partitions
  
  "Using a producer or a consumer, get a list of partitions
   for a given topic.

   <!> Producer will block for ever is the topic doesn't exist
       and dynamic creation has been disabled. Consumer will
       return an empty list."

  [producer|consumer ktopic]

  (try (map convert/PartitionInfo->hmap
            (.partitionsFor producer|consumer
                            ktopic))
       (catch Throwable _
         nil)))




(defn metrics

  "Get the metrics of a producer or a consumer as a map"

  [producer|consumer]

  (reduce (fn [metrics' [^MetricName m-name ^Metric metric]]
            (assoc metrics'
                   (keyword (.group m-name)
                            (.name  m-name))
                   {:description (.description m-name)
                    :tags        (reduce (fn [tags [k v]] (assoc tags (keyword k) v))
                                         {}
                                         (.tags m-name))
                    :value       (.value metric)}))
          {}
          (.metrics producer|consumer)))




(defn close

  "Close a producer or a consumer.

   A producer will wait until all sends are done or the optional
   timeout is elapsed.

   A consumer will wait at most  until the optional timeout or
   a default one of 30 seconds giving a chance for a cleanup such
   as commiting offsets."

  ([producer|consumer]
   
   (try (.close producer|consumer)
        true
        (catch IllegalStateException _
          true)
        (catch Throwable _
          false)))


  ([producer|consumer timeout-ms]

   (try (.close producer|consumer
                (long (* 1000 (max 0 timeout-ms)))
                TimeUnit/MICROSECONDS)
        true
        (catch Throwable _
          false))))




;;; Produce


(defn psend

  "Send a message to Kafka via a producer.

   message : a map (cf. milena.converters/hmap->ProducerRecord)
   f-cb : an optional callback for when the send completes
          (cf. milena.converters/f->Callback)

   Returns a future resolving to metadata about the record, the same map
   given to 'f-cb' (cf. milena.converters/RecordMetadata->hmap"

  [^KafkaProducer producer message & [f-cb]]

  (when message
    (.send producer
           (convert/hmap->ProducerRecord message)
           (some-> f-cb
                   convert/f->Callback))))




(defn pderef

  "Deref a future returned by milena.core/ksend by converting
   the metadata about the sent record to a map."

  ([kfuture]

   (convert/RecordMetadata->hmap (deref kfuture)))


  ([kfuture timeout-ms timeout-val]

   (convert/RecordMetadata->hmap (deref kfuture
                                        timeout-ms
                                        timeout-val))))




(defn pflush

  "Flush a producer. Will block until all messages are effectively sent.

   Returns true or false whether the flush succeeded or not."

  [^KafkaProducer producer]

  (try (.flush producer)
       true
       (catch Throwable _
         false)))




;;; Consume


(defn cinterrupt

  "From another thread, unblock the consumer. The blocking thread
   will throw an org.apache.kafka.common.errors.WakeupException.

   If the thread isn't blocking on a fn which can throw such an
   exception, the next call to such a fn will raise it instead.

   Must be used sparingly, not to compensate for a bad design."

  [^KafkaConsumer consumer]

  (.wakeup consumer)
  consumer)




(defmacro safe-consume

  "If the body doesn't compute before the required timeout,
   milena.core/interrupt-consumer will be called on the consumer."

  [consumer timeout-ms & body]

  `(locking consumer
     (let [consumer# ~consumer
           timeout#  ~timeout-ms
           p#        (promise)
           ft#       (future (Thread/sleep timeout#)
                             (when-not (realized? p#)
                               (interrupt-consumer consumer#)))
           ret#      (try ~@body
                          (catch Throwable e#
                            (future-cancel ft#)
                            (throw e#)))]
       (deliver p#
                ret#)
       (future-cancel ft#)
       ret#)))




(defn ctopics

  "Get metadata about partitions for all topics the consumer
   is authorized to consume.

   cf. milena.converters/PartitionInfo->hmap"

  [^KafkaConsumer consumer]

  (try (reduce (fn [ktopics [ktopic p-i]]
                 (assoc ktopics
                        ktopic
                        (map convert/PartitionInfo->hmap
                             p-i)))
               {}
               (.listTopics consumer))
       (catch Throwable _
         nil)))




(defn- -subscribe

  "Subscribe a consumer.
  
   Helper for milena.core/listen."

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

  "Subscribe a consumer to topics or assign it to partition.

   An assignement precisely refers to a [topic partition] whereas
   a subscription only ask for a topic, the partition being assigned
   dynamically.

   source : a regular expression
          | a list of topics as strings to subscribe to
          | a list of [topic partition] to be assigned to
          | nil for explicitly stopping listening
   f-rebalance : a fn being called when the consumer has been
                 assigned or revoked (optional unless 'source' is a regex)
                 (cf. milena.converters/f->ConsumerRebalanceListener)

   <!> A consumer can only consume from one type of source, it will throws if
       you try to mix, for instance, subscriptions and assignments"

  [^KafkaConsumer consumer source & [f-rebalance]]

  (if source
      (if (sequential? source)
          (if (sequential? (first source))
              (.assign consumer
                       (map convert/to-TopicPartition
                            source))
              (-subscribe consumer
                          source
                          f-rebalance))
          (do (assert f-rebalance)
              (-subscribe consumer
                          source
                          f-rebalance)))
      (.unsubscribe consumer))
  consumer)
 



(defn listening

  "Get all the partitions the consumer is assigned to as well as the
   subscriptions.

   cf. milena.core/listen"

  [^KafkaConsumer consumer]

  {:partitions    (into #{}
                        (map convert/TopicPartition->vec
                             (.assignment consumer)))
   :subscriptions (into #{}
                        (.subscription consumer))})




(defn listening?

  "Is the consumer listening to the given topic / [topic partition] ?"

  [^KafkaConsumer consumer source]

  (contains? (get (listening consumer)
                  (if (string? source)
                      :subscriptions
                      :partitions))
             source))




(defn offsets-ts

  "Search for offsets by timestamp

   kpartitions+ts : a map of [topic partition] -> timestamp in unix time

   Returns a map of [topic partition] -> offset where offset is refering
   to the first message published at or after the corresponding timestamp.

   ----------

   ktopic : topic
   kpart : partition number
   ts : timestamp in unix time
   
   Returns the offset

   <!> Will block forever if the topic doesn't exist and dynamic creation
       has been disabled server-side."

  ([^KafkaConsumer consumer kpartitions+ts]
   
   (->> (.offsetsForTimes consumer
                          (reduce-kv (fn [t-p+ts kpartition ts]
                                       (assoc t-p+ts
                                              (convert/to-TopicPartition kpartition)
                                              (max 0 ts)))
                                     {}
                                     kpartitions+ts))
        (reduce (fn [offsets [t-p o+ts]]
                  (assoc offsets
                         (convert/TopicPartition->vec      t-p)
                         (convert/OffsetAndTimestamp->hmap o+ts)))
                {})))


  ([^KafkaConsumer consumer ktopic kpart ts]

   (let [tp (convert/to-TopicPartition ktopic
                                       kpart)]
     (some-> (get (.offsetsForTimes consumer {tp ts})
                  tp)
             convert/OffsetAndTimestamp->hmap))))




(defn- -reduce-offsets

  "Remap offsets as [topic partition] -> offset"

  [t-p+o's]

  (reduce (fn [offsets [t-p offset]]
            (assoc offsets
                   (convert/TopicPartition->vec t-p)
                   offset))
          {}
          t-p+o's))



(defn offsets-first

  "Get the earliest offsets exiting for the required partitions

   kpartitions : a list of [topic partition]

   Returns a map of [topic partition] -> offset

   ----------

   ktopic : topic
   kpart : partition

   Returns the offset

   <!> Will block forever if the topic doesn't exist and dynamic
       creation has been disabled server-side."

  ([^KafkaConsumer consumer kpartitions]

   (-reduce-offsets (.beginningOffsets consumer
                                       (map convert/to-TopicPartition
                                            kpartitions))))


  ([^KafkaConsumer consumer ktopic kpart]

   (let [t-p (convert/to-TopicPartition ktopic
                                        kpart)]
     (get (.beginningOffsets consumer
                             [t-p])
          t-p))))




(defn offsets-latest

  "Get the latest offsets existing for the required partitions

   cf. milena.core/offsets-earliest"

  ([^KafkaConsumer consumer kpartitions]

   (-reduce-offsets (.endOffsets consumer
                                 (map convert/to-TopicPartition
                                      kpartitions))))


  ([^KafkaConsumer consumer ktopic kpart]

   (let [t-p (convert/to-TopicPartition ktopic
                                        kpart)]
     (get (.endOffsets consumer
                       [t-p])
          t-p))))




(defn seek

  "Seek a partition or a list of [topic partition] to a new position.

   ktopic : topic
   kpart : partition number
   position : new position

   -----------

   kpartitions : a list of [topic partition]
   position : new position"

  ([^KafkaConsumer consumer ktopic kpart position]

   (.seek consumer
          (convert/to-TopicPartition ktopic
                                     kpart)
          (max 0 position))
   consumer)


  ([consumer kpartitions position]

   (doseq [[ktopic kpart] kpartitions] (seek consumer
                                             position
                                             ktopic
                                             kpart))
   consumer))




(defn rewind

  "Rewind a consumer for the required partitions.

   kpartitions : a list of [topic partition]
   
   ----------

   ktopic : topic
   kpart : partition"

  ([^KafkaConsumer consumer kpartitions]

   (.seekToBeginning consumer
                     (map convert/to-TopicPartition
                          kpartitions))
   consumer)


  ([^KafkaConsumer consumer ktopic kpart]

   (.seekToBeginning consumer
                     [(convert/to-TopicPartition ktopic
                                                 kpart)])
   consumer))




(defn forward

  "Fast forward a consumer for the required partitions.

   cf. milena.core/rewind"

  ([^KafkaConsumer consumer kpartitions]

   (.seekToEnd consumer
               (map convert/to-TopicPartition
                    kpartitions))
   consumer)


  ([^KafkaConsumer consumer ktopic kpart]

   (.seekToEnd consumer
               [(convert/to-TopicPartition ktopic
                                           kpart)])
   consumer))




(defn pause

  "Temporarely pause consumption for a partition or a list of [topic partition]"

  ([^KafkaConsumer consumer kpartitions]

   (.pause consumer
           (map convert/to-TopicPartition
                kpartitions))
   consumer)


  ([^KafkaConsumer consumer ktopic kpart]

   (.pause consumer
           [(convert/to-TopicPartition ktopic
                                       kpart)])
   consumer))




(defn paused

  "Get a list of [topic partition] currently being paused.

   cf. milena.core/pause"

  [^KafkaConsumer consumer]

  (into #{}
        (map convert/TopicPartition->vec
             (.paused consumer))))




(defn paused?

  "Is a [topic partition] currenly paused ?

   cf. milena.core/pause"

  ([consumer kpartition]

   (boolean (some #(= %
                      kpartition)
                  (paused consumer))))


  ([consumer ktopic kpart]

   (paused? consumer
            [ktopic kpart])))




(defn resume

  "Resume a partition or a list of [topic partition].

   cf. milena.core/pause"

  ([^KafkaConsumer consumer kpartitions]

   (.resume consumer
            (map convert/to-TopicPartition
                 kpartitions))
   consumer)


  ([^KafkaConsumer consumer ktopic kpart]

   (.resume consumer
            [(convert/to-TopicPartition ktopic
                                        kpart)])
   consumer))




(defn poll

  "Poll messages. If there aren't any, wait until there are some or
  until 'timeout-ms' is elapsed.
  
  or wait up to 'timeout' if there aren't any.

   A timeout of 0 returns what is available in the consumer buffer
   without blocking."

  [^KafkaConsumer consumer & [timeout-ms]]

  (try (let [^ConsumerRecords records (.poll consumer
                                             (if timeout-ms
                                                 (max 0
                                                      timeout-ms)
                                                 Long/MAX_VALUE))]
         (when-not (.isEmpty records)
             (map convert/ConsumerRecord->hmap
                  (iterator-seq (.iterator records)))))
       (catch WakeupException       _
         nil)
       (catch InterruptException    _
         nil)
       (catch IllegalStateException _
         ;; happens when timeout < 0
         nil)))




(defn poll-reduce

  "Poll and reduce messages.

   f : a classic 2-args reducing fn 
   seed : the initial value for the reducing fn

   Will stop when there are no more messages or 'f' returns
   a (reduced) value, just like in (reduce).

   cf. milena.core/poll"

  [consumer timeout-ms f acc]

  (loop [acc acc]
    (if-let [msgs (poll consumer
                        timeout-ms)]
      (let [acc' (reduce f
                         acc
                         msgs)]
        (if (reduced? acc')
            acc'
            (recur acc')))
      acc)))




(defn position

  "Returns the current position of the consumer, or nil
   if something is wrong/unavailable.

   ktopic : topic
   kpart : partition number

   Returns the current position

   ----------

   kpartitions : a list of [topic partition]

   Returns a map of [topic partition] -> current position"

  ([^KafkaConsumer consumer ktopic kpart]

   (try (.position consumer
                   (convert/to-TopicPartition ktopic
                                              kpart))
        (catch Throwable _
          nil)))


  ([consumer kpartitions]

   (reduce (fn [positions [ktopic kpart :as kpartition]]
             (assoc positions
                    kpartition
                    (position consumer
                              ktopic
                              kpart)))
           {}
           kpartitions)))




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

  ([^KafkaConsumer consumer ktopic kpart]

   (try (.offset ^OffsetAndMetadata (.committed consumer
                                                (convert/to-TopicPartition ktopic
                                                                           kpart)))
        (catch Throwable _
          nil)))


  ([consumer kpartitions]

   (reduce (fn [commits [ktopic kpart :as kpartition]]
             (assoc commits
                    kpartition
                    (committed consumer
                               ktopic
                               kpart)))
           {}
           kpartitions)))




(defn commit-sync

  "Blocking commit of consumer offsets.

   offsets : optional map of [topic partition] -> offset

   If 'offsets' is not provided, it will commit offsets
   returned on the last call to milena.core/poll on this
   consumer."

  ([^KafkaConsumer consumer]

   (try (.commitSync consumer)
        true
        (catch Throwable _
          false)))


  ([^KafkaConsumer consumer offsets]

   (try (.commitSync consumer
                     (reduce-kv (fn [offsets' kpartition offset]
                                  (assoc offsets'
                                         (convert/to-TopicPartition kpartition)
                                         (OffsetAndMetadata. offset)))
                                {}
                                offsets))
        true
        (catch Throwable _
          false))))




(defn commit-async

  "Non-blocking commit of consumer offsets.
 
   cf. milena.core/commit-sync
       milena.converters/f->OffsetCommitCallback"

  ([^KafkaConsumer consumer]

   (.commitAsync consumer)
   consumer)


  ([^KafkaConsumer consumer callback]

   (.commitAsync consumer
                 (convert/f->OffsetCommitCallback callback))
   consumer)


  ([^KafkaConsumer consumer offsets callback]

   (.commitAsync consumer
                 (reduce (fn [offsets' [kpartition offset]]
                           (assoc offsets'
                                  (convert/to-TopicPartition kpartition)
                                  (OffsetAndMetadata. offset))
                         {}
                         offsets)
                 (convert/f->OffsetCommitCallback callback))
   consumer)))
