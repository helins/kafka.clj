(ns dvlopt.kafka.-interop.java

  "Convert clojure data structures to java objects."

  {:author "Adam Helinski"}

  (:require [dvlopt.kafka              :as K]
            [dvlopt.kafka.-interop     :as K.-interop]
            [dvlopt.kafka.-interop.clj :as K.-interop.clj]
            [dvlopt.void               :as void])
  (:import java.time.Duration
           (java.util Map
                      Properties)
           java.util.concurrent.TimeUnit
           (org.apache.kafka.clients.admin AlterConfigsOptions
                                           Config
                                           ConfigEntry
                                           CreateAclsOptions
                                           CreatePartitionsOptions
                                           CreateTopicsOptions
                                           DeleteAclsOptions
                                           DeleteRecordsOptions
                                           DeleteTopicsOptions
                                           DescribeAclsOptions
                                           DescribeClusterOptions
                                           DescribeConfigsOptions
                                           DescribeConsumerGroupsOptions
                                           DescribeTopicsOptions
                                           ListConsumerGroupOffsetsOptions
                                           ListConsumerGroupsOptions
                                           ListTopicsOptions
                                           NewPartitions
                                           NewTopic
                                           RecordsToDelete)
           (org.apache.kafka.clients.consumer ConsumerRebalanceListener
                                              OffsetAndMetadata
                                              OffsetCommitCallback)
           (org.apache.kafka.clients.producer Callback
                                              ProducerRecord)
           org.apache.kafka.common.TopicPartition
           (org.apache.kafka.common.acl AclBinding
                                        AclBindingFilter
                                        AccessControlEntry
                                        AccessControlEntryFilter
                                        AclOperation
                                        AclPermissionType)
           (org.apache.kafka.common.config ConfigResource
                                           ConfigResource$Type)
           org.apache.kafka.common.header.Header
           (org.apache.kafka.common.resource PatternType
                                             ResourcePattern
                                             ResourcePatternFilter
                                             ResourceType)
           (org.apache.kafka.common.serialization ByteArrayDeserializer
                                                  ByteArraySerializer
                                                  ByteBufferDeserializer
                                                  ByteBufferSerializer
                                                  DoubleDeserializer
                                                  DoubleSerializer
                                                  ExtendedDeserializer
                                                  ExtendedSerializer
                                                  IntegerDeserializer
                                                  IntegerSerializer
                                                  LongDeserializer
                                                  LongSerializer
                                                  Serde
                                                  Serdes
                                                  StringDeserializer
                                                  StringSerializer)
           (org.apache.kafka.streams KafkaStreams$StateListener
                                     KeyValue
                                     StreamsConfig
                                     Topology$AutoOffsetReset)
           (org.apache.kafka.streams.kstream Aggregator
                                             Consumed
                                             ForeachAction
                                             Initializer
                                             Joined
                                             JoinWindows
                                             KeyValueMapper
                                             Materialized
                                             Merger
                                             Predicate
                                             Produced
                                             Reducer
                                             Serialized
                                             SessionWindows
                                             TimeWindows
                                             Transformer
                                             TransformerSupplier
                                             ValueJoiner
                                             ValueMapper
                                             ValueMapperWithKey
                                             ValueTransformerWithKey
                                             ValueTransformerWithKeySupplier
                                             Window
                                             Windowed)
           (org.apache.kafka.streams.processor Processor
                                               ProcessorContext
                                               ProcessorSupplier
                                               PunctuationType
                                               Punctuator
                                               StreamPartitioner
                                               TimestampExtractor
                                               TopicNameExtractor)
           (org.apache.kafka.streams.state KeyValueBytesStoreSupplier
                                           SessionBytesStoreSupplier
                                           WindowBytesStoreSupplier
                                           StoreBuilder
                                           Stores)))




;; For sorting vars in alpĥabetic order


(declare access-control-entry
         access-control-entry-filter
         acl-operation
         acl-permission-type
         config-entry
         header
         key-value-bytes-store-supplier--in-memory
         key-value-bytes-store-supplier--regular
         resource-pattern
         resource-pattern-filter
         resource-type
         stream-partitioner
         timestamp-extractor
         topic-partition
         topology$auto-offset-reset
         session-bytes-store-supplier
         window-bytes-store-supplier)




;;;;;;;;;; Private


(def -*store-number

  ;; Keeping track of the number of stores for generating store names.

  (atom -1))




(defn- -record-from-ctx

  ;; Used when a record needs to be presented from a Kafka Streams context.
  ;;
  ;; Contains what is needed besides the key and the value.

  [^ProcessorContext ctx]

  (void/assoc-some {::K/offset    (.offset    ctx)
                    ::K/partition (.partition ctx)
                    ::K/timestamp (.timestamp ctx)
                    ::K/topic     (.topic     ctx)}
                   ::K/headers (K.-interop.clj/headers (.headers ctx))))




(defn- -required-arg

  ;; Throw if the value is nil.
  ;; For missing mandatory arguments.

  [kw arg]

  (when (nil? arg)
    (throw (IllegalArgumentException. (str kw " property must be provided."))))
  arg)




(defn- -store-name

  ;; Generates a name for a state store.

  []

  (format "dvlopt.kafka-store-%08d"
          (swap! -*store-number
                 inc)))




;;;;;;;;;; Java standard library


(defn duration

  ^Duration

  [[^long interval unit]]

  (condp identical?
         unit
    :nanoseconds  (Duration/ofNanos interval)
    :microseconds (Duration/ofNanos (* 1000
                                       interval))
    :milliseconds (Duration/ofMillis interval)
    :seconds      (Duration/ofSeconds interval)
    :minutes      (Duration/ofMinutes interval)
    :hours        (Duration/ofHours interval)
    :days         (Duration/ofDays interval)))




(defn thread$uncaught-exception-handler

  ;; Used by `dvlopt.kstreams/app`.

  ^Thread$UncaughtExceptionHandler

  [f]

  (reify

    Thread$UncaughtExceptionHandler

      (uncaughtException [_ thread exception]
        (f exception
           thread))))




(defn time-unit

  ^TimeUnit

  [unit]

  (condp identical?
         unit
    :nanoseconds  TimeUnit/NANOSECONDS
    :microseconds TimeUnit/MICROSECONDS
    :milliseconds TimeUnit/MILLISECONDS
    :seconds      TimeUnit/SECONDS
    :minutes      TimeUnit/MINUTES
    :hours        TimeUnit/HOURS
    :days         TimeUnit/DAYS))




(defn to-milliseconds

  ;; For when the java library requires milliseconds.

  ^long

  [[interval unit]]

  (.convert TimeUnit/MILLISECONDS
            interval
            (time-unit unit)))




;;;;;;;;;; org.apache.kafka.clients.admin.*


(defn admin-timeout

  ;; Helper for all admin functions accepting a timeout.

  ^Integer

  [options]

  (when-let [timeout (::K/timeout options)]
    (Integer. (to-milliseconds timeout))))




(defn alter-configs-options

  ;; Options for `dvlopt.kafka.admin/alter-configuration`.
  ;;
  ;; .shouldValidate looks a bit useless.

  ^AlterConfigsOptions
   
  [options]

  (let [aco (AlterConfigsOptions.)]
    (some->> (admin-timeout options)
             (.timeoutMs aco))
    aco))



(defn config

  ;; Cf. `dvlopt.kafka.admin/alter-configuration`

  ^Config

  [entries]

  (Config. (map config-entry
                entries)))




(defn config-entry

  ;; Cf. `config`

  (^ConfigEntry
    
   [[kw value]]

   (config-entry kw
                 value))


  (^ConfigEntry
    
   [kw value]

   (ConfigEntry. (name kw)
                 (str value))))




(defn create-acls-options

  ;; Options for `dvlopt.kafka.admin/create-acls`.

  ^CreateAclsOptions

  [options]

  (let [cao (CreateAclsOptions.)]
    (some->> (admin-timeout options)
             (.timeoutMs cao))
    cao))



(defn create-partitions-options

  ;; "Options for `milena.admin/create-partitions`.
  ;;
  ;; .shouldValidate looks a bit useless.

  ^CreatePartitionsOptions

  [options]

  (let [cto (CreatePartitionsOptions.)]
    (some->> (admin-timeout options)
             (.timeoutMs cto))
    cto))




(defn create-topics-options

  ;; Options for `milena.admin/topics-create`.
  ;;
  ;; .shouldValidate looks a bit useless.

  ^CreateTopicsOptions

  [options]

  (let [cto (CreateTopicsOptions.)]
    (some->> (admin-timeout options)
             (.timeoutMs cto))
    cto))




(defn delete-acls-options

  ;; Options for `dvlopt.kafka.admin/delete-acls`.

  ^DeleteAclsOptions

  [options]

  (let [dao (DeleteAclsOptions.)]
    (some->> (admin-timeout options)
             (.timeoutMs dao))
    dao))




(defn delete-records-options

  ;; Options for `dvlopt.kafka.admin/delete-records`.

  ^DeleteRecordsOptions

  [options]

  (let [dro (DeleteRecordsOptions.)]
    (some->> (admin-timeout options)
             (.timeoutMs dro))
    dro))




(defn delete-topics-options

  ;; Options for `milena.admin/delete-topics`.
  
  ^DeleteTopicsOptions

  [options]

  (let [dto (DeleteTopicsOptions.)]
    (some->> (admin-timeout options)
             (.timeoutMs dto))
    dto))




(defn describe-acls-options

  ;; Options for `dvlopt.kafka.admin/acls`.

  ^DescribeAclsOptions

  [options]

  (let [dao (DescribeAclsOptions.)]
    (some->> (admin-timeout options)
             (.timeoutMs dao))
    dao))



(defn describe-cluster-options

  ;; Options for `dvlopt.kafka.admin/cluster`.

  ^DescribeClusterOptions

  [options]

  (let [dco (DescribeClusterOptions.)]
    (some->> (admin-timeout options)
             (.timeoutMs dco))
    dco))




(defn describe-configs-options

  ;; Options for `dvlopt.kafka.admin/configuration`.

  ^DescribeConfigsOptions

  [options]

  (let [dco (DescribeConfigsOptions.)]
    (some->> (admin-timeout options)
             (.timeoutMs dco))
    dco))




(defn describe-consumer-groups-options

  ;; Options for `dvlopt.kafka.admin/describe-consumer-groups`.

  [options]

  (let [dcgo (DescribeConsumerGroupsOptions.)]
    (some->> (admin-timeout options)
             (.timeoutMs dcgo))
    dcgo))




(defn describe-topics-options

  ;; Options for `dvlopt.kafka.admin/describe-topics`.

  ^DescribeTopicsOptions

  [options]

  (let [dto (DescribeTopicsOptions.)]
    (some->> (admin-timeout options)
             (.timeoutMs dto))
    dto))




(defn list-consumer-group-offsets-options

  ;; Options for `dvlopt.kafka.admin/consumer-group-offsets`.

  ^ListConsumerGroupOffsetsOptions

  [options]

  (let [lcgo (ListConsumerGroupOffsetsOptions.)]
    (some->> (admin-timeout lcgo)
             (.timeoutMs lcgo))
    (when-let [topic-partitions (::K/topic-partitions options)]
      (.topicPartitions lcgo
                        (map topic-partition
                             topic-partitions)))
    lcgo))




(defn list-consumer-groups-options

  ;; Options for `dvlopt.kafka.admin/consumer-groups`.

  ^ListConsumerGroupsOptions

  [{:as   options
    :keys [timeout]}]

  (doto (ListConsumerGroupsOptions.)
    (.timeoutMs (Integer. (to-milliseconds timeout)))))




(defn list-topics-options

  ;; Options for `dvlopt.kafka.admin/topics`.

  ^ListTopicsOptions

  [options]

  (let [lto (ListTopicsOptions.)]
    (some->> (admin-timeout options)
             (.timeoutMs lto))
    (.listInternal lto
                   (void/obtain ::K/internal?
                                options
                                K/defaults))
    lto))




(defn new-partitions

  ;; Cf. `dvlopt.kafka.admin/increase-partitions`

  ^NewPartitions

  [options]

  (NewPartitions/increaseTo (:dvlopt.kafka.admin/number-of-partitions options)
                            (:dvlopt.kafka.admin/replica-assigments options)))




(defn new-topic

  ;; Options for `dvlopt.kafka.admin/create-topics`.

  ^NewTopic

  [topic options]

  (let [new-topic (if-let [replica-assignments (:dvlopt.kafka.admin/replica-assigments options)]
                    (NewTopic. topic
                               replica-assignments)
                    (NewTopic. topic
                               (void/obtain :dvlopt.kafka.admin/number-of-partitions
                                            options
                                            K/defaults)
                               (void/obtain :dvlopt.kafka.admin/replication-factor
                                            options
                                            K/defaults)))]
    (some->> (:dvlopt.kafka.admin/topic-configuration options)
             (.configs new-topic))
    new-topic))




(defn records-to-delete

  ;; Cf. `dvlopt.kafka.admin/delete-records`

  ^RecordsToDelete

  [^long offset]

  (RecordsToDelete/beforeOffset offset))




;;;;;;;;;; org.apache.kafka.clients.consumer.*


(defn consumer-rebalance-listener

  ;; Cf. `dvlopt.kafka.in/register-for`

  ^ConsumerRebalanceListener

  [f]

  (reify
    
    ConsumerRebalanceListener

      (onPartitionsAssigned [_ tps]
        (f :assignment
           (map K.-interop.clj/topic-partition
                tps)))


      (onPartitionsRevoked [_ tps]
        (f :revocation
           (map K.-interop.clj/topic-partition
                tps)))))




(defn offset-commit-callback

  ;; Cf. `dvlopt.kafka.in/commit-offsets-async`

  ^OffsetCommitCallback

  [f]

  (reify
    
    OffsetCommitCallback
      
      (onComplete [_ offsets exception]
        (f exception
           (reduce (fn [offsets' [tp ^OffsetAndMetadata om]]
                     (assoc offsets'
                            (K.-interop.clj/topic-partition tp)
                            (.offset om)))
                   {}
                   offsets)))))



(defn topic-partition->offset-and-metadata

  ;; Cf. `dvlopt.kafka.in/commit-offsets-async`

  ^Map

  [tp->o]

  (reduce-kv (fn reduce-topic-partitions [offsets' tp offset]
               (assoc offsets'
                      (topic-partition tp)
                      (OffsetAndMetadata. offset)))
             {}
             tp->o))




;;;;;;;;;; org.apache.kafka.clients.producer.*


(defn callback

  ;; Cf. `dvlopt.kafka.produce/send`

  ^Callback

  [f]

  (reify
    
    Callback

      (onCompletion [_ r-m exception]
        (f exception
           (some-> r-m
                   (K.-interop.clj/record-metadata))))))




(defn producer-record

  ;; Cf. `dvlopt.kafka.produce/send`

  ^ProducerRecord

  [record]

  (ProducerRecord. (::K/topic record)
                   (some-> (::K/partition record)
                           int)
                   (::K/timestamp record)
                   (::K/key record)
                   (::K/value record)
                   (map header
                        (::K/headers record))))




;;;;;;;;;; org.apache.kafka.common.*


(defn topic-partition

  ;; Used very often, about everywhere.

  (^TopicPartition
    
   [[topic partition]]

   (topic-partition topic
                    partition))


  (^TopicPartition
    
   [topic partition]

   (TopicPartition. topic
                    (int partition))))




;;;;;;;;;; org.apache.kafka.common.acl.*


(defn access-control-entry

  ;; Cf. `acl-binding`

  ^AccessControlEntry

  [options]

  (AccessControlEntry. (:dvlopt.kafka.admin/principal options)
                       (or (::K/host options)
                           "*")
                       (acl-operation (void/obtain :dvlopt.kafka.admin/operation
                                                   options
                                                   K/defaults))
                       (acl-permission-type (void/obtain :dvlopt.kafka.admin/permission
                                                         options
                                                         K/defaults))))




(defn access-control-entry-filter

  ;; Cf. `acl-binding-filter`

  ^AccessControlEntryFilter

  [options]

  (AccessControlEntryFilter. (:dvlopt.kafka.admin/principal options)
                             (or (::K/host options)
                                 "*")
                             (acl-operation (void/obtain :dvlopt.kafka.admin/operation
                                                         options
                                                         K/defaults))
                             (acl-permission-type (void/obtain :dvlopt.kafka.admin/permission
                                                               options
                                                               K/defaults))))




(defn acl-binding

  ;; Cf. `dvlopt.kafka.admin/create-acls`

  ^AclBinding

  [options]

  (AclBinding. (resource-pattern options)
               (access-control-entry options)))




(defn acl-binding-filter

  ;; Cf. `dvlopt.kafka.admin/acls`

  ^AclBindingFilter
    
  [options]

  (AclBindingFilter. (resource-pattern-filter options)
                     (access-control-entry-filter options)))




(defn acl-operation

  ^AclOperation

  [kw]

  (condp identical?
         kw
    nil                     AclOperation/ALL
    :all                    AclOperation/ALL
    :alter                  AclOperation/ALTER
    :alter-configuration    AclOperation/ALTER_CONFIGS
    :any                    AclOperation/ANY
    :cluster-action         AclOperation/CLUSTER_ACTION
    :create                 AclOperation/CREATE
    :delete                 AclOperation/DELETE
    :describe               AclOperation/DESCRIBE
    :describe-configuration AclOperation/DESCRIBE_CONFIGS
    :idempotent-write       AclOperation/IDEMPOTENT_WRITE
    :read                   AclOperation/READ
    :unknown                AclOperation/UNKNOWN
    :write                  AclOperation/WRITE))




(defn acl-permission-type

  ^AclPermissionType

  [kw]

  (condp identical?
         kw
    nil      AclPermissionType/ANY
    :any     AclPermissionType/ANY
    :allow   AclPermissionType/ALLOW
    :deny    AclPermissionType/DENY
    :unknown AclPermissionType/UNKNOWN))




;;;;;;;;;; org.apache.kafka.common.config.*


(defn config-resource$type

  ;; Cf. `config-resources`

  ^ConfigResource$Type
    
   [kw]

  (condp identical?
         kw
    nil         ConfigResource$Type/UNKNOWN
    ::K/brokers ConfigResource$Type/BROKER
    ::K/topics  ConfigResource$Type/TOPIC))




(defn config-resources

  ;; Cf. `dvlopt.kafka.admin/configuration`

  [resources]

  (reduce-kv (fn resources-by-type [resources' type names]
               (let [type' (config-resource$type type)]
                 (into resources'
                       (map (fn map-names [name]
                              (ConfigResource. type'
                                               name))
                            names))))
             []
             resources))




;;;;;;;;;; org.apache.kafka.common.header.*


(defn header

  ;; Used for producing records.

  (^Header

   [[k v]]

   (header k
           v))
    

  (^Header

   [k v]

   (reify
 
     Header
 
       (key [_]
         k)
 
       (value [_]
         v))))




;;;;;;;;;; org.apache.kafka.common.resource.*


(defn pattern-type

  ^PatternType

  [kw]

  (condp identical?
         kw
    nil       PatternType/ANY
    :any      PatternType/ANY
    :exactly  PatternType/LITERAL
    :match    PatternType/MATCH
    :prefixed PatternType/PREFIXED
    :unknown  PatternType/UNKNOWN))




(defn resource-pattern

  ;; Cf. `dvlopt.kafka.admin/create-acls`

  ^ResourcePattern

  [options]

  (let [[pattern-type
         resource-name] (void/obtain :dvlopt.kafka.admin/name-pattern
                                     options
                                     K/defaults)]
    (ResourcePattern. (resource-type (void/obtain :dvlopt.kafka.admin/resource-type
                                                  options
                                                  K/defaults))
                      (or resource-name
                          ResourcePattern/WILDCARD_RESOURCE)
                      (pattern-type pattern-type))))




(defn resource-pattern-filter

  ;; Cf. `dvlopt.kafka.admin/acls`

  ^ResourcePatternFilter

  [options]

  (let [[pattern-type
         resource-name] (void/obtain :dvlopt.kafka.admin/name-pattern
                                     options
                                     K/defaults)]
    (ResourcePatternFilter. (resource-type (void/obtain :dvlopt.kafka.admin/resource-type 
                                                        options
                                                        K/defaults))
                            resource-name
                            (pattern-type pattern-type))))




(defn resource-type

  ^ResourceType
    
  [kw]

  (condp identical?
         kw
    nil               ResourceType/ANY
    :any              ResourceType/ANY
    :cluster          ResourceType/CLUSTER
    :consumer-group   ResourceType/GROUP
    :topic            ResourceType/TOPIC
    :transactional-id ResourceType/TRANSACTIONAL_ID
    :unknown          ResourceType/UNKNOWN))




;;;;;;;;;; org.apache.kafka.common.serialization.*


(defn extended-deserializer

  ^ExtendedDeserializer

  [kw-or-f]

  (if (fn? kw-or-f)
    (or (::K/original-deserializer (meta kw-or-f))
        (reify ExtendedDeserializer
            
          (close [_] nil)

          (configure [_ _ _] nil)

          (deserialize [_ topic data]
            (kw-or-f data
                     {::K/topic topic}))

          (deserialize [_ topic headers data]
            (kw-or-f data
                     (void/assoc-some {::K/topic topic}
                                      ::K/headers (K.-interop.clj/headers headers))))))
    (if-some [deserializer (get K/deserializers
                                kw-or-f)]
      (::K/original-deserializer (meta deserializer))
      (throw (IllegalArgumentException. (format "Unknown built-in deserializer requested : %s"
                                                kw-or-f))))))




(defn extended-serializer

  ^ExtendedSerializer

  [kw-or-f]

  (if (fn? kw-or-f)
    (or (::K/original-serializer (meta kw-or-f))
        (reify ExtendedSerializer

          (close [_]
            nil)

          (configure [_ _ _]
            nil)

          (serialize [_ topic data]
            (kw-or-f data
                     {::K/topic topic}))

          (serialize [_ topic headers data]
            (kw-or-f data
                     (void/assoc-some {::K/topic topic}
                                      ::K/headers (K.-interop.clj/headers headers))))))
    (if-some [serializer (get K/serializers
                              kw-or-f)]
      (::K/original-serializer (meta serializer))
      (throw (IllegalArgumentException. (format "Unknown built-in serializer requested : %s"
                                                kw-or-f))))))




(defn serde

  ^Serde

  [serializer deserializer]

  (Serdes/serdeFrom (extended-serializer serializer)
                    (extended-deserializer deserializer)))




(defn serde-key

  ^Serde

  [options]

  (serde (void/obtain ::K/serializer.key
                      options
                      K/defaults)
         (void/obtain ::K/deserializer.key
                      options
                      K/defaults)))




(defn serde-value

  ^Serde

  [options]

  (serde (void/obtain ::K/serializer.value
                      options
                      K/defaults)
         (void/obtain ::K/deserializer.value
                      options
                      K/defaults)))




;;;;;;;;;; org.apache.kafka.streams.*


(defn consumed

  ;; Cf. `dvlopt.kstreams.builder/stream` amongst other

  ^Consumed

  [options]

  (Consumed/with (serde-key options)
                 (serde-value options)
                 (some-> (:dvlopt.kstreams/extract-timestamp options)
                         timestamp-extractor)
                 (topology$auto-offset-reset (void/obtain :dvlopt.kstreams/offset-reset
                                                          options
                                                          K/defaults))))




(defn kafka-streams$state-listener

  ;; Cf. `dvlopt.kstreams/app`

  ^KafkaStreams$StateListener

  [f]

  (reify

    KafkaStreams$StateListener

      (onChange [_ state-new state-old]
        (f (K.-interop.clj/kafka-streams$state state-old)
           (K.-interop.clj/kafka-streams$state state-new)))))




(defn key-value

  ;; Cf. `dvlopt.kstreams.store/kv-put`

  (^KeyValue

   [[k v]]

   (KeyValue. k
              v))


  (^KeyValue

   [k v]

   (KeyValue. k
              v)))




(defn streams-config

  ;; Cf. `dvlopt.kstreams/app`

  ^Properties

  [application-id options]

  (let [properties (Properties.)]
    (doseq [[k
             v] (:dvlopt.kstreams/configuration options)]
      (.setProperty properties
                    k
                    v))
    (doseq [[k
             v] (:dvlopt.kafka.admin/configuration.topics options)]
      (.setProperty properties
                    (StreamsConfig/topicPrefix k)
                    v))
    (doseq [[k
             v] (:dvlopt.kafka.in/configuration options)]
      (.setProperty properties
                    (StreamsConfig/consumerPrefix k)
                    v))
    (doseq [[k
             v] (:dvlopt.kafka.produce/configuration options)]
      (.setProperty properties
                    (StreamsConfig/producerPrefix k)
                    v))
    (.setProperty properties
                  "application.id"
                  application-id)
    (.setProperty properties
                  "bootstrap.servers"
                  (K.-interop/nodes-string (void/obtain ::K/nodes
                                                        options
                                                        K/defaults)))
    properties))




(defn topology$auto-offset-reset

  ;; Cf. `dvlopt.kstreams.topology/add-source` amongst other.

  ^Topology$AutoOffsetReset

  [offset-reset]

  (case offset-reset
    :earliest Topology$AutoOffsetReset/EARLIEST
    :latest   Topology$AutoOffsetReset/LATEST))




;;;;;;;;;; org.apache.kafka.streams.kstream.*


(defn aggregator

  ;; Cf. `dvlopt.kstreams.stream/reduce-*` for instance

  ^Aggregator

  [f]

  (reify

    Aggregator

      (apply [_ k v agg]
        (f agg
           k
           v))))



(defn foreach-action

  ;; Cf. `dvlopt.kstreams.stream/do-kv`

  ^ForeachAction

  [f]

  (reify

    ForeachAction

      (apply [_ k v]
        (f k
           v))))




(defn initializer

  ;; Cf. `dvlopt.kstreams.stream/reduce-*` for instance

  ^Initializer

  [f]

  (reify

    Initializer

      (apply [_]
        (f))))




(defn join-windows

  ;; Cf. `dvlopt.kstreams.stream/join-with-stream` and variations

  ^JoinWindows

  [interval options]

  (doto (JoinWindows/of (to-milliseconds interval))
    (.until (to-milliseconds (void/obtain :dvlopt.kstreams.store/retention
                                          options
                                          K/defaults)))))



(defn joined

  ;; Cf. `dvlopt.kstreams.stream/join-with-stream` and variations

  ^Joined

  [options]

  (Joined/with (serde-key options)
               (serde-value (:dvlopt.kstreams/left options))
               (serde-value (:dvlopt.kstreams/right options))))




(defn key-value-mapper

  ;; Cf. `dvlopt.kstreams.stream/map`

  ^KeyValueMapper

  [f]

  (reify

    KeyValueMapper

      (apply [_ k v]
        (key-value (f k
                      v)))))




(defn key-value-mapper--flat

  ;; Cf. `dvlopt.kstreams.stream/fmap`

  [f]

  (reify

    KeyValueMapper

      (apply [_ k v]
        (map key-value
             (f k
                v)))))




(defn key-value-mapper--raw

  ;; Cf. `dvlopt.kstreams.stream/group-by` amongst other

  ^KeyValueMapper

  [f]

  (reify

    KeyValueMapper

      (apply [_ k v]
        (f k
           v))))




(defn- -materialized--configure

  ;; Helper for `materialized--*`.

  ^Materialized

  [^Materialized materialized options]

  (doto materialized
    (.withKeySerde   (serde-key   options))
    (.withValueSerde (serde-value options)))
  (if (void/obtain :dvlopt.kstreams.store/cache?
                   options
                   K/defaults)
    (.withCachingEnabled materialized)
    (.withCachingDisabled materialized))
  (if (void/obtain :dvlopt.kstreams.store/changelog?
                   options
                   K/defaults)
    (.withLoggingEnabled materialized
                         (or (:dvlopt.kstreams.store/configuration.changelog options)
                             {}))
    (.withLoggingDisabled materialized))
  materialized)




(defn materialized--by-name

  ;; Cf. `dvlopt.kstreams.stream/reduce-*`

  ^Materialized

  [options]

  (let [^String store-name (or (:dvlopt.kstreams.store/name options)
                               (-store-name))
        materialized       (Materialized/as store-name)]
    (-materialized--configure materialized
                              options)
    (doto materialized
      (.withKeySerde (serde-key options))
      (.withValueSerde (serde-value options)))))




(defn materialized--kv

  ;; Often used when Kafka Streams tables and such are involved.

  ^Materialized

  [options]

  (let [store-name (or (:dvlopt.kstreams.store/name options)
                       (-store-name))
        supplier   (condp identical?
                          (void/obtain :dvlopt.kstreams.store/type
                                       options
                                       K/defaults)
                     :kv.in-memory (key-value-bytes-store-supplier--in-memory store-name)
                     :kv.regular   (key-value-bytes-store-supplier--regular store-name))]
    (-materialized--configure (Materialized/as ^KeyValueBytesStoreSupplier supplier)
                              options)))




(defn merger

  ;; Cf. `dvlopt.kstreams.stream/reduce-session-windows`

  ^Merger

  [f]

  (reify

    Merger

      (apply [_ k acc-1 acc-2]
        (f acc-1
           acc-2
           k))))




(defn predicate

  ;; Cf. `dvlopt.kstreams.stream/filter-kv`

  ^Predicate

  [pred]

  (reify

    Predicate

      (test [_ k v]
        (pred k
              v))))



(defn produced

  ;; Cf. `dvlopt.kstreams.stream/through-topic` for instance

  ^Produced

  [options]

  (Produced/with (serde-key options)
                 (serde-value options)
                 (some-> (:dvlopt.kstreams/select-partition options)
                         stream-partitioner)))




(defn reducer

  ;; Cf. `dvlopt.kstreams.stream/reduce-*` for instance

  ^Initializer

  [seed]

  (if (fn? seed)
    (reify

      Initializer

        (apply [_]
          (seed)))
    (reify

      Initializer

        (apply [_]
          seed))))




(defn serialized

  ;; Cf. `dvlopt.kstreams.stream/group-by`

  ^Serialized

  [options]

  (Serialized/with (serde-key options)
                   (serde-value options)))




(defn session-windows

  ;; Cf. `dvlopt.kstreams.stream/window-by-session`

  ^SessionWindows

  [interval options]

  (doto (SessionWindows/with interval)
    (.until (to-milliseconds (void/obtain :dvlopt.kstreams.store/retention
                                          options
                                          K/defaults)))))




(defn time-windows

  ;; Cf. `dvlopt.kstreams.stream/window`

  ^TimeWindows

  [interval options]

  (let [window (TimeWindows/of (to-milliseconds interval))]
    (.until window
            (to-milliseconds (void/obtain :dvlopt.kstreams.store/retention
                                          options
                                          K/defaults)))
    (if-let [slide (:dvlopt.kstreams.store/hop options)]
      (.advanceBy window
                  (to-milliseconds slide))
      window)))




(defn transformer

  ;; Cf. `dvlopt.kstreams.stream/process`

  ^Transformer

  [options]

  (let [on-record (:dvlopt.kstreams/processor.on-record options)
        v*ctx     (volatile! nil)
        v*state   (volatile! nil)]
    (reify

      Transformer

        (init [_ ctx]
          (when on-record
            (vreset! v*ctx
                     ctx)
            (when-some [init (:dvlopt.kstreams/processor.init options)]
              (vreset! v*state
                       (init ctx)))))


        (transform [_ k v]
          (void/call on-record
                     @v*ctx
                     @v*state
                     (void/assoc-some (-record-from-ctx @v*ctx)
                                      ::K/key   k
                                      ::K/value v))
          nil)


        (close [_]
          (void/call (:dvlopt.kstreams/processor.close options))))))




(defn transformer-supplier

  ;; Cf. `dvlopt.kstreams.stream/process`

  ^TransformerSupplier

  [x]

  (if (fn? x)
    (reify

      TransformerSupplier

        (get [_]
          (transformer (x))))
    (reify

      TransformerSupplier

        (get [_]
          (transformer x)))))




(defn value-joiner

  ;; Cf. `dvlopt.kstreams.stream/join-with-stream` and variations

  ^ValueJoiner

  [f]

  (reify

    ValueJoiner

      (apply [_ v1 v2]
        (f v1
           v2))))




(defn value-mapper-with-key

  ;; Cf. `dvlopt.kstreams.stream/map-values`

  ^ValueMapperWithKey

  [f]

  (reify

    ValueMapperWithKey

      (apply [_ k v]
        (f k
           v))))




(defn value-transformer-with-key

  ;; Cf. `dvlopt.kstreams.stream/process-values`

  ^ValueTransformerWithKey

  [options]

  (let [init'     (or (:dvlopt.kstreams/processor.init options)
                      void/no-op)  
        on-record (or (:dvlopt.kstreams/processor.on-record options)
                       void/no-op)
        close'    (or (:dvlopt.kstreams/processor.close options)
                       void/no-op)
        v*ctx     (volatile! nil)
        v*state   (volatile! nil)]
    (reify

      ValueTransformerWithKey

        (init [_ ctx]
          (vreset! v*ctx
                   ctx)
          (vreset! v*state
                   (init' ctx)))


        (transform [_ k v]
          (on-record @v*ctx
                     @v*state
                     (void/assoc-some (-record-from-ctx @v*ctx)
                                      ::K/key   k
                                      ::K/value v)))


        (close [_]
          (close')))))




(defn value-transformer-with-key-supplier

  ;; Cf. `dvlopt.kstreams.stream/process-values`

  ^ValueTransformerWithKeySupplier

  [x]

  (if (fn? x)
    (reify

      ValueTransformerWithKeySupplier

        (get [_]
          (transformer (x))))
    (reify

      ValueTransformerWithKeySupplier

        (get [_]
          (transformer x)))))




(defn window

  ;; Cf. `windowed`

  ^Window

  [from-timestamp to-timestamp]

  (Window. from-timestamp
           to-timestamp))




(defn windowed

  ;; Cf. `dvlopt.kstreams.store/ss-put`

  ^Windowed

  [k from-timestamp to-timestamp]

  (Windowed. k
             (window from-timestamp
                     to-timestamp)))




;;;;;;;;;; org.apache.kafka.streams.processor.*


(defn processor

  ;; Cf. `dvlopt.kstreams.topology/add-processor`

  ^Processor

  [options]

  (let [on-record (:dvlopt.kstreams/processor.on-record options)
        v*ctx     (volatile! nil)
        v*state   (volatile! nil)]
    (reify

      Processor

        (init [_ ctx]
          (when on-record
            (vreset! v*ctx
                     ctx)
            (when-let [init (:dvlopt.kstreams/processor.init options)]
              (vreset! v*state
                       (init ctx)))))


        (process [_ k v]
          (void/call on-record
                     @v*ctx
                     @v*state
                     (void/assoc-some (-record-from-ctx @v*ctx)
                                      ::K/key   k
                                      ::K/value v)))


        (close [_]
          (void/call (:dvlopt.kstreams/processor.close options))))))




(defn processor-supplier

  ;; Cf. `dvlopt.kstreams.topology/add-processor`

  ^ProcessorSupplier

  [x]

  (if (fn? x)
    (reify

      ProcessorSupplier

        (get [_]
          (processor (x))))
    (reify

      ProcessorSupplier

        (get [_]
          (processor x)))))




(defn punctuation-type

  ;; Cf. `dvlopt.kstreams.ctx/schedule`

  ^PunctuationType

  [time-type]

  (case (or time-type
            :stream-time)
    :stream-time     PunctuationType/STREAM_TIME
    :wall-clock-time PunctuationType/WALL_CLOCK_TIME))




(defn punctuator

  ;; Cf. `dvlopt.kstreams.ctx/schedule`

  ^Punctuator

  [f]

  (reify

    Punctuator

      (punctuate [_ timestamp]
        (f timestamp))))




(defn stream-partitioner

  ;; Cf. `dvlopt.kstreams.topology/add-sink`

  ^StreamPartitioner

  [f]

  (reify

    StreamPartitioner

      (partition [_ topic k v number-of-partitions]
        (f topic
           number-of-partitions
           k
           v))))




(defn timestamp-extractor

  ;; Cf. `dvlopt.kstreams.topology/add-source`

  ^TimestampExtractor

  [f]

  (reify

    TimestampExtractor

      (extract [_ record previous-timestamp]
        (f previous-timestamp
           (K.-interop.clj/consumer-record record)))))




(defn topic-name-extractor

  ;; Cf. `dvlopt.kstreams.stream/sink-topic`

  ^TopicNameExtractor

  [f]

  (reify

    TopicNameExtractor

      (extract [_ k v record-context]
        (f (void/assoc-some (K.-interop.clj/record-context record-context)
                            ::K/key   k
                            ::K/value v)))))




;;;;;;;;;; org.apache.kafka.streams.state.*


(defn key-value-bytes-store-supplier--in-memory

  ^KeyValueBytesStoreSupplier

  [store-name]

  (Stores/inMemoryKeyValueStore store-name))




(defn key-value-bytes-store-supplier--lru-map

  ^KeyValueBytesStoreSupplier

  [store-name options]

  (Stores/lruMap store-name
                 (void/obtain :dvlopt.kstreams.store/lru-size
                              options
                              K/defaults)))





(defn key-value-bytes-store-supplier--regular

  ^KeyValueBytesStoreSupplier

  [store-name]

  (Stores/persistentKeyValueStore store-name))




(defn store-builder

  ;; Cf. `dvlopt.kstreams.topology/add-store`

  ^StoreBuilder

  [options]

  (let [              store-name   (or (:dvlopt.kstreams.store/name options)
                                       (-store-name))
                      serde-key'   (serde-key options)
                      serde-value' (serde-value options)
        ^StoreBuilder builder      (condp identical?
                                         (void/obtain :dvlopt.kstreams.store/type
                                                      options
                                                      K/defaults)
                                    :kv.in-memory (Stores/keyValueStoreBuilder (key-value-bytes-store-supplier--in-memory store-name)
                                                                               serde-key'
                                                                               serde-value')
                                    :kv.lru       (Stores/keyValueStoreBuilder (key-value-bytes-store-supplier--lru-map store-name
                                                                                                                        options)
                                                                               serde-key'
                                                                               serde-value')
                                    :kv.regular   (Stores/keyValueStoreBuilder (key-value-bytes-store-supplier--regular store-name)
                                                                               serde-key'
                                                                               serde-value')
                                    :session      (Stores/sessionStoreBuilder (session-bytes-store-supplier store-name
                                                                                                            options)
                                                                              serde-key'
                                                                              serde-value')
                                    :window       (Stores/windowStoreBuilder (window-bytes-store-supplier store-name
                                                                                                          options)
                                                                             serde-key'
                                                                             serde-value'))]
    (when (void/obtain :dvlopt.kstreams.store/cache?
                       options
                       K/defaults)
      (.withCachingEnabled builder))
    (if (void/obtain :dvlopt.kstreams.store/changelog?
                     options
                     K/defaults)
      (.withLoggingEnabled builder
                           (or (:dvlopt.kstreams.store/configuration.changelog options)
                               {}))
      (.withLoggingDisabled builder))
    builder))




(defn session-bytes-store-supplier

  ^SessionBytesStoreSupplier

  [store-name options]

  (Stores/persistentSessionStore store-name
                                 (to-milliseconds (void/obtain :dvlopt.kstreams.store/retention
                                                               options
                                                               K/defaults))))




(defn window-bytes-store-supplier

  ^WindowBytesStoreSupplier

  [store-name options]

  (Stores/persistentWindowStore store-name
                                (to-milliseconds (void/obtain :dvlopt.kstreams.store/retention
                                                              options
                                                              K/defaults))
                                (int (void/obtain :dvlopt.kstreams.store/segments
                                                  options
                                                  K/defaults))
                                (to-milliseconds (-required-arg :dvlopt.kstreams.store/interval
                                                                (:dvlopt.kstreams.store/interval options)))
                                (void/obtain :dvlopt.kstreams.store/duplicate-keys?
                                             options
                                             K/defaults)))
