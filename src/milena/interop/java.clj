(ns milena.interop.java

  "Convert clojure data structures to java objects."

  {:author "Adam Helinski"}

  (:require [milena.interop     :as M.interop]
            [milena.interop.clj :as M.interop.clj])
  (:import java.util.Map
           org.apache.kafka.common.TopicPartition
           (org.apache.kafka.common.config ConfigResource
                                           ConfigResource$Type)
           (org.apache.kafka.common.resource ResourceType
                                             Resource
                                             ResourceFilter)
           (org.apache.kafka.common.acl AclBinding
                                        AclBindingFilter
                                        AccessControlEntry
                                        AccessControlEntryFilter
                                        AclOperation
                                        AclPermissionType)
           (org.apache.kafka.common.serialization Serde
                                                  Serdes)
           (org.apache.kafka.clients.admin DescribeClusterOptions
                                           ListTopicsOptions
                                           DescribeTopicsOptions
                                           NewTopic
                                           CreateTopicsOptions
                                           DeleteTopicsOptions
                                           CreatePartitionsOptions
                                           DescribeConfigsOptions
                                           NewPartitions
                                           Config
                                           ConfigEntry
                                           AlterConfigsOptions
                                           DescribeAclsOptions
                                           CreateAclsOptions
                                           DeleteAclsOptions)
           (org.apache.kafka.clients.producer ProducerRecord
                                              Callback)
           (org.apache.kafka.clients.consumer ConsumerRebalanceListener
                                              OffsetAndMetadata
                                              OffsetCommitCallback)
           (org.apache.kafka.streams KafkaStreams$StateListener
                                     StreamsConfig
                                     Topology$AutoOffsetReset
                                     Consumed
                                     KeyValue)
           (org.apache.kafka.streams.processor TimestampExtractor
                                               StreamPartitioner
                                               Processor
                                               ProcessorSupplier
                                               ProcessorContext
                                               PunctuationType
                                               Punctuator)
           (org.apache.kafka.streams.state Stores
                                           StoreBuilder)
           (org.apache.kafka.streams.kstream Serialized
                                             Produced
                                             Materialized
                                             Predicate
                                             KeyValueMapper
                                             ValueMapper
                                             ValueJoiner
                                             JoinWindows
                                             Joined
                                             Initializer
                                             Aggregator
                                             Reducer
                                             Transformer
                                             TransformerSupplier
                                             ValueTransformer
                                             ValueTransformerSupplier
                                             ForeachAction
                                             TimeWindows
                                             SessionWindows
                                             Window
                                             Windowed)))




;;;;;;;;;; Private


(defn- -no-op

  ""

  ([]

   nil)


  ([_]

   nil)


  ([_ _]

   nil)


  ([_ _ _]

   nil)


  ([_ _ _ _]

   nil)


  ([_ _ _ _ & _]

   nil))




(def -*store-number

  ""

  (atom -1))




(defn- -store-name

  ""

  []

  (format "milena-store-%08d"
          (swap! -*store-number
                 inc)))




(defn- -record-from-ctx

  ""

  [^ProcessorContext ctx]

  {:topic     (.topic     ctx)
   :partition (.partition ctx)
   :offset    (.offset    ctx)
   :timestamp (.timestamp ctx)})




;;;;;;;;;; Helpers


(defn at-least-0

  "Converts a number `n` (nilable) to a positive int, a negative value resulting in 0."

  [n]

  (when (number? n)
    (int (max n
              0))))




;;;;;;;;;; java std


(defn thread$uncaught-exception-handler

  ""

  ;; TODO docstring

  ^Thread$UncaughtExceptionHandler

  [f]

  (reify

    Thread$UncaughtExceptionHandler

      (uncaughtException [_ t e]
        (f e
           t))))




;;;;;;;;;; org.apache.kafka.common.*


(defn topic-partition

  "@ topic
     Topic name.

   @ partition
     Partition number.

   => org.apache.kafka.common.TopicPartition


   Ex. (topic-partition [\"my-topic\" 0])

       (topic-partition \"my-topic\"
                        0)"

  ^TopicPartition

  ([[topic partition]]

   (topic-partition topic
                    partition))


  ([topic partition]

   (TopicPartition. topic
                    (int partition))))




;;;;;;;;;; org.apache.kafka.common.config.*


(defn config-resource$type

  "@ kw
     One of :unknown or nil
            :brokers
            :topics

   => org.apache.kafka.common.config.ConfigResource$Type"

  ^ConfigResource$Type

  ([]

   ConfigResource$Type/UNKNOWN)


  ([kw]

   (condp identical?
          kw
     nil      ConfigResource$Type/UNKNOWN
     :unknown ConfigResource$Type/UNKNOWN
     :brokers ConfigResource$Type/BROKER
     :topics  ConfigResource$Type/TOPIC)))




(defn config-resource

  "@ name
     Resource name
  
   @ type (nilable)
     Resource type.
     Cf. `config-resource$type`
  
   => org.apache.kafka.common.config.ConfigResource
  

   Ex. (config-resource \"my-topic\"
                        :topic)"

  ^ConfigResource

  ([name]

   (ConfigResource. ConfigResource$Type/UNKNOWN
                    name))


  ([name type]

   (ConfigResource. (config-resource$type type)
                    name)))




(defn config-resources

  "@ resources
     A map of resource type to a list of resource names.
     Cf. `config-resource$type`
  
   => A list for org.apache.kafka.common.config.ConfigResource
  
  
   Ex. (config-resources {:topics #{\"some-topic\"
                                    \"another-topic\"}
                          :brokers #{\"0\"
                                     \"1\"}})"

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




;;;;;;;;;; org.apache.kafka.common.resource.*


(defn resource-type

  "@ kw (nilable)
     One of :any or nil
            :cluster
            :groupc
            :topic
            :transactional-id
            :unknown

   => org.apache.kafka.common.resource.ResourceType"

  ^ResourceType

  ([]

   ResourceType/ANY)


  ([kw]

   (condp identical?
          kw
     nil               ResourceType/ANY
     :any              ResourceType/ANY
     :cluster          ResourceType/CLUSTER
     :group            ResourceType/GROUP
     :topic            ResourceType/TOPIC
     :transactional-id ResourceType/TRANSACTIONAL_ID
     :unknown          ResourceType/UNKNOWN)))




(defn resource

  "@ args (nilable)
     {:name
       Resource name.

      :type (nilable)
       Cf. `resource-type`}

   => org.apache.kafka.common.resource.Resource


   Ex. (resource {:name  \"my-topic\"
                  :?type :topic})"

  ^Resource

  ([{:as   args
     :keys [name
            type]}]

   (resource name
             type))


  ([name type]

   (Resource. (resource-type type)
              name)))




(defn resource-filter

  "@ args (nilable)
     {:name (nilable)
       Resource name.

      :type (nilable)
       Resource type.
       Cf. `resource-type`}

   => org.apache.kafka.common.resource.ResourceFilter
  
   Ex. (resource-filter)
  
       (resource-filter {:name \"my-topic\"
                         :type :topic})"

  ^ResourceFilter

  ([]

   ResourceFilter/ANY)


  ([{:as   ?args
     :keys [?name
            ?type]}]

   (resource-filter ?name
                    ?type))


  ([?name ?type]

   (ResourceFilter. (resource-type ?type)
                    ?name)))




;;;;;;;;;; org.apache.kafka.common.acl.*


(defn acl-operation

  "@ kw (nilable)
     One of :all               or nil
            :alter
            :alter-config
            :any
            :cluster-action
            :create
            :delete
            :describe
            :describe-configs
            :idempotent-write
            :read
            :unknown
            :write

   => org.apache.kafka.common.acl.AclOperation"

  ^AclOperation

  [kw]

  (condp identical?
         kw
    nil               AclOperation/ALL
    :all              AclOperation/ALL
    :alter            AclOperation/ALTER
    :alter-config     AclOperation/ALTER_CONFIGS
    :any              AclOperation/ANY
    :cluster-action   AclOperation/CLUSTER_ACTION
    :create           AclOperation/CREATE
    :delete           AclOperation/DELETE
    :describe         AclOperation/DESCRIBE
    :describe-configs AclOperation/DESCRIBE_CONFIGS
    :idempotent-write AclOperation/IDEMPOTENT_WRITE
    :read             AclOperation/READ
    :unknown          AclOperation/UNKNOWN
    :write            AclOperation/WRITE))




(defn acl-permission-type

  "@ kw (nilable)
     One of :any or nil
            :allow
            :deny
            :unknown

   => org.apache.kafka.common.acl.AclPermissionType"

  ^AclPermissionType

  [kw]

  (condp identical?
         kw
    nil      AclPermissionType/ANY
    :any     AclPermissionType/ANY
    :allow   AclPermissionType/ALLOW
    :deny    AclPermissionType/DENY
    :unknown AclPermissionType/UNKNOWN))




(defn access-control-entry

  "@ args
     {:principal

      :host (nilable)
       The host name where \"*\" involves all hosts.

      :permission (nilable)
       Cf. `acl-permission-type`

      :operation (nilable)
       Cf. `acl-operation`}
         

   => org.apache.kafka.common.acl.AccessControlEntry"

  ^AccessControlEntry

  ([{:as   args
     :keys [principal
            host
            permission
            operation]
     :or   {host            "*"
            operation       :all
            permission-type :any}}]

   (access-control-entry principal
                         host
                         permission
                         operation))


  ([principal host permission operation]

   (AccessControlEntry. principal
                        (or host
                            "*")
                        (acl-operation operation)
                        (acl-permission-type permission))))




(defn access-control-entry-filter

  "@ args (nilable)
     {:principal (nilable)
      
      :host (nilable)
       The host name where \"*\" involves any host.

      :permission (nilable)
       Cf. `acl-permission-type`

      :operation (nilable)
       Cf. `acl-operation`}


  
   => org.apache.kafka.common.acl.AccessControlEntryFilter

  
   Ex. (access-control-entry-filter {:permission :allow
                                     :operation  :create})"

  ^AccessControlEntryFilter

  ([]

   AccessControlEntryFilter/ANY)


  ([{:as   args
     :keys [principal
            host
            permission
            operation]
     :or   {host            "*"
            operation       :all
            permission-type :any}}]

   (access-control-entry-filter principal
                                host
                                permission
                                operation))


  ([principal host permission operation]

   (AccessControlEntryFilter. principal
                              (or host
                                  "*")
                              (acl-operation operation)
                              (acl-permission-type permission))))




(defn acl-binding

  "@ args
     {:resource
       Cf. `resource`

      :access-control
       Cf. `access-control-entry`}  
  
   => org.apache.kafka.common.acl.AclBinding"

  ^AclBinding

  ([{:as   args
     :keys [resource
            access-control]}]

   (acl-binding resource
                access-control))


  ([resource access-control]

    (AclBinding. (milena.interop.java/resource resource)
                 (access-control-entry access-control))))




(defn acl-binding-filter

  "@ args (nilable)
     {:resource (nilable)
       Cf. `resource-filter`

      :access-control (nilable)
       Cf. `access-control-entry-filter`}

   => org.apache.kafka.common.acl.AclBindingFilter
  
   
   Ex. (acl-binding-filter {:resource       {:name \"my-topic\"
                                             :type :topic}
                            :access-control {:permission :allow
                                             :operation  :alter}})"

  ^AclBindingFilter

  ([]

   AclBindingFilter/ANY)


  ([{:as   args
     :keys [resource
            access-control]}]

   (acl-binding-filter resource
                       access-control))


  ([resource access-control]

   (AclBindingFilter. (resource-filter resource)
                      (access-control-entry-filter access-control))))




;;;;;;;;;; org.apache.kafka.common.serialization.*


;; TODO docstrings


(defn serde

  ""

  ^Serde

  [serializer deserializer]

  (when (and serializer
             deserializer)
    (Serdes/serdeFrom serializer
                      deserializer)))




(defn serde-key

  ""

  ^Serde

  [{:as   opts
    :keys [serializer
           serializer-key
           deserializer
           deserializer-key]}]

  (serde (or serializer-key
             serializer)
         (or deserializer-key
             deserializer)))




(defn serde-value

  ""

  ^Serde

  [{:as   opts
    :keys [serializer
           serializer-value
           deserializer
           deserializer-value]}]

  (serde (or serializer-value
             serializer)
         (or deserializer-value
             deserializer)))




;;;;;;;;;; org.apache.kafka.clients.admin.*


(defn describe-cluster-options

  "Options for `milena.admin/cluster`.

   @ args (nilable)
     {:timeout-ms (nilable)
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.}
  
   => org.apache.kafka.clients.admin.DescribeClusterOptions object"

  ^DescribeClusterOptions

  [{:as   args
    :keys [timeout-ms]}]

  (doto (DescribeClusterOptions.)
    (.timeoutMs (at-least-0 timeout-ms))))




(defn list-topics-options

  "Options for `milena.admin/topics`.

   @ args (nilable)
     {:timeout-ms (nilable)
        An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
        if the values couldn't be retrieved within this timeout.

      :internal?
       Should internal topics be retrieved ?}

   => org.apache.kafka.clients.admin.ListTopicsOptions object"

  ^ListTopicsOptions

  ([{:as  args
     :keys [timeout-ms
            internal?]
     :or   {internal? false}}]

   (list-topics-options timeout-ms
                        internal?))


  ([timeout-ms internal?]

   (doto (ListTopicsOptions.)
     (.timeoutMs    (at-least-0 timeout-ms))
     (.listInternal internal?))))




(defn describe-topics-options

  "Options for `milena.admin/topics-decribe`.
  
   @ args (nilable)
     {:timeout-ms (nilable)
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.}
  
   => org.apache.kafka.clients.admin.DescribeTopicsOptions object"

  ^DescribeTopicsOptions

  [{:as   args
    :keys [timeout-ms]}]

  (doto (DescribeTopicsOptions.)
    (.timeoutMs (at-least-0 timeout-ms))))




(defn new-topic

  "For creating new topics via `milena.admin/topics-create`.

   A topic can be created either by specifying the number of partitions and the replication factor, either
   by specyfying directly the assignments.


   @ topic
     The topic name

   @ args (nilable)
     {:partitions (nilable)
       The number of partition for this new topic

      :replication-factor (nilable)
       The replication factor for each partition

      :assigments (nilable)
       A map of partition number -> list of replica ids (i.e. broker ids).

       Although not enforced, it is generally a good idea for all partitions to have
       the same number of replicas.

      :config (nilable)
       Kafka configuration

       Cf. https://kafka.apache.org/documentation/#topicconfigs}

   => org.apache.kafka.clients.admin.NewTopic object
  

   Ex. (new-topic \"topic-name\"
                  {:partitions        4
                   :replication-facor 3})"

  ^NewTopic

  [topic {:as   args
          :keys [partitions
                 replication-factor
                 assignments
                 config]
          :or   {partitions         1
                 replication-factor 1}}]

  (let [new-topic (if assignments
                    (NewTopic. topic
                               assignments)
                    (NewTopic. topic
                               partitions
                               replication-factor))]
    (when config
      (.configs new-topic
                (M.interop/stringify-keys config)))
    new-topic))




(defn new-partitions

  "For increasing the number of partitions.

   @ arg
     {:n
       New number of partitions.

      :assignments
       Matrix of 'number of new partitions' x 'replication factor' where each entry is a broker number and the first one
       is the prefered leader.

       Ex. For 3 new partitions and a replication factor of 2 :
           
             [[1 2]    
              [2 3]
              [3 1]]
      }
       
   => org.apache.kafka.clients.admin.NewPartitions"

  ^NewPartitions

  [{:as   arg
    :keys [n
           assignments]}]

  (NewPartitions/increaseTo n
                            assignments))




(defn topics-to-new-partitions

  "@ hmap
     Map of topic name to new partitions.
     Cf. `new-partitions`

   => Map of topic name to org.apache.kafka.clients.admin.NewPartitions"

  ^Map

  [hmap]

  (reduce-kv (fn reduce-hmap [hmap topic partitions]
               (assoc hmap
                      topic
                      (new-partitions partitions)))
             {}
             hmap))




(defn create-topics-options

  "Options for `milena.admin/topics-create`.
  
   @ args (nilable)
     {:timeout-ms (nilable)
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.

      :fake (nilable)
       Validate request without actually creating the topic for real ?}
  
   => org.apache.kafka.clients.admin.CreateTopicsOptions"


  (^CreateTopicsOptions

   [{:as   args
     :keys [timeout-ms
            fake?]}]

   (create-topics-options timeout-ms
                          fake?))

  (^CreateTopicsOptions

   [timeout-ms fake?]

   (doto (CreateTopicsOptions.)
     (.timeoutMs    (at-least-0 timeout-ms))
     (.validateOnly fake?))))




(defn delete-topics-options

  "Options for `milena.admin/topics-delete`.
  
   @ args (nilable)
     {:timeout-ms (nilable)
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.}
  
   => org.apache.kafka.clients.admin.DeleteTopicsOptions"

  ^DeleteTopicsOptions

  [{:as   args
    :keys [timeout-ms]}]

  (doto (DeleteTopicsOptions.)
    (.timeoutMs (at-least-0 timeout-ms))))




(defn create-partitions-options

  "Options for `milena.admin/partitions-create`.

   @ args (nilable)
     {:fake (nilable)
       Validate request without actually creating the topic for real ?}

   => org.apache.kafka.clients.admin.CreatePartitionsOptions"

  (^CreatePartitionsOptions

   []

   (CreatePartitionsOptions.))


  (^CreatePartitionsOptions

   [{:as   args
     :keys [fake?]}]

   (doto (create-partitions-options)
     (.validateOnly fake?))))




(defn describe-configs-options

  "Options for `milena.admin/config`.
  
   @ args
     {:timeout-ms
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.}
  
   => org.apache.kafka.clients.admin.DescribeConfigsOptions"

  ^DescribeConfigsOptions

  [{:as   args
    :keys [timeout-ms]}]

  (doto (DescribeConfigsOptions.)
    (.timeoutMs (at-least-0 timeout-ms))))




(defn config-entry

  "@ kw
     Name.

   @ value
     Stringifyable value.

   => org.apache.kafka.clients.admin.ConfigEntry"

  ^ConfigEntry

  ([[kw value]]

    (config-entry kw
                  value))


  ([kw value]

   (ConfigEntry. (name kw)
                 (str value))))




(defn config

  "@ entries
     List of configuration entries.
     Cf. `config-entry`
  
   => org.apache.kafka.clients.admin.Config"

  ^Config

  [entries]

  (Config. (map config-entry
                entries)))




(defn alter-configs

  "@ configs
     A map of configurations organized by names and types.
  
   => Map of resource type to map of resource name to configuration.
      Cf. `config-resource$type`
  
  
   Ex. (alter-configs {:brokers {\"0\" {:delete.topic.enable true}}
                       :topics  {\"my-topic\" {:cleanup.policy \"compact\"}}})"

  [configs]

  (reduce-kv (fn by-type [configs' type resources]
               (let [type' (config-resource$type type)]
                 (reduce-kv (fn reduce-resources [configs'2 name entries]
                              (assoc configs'2
                                     (ConfigResource. type'
                                                      name)
                                     (config entries)))
                            configs'
                            resources)))
             {}
             configs))




(defn alter-configs-options

  "@ args (nilable)
     {:timeout-ms (nilable)
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.

      :fake (nilable)
       Validate request without actually creating the topic for real ?}
  
   => org.apache.kafka.clients.admin.AlterConfigsOptions"

  ^AlterConfigsOptions

  ([{:as   args
     :keys [timeout-ms
            fake?]
     :or   {fake? false}}]

   (alter-configs-options timeout-ms
                          fake?))


  ([timeout-ms fake?]

   (doto (AlterConfigsOptions.)
     (.timeoutMs    (at-least-0 timeout-ms))
     (.validateOnly fake?))))




(defn describe-acls-options

  "@ args (nilable)
     {:timeout-ms (nilable)
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.}

   => org.apache.kafka.clients.admin.DescribeAclsOptions"

  ^DescribeAclsOptions

  [{:as   args
    :keys [timeout-ms]}]

  (doto (DescribeAclsOptions.)
    (.timeoutMs (at-least-0 timeout-ms))))




(defn create-acls-options

  "@ args (nilable)
     {:timeout-ms (nilable)
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.}

   => org.apache.kafka.clients.admin.CreateAclsOptions"

  ^CreateAclsOptions

  [{:as   args
    :keys [timeout-ms]}]

  (doto (CreateAclsOptions.)
    (.timeoutMs (at-least-0 timeout-ms))))




(defn delete-acls-options

  "@ args (nilable)
     {:timeout-ms (nilable)
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.}

   => org.apache.kafka.clients.admin.DeleteAclsOptions"

  ^DeleteAclsOptions

  [{:as   args
    :keys [timeout-ms]}]

  (doto (DeleteAclsOptions.)
    (.timeoutMs (at-least-0 timeout-ms))))




;;;;;;;;;; org.apache.kafka.clients.producer.*


(defn producer-record

  "@ record
     {:topic
       Topic name.

      :partition (nilable)
       Optional partition number.

      :key (nilable)
       Optional key.

      :value (nilable)
       Optional value.

      :timestamp (nilable)
       Optional unix timestamp.

   =>  org.apache.kafka.clients.producer.ProducerRecord
  
  
   Ex. (producer-record {:topic  \"my-topic\"
                         :key   \"some-key\"
                         :value 42})"

  ^ProducerRecord

  [{:as   record
    :keys [topic
           partition
           key
           value
           timestamp]}]

  (ProducerRecord. topic
                   (some-> partition
                           int)
                   timestamp
                   key
                   value
                   nil))




(defn callback

  "@ f
     Fn taking an exception and a map representing metadata about the record that has been sent.
     Only one of them will be non-nil.
     Cf. `milena.interop.clj/record-metadata

  => org.apache.kafka.clients.producer.Callback


  Ex. (callback (fn [exception meta]
                  (when-not exception
                    (println :committed meta))))"

  ^Callback

  [f]

  (reify
    
    Callback

      (onCompletion [_ r-m exception]
        (f exception
           (some-> r-m
                   (M.interop.clj/record-metadata))))))




;;;;;;;;;; org.apache.kafka.clients.consumer.*


(defn consumer-rebalance-listener

  "@ f
     Fn taking `assigned?` and a list of [topic partition].

       + assigned?
         Are these topic-partitions assigned or revoked ?

     Cf. `milena.interop.clj/topic-partition`
         

   => org.apache.kafka.clients.consumer.ConsumerRebalanceListener"

  ^ConsumerRebalanceListener

  [f]

  (reify
    
    ConsumerRebalanceListener

      (onPartitionsAssigned [_ tps]
        (f true
           (map M.interop.clj/topic-partition
                tps)))


      (onPartitionsRevoked [_ tps]
        (f false
           (map M.interop.clj/topic-partition
                tps)))))




(defn topic-partition-to-offset

  "@ tp->o
     Map of [topic partition] to offsets.

   => Map of org.apache.kafka.common.TopicPartition to org.apache.kafka.clients.consumer.OffsetAndMetadata."

  [tp->o]

  (reduce-kv (fn reduce-topic-partitions [offsets' tp offset]
               (assoc offsets'
                      (topic-partition tp)
                      (OffsetAndMetadata. offset)))
             {}
             tp->o))




(defn offset-commit-callback

  "@ f
     Fn taking `exception` and `offsets`, one of them being nil depending whether an error occured or not.
   
     + `offsets` (nilable)
       Map of [topic partition] to offsets.

   => org.apache.kafka.clients.consumer.OffsetCommitCallback"

  ^OffsetCommitCallback

  [f]

  (reify
    
    OffsetCommitCallback
      
      (onComplete [_ offsets exception]
        (f exception
           (reduce (fn [offsets' [tp ^OffsetAndMetadata om]]
                     (assoc offsets'
                            (M.interop.clj/topic-partition tp)
                            (.offset om)))
                   {}
                   offsets)))))




;;;;;;;;;; org.apache.kafka.streams.*


(defn kafka-streams$state-listener

  ""

  ^KafkaStreams$StateListener

  [f]

  (reify

    KafkaStreams$StateListener

      (onChange [_ state-new state-old]
        (f (M.interop.clj/kafka-streams$state state-new)
           (M.interop.clj/kafka-streams$state state-old)))))




(defn streams-config

  ""

  ;; TODO docstring

  ^StreamsConfig

  [application-name {:as   opts
                     :keys [nodes
                            config]
                     :or   {nodes [["localhost" 9092]]}}]

  (StreamsConfig. (merge (M.interop/stringify-prefixed-keys config)
                         {"bootstrap.servers" (M.interop/nodes-string nodes)
                          "application.id"    application-name})))




(defn topology$auto-offset-reset

  ""

  ^Topology$AutoOffsetReset

  [offset-reset]

  (case offset-reset
    nil       nil
    :earliest Topology$AutoOffsetReset/EARLIEST
    :latest   Topology$AutoOffsetReset/LATEST))




(declare timestamp-extractor)




(defn consumed

  ""

  ^Consumed

  [{:as   opts
    :keys [extract-timestamp
           offset-reset]}]

  (Consumed/with (serde-key opts)
                 (serde-value opts)
                 (some-> extract-timestamp
                         timestamp-extractor)
                 (topology$auto-offset-reset offset-reset)))




(defn key-value

  ""

  (^KeyValue

   [[k v]]

   (KeyValue. k
              v))


  (^KeyValue

   [k v]

   (KeyValue. k
              v)))




;;;;;;;;;; org.apache.kafka.streams.processor.*


(defn timestamp-extractor

  ""

  ^TimestampExtractor

  [f]

  (reify

    TimestampExtractor

      (extract [_ record previous-ts]
        (f previous-ts
           (M.interop.clj/consumer-record record)))))




(defn stream-partitioner

  ""

  ^StreamPartitioner

  [f]

  (reify

    StreamPartitioner

      (partition [_ k v n-partitions]
        (f k
           v
           n-partitions))))




(defn processor

  ""

  ^Processor

  [{init'    :init
    process' :process
    close'   :close}]

  (let [init'2    (or init'
                      -no-op)
        process'2 (or process'
                      -no-op)
        close'2   (or close'
                      -no-op)
        v*ctx     (volatile! nil)]
    (reify

      Processor

        (init [_ ctx]
          (vreset! v*ctx
                   ctx)
          (init'2 ctx))


        (process [_ k v]
          (process'2 (merge (-record-from-ctx @v*ctx)
                            {:key   k
                             :value v})))


        (close [_]
          (close'2)))))




(defn processor-supplier

  ""

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

  ""

  ^PunctuationType

  [time-type]

  (case (or time-type
            :stream-time)
    :stream-time     PunctuationType/STREAM_TIME
    :wall-clock-time PunctuationType/WALL_CLOCK_TIME))




(defn punctuator

  ""

  ^Punctuator

  [f]

  (reify

    Punctuator

      (punctuate [_ timestamp]
        (f timestamp))))




;;;;;;;;;; org.apache.kafka.streams.state.*


(defn- -store-builder--configure

  ""

  ^StoreBuilder

  [^StoreBuilder builder {:as   opts
                          :keys [cache?
                                 changelog?
                                 changelog-config]}]
  (when (or cache?
          (not (contains? opts
                          :cache?)))
    ;; TODO weird, no .withCachingDisabled ?
    (.withCachingEnabled builder))
  (if (or changelog?
          (not (contains? opts
                          :changelog?)))
    (when changelog-config
      (.withLoggingEnabled builder
                           (M.interop/stringify-keys changelog-config)))
    (.withLoggingDisabled builder))
  builder)




(defn store-builder--kv

  ""

  ^StoreBuilder

  [{:as   opts
    :keys [name
           type]}]

  (let [name' (or name
                  (-store-name))]
    (-store-builder--configure (Stores/keyValueStoreBuilder (case (or type
                                                                      :persistent)
                                                              :persistent (Stores/persistentKeyValueStore name')
                                                              :in-memory  (Stores/inMemoryKeyValueStore   name'))
                                                            (serde-key opts)
                                                            (serde-value opts))
                               opts)))




(defn store-builder--ws

  ""

  ^StoreBuilder

  [{:as   opts
    :keys [name
           retention
           segments
           size
           duplicates?]}]

  (let [name' (or name
                  (-store-name))]
    (-store-builder--configure (Stores/windowStoreBuilder (Stores/persistentWindowStore name'
                                                                                        retention
                                                                                        (int segments)
                                                                                        size
                                                                                        duplicates?)
                                                          (serde-key opts)
                                                          (serde-value opts))
                               opts)))




(defn store-builder--ss

  ""

  ^StoreBuilder

  [{:as   opts
    :keys [name
           retention]}]

  (let [name' (or name
                  (-store-name))]
    (-store-builder--configure (Stores/sessionStoreBuilder (Stores/persistentSessionStore name'
                                                                                          retention)
                                                           (serde-key opts)
                                                           (serde-value opts))
                               opts)))




(defn store-builder

  ""

  ^StoreBuilder

  [{:as   opts
    :keys [type]}]

  (case (or type
            :kv/persistent)
    :kv/persistent (store-builder--kv (assoc opts
                                             :type
                                             :persistent))
    :kv/in-memory  (store-builder--kv (assoc opts
                                             :type
                                             :in-memory))
    :windows       (store-builder--ws opts)
    :sessions      (store-builder--ss opts)))




;;;;;;;;;; org.apache.kafka.streams.kstream.*


(defn serialized

  ""

  ^Serialized

  [opts]

  (Serialized/with (serde-key opts)
                   (serde-value opts)))




(defn produced

  ""

  ^Produced

  [{:as   opts
    :keys [partition-stream]}]

  (Produced/with (serde-key opts)
                 (serde-value opts)
                 (some-> partition-stream
                         stream-partitioner)))




(defn- -materialized--configure

  ""

  ^Materialized

  [^Materialized materialized {:as   opts
                               :keys [cache?
                                      changelog?
                                      changelog-config]}]

  (doto materialized
    (.withKeySerde   (serde-key   opts))
    (.withValueSerde (serde-value opts)))
  (if (or cache?
          (not (contains? opts
                          :cache?)))
    (.withCachingEnabled materialized)
    (.withCachingDisabled materialized))
  (if (or changelog?
          (not (contains? opts
                          :changelog?)))
    (when changelog-config
      (.withLoggingEnabled materialized
                           (M.interop/stringify-keys changelog-config)))
    (.withLoggingDisabled materialized))
  materialized)




(defn materialized--by-name

  ""

  ^Materialized

  [{:as   opts
    :keys [name]}]

  (let [^String name' (or name
                          (-store-name))]
    (-materialized--configure (Materialized/as name')
                              opts)))




(defn materialized--kv

  ""

  ^Materialized

  [{:as   opts
    :keys [name
           type]}]

  (let [name' (or name
                  (-store-name))]
    (-materialized--configure (Materialized/as (case (or type
                                                         :persistent)
                                                 :persistent (Stores/persistentKeyValueStore name')
                                                 :in-memory  (Stores/inMemoryKeyValueStore   name')))
                              opts)))




(defn predicate

  ""

  ^Predicate

  [pred?]

  (reify

    Predicate

      (test [_ k v]
        (pred? k
               v))))




(defn value-mapper

  ""

  ^ValueMapper

  [f]

  (reify

    ValueMapper

      (apply [_ v]
        (f v))))




(defn key-value-mapper

  ""

  ^KeyValueMapper

  [f]

  (reify

    KeyValueMapper

      (apply [_ k v]
        (key-value (f k
                      v)))))




(defn key-value-mapper--raw

  ""

  ^KeyValueMapper

  [f]

  (reify

    KeyValueMapper

      (apply [_ k v]
        (f k
           v))))




(defn key-value-mapper--key

  ""

  ^KeyValueMapper

  [f]

  (reify

    KeyValueMapper

      (apply [_ k v]
        (f k))))




(defn key-value-mapper--flat

  ""

  [f]

  (reify

    KeyValueMapper

      (apply [_ k v]
        (map key-value
             (f k
                v)))))




(defn value-joiner

  ""

  ^ValueJoiner

  [f]

  (reify

    ValueJoiner

      (apply [_ v1 v2]
        (f v1
           v2))))




(defn join-windows

  ""

  ^JoinWindows

  [interval-ms]

  (JoinWindows/of interval-ms))




(defn joined

  ""

  ^Joined

  [{:as   opts
    :keys [serializer
           serializer-key
           serializer-value
           serializer-value-left
           serializer-value-right
           deserializer
           deserializer-key
           deserializer-value
           deserializer-value-left
           deserializer-value-right]}]

  (Joined/with (serde (or serializer-key
                          serializer)
                      (or deserializer-key
                          deserializer))
               (serde (or serializer-value-left
                          serializer-value
                          serializer)
                      (or deserializer-value-left
                          deserializer-value
                          deserializer))
               (serde (or serializer-value-right
                          serializer-value
                          serializer)
                      (or deserializer-value-right
                          deserializer-value
                          deserializer))))




(defn initializer

  ""

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




(defn aggregator

  ""

  ^Aggregator

  [f]

  (reify

    Aggregator

      (apply [_ k v agg]
        (f agg
           k
           v))))




(defn reducer

  ""

  ^Reducer

  [f]

  (reify
 
    Reducer

      (apply [_ acc v]
        (f acc
           v))))




(defn transformer

  ""

  ^Transformer

  [{init'      :init
    transform' :transform
    close'     :close}]

  (let [init'2      (or init'
                        -no-op)
        transform'2 (or transform'
                        -no-op)
        close'2     (or close'
                        -no-op)
        v*ctx       (volatile! nil)]
    (reify

      Transformer

        (init [_ ctx]
          (vreset! v*ctx
                   ctx)
          (init'2 ctx))


        (transform [_ k v]
          (key-value (transform'2 (merge (-record-from-ctx @v*ctx)
                                         {:key   k
                                          :value v}))))


        (close [_]
          (close'2)))))




(defn transformer-supplier

  ""

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




(defn value-transformer

  ""

  ^ValueTransformer

  [{init'      :init
    transform' :transform
    close'     :close}]

  (let [init'2      (or init'
                        -no-op)
        transform'2 (or transform'
                        -no-op)
        close'2     (or close'
                        -no-op)
        v*ctx       (volatile! nil)]
    (reify

      ValueTransformer

        (init [_ ctx]
          (vreset! v*ctx
                   ctx)
          (init'2 ctx))


        (transform [_ v]
          (transform'2 (assoc (-record-from-ctx @v*ctx)
                              :value
                              v)))


        (close [_]
          (close'2)))))




(defn value-transformer-supplier

  ""

  ^ValueTransformerSupplier

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




(defn foreach-action

  ""

  ^ForeachAction

  [f]

  (reify

    ForeachAction

      (apply [_ k v]
        (f k
           v))))




(defn time-windows

  ""

  ^TimeWindows

  [{:as   opts
    :keys [interval
           hop
           ^long retention]}]

  (let [window (TimeWindows/of interval)]
    (some->> hop
            (.advanceBy window))
    (some->> retention
             (.until window))
    window))




(defn session-windows

  ""

  ^SessionWindows

  [{:as   opts
    :keys [interval
           retention]}]

  (let [window (SessionWindows/with interval)]
    (some->> retention
             (.until window))
    window))




(defn window

  ""

  ^Window

  [start end]

  (Window. start
           end))




(defn windowed

  ""

  ^Windowed

  [k start end]

  (Windowed. k
             (window start
                     end)))
