(ns milena.interop.java

  "Convert clojure data structures to java objects"

  {:author "Adam Helinski"}

  (:require [milena.interop     :as $.interop]
            [milena.interop.clj :as $.interop.clj])
  (:import org.apache.kafka.common.TopicPartition
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
           (org.apache.kafka.clients.admin DescribeClusterOptions
                                           ListTopicsOptions
                                           DescribeTopicsOptions
                                           NewTopic
                                           CreateTopicsOptions
                                           DeleteTopicsOptions
                                           DescribeConfigsOptions
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
                                              OffsetCommitCallback)))




;;;;;;;;;; Helpers


(defn at-least-0

  "Converts a number to a positive int, a negative value resulting in 0."

  [?n]

  (when (number? ?n)
    (int (max ?n
              0))))




;;;;;;;;;; org.apache.kafka.common


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




;;;;;;;;;; org.apache.kafka.common.config


(defn config-resource$type

  "@ ?kw
     One of :unknown or nil
            :brokers
            :topics

   => org.apache.kafka.common.config.ConfigResource$Type"

  ^ConfigResource$Type

  ([]

   ConfigResource$Type/UNKNOWN)


  ([?kw]

   (condp identical?
          ?kw
     nil      ConfigResource$Type/UNKNOWN
     :unknown ConfigResource$Type/UNKNOWN
     :brokers ConfigResource$Type/BROKER
     :topics  ConfigResource$Type/TOPIC)))




(defn config-resource

  "@ name
     Resource name
  
   @ ?type
     Resource type.
     Cf. `config-resource$type`
  
   => org.apache.kafka.common.config.ConfigResource
  

   Ex. (config-resource \"my-topic\"
                        :topic)"

  ^ConfigResource

  ([name]

   (ConfigResource. ConfigResource$Type/UNKNOWN
                    name))


  ([name ?type]

   (ConfigResource. (config-resource$type ?type)
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




;;;;;;;;;; org.apache.kafka.common.resource


(defn resource-type

  "@ ?kw
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


  ([?kw]

   (condp identical?
          ?kw
     nil               ResourceType/ANY
     :any              ResourceType/ANY
     :cluster          ResourceType/CLUSTER
     :group            ResourceType/GROUP
     :topic            ResourceType/TOPIC
     :transactional-id ResourceType/TRANSACTIONAL_ID
     :unknown          ResourceType/UNKNOWN)))




(defn resource

  "@ ?args
     {:name
       Resource name.

      :?type
       Cf. `resource-type`}

   => org.apache.kafka.common.resource.Resource


   Ex. (resource {:name  \"my-topic\"
                  :?type :topic})"

  ^Resource

  ([{:as   ?args
     :keys [name
            ?type]}]

   (resource name
             ?type))


  ([name ?type]

   (Resource. (resource-type ?type)
              name)))




(defn resource-filter

  "@ ?args
     {:?name
       Resource name.

      :?type
       Resource type.
       Cf. `resource-type`}

   => org.apache.kafka.common.resource.ResourceFilter
  
   Ex. (resource-filter)
  
       (resource-filter {:?name \"my-topic\"
                         :?type :topic})"

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




;;;;;;;;;; org.apache.kafka.common.acl


(defn acl-operation

  "@ ?kw
     One of :all              
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

  [?kw]

  (condp identical?
         ?kw
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

  "@ ?kw
     One of :any or nil
            :allow
            :deny
            :unknown

   => org.apache.kafka.common.acl.AclPermissionType"

  ^AclPermissionType

  [?kw]

  (condp identical?
         ?kw
    nil      AclPermissionType/ANY
    :any     AclPermissionType/ANY
    :allow   AclPermissionType/ALLOW
    :deny    AclPermissionType/DENY
    :unknown AclPermissionType/UNKNOWN))




(defn access-control-entry

  "@ args
     {:principal

      :?host
       The host name where \"*\" involves all hosts.

      :?permission
       Cf. `acl-permission-type`

      :?operation
       Cf. `acl-operation`}
         

   => org.apache.kafka.common.acl.AccessControlEntry"

  ^AccessControlEntry

  ([{:as   args
     :keys [principal
            ?host
            ?permission
            ?operation]
     :or   {?host            "*"
            ?operation       :all
            ?permission-type :any}}]

   (access-control-entry principal
                         ?host
                         ?permission
                         ?operation))


  ([principal ?host ?permission ?operation]

   (AccessControlEntry. principal
                        (or ?host
                            "*")
                        (acl-operation ?operation)
                        (acl-permission-type ?permission))))




(defn access-control-entry-filter

  "@ ?args
     {:?principal
      
      :?host
       The host name where \"*\" involves any host.

      :?permission
       Cf. `acl-permission-type`

      :?operation
       Cf. `acl-operation`}


  
   => org.apache.kafka.common.acl.AccessControlEntryFilter

  
   Ex. (access-control-entry-filter {:?permission :allow
                                     :?operation  :create})"

  ^AccessControlEntryFilter

  ([]

   AccessControlEntryFilter/ANY)


  ([{:as   ?args
     :keys [?principal
            ?host
            ?permission
            ?operation]
     :or   {?host            "*"
            ?operation       :all
            ?permission-type :any}}]

   (access-control-entry-filter ?principal
                                ?host
                                ?permission
                                ?operation))


  ([?principal ?host ?permission ?operation]

   (AccessControlEntryFilter. ?principal
                              (or ?host
                                  "*")
                              (acl-operation ?operation)
                              (acl-permission-type ?permission))))




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

  "@ ?args
     {:?resource
       Cf. `resource-filter`

      :?access-control
       Cf. `access-control-entry-filter`}

   => org.apache.kafka.common.acl.AclBindingFilter
  
   
   Ex. (acl-binding-filter {:?resource       {:?name \"my-topic\"
                                              :?type :topic}
                            :?access-control {:?permission :allow
                                              :?operation  :alter}})"

  ^AclBindingFilter

  ([]

   AclBindingFilter/ANY)


  ([{:as   ?args
     :keys [?resource
            ?access-control]}]

   (acl-binding-filter ?resource
                       ?access-control))


  ([?resource ?access-control]

   (AclBindingFilter. (resource-filter ?resource)
                      (access-control-entry-filter ?access-control))))




;;;;;;;;;; org.apache.kafka.clients.admin


(defn describe-cluster-options

  "Options for `milena.admin/cluster`.

   @ ?args
     {:?timeout-ms
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.}
  
   => org.apache.kafka.clients.admin.DescribeClusterOptions object"

  ^DescribeClusterOptions

  [{:as   ?args
    :keys [?timeout-ms]}]

  (doto (DescribeClusterOptions.)
    (.timeoutMs (at-least-0 ?timeout-ms))))




(defn list-topics-options

  "Options for `milena.admin/topics`.

   @ ?args
     {:?timeout-ms
        An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
        if the values couldn't be retrieved within this timeout.

      :internal?
       Should internal topics be retrieved ?}

   => org.apache.kafka.clients.admin.ListTopicsOptions object"

  ^ListTopicsOptions

  ([{:as  ?args
     :keys [?timeout-ms
            internal?]
     :or   {internal? false}}]

   (list-topics-options ?timeout-ms
                        internal?))


  ([?timeout-ms internal?]

   (doto (ListTopicsOptions.)
     (.timeoutMs    (at-least-0 ?timeout-ms))
     (.listInternal internal?))))




(defn describe-topics-options

  "Options for `milena.admin/topics-decribe`.
  
   @ ?args
     {:?timeout-ms
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.}
  
   => org.apache.kafka.clients.admin.DescribeTopicsOptions object"

  ^DescribeTopicsOptions

  [{:as   ?args
    :keys [?timeout-ms]}]

  (doto (DescribeTopicsOptions.)
    (.timeoutMs (at-least-0 ?timeout-ms))))




(defn new-topic

  "For creating new topics via `milena.admin/topics-create`.

   A topic can be created either by specifying the number of partitions and the replication factor, either
   by specyfying directly the assignments.


   @ topic
       The topic name

   @ ?args
       {:?partitions
         The number of partition for this new topic

        :?replication-factor
         The replication factor for each partition

        :?assigments
         A map of partition number -> list of replica ids (i.e. broker ids).

         Although not enforced, it is generally a good idea for all partitions to have
         the same number of replicas.

        :?config
         Kafka configuration

         Cf. https://kafka.apache.org/documentation/#topicconfigs}

   => org.apache.kafka.clients.admin.NewTopic object
  

   Ex. (new-topic \"topic-name\"
                  {:?partitions        4
                   :?replication-facor 3})"

  ^NewTopic

  [topic {:as   ?args
          :keys [?partitions
                 ?replication-factor
                 ?assignments
                 ?config]
          :or   {?partitions         1
                 ?replication-factor 1}}]

  (let [new-topic (if ?assignments
                    (NewTopic. topic
                               ?assignments)
                    (NewTopic. topic
                               ?partitions
                               ?replication-factor))]
    (when ?config
      (.configs new-topic
                ($.interop/stringify-keys ?config)))
    new-topic))




(defn create-topics-options

  "Options for `milena.admin/topics-create`.
  
   @ ?args
     {:?timeout-ms
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.

      :?fake
       Validate request without actually creating the topic for real ?}
  
   => org.apache.kafka.clients.admin.CreateTopicsOptions"

  ^CreateTopicsOptions

  ([{:as   ?args
     :keys [?timeout-ms
            fake?]}]

   (create-topics-options ?timeout-ms
                          fake?))

  ([?timeout-ms fake?]

   (doto (CreateTopicsOptions.)
     (.timeoutMs    (at-least-0 ?timeout-ms))
     (.validateOnly fake?))))




(defn delete-topics-options

  "Options for `milena.admin/topics-delete`.
  
   @ ?args
     {:?timeout-ms
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.}
  
   => org.apache.kafka.clients.admin.DeleteTopicsOptions"

  ^DeleteTopicsOptions

  [{:as   ?args
    :keys [?timeout-ms]}]

  (doto (DeleteTopicsOptions.)
    (.timeoutMs (at-least-0 ?timeout-ms))))




(defn describe-configs-options

  "Options for `milena.admin/config`.
  
   @ ?args
     {:?timeout-ms
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.}
  
   => org.apache.kafka.clients.admin.DescribeConfigsOptions"

  ^DescribeConfigsOptions

  [{:as   ?args
    :keys [?timeout-ms]}]

  (doto (DescribeConfigsOptions.)
    (.timeoutMs (at-least-0 ?timeout-ms))))




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

  "@ ?args
     {:?timeout-ms
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.

      :?fake
       Validate request without actually creating the topic for real ?}
  
   => org.apache.kafka.clients.admin.AlterConfigsOptions"

  ^AlterConfigsOptions

  ([{:as   ?args
     :keys [?timeout-ms
            fake?]
     :or   {fake? false}}]

   (alter-configs-options ?timeout-ms
                          fake?))


  ([?timeout-ms fake?]

   (doto (AlterConfigsOptions.)
     (.timeoutMs    (at-least-0 ?timeout-ms))
     (.validateOnly fake?))))




(defn describe-acls-options

  "@ ?args
     {:?timeout-ms
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.}

   => org.apache.kafka.clients.admin.DescribeAclsOptions"

  ^DescribeAclsOptions

  [{:as   ?args
    :keys [?timeout-ms]}]

  (doto (DescribeAclsOptions.)
    (.timeoutMs (at-least-0 ?timeout-ms))))




(defn create-acls-options

  "@ ?args
     {:?timeout-ms
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.}

   => org.apache.kafka.clients.admin.CreateAclsOptions"

  ^CreateAclsOptions

  [{:as   ?args
    :keys [?timeout-ms]}]

  (doto (CreateAclsOptions.)
    (.timeoutMs (at-least-0 ?timeout-ms))))




(defn delete-acls-options

  "@ ?args
     {:?timeout-ms
       An optional timeout in milliseconds, futures will throw a org.apache.kafka.common.errors.TimeoutException
       if the values couldn't be retrieved within this timeout.}

   => org.apache.kafka.clients.admin.DeleteAclsOptions"

  ^DeleteAclsOptions

  [{:as   ?args
    :keys [?timeout-ms]}]

  (doto (DeleteAclsOptions.)
    (.timeoutMs (at-least-0 ?timeout-ms))))




;;;;;;;;;; org.apache.kafka.clients.producer


(defn producer-record

  "@ record
     {:topic
       Topic name.

      :?partition
       Optional partition number.

      :?key
       Optional key.

      :?value
       Optional value.

      :?timestamp
       Optional unix timestamp.

   =>  org.apache.kafka.clients.producer.ProducerRecord
  
  
   Ex. (producer-record {:topic  \"my-topic\"
                         :?key   \"some-key\"
                         :?value 42})"

  ^ProducerRecord

  [{:as   record
    :keys [topic
           ?partition
           ?key
           ?value
           ?timestamp]}]

  (ProducerRecord. topic
                   (some-> ?partition
                           int)
                   ?timestamp
                   ?key
                   ?value
                   nil))




(defn callback

  "@ f
     Fn taking an exception and a map representing metadata about the record that has been sent.
     Only one of them will be non-nil.
     Cf. `milena.interop.clj/record-metadata

  => org.apache.kafka.clients.producer.Callback


  Ex. (callback (fn [?exception ?meta]
                  (when-not ?exception
                    (println :committed ?meta))))"

  ^Callback

  [f]

  (reify
    
    Callback

      (onCompletion [_ r-m exception]
        (f exception
           (some-> r-m
                   ($.interop.clj/record-metadata))))))




;;;;;;;;;; org.apache.kafka.clients.consumer


(defn consumer-rebalance-listener

  "@ f
     Fn taking assigned? and a list of [topic partition].

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
           (map $.interop.clj/topic-partition
                tps)))


      (onPartitionsRevoked [_ tps]
        (f false
           (map $.interop.clj/topic-partition
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
     Fn taking ?exception and ?offsets, one of them being nil depending whether an error occured or not.
   
     + ?offsets
       Map of [topic partition] to offsets.

   => org.apache.kafka.clients.consumer.OffsetCommitCallback"

  ^OffsetCommitCallback

  [f]

  (reify
    
    OffsetCommitCallback
      
      (onComplete [_ ?offsets ?exception]
        (f ?exception
           (reduce (fn [offsets' [tp ^OffsetAndMetadata om]]
                     (assoc offsets'
                            ($.interop.clj/topic-partition tp)
                            (.offset om)))
                   {}
                   ?offsets)))))
