(ns milena.interop.clj

  "Convert java objects to clojure data structures."

  {:author "Adam Helinski"}

  (:require [milena.interop :as M.interop])
  (:import (org.apache.kafka.common Node
                                    Metric
                                    MetricName
                                    TopicPartition
                                    PartitionInfo
                                    TopicPartitionInfo)
           (org.apache.kafka.common.config ConfigResource
                                           ConfigResource$Type)
           (org.apache.kafka.common.resource ResourceType
                                             Resource
                                             ResourceFilter)
           (org.apache.kafka.common.acl AclOperation
                                        AclPermissionType
                                        AccessControlEntry
                                        AccessControlEntryFilter
                                        AclBinding
                                        AclBindingFilter)
           (org.apache.kafka.clients.admin DescribeClusterResult
                                           TopicListing
                                           DescribeTopicsResult
                                           TopicDescription
                                           CreateTopicsResult
                                           DeleteTopicsResult
                                           CreatePartitionsResult
                                           DescribeConfigsResult
                                           AlterConfigsResult
                                           Config
                                           ConfigEntry
                                           DescribeAclsResult
                                           CreateAclsResult
                                           DeleteAclsResult
                                           DeleteAclsResult$FilterResults
                                           DeleteAclsResult$FilterResult)
           org.apache.kafka.clients.producer.RecordMetadata
           (org.apache.kafka.clients.consumer OffsetAndTimestamp
                                              ConsumerRecord
                                              ConsumerRecords)
           (org.apache.kafka.streams KafkaStreams$State
                                     TopologyDescription
                                     TopologyDescription$GlobalStore
                                     TopologyDescription$Processor
                                     TopologyDescription$Source
                                     TopologyDescription$Subtopology
                                     TopologyDescription$Node
                                     KeyValue)
           (org.apache.kafka.streams.kstream Window
                                             Windowed)
           (org.apache.kafka.streams.state KeyValueIterator
                                           WindowStoreIterator)))




;;;;;;;;;; org.apache.kafka.common.*


(defn node

  "@ n
     org.apache.kafka.common.Node

   => {:host
        Host name for this node.

       :port
        The port this node is listening on.

       :rack (nilable)
        The name of the rack this node is on.

       :id
        The numerical ID of this node.}"

  [^Node n]

  {:host  (.host n)
   :port  (.port n)
   :rack (.rack n)
   :id    (.id   n)})




(defn metric-name

  "@ m-n
     org.apache.kafka.common.MetricName

   => {:group
        Logical group name of the metrics to which this metric belongs.

       :name
        String name of the metric.

       :description
        String description for human consumption.

       :tags
        Map of kws to strings, additional key-values.}"

  [^MetricName m-n]

  {:group       (.group       m-n)
   :name        (.name        m-n)
   :description (.description m-n)
   :tags        (M.interop/keywordize-keys (.tags m-n))})




(defn metrics

  "@ metrics
     Metrics about a producer or a consumer.

   => Map of :metric-group/metric-name to data.

      + data
        {:description
          String description for human consumption.

         :tags
          Map of kws to strings, additional key-values.

         :value
          Numerical value of the metric.}

      Cf. `metric-name`"

  [metrics]

  (reduce (fn reduce-metrics [metrics' [^MetricName metric-name ^Metric metric]]
            (assoc metrics'
                   (keyword (.group metric-name)
                            (.name  metric-name))
                   {:description (.description metric-name)
                    :tags        (M.interop/keywordize-keys (.tags metric-name))
                    :value       (.value metric)}))
          {}
          metrics))




(defn topic-partition

  "@ tp
     org.apache.kafka.common.TopicPartition

   => [topic partition]

      + topic
        Topic name.

      + partition
        Partition number."

  [^TopicPartition tp]

  [(.topic tp) (.partition tp)])




(defn topic-partition-to-offset

  "@ tp->o
     Map of org.apache.kafka.common.TopicPartition to offset.

   => Map of [topic partition] to offset.
      Cf. `topic-partition`"

  [tp->o]

  (reduce (fn [offsets [tp offset]]
            (assoc offsets
                   (topic-partition tp)
                   offset))
          {}
          tp->o))




(defn partition-info 

  "@ p-i
     org.apache.kafka.common.PartitionInfo

   => {:topic
        Topic name.
  
       :partition
        Partition number.

       :leader
        Leader node of the partition of nil if none.
    
       :replicas
        List of replicating nodes for this partition, and which are in sync.}
  
  Cf. `node`"

  [^PartitionInfo pi]
  
  {:topic     (.topic pi)
   :partition (.partition pi)
   :leader    (node (.leader pi))
   :replicas  (let [synced (into #{}
                                 (.inSyncReplicas pi))]
                (map (fn map-replica [replica]
                       (let [replica' (node replica)]
                         (assoc replica'
                                :synced?
                                (contains? synced
                                           replica))))
                     (.replicas pi)))})




(defn topic-partition-info

  "@ tpo
     org.apache.kafka.common.TopicPartitionInfo

   => {:partition 
        Partition number.

       :leader
        Leader node of the partition or nil if none.

       :replicas
        List of replicating nodes for this partition, and which are in sync.}
  
   Cf. `node`"

  [^TopicPartitionInfo tpo]

  {:partition (.partition tpo)
   :leader    (node (.leader tpo))
   :replicas  (let [synced (into #{}
                                 (.isr tpo))]
                (map (fn map-replica [replica]
                       (let [replica' (node replica)]
                         (assoc replica'
                                :synced?
                                (contains? synced
                                           replica))))
                     (.replicas tpo)))})




;;;;;;;;;; org.apache.kafka.common.config.*


(defn config-resource$type

  "@ cr$t
     org.apache.kafka.common.config.ConfigResource.Type

   => One of :brokers
             :topics
             :unknown"

  [^ConfigResource$Type cr$t]

  (condp identical?
         cr$t
    ConfigResource$Type/BROKER  :brokers
    ConfigResource$Type/TOPIC   :topics
    ConfigResource$Type/UNKNOWN :unknown))




(defn config-entry

  "@ ce
     org.apache.kafka.clients.admin.ConfigEntry

   => {:name
        Name of the configuration.

       :value
        Value for this configuration.
  
       :default?
        Is this the default value ?
    
       :read-only?
        Is this configuration read only ?
    
       :sensitive?
        Is the value sensitive and ought to be hidden ?}"

  [^ConfigEntry ce]

  {:name       (.name        ce)
   :value      (.value       ce)
   :default?   (.isDefault   ce)
   :read-only? (.isReadOnly  ce)
   :sensitive? (.isSensitive ce)})




(defn config

  "@ c
     org.apache.kafka.clients.admin.Config

  => Map of :config-name to configuration.
     Cf. `config-entry`"

  [^Config c]

  (reduce (fn clojurify-entry [config entry]
            (let [entry' (config-entry entry)]
              (assoc config
                     (keyword (:name entry'))
                     entry')))
          {}
          (.entries c)))




;;;;;;;;;; org.apache.kafka.common.resource.*


(defn resource-type

  "@ rt
     org.apache.kafka.common.resource.ResourceType

   => One of :any
             :cluster
             :group
             :topic
             :transactional-id
             :unknown"

  [^ResourceType rt]

  (condp identical?
         rt
    ResourceType/ANY              :any
    ResourceType/CLUSTER          :cluster
    ResourceType/GROUP            :group
    ResourceType/TOPIC            :topic
    ResourceType/TRANSACTIONAL_ID :transactional-id
    ResourceType/UNKNOWN          :unknown))




(defn resource

  "@ r
     org.apache.kafka.common.resource.Resource

   => {:name
        Resource name.

       :type
        Resource type.
        Cf. `resource-type`}"

  [^Resource r]

  {:name     (.name r)
   :type     (resource-type (.resourceType r))})




(defn resource-filter

  "@ rf
     org.apache.kafka.common.resource.ResourceFilter

   => {:name (nilable)
        Resource name.

       :type
        Resource type.
        Cf. `resource-type`}"

  [^ResourceFilter rf]

  {:name (.name rf)
   :type (resource-type (.resourceType rf))})




;;;;;;;;;; org.apache.kafka.common.acl.*


(defn acl-operation

  "@ ao
     org.apache.kafka.common.acl.AclOperation

   => One of :all
             :alter
             :alter-config
             :any
             :cluster-action
             :create
             :delete
             :describe
             :describe-config
             :idempotent-write
             :read
             :unknown
             :write"

  [^AclOperation ao]

  (condp identical?
         ao
    AclOperation/ALL              :all
    AclOperation/ALTER            :alter
    AclOperation/ALTER_CONFIGS    :alter-config
    AclOperation/ANY              :any
    AclOperation/CLUSTER_ACTION   :cluster-action
    AclOperation/CREATE           :create
    AclOperation/DELETE           :delete
    AclOperation/DESCRIBE         :describe
    AclOperation/DESCRIBE_CONFIGS :describe-config
    AclOperation/IDEMPOTENT_WRITE :idempotent-write
    AclOperation/READ             :read
    AclOperation/UNKNOWN          :unknown
    AclOperation/WRITE            :write))




(defn acl-permission-type

  "@ apt
     org.apache.kafka.common.acl.AclPermissionType

   => One of :allow
             :any
             :deny
             :unknown"

  [^AclPermissionType apt]

  (condp identical?
         apt
    AclPermissionType/ALLOW   :allow
    AclPermissionType/ANY     :any
    AclPermissionType/DENY    :deny
    AclPermissionType/UNKNOWN :unknown))




(defn access-control-entry

  "@ ace
     org.apache.kafka.common.acl.AccessControlEntry

   => {:principal
    
       :host
        Host name.

       :permission
        Cf. `acl-permission-type`

       :operation
        Cf. `acl-operation`}"

  [^AccessControlEntry ace]

  {:principal       (.principal      ace)
   :host            (.host           ace)
   :permission      (.permissionType ace)
   :operation       (.operation      ace)})




(defn access-control-entry-filter

  "@ acef
     org.apache.kafka.common.acl.AccessControlEntryFilter

   => {:principal (nilable)

       :host (nilable)
        Host name or nil.

       :permission
        Cf. `acl-permission-type`

       :operation
        Cf. `acl-operation`}"

  [^AccessControlEntryFilter acef]

  {:principal      (.principal      acef)
   :host           (.host           acef)
   :permission      (.permissionType acef)
   :operation       (.operation      acef)})




(defn acl-binding

  "@ ab
     org.apache.kafka.common.acl.AclBinding

   => {:resource
        Cf. `resource`

       :access-control
        Cf. `access-control-entry`}"

  [^AclBinding ab]

  {:resource       (resource (.resource ab))
   :access-control (access-control-entry (.entry ab))})




(defn acl-binding-filter

  "@ abf
     org.apache.kafka.common.acl.AclBindingFilter

   => {:resource
        Cf. `resource-filter`

       :access-control
        Cf. `access-control-entry-filter`}"

  [^AclBindingFilter abf]

  {:resource       (resource-filter (.resourceFilter abf))
   :access-control (access-control-entry-filter (.entryFilter abf))})




;;;;;;;;;; org.apache.kafka.clients.admin.*


(defn describe-cluster-result

  "@ dcr
     org.apache.kafka.clients.admin.DescribeClusterResult
  
   => {:id
        Future, the id of the cluster.

       :nodes
        Future, a list of the cluster's nodes.

       :controller
        Future, the controller node.}
  
   Cf. `node`"

  [^DescribeClusterResult dcr]

  {:id         (.clusterId dcr)
   :nodes      (M.interop/future-proxy (.nodes dcr)
                                       (fn proxy-nodes [nodes]
                                         (map node
                                              nodes)))
   :controller (M.interop/future-proxy (.controller dcr)
                                       node)})




(defn topic-listings

  "@ tls
     List of org.apache.kafka.clients.admin.TopicListing
  
   => A map of topic-names to topic-data.
      
      + topic-data
        {:internal?
          Is this topic internal?}"

  [^TopicListing tls]

  (reduce (fn reduce-tls [topics ^TopicListing tl]
            (assoc topics
                   (.name tl)
                   {:internal? (.isInternal tl)}))
          {}
          tls))




(defn topic-description

  "@ td
     org.apache.kafka.clients.admin.TopicDescription
  
   => {:internal?
        Is this topic internal ?

       :partitions
        A list of topic-partitions sorted by partition number.

        Cf. `topic-partition-info`}"

  [^TopicDescription td]

  {:internal?  (.isInternal td)
   :partitions (map topic-partition-info
                    (.partitions td))})




(defn topic-descriptions

  "@ tds
     List of org.apache.kafka.clients.admin.TopicDescription
  
   => A map of topic-name to future topic description.

      Cf. `topic-description`"

  [tds]

  (reduce (fn reduce-tds [topics [topic-name f*topic-description]]
            (assoc topics
                   topic-name
                   (M.interop/future-proxy f*topic-description
                                           topic-description)))
          {}
          tds))




(defn describe-topics-result

  "@ dtr
     org.apache.kafka.clients.admin.DescribeTopicsResult
  
   => Topic descriptions.
      
      Cf. `topic-descriptions`"

  [^DescribeTopicsResult dtr]

  (topic-descriptions (.values dtr)))




(defn create-topics-result

  "@ ctr
     org.apache.kafka.clients.admin.CreateTopicsResult
  
   => Map of topic-name to future throwing on deref if an error occured."

  [^CreateTopicsResult ctr]

  (into {}
        (.values ctr)))




(defn delete-topics-result

  "@ dtr
     org.apache.kafka.clients.admin.DeleteTopicsResult
  
   => Map of topic-name to future throwing on deref if an error occured."

  [^DeleteTopicsResult dtr]

  (into {}
        (.values dtr)))




(defn create-partitions-result

  "@ cpr
     org.apache.kafka.clients.admin.CreatePartitionsResult

   => Map of topic name to future throwing on deref if an error occured."

  [^CreatePartitionsResult cpr]

  (into {}
        (.values cpr)))




(defn- -configs-result

  "Helper for `describe-configs-result`
              `alter-configs-result`"

  [f results]

  (reduce (fn by-type [configs [^ConfigResource cr v]]
            (let [type (config-resource$type (.type cr))]
              (update configs
                      type
                      (fn add-resource [resources]
                        (assoc resources
                               (.name cr)
                               (f v))))))
          {}
          results))




(defn describe-configs-result

  "@ dcr
     org.apache.kafka.clients.admin.DescribeConfigsResult
  
   => Map of resource type to map of ressource name to future configuration map.
      Cf. `config-resource$type`"

  [^DescribeConfigsResult dcr]

  (-configs-result (fn future-config [f*config]
                     (M.interop/future-proxy f*config
                                             config))
                   (.values dcr)))




(defn alter-configs-result

  "@ acr
     org.apache.kafka.clients.admin.AlterConfigsResult
  
   => Map of resource type to map of resource name to future throwing on deref if an
      error occured.
      Cf. `config-resource$type`"

  [^AlterConfigsResult acr]

  (-configs-result identity
                   (.values acr)))




(defn describe-acls-result

  "@ dar
     org.apache.kafka.clients.admin.DescribeAclsResult

   => Future resolving to a list of ACLs.
      Cf. `acl-binding`"

  [^DescribeAclsResult dar]

  (M.interop/future-proxy (.values dar)
                          (fn map-acl-bindings [acl-bindings]
                            (map acl-binding
                                 acl-bindings))))




(defn create-acls-result

  "@ car
     org.apache.kafka.clients.admin.CreateAclsResult

   => Map of ACLs to future throwing on deref if an error occured.
      Cf. `acl-binding`"

  [^CreateAclsResult car]

  (reduce (fn reduce-acl-bindings [acl-bindings acl-binding' f*]
            (assoc acl-bindings
                   (acl-binding acl-binding')
                   f*))
          {}
          (.values car)))




(defn delete-acls-result$filter-result

  "@ dar$fr
     org.apache.kafka.clients.admin.DeleteAclsResult.FilterResult

   => {:exception (nilable)
        Exception or nil if no error occured.

       :acl (nilable)
        ACL of nil if an error occured.}"

  [^DeleteAclsResult$FilterResult dar$fr]

  {:exception (.exception dar$fr)
   :acl       (acl-binding (.binding dar$fr))})




(defn delete-acls-result$filter-results

  "@ dar$frs
     org.apache.kafka.clients.admin.DeleteAclsResult.FilterResult

   => List of results.
      Cf. `delete-acls-result$filter-result`"

  [^DeleteAclsResult$FilterResults dar$frs]

  (map delete-acls-result$filter-result
       (.values dar$frs)))




(defn delete-acls-result

  "@ dar
     org.apache.kafka.clients.admin.DeleteAclsResult

   => Map of ACL filters to futures resolving to results.
      Cf. `delete-acls-result$filter-result`"

  [^DeleteAclsResult dar]

  (reduce (fn reduce-filters [filters [abf f*results]]
            (assoc filters
                   (acl-binding-filter abf)
                   (M.interop/future-proxy f*results
                                           delete-acls-result$filter-results)))
          {}
          (.values dar)))




;;;;;;;;;; org.apache.kafka.clients.producer.*


(defn record-metadata

  "@ rm
     org.apache.kafka.clients.producer.RecordMetadata

   => {:topic
        Name of the topic the record was appended to.

       :?partition (nilable)
        Partition number of the topic or nil if unknown.

       :timestamp
        Unix timestamp of the new record.

       :offset
        Offset of the new record.}"

  [^RecordMetadata rm]

  {:topic     (.topic     rm)
   :partition (let [partition (.partition rm)]
                (when (not= partition
                            RecordMetadata/UNKNOWN_PARTITION)
                  partition))
   :timestamp (.timestamp rm)
   :offset    (.offset    rm)})




;;;;;;;;;; org.apache.kafka.clients.consumer.*


(defn offset-and-timestamp

  "@ oat
     org.apache.kafka.clients.consumer.OffsetAndTimestamp

   => {:timestamp
        Unix timestamp of the record.

       :offset
        Position in the partition.}"

  [^OffsetAndTimestamp oat]

  {:timestamp (.timestamp oat)
   :offset    (.offset    oat)})




(defn consumer-record

  "@ cr
     org.apache.kafka.clients.consumer.ConsumerRecord

   => {:topic
        Topic name.

       :partition
        Partition number.

       :offset
        Record offset.
    
       :timestamp
        Unix timestamp.

       :key
        Deserialized key.

       :value
        Deserialized value.}"

  [^ConsumerRecord cr]

  {:topic     (.topic     cr)
   :partition (.partition cr)
   :offset    (.offset    cr)
   :timestamp (.timestamp cr)
   :key       (.key       cr)
   :value     (.value     cr)})




(defn consumer-records-by-partitions

  "@ crs
     org.apache.kafka.clients.consumer.ConsumerRecords

   => Map of [topic partition] to list of records.
      Cf. `topic-partition`
          `consumer-record`"

  [^ConsumerRecords crs]

  (reduce (fn reduce-partitions [partitions ^TopicPartition tp]
            (assoc partitions
                   (topic-partition tp)
                   (map consumer-record
                        (.records crs
                                  tp))))
          {}
          (.partitions crs)))




;;;;;;;;;; org.apache.kafka.streams.*


;; TODO docstrings


(defn kafka-streams$state

  ""

  [^KafkaStreams$State ks$s]

  (condp identical?
         ks$s
    KafkaStreams$State/CREATED          :created
    KafkaStreams$State/ERROR            :error
    KafkaStreams$State/NOT_RUNNING      :not-running
    KafkaStreams$State/PENDING_SHUTDOWN :pending-shutdown
    KafkaStreams$State/REBALANCING      :rebalancing
    KafkaStreams$State/RUNNING          :running))




(defn- -topology-description$node--name

  ""

  [^TopologyDescription$Node n]

  (.name n))




(defn -topology-description$node--names

  ""

  [nodes]

  (into #{}
        (map -topology-description$node--name
             nodes)))




(defn topology-description$node

  ""

  [^TopologyDescription$Node n]

  {:name     (.name n)
   :parents  (-topology-description$node--names (.predecessors n))
   :children (-topology-description$node--names (.successors   n))})




(defn topology-description$processor

  ""

  [^TopologyDescription$Processor td$p]

  (assoc (topology-description$node td$p)
         :stores
         (into #{}
               (.stores td$p))))




(defn topology-description$source

  ""

  [^TopologyDescription$Source td$s]

  (assoc (topology-description$node td$s)
         :topics
         (.topics td$s)))




(defn topology-description$global-store

  ""

  [^TopologyDescription$GlobalStore td$gs]

  {:processor (topology-description$processor (.processor td$gs))
   :source    (topology-description$source    (.source    td$gs))})




(defn topology-description$subtopology

  ""

  [^TopologyDescription$Subtopology td$s]

  {:id    (.id td$s)
   :nodes (reduce (fn to-map [hmap node]
                    (assoc hmap
                           (:name node)
                           (dissoc node
                                   :name)))
                  {}
                  (map topology-description$node
                       (.nodes td$s)))})




(defn topology-description

  ""

  [^TopologyDescription tp]

  {:global-stores (into #{}
                        (map topology-description$global-store
                             (.globalStores tp)))
   :subtopologies (into #{}
                        (map topology-description$subtopology
                             (.subtopologies tp)))})




(defn key-value

  ""

  [^KeyValue kv]

  {:key   (.-key   kv)
   :value (.-value kv)})




(defn key-value--ws

  ""

  [^KeyValue kv]

  {:timestamp (.-key   kv)
   :value     (.-value kv)})




(declare windowed)




(defn key-value--windowed

  ""

  [^KeyValue kv]

  (assoc (windowed (.-key kv))
         :value
         (.-value kv)))




;;;;;;;;;; org.apache.kafka.streams.state.*


(defn key-value-iterator

  ""

  ([kvi]

   (key-value-iterator kvi
                       key-value))


  ([^KeyValueIterator kvi f]

   (lazy-seq
     (if (.hasNext kvi)
       (cons (f (.next kvi))
             (key-value-iterator kvi
                                 f))
       (.close kvi)))))




(defn key-value-iterator--windowed

  ""

  [kvi]

  (key-value-iterator kvi
                      key-value--windowed))




(defn window-store-iterator

  ""

  [wsi]

  (key-value-iterator wsi
                      key-value--ws))




;;;;;;;;;; org.apache.kafka.streams.kstream.*


(defn window

  ""

  [^Window w]

  {:start (.start w)
   :end   (.end   w)})




(defn windowed

  ""

  [^Windowed w]

  (assoc (window (.window w))
         :key
         (.key w)))
