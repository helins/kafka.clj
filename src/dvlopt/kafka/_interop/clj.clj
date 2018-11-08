(ns dvlopt.kafka.-interop.clj

  "Convert java objects to clojure data structures."

  {:author "Adam Helinski"}

  (:require [dvlopt.kafka          :as K]
            [dvlopt.kafka.-interop :as K.-interop]
            [dvlopt.void           :as void])
  (:import (org.apache.kafka.clients.admin AlterConfigsResult
                                           Config
                                           ConfigEntry
                                           ConsumerGroupDescription
                                           ConsumerGroupListing
                                           CreateAclsResult
                                           CreatePartitionsResult
                                           CreateTopicsResult
                                           DeleteAclsResult
                                           DeleteAclsResult$FilterResult
                                           DeleteAclsResult$FilterResults
                                           DeletedRecords
                                           DeleteRecordsResult
                                           DeleteTopicsResult
                                           DescribeAclsResult
                                           DescribeConfigsResult
                                           DescribeConsumerGroupsResult
                                           DescribeClusterResult
                                           DescribeTopicsResult
                                           ListConsumerGroupOffsetsResult
                                           ListConsumerGroupsResult
                                           MemberAssignment
                                           MemberDescription
                                           TopicDescription
                                           TopicListing)
           (org.apache.kafka.clients.consumer ConsumerRecord
                                              ConsumerRecords
                                              OffsetAndMetadata
                                              OffsetAndTimestamp)
           org.apache.kafka.clients.producer.RecordMetadata
           (org.apache.kafka.common ConsumerGroupState
                                    Metric
                                    MetricName
                                    Node
                                    PartitionInfo
                                    TopicPartition
                                    TopicPartitionInfo)
           (org.apache.kafka.common.acl AclOperation
                                        AclPermissionType
                                        AccessControlEntry
                                        AccessControlEntryFilter
                                        AclBinding
                                        AclBindingFilter)
           (org.apache.kafka.common.config ConfigResource
                                           ConfigResource$Type)
           (org.apache.kafka.common.header Header
                                           Headers)
           (org.apache.kafka.common.resource PatternType
                                             ResourcePattern
                                             ResourcePatternFilter
                                             ResourceType)
           (org.apache.kafka.streams KafkaStreams$State
                                     KeyValue
                                     TopologyDescription
                                     TopologyDescription$GlobalStore
                                     TopologyDescription$Node
                                     TopologyDescription$Processor
                                     TopologyDescription$Source
                                     TopologyDescription$Subtopology)
           (org.apache.kafka.streams.kstream Window
                                             Windowed)
           (org.apache.kafka.streams.processor Cancellable
                                               RecordContext)
           (org.apache.kafka.streams.state KeyValueIterator
                                           WindowStoreIterator)
           java.lang.AutoCloseable
           java.util.Iterator))




;;;;;;;;;; org.apache.kafka.common.*


(defn consumer-group-state

  ;; Cf. `consumer-group-description`

  [^ConsumerGroupState cgs]

  (condp identical?
         cgs
    ConsumerGroupState/COMPLETING_REBALANCE :complete-rebalance
    ConsumerGroupState/DEAD                 :dead
    ConsumerGroupState/EMPTY                :empty
    ConsumerGroupState/PREPARING_REBALANCE  :preparing-rebalance
    ConsumerGroupState/STABLE               :stable
    ConsumerGroupState/UNKNOWN              :unknown))




(defn node

  ;; Used quite a few times.

  [^Node n]

  (void/assoc-some {::K/host (.host n)
                    ::K/id   (.id   n)
                    ::K/port (.port n)}
                   ::K/rack (.rack n)))




(defn metrics

  ;; Cf. `dvlopt.kafka.produce/metrics` 

  [metrics]

  (reduce (fn reduce-metrics [metrics' [^MetricName metric-name ^Metric metric]]
            (let [group (.group metric-name)]
              (assoc metrics'
                     group
                     (assoc (get metrics'
                                 group
                                 {})
                            (.name metric-name)
                            (void/assoc-some {::K/floating-value (.value metric)
                                              ::K/properties     (into {} (.tags metric-name))}
                                             ::K/description (not-empty (.description metric-name)))))))
          {}
          metrics))




(defn topic-partition

  ;; Used very often, about everywhere.

  [^TopicPartition tp]

  [(.topic tp) (.partition tp)])




(defn topic-partition->offset

  ;; Cf. `dvlopt.kafka.consume/beginning-offsets`

  [tp->o]

  (reduce (fn [offsets [tp offset]]
            (assoc offsets
                   (topic-partition tp)
                   offset))
          {}
          tp->o))




(defn partition-info 

  ;; Cf. `dvlopt.kafka.produce/partitions`

  [^PartitionInfo pi]
  
  (void/assoc-some {::K/partition     (.partition pi)
                    ::K/replica-nodes (let [synced (into #{}
                                                         (.inSyncReplicas pi))]
                                        (map (fn map-replica [replica]
                                               (let [replica' (node replica)]
                                                 (assoc replica'
                                                        ::K/synced?
                                                        (contains? synced
                                                                   replica))))
                                             (.replicas pi)))
                    ::K/topic         (.topic pi)}
                   ::K/leader-node (node (.leader pi))))




(defn topic-partition-info

  ;; Cf. `dvlopt.kafka.admin/describe-topics`

  [^TopicPartitionInfo tpo]

  {::K/leader-node   (node (.leader tpo))
   ::K/partition     (.partition tpo)
   ::K/replica-nodes (let [synced (into #{}
                                        (.isr tpo))]
                       (map (fn map-replica [replica]
                              (let [replica' (node replica)]
                                (assoc replica'
                                       ::K/synced?
                                       (contains? synced
                                                  replica))))
                            (.replicas tpo)))})




;;;;;;;;;; org.apache.kafka.common.config.*


(defn config-resource$type

  ;; Cf. `-configs-result`

  [^ConfigResource$Type cr$t]

  (condp identical?
         cr$t
    ConfigResource$Type/BROKER  ::K/brokers
    ConfigResource$Type/TOPIC   ::K/topics
    ConfigResource$Type/UNKNOWN ::K/unknown))




(defn config-entry

  ;; Cf. `config`

  [^ConfigEntry ce]

  {::K/default?                   (.isDefault   ce)
   ::K/name                       (.name        ce)
   ::K/read-only?                 (.isReadOnly  ce)
   ::K/string-value               (.value       ce)
   :dvlopt.kafka.admin/sensitive? (.isSensitive ce)})




(defn config

  ;; Cf. `describe-configs-result`

  [^Config c]

  (reduce (fn clojurify-entry [config entry]
            (let [entry' (config-entry entry)]
              (assoc config
                     (::K/name entry')
                     entry')))
          {}
          (.entries c)))




;;;;;;;;;; org.apache.kafka.common.header.*


(defn header

  ;; Used for consuming records.

  [^Header h]

  (if-some [v (.value h)]
    [(.key h)
     v]
    [(.key h)]))




(defn headers

  ;; Used for consuming records.

  [^Headers hs]

  (not-empty (map header
                  (.toArray hs))))




;;;;;;;;;; org.apache.kafka.common.resource.*


(defn pattern-type

  [^PatternType pt]

  (condp identical?
         pt
    nil
    PatternType/ANY      :any
    PatternType/LITERAL  :exactly
    PatternType/MATCH    :match
    PatternType/PREFIXED :prefixed
    PatternType/UNKNOWN  :unknown))




(defn resource-type

  ;; Cf. `resource-pattern`
  ;;     `resource-filter`

  [^ResourceType rt]

  (condp identical?
         rt
    ResourceType/ANY              :any
    ResourceType/CLUSTER          :cluster
    ResourceType/GROUP            :group
    ResourceType/TOPIC            :topic
    ResourceType/TRANSACTIONAL_ID :transactional-id
    ResourceType/UNKNOWN          :unknown))




(defn resource-pattern

  ;; Cf. `acl-binding`

  [^ResourcePattern rp]

  {:dvlopt.kafka.admin/name-pattern  [(pattern-type (.patternType rp))
                                      (.name rp)]
   :dvlopt.kafka.admin/resource-type (resource-type (.resourceType rp))})




(defn resource-pattern-filter

  ;; Cf. `acl-binding-filter`

  [^ResourcePatternFilter rpf]

  {:dvlopt.kafka.admin/name-pattern  [(pattern-type (.patternType rpf))
                                      (.name rpf)]
   :dvlopt.kafka.admin/resource-type (resource-type (.resourceType rpf))})




;;;;;;;;;; org.apache.kafka.common.acl.*


(defn acl-operation

  [^AclOperation ao]

  (condp identical?
         ao
    AclOperation/ALL              :all
    AclOperation/ALTER            :alter
    AclOperation/ALTER_CONFIGS    :alter-configuration
    AclOperation/ANY              :any
    AclOperation/CLUSTER_ACTION   :cluster-action
    AclOperation/CREATE           :create
    AclOperation/DELETE           :delete
    AclOperation/DESCRIBE         :describe
    AclOperation/DESCRIBE_CONFIGS :describe-configuration
    AclOperation/IDEMPOTENT_WRITE :idempotent-write
    AclOperation/READ             :read
    AclOperation/UNKNOWN          :unknown
    AclOperation/WRITE            :write))




(defn acl-permission-type

  [^AclPermissionType apt]

  (condp identical?
         apt
    AclPermissionType/ALLOW   :allow
    AclPermissionType/ANY     :any
    AclPermissionType/DENY    :deny
    AclPermissionType/UNKNOWN :unknown))




(defn access-control-entry

  ;; Cf. `acl-binding`

  [^AccessControlEntry ace]

  {::K/host                       (.host ace)
   :dvlopt.kafka.admin/operation  (acl-operation (.operation ace))
   :dvlopt.kafka.admin/permission (acl-permission-type (.permissionType ace))
   :dvlopt.kafka.admin/principal  (.principal ace)})




(defn access-control-entry-filter

  ;; Cf. `acl-binding-filter`

  [^AccessControlEntryFilter acef]

  (void/assoc-some {:dvlopt.kafka.admin/operation  (acl-operation (.operation acef))
                    :dvlopt.kafka.admin/permission (acl-permission-type (.permissionType acef))}
                   ::K/host                      (.host acef)
                   :dvlopt.kafka.admin/principal (.principal acef)))




(defn acl-binding

  ;; Cf. `describe-acls-result`
  ;;     `create-acls-result`

  [^AclBinding ab]

  (merge (access-control-entry (.entry ab))
         (resource-pattern (.pattern ab))))




(defn acl-binding-filter

  ;; Cf. `delete-acls-result`

  [^AclBindingFilter abf]

  (merge (access-control-entry-filter (.entryFilter abf))
         (resource-pattern-filter (.patternFilter abf))))




;;;;;;;;;; org.apache.kafka.clients.admin.*


(defn describe-cluster-result

  ;; Cf. `dvlopt.kafka.admin/cluster`

  [^DescribeClusterResult dcr]

  {::K/f*controller-node (K.-interop/future-proxy (.controller dcr)
                                                  node)
   ::K/f*id-string       (.clusterId dcr)
   ::K/f*nodes           (K.-interop/future-proxy (.nodes dcr)
                                                  (fn proxy-nodes [nodes]
                                                    (map node
                                                        nodes)))})




(defn topic-listings

  ;; Cf. `dvlopt.kafka.admin/topics`
  
  [^TopicListing tls]

  (reduce (fn reduce-tls [topics ^TopicListing tl]
            (assoc topics
                   (.name tl)
                   {::K/internal? (.isInternal tl)}))
          {}
          tls))




(defn topic-description

  ;; Cf. `describe-topics-result`

  [^TopicDescription td]

  {::K/internal?                              (.isInternal td)
   :dvlopt.kafka.admin/partition-descriptions (map topic-partition-info
                                                   (.partitions td))})





(defn describe-topics-result
  
  ;; Cf. `dvlopt.kafka.admin/describe-topics`

  [^DescribeTopicsResult dtr]

  (reduce (fn reduce-tds [topics [topic-name f*topic-description]]
            (assoc topics
                   topic-name
                   (K.-interop/future-proxy f*topic-description
                                            topic-description)))
          {}
          (.values dtr)))




(defn create-topics-result

  ;; Cf. `dvlopt.kafka.admin/create-topics`

  [^CreateTopicsResult ctr]

  (into {}
        (.values ctr)))




(defn delete-topics-result

  ;; Cf. `dvlopt.kafka.admin/delete-topics`

  [^DeleteTopicsResult dtr]

  (into {}
        (.values dtr)))




(defn create-partitions-result

  ;; Cf. `dvlopt.kafka.admin/increase-partitions`

  [^CreatePartitionsResult cpr]

  (into {}
        (.values cpr)))




(defn deleted-records

  [^DeletedRecords dr]

  {::K/lowest-offset (.lowWatermark dr)})




(defn delete-records-result

  ;; Cf. `dvlopt.kafka.admin/delete-records`

  [^DeleteRecordsResult drr]

  (reduce (fn convert-water-marks [hmap [tp f*dr]]
            (assoc hmap
                   (topic-partition tp)
                   (K.-interop/future-proxy f*dr
                                            deleted-records)))
          {}
          (.lowWatermarks drr)))




(defn- -configs-result

  ;; Helper for `describe-configs-result`
  ;;            `alter-configs-result`

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

  ;; Cf. `dvlopt.kafka.admin/configuration`

  [^DescribeConfigsResult dcr]

  (-configs-result (fn future-config [f*config]
                     (K.-interop/future-proxy f*config
                                              config))
                   (.values dcr)))




(defn alter-configs-result

  ;; Cf. `dvlopt.kafka.admin/alter-configuration`

  [^AlterConfigsResult acr]

  (-configs-result identity
                   (.values acr)))




(defn consumer-group-listing

  [^ConsumerGroupListing cgl]

  {:dvlopt.kafka.admin/consumer-group.simple? (.isSimpleConsumerGroup cgl)})




(defn list-consumer-groups-result

  ;; Cf. `dvlopt.kafka.admin/consumer-groups`

  [^ListConsumerGroupsResult lcgr]

  (K.-interop/future-proxy (.all lcgr)
                           (fn convert-consumer-groups [consumer-groups]
                             (reduce (fn convert-consumer-group [consumer-groups' ^ConsumerGroupListing cgl]
                                       (assoc consumer-groups'
                                              (.groupId cgl)
                                              (consumer-group-listing cgl)))
                                     {}
                                     consumer-groups))))




(defn member-assignment

  ;; Cf. `member-description`

  [^MemberAssignment ma]

  {::K/topic-partitions (into #{}
                              (map topic-partition
                                   (.topicPartitions ma)))})




(defn member-description

  ;; Cf. `consumer-group-description`

  [^MemberDescription md]

  {::K/client-id                                  (.clientId md)
   ::K/consumer-id                                (.consumerId md)
   ::K/host                                       (.host md)
   :dvlopt.kafka.admin/consumer-group.assignments (member-assignment (.assignment md))})




(defn consumer-group-description

  ;; Cf. `describe-consumer-groups-result`

  [^ConsumerGroupDescription cgd]

  (void/assoc-some {::K/coordinator-node                       (node (.coordinator cgd))
                    ::K/group-id                               (.groupId cgd)
                    :dvlopt.kafka.admin/consumer-group.simple? (.isSimpleConsumerGroup cgd)
                    :dvlopt.kafka.admin/consumer-group.state   (consumer-group-state (.state cgd))}
                    :dvlopt.kafka.admin/consumer-group.members (not-empty (map member-description
                                                                               (.members cgd)))
                    :dvlopt.kafak.admin/partition-assignor     (not-empty (.partitionAssignor cgd))))




(defn describe-consumer-groups-result

  ;; Cf. `dvlopt.kafka.admin/describe-consumer-groups`

  [^DescribeConsumerGroupsResult dcgr]

  (reduce (fn to-map [hmap [consumer-group future-cgd]]
            (assoc hmap
                   consumer-group
                   (K.-interop/future-proxy future-cgd
                                            consumer-group-description)))
          {}
          (.describedGroups dcgr)))




(defn list-consumer-group-offsets-result

  ;; Cf. `dvlopt.kafka.admin/consumer-group-offsets`

  [^ListConsumerGroupOffsetsResult lcgor]

  (K.-interop/future-proxy (.partitionsToOffsetAndMetadata lcgor)
                           (fn to-clj [partitions->offsets+metadata]
                             (reduce (fn offsets [hmap [tp ^OffsetAndMetadata offset+metadata]]
                                       (assoc hmap
                                              (topic-partition tp)
                                              (void/assoc-some {::K/offset (.offset offset+metadata)}
                                                               ::K/meta (not-empty (.metadata offset+metadata)))))
                                     {}
                                     partitions->offsets+metadata))))




(defn describe-acls-result

  ;; Cf. `dvlopt.kafka.admin/acls`

  [^DescribeAclsResult dar]

  (K.-interop/future-proxy (.values dar)
                           (fn map-acl-bindings [acl-bindings]
                             (map acl-binding
                                  acl-bindings))))




(defn create-acls-result

  ;; Cf. `dvlopt.kafka.admin/create-acls`

  [^CreateAclsResult car]

  (reduce (fn reduce-acl-bindings [acl-bindings acl-binding' f*]
            (assoc acl-bindings
                   (acl-binding acl-binding')
                   f*))
          {}
          (.values car)))




(defn delete-acls-result$filter-result

  ;; Cf. `delete-acls-result$filter-results`

  [^DeleteAclsResult$FilterResult dar$fr]

  (if-let [acl-binding' (.binding dar$fr)]
    [:acl       (acl-binding acl-binding')]
    [:exception (.exception dar$fr)]))




(defn delete-acls-result$filter-results

  ;; Cf. `delete-acls-result`

  [^DeleteAclsResult$FilterResults dar$frs]

  (map delete-acls-result$filter-result
       (.values dar$frs)))




(defn delete-acls-result

  ;; Cf. `dvlopt.kafka.admin/delete-acls`

  [^DeleteAclsResult dar]

  (reduce (fn reduce-filters [filters [abf f*results]]
            (assoc filters
                   (acl-binding-filter abf)
                   (K.-interop/future-proxy f*results
                                            delete-acls-result$filter-results)))
          {}
          (.values dar)))




;;;;;;;;;; org.apache.kafka.clients.producer.*


(defn record-metadata

  ;; Cf. `dvlopt.kafka.produce/send`

  [^RecordMetadata rm]

  (void/assoc-some {::K/topic (.topic rm)}
                   ::K/offset    (when (.hasOffset rm)
                                   (.offset rm))
                   ::K/partition (let [partition (.partition rm)]
                                   (when (not= partition
                                               RecordMetadata/UNKNOWN_PARTITION)
                                     partition))
                   ::K/timestamp (when (.hasTimestamp rm)
                                   (.timestamp rm))))




;;;;;;;;;; org.apache.kafka.clients.consumer.*


(defn offset-and-timestamp

  ;; Cf. `dvlopt.kafka.consume/offsets-for-timestamps`

  [^OffsetAndTimestamp oat]

  {::K/offset    (.offset    oat)
   ::K/timestamp (.timestamp oat)})




(defn consumer-record

  ;; Cf. `dvlopt.kafka.consume/poll`

  [^ConsumerRecord cr]

  (void/assoc-some {::K/offset    (.offset    cr)
                    ::K/partition (.partition cr)
                    ::K/timestamp (.timestamp cr)
                    ::K/topic     (.topic     cr)}
                   ::K/headers (headers (.headers cr))
                   ::K/key     (.key cr)
                   ::K/value   (.value cr)))




;;;;;;;;;; org.apache.kafka.streams.*


(defn kafka-streams$state

  ;; Cf. `dvlopt.kstreams/state`

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

  ;; Cf. `topology-description`

  [^TopologyDescription$Node n]

  (.name n))




(defn -topology-description$node--names

  ;; Cf. `topology-description`

  [nodes]

  (into #{}
        (map -topology-description$node--name
             nodes)))




(defn topology-description$node

  ;; Cf. `topology-description`

  [^TopologyDescription$Node n]

  (void/assoc-some {}
                   :dvlopt.kstreams/children (not-empty (-topology-description$node--names (.successors n)))
                   :dvlopt.kstreams/parents  (not-empty (-topology-description$node--names (.predecessors n)))))




(defn topology-description$processor

  ;; Cf. `topology-description`

  [^TopologyDescription$Processor td$p]

  {:dvlopt.kstreams/name        (.name td$p)
   :dvlopt.kstreams.stores/name (into #{}
                                      (.stores td$p))})




(defn topology-description$source

  ;; Cf. `topology-description`

  [^TopologyDescription$Source td$s]

  (assoc (topology-description$node td$s)
         ::K/topic
         (.topics td$s)))




(defn topology-description$global-store

  ;; Cf. `topology-description`

  [^TopologyDescription$GlobalStore td$gs]

  (let [^TopologyDescription$Processor td$p (.processor td$gs)
        ^TopologyDescription$Source    td$s (.source td$gs)]
    {::K/topic                       (.topics td$s)
     :dvlopt.kstreams/processor.name (.name td$p)
     :dvlopt.kstreams/source.name    (.name td$s)
     :dvlopt.kstreams.stores/name    (first (.stores td$p))
     :dvlopt.kstreams/subgraph-type :global-store}))




(defn topology-description$subtopology

  ;; Cf. `topology-description`

  [^TopologyDescription$Subtopology td$s]

  {:dvlopt.kstreams/nodes         (reduce (fn to-map [nodes ^TopologyDescription$Node n]
                                            (assoc nodes
                                                   (.name n)
                                                   (topology-description$node n)))
                                          {}
                                          (.nodes td$s))
   :dvlopt.kstreams/subgraph-type :subtopology})




(defn topology-description

  ;; Cf. `dvlopt.kstreams.low/describe`

  [^TopologyDescription tp]

  (let [description (reduce (fn ??? [description' ^TopologyDescription$GlobalStore td$gs]
                              (assoc description'
                                     (.id td$gs)
                                     (topology-description$global-store td$gs)))
                            {}
                            (.globalStores tp))]
    (reduce (fn ??? [description' ^TopologyDescription$Subtopology td$s]
              (assoc description'
                     (.id td$s)
                     (topology-description$subtopology td$s)))
            description
            (.subtopologies tp))))




(defn key-value

  ;; Cf. `key-value-iterator`

  [^KeyValue kv]

  {::K/key   (.-key   kv)
   ::K/value (.-value kv)})




(defn key-value--ws

  ;; Cf. `window-store-iterator`

  [^KeyValue kv]

  {::K/timestamp (.-key   kv)
   ::K/value     (.-value kv)})




(declare windowed)




(defn key-value--windowed

  ;; Cf. `key-value-iterator--windowed`

  [^KeyValue kv]

  (assoc (windowed (.-key kv))
         ::K/value
         (.-value kv)))




;;;;;;;;;; org.apache.kafka.streams.processor.*


(defn cancellable

  ;; Cf. `dvlopt.kstreams.ctx/schedule`

  [^Cancellable c]

  (fn cancel-scheduled-operation []
    (.cancel c)))




(defn record-context

  ;; Cf. `dvlopt.kstreams.high.stream/sink-partition

  [^RecordContext rc]

  (void/assoc-some {}
                   ::K/headers   (headers (.headers rc))
                   ::K/offset    (let [offset (.offset rc)]
                                   (when (nat-int? offset)
                                     offset))
                   ::K/partition (let [partition (.partition rc)]
                                   (when (nat-int? partition)
                                     partition))
                   ::K/timestamp (let [timestamp (.timestamp rc)]
                                   (when (nat-int? timestamp)
                                     timestamp))
                   ::K/topic     (.topic rc)))




;;;;;;;;;; org.apache.kafka.streams.state.*


(defn key-value-iterator

  ;; Cf. `dvlopt.kstreams.stores/kv-get` and variations

  (^AutoCloseable

   [kvi]

   (key-value-iterator kvi
                       key-value))


  (^AutoCloseable

   [^KeyValueIterator kvi f]

   (reify
     
     AutoCloseable

       (close [_]
         (.close kvi))


     Iterator

       (hasNext [_]
         (.hasNext kvi))

       (next [_]
         (f (.next kvi))))))




(defn key-value-iterator--windowed

  ^AutoCloseable

  [kvi]

  (key-value-iterator kvi
                      key-value--windowed))




(defn window-store-iterator

  ^AutoCloseable

  [wsi]

  (key-value-iterator wsi
                      key-value--ws))




;;;;;;;;;; org.apache.kafka.streams.kstream.*


(defn window

  ;; Cf. `key-value-iterator--windowed`

  [^Window w]

  {::K/timestamp.from (.start w)
   ::K/timestamp.to   (.end   w)})




(defn windowed

  ;; Cf. `key-value-iterator--windowed`

  [^Windowed w]

  (assoc (window (.window w))
         ::K/key
         (.key w)))
