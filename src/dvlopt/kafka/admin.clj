(ns dvlopt.kafka.admin
  
  "Kafka administration.
  
  
   Functions accepting a timeout relies on the default request timeout of the admin client when none
   is provided. Time intervals are described in `dvlopt.kafka`."

  {:author "Adam Helinski"}

  (:require [dvlopt.kafka               :as K]
            [dvlopt.kafka.-interop      :as K.-interop]
            [dvlopt.kafka.-interop.clj  :as K.-interop.clj]
            [dvlopt.kafka.-interop.java :as K.-interop.java]
            [dvlopt.void                :as void])
  (:import java.util.concurrent.TimeUnit
           org.apache.kafka.clients.admin.AdminClient
           org.apache.kafka.common.config.ConfigResource
           org.apache.kafka.common.TopicPartitionInfo))




;;;;;;;;;; Starting and stopping an admin


(defn admin 

  "Builds an admin client.

   A map of options may be given :

     ::configuration
       Map of Kafka admin properties.
       Cf. https://kafka.apache.org/documentation/#adminclientconfigs

     :dvlopt.kafka/nodes
       List of [host port].


   Ex. (client {::configuration     {\"client.id\" \"some_id\"}
                :dvlopt.kafka/nodes [[\"some_host\" 9092]]})"

  (^AdminClient
    
   []

   (admin nil))


  (^AdminClient

   [options]

   (AdminClient/create (K.-interop/resource-configuration (::configuration options)
                                                          (void/obtain ::K/nodes
                                                                       options
                                                                       K/defaults)))))




(defn close

  "Closes the admin client and releases all associated resources.
  
   A map of options may be given :

     :dvlopt.kafka/timeout
      Grace period during which current operations will be allowed to complete and no new ones will be accepted.
      Futures related to unfinished operations will throw when dereferenced.
      Cf. Namespace description"

  ([^AdminClient admin]

   (.close admin))


  ([^AdminClient admin options]

   (if-let [[timeout-duration
             unit]            (::K/timeout options)]
     (.close admin
             timeout-duration
             (K.-interop.java/time-unit unit))
     (.close admin))))




;;;;;;;;;; Misc


(defn cluster

  "Requests a map containing information about the nodes in the cluster :

     :dvlopt.kafka/f*id
      Future, the id of the cluster.

     :dvlopt.kafka/f*nodes
      Future, a list of the cluster's nodes.

     :dvlopt.kafka/f*controller-node
      Future, the controller node.


   A map of options may be given :
  
     :dvlopt.kafka/timeout
      Cf. Namespace description"

  ([admin]

   (cluster admin
            nil))


  ([^AdminClient admin options]

   (K.-interop.clj/describe-cluster-result (if options
                                             (.describeCluster admin
                                                               (K.-interop.java/describe-cluster-options options))
                                             (.describeCluster admin)))))




;;;;;;;;;; Topics and partitions


(defn topics

  "Requests a future resolving to a map of topic name -> map containing :

    :dvlopt.kafka/internal?
     Is this topic internal ?


   A map of options may be given :

     :dvlopt.kafka/internal?
       Should internal topics be retrieved ?

     :dvlopt.kafka/timeout
      Cf. Namespace description"

  ([admin]

   (topics admin
           nil))


  ([^AdminClient admin options]

   (K.-interop/future-proxy (.listings (.listTopics admin
                                                    (K.-interop.java/list-topics-options options)))
                            K.-interop.clj/topic-listings)))




(defn describe-topics

  "Given a list of topics to describe, requests a map of topic name -> future resolving to map containing :

     :dvlopt.kafka/internal?
      Is this topic internal ?

     ::partition-descriptions
      List of partition descriptions sorted by partition number, ie. maps containing :

        :dvlopt.kafka/leader-node
         Cf. `dvlopt.kafka` for descriptions of Kafka nodes.

        :dvlopt.kafka/partition
         Partition number.

        :dvlopt.kafka/replica-nodes
         Cf. `dvlopt.kafka` for descriptions of Kafka nodes.
  

    A map of options may be given :

     :dvlopt.kafka/timeout
      Cf. Namespace description"

  ([admin topics]

   (describe-topics admin
                    topics
                    nil))


  ([^AdminClient admin topics options]

   (K.-interop.clj/describe-topics-result (if options
                                            (.describeTopics admin
                                                             topics
                                                             (K.-interop.java/describe-topics-options options))
                                            (.describeTopics admin
                                                             topics)))))




(defn create-topics

  "Requests the creation of new topics.
  
   This operation is supported by brokers with version 0.10.1.0 or higher.

   Returns a map of topic name -> future throwing if an error occured.

   New topics are specified by a map of topic names -> argument maps.

   An argument map consists of either the number of partitions with the replication factor or
   a manual assignment of partition numbers  to replica ids. Although not inforced, it is generally a
   good idea for all partitions to have the same number of replicas.

   Also, each topic can be configured as needed (cf. https://kafka.apache.org/documentation/#topicconfigs).


   A map of options may be given :

     :dvlopt.kafka/timeout
      Cf. Namespace description


   Ex. (create-topics admin
                      {\"first-new-topic\"  {::number-of-partitions 4
                                             ::replication-factor   1
                                             ::topic-configuration  {\"cleanup.policy\" \"compact\"}}
                       \"second-new-topic\" {::replica-assignments  {0 [0 1]
                                                                     1 [2 3]}}}
                      {:dvlopt.kafka/timeout [5 :seconds]})"

  ([admin new-topics]
   
   (create-topics admin
                  new-topics
                  nil))


  ([^AdminClient admin new-topics options]

   (let [new-topics' (map (fn map-topics [[topic topic-options]]
                            (K.-interop.java/new-topic topic
                                                       topic-options))
                          new-topics)]
     (K.-interop.clj/create-topics-result (if options
                                            (.createTopics admin
                                                           new-topics'
                                                           (K.-interop.java/create-topics-options options))
                                            (.createTopics admin
                                                           new-topics'))))))




(defn delete-topics

  "Requests the deletion of topics.

   Returns a map of topic name -> future throwing if an error occured.


   A map of options may be given :

     :dvlopt.kafka/timeout
      Cf. Namespace description


   Ex. (delete-topics admin
                      [\"some-topic\"
                       \"another-topic\"]
                      {:dvlopt.kafka/timeout [5 :seconds]})"

  ([admin topics]

   (delete-topics admin
                  topics
                  nil))


  ([^AdminClient admin topics options]

   (K.-interop.clj/delete-topics-result (if options
                                          (.deleteTopics admin
                                                         topics
                                                         (K.-interop.java/delete-topics-options options))
                                          (.deleteTopics admin
                                                         topics)))))




(defn increase-partitions

  "Requests an increase of the number of partitions for the given topics.

   <!> Does not repartition existing topics and the partitioning of new records will be different <!>

   For each topic, the new number of partitions has to be specified. Can also be supplied the assignements
   of the new partitions in form of a list of list of replica ids where the first one is the prefered leader.

   Returns a map of topic name -> future throwing if an error occured.

   A map of options may be given :

     :dvlopt.kafka/timeout
      Cf. Namespace description
   

   Ex. Given 2 partitions with a replication factor of 3, increases the number to 4 and specifies assignments :
  
       (partitions-create admin
                          {\"some_topic\" {::number-of-partitions 4
                                           ::replica-assignments  [[0 1 2]      ;; replicas for partition 3, replica 0 being the prefered leader
                                                                   [1 2 3]]}}   ;; replicas for partition 4, replica 1 being the prefered leader
                          {:dvlopt.kafka/timeout [5 :seconds]})"

  ([admin topics->new-partitions]

   (increase-partitions admin
                        topics->new-partitions
                        nil))


  ([^AdminClient admin topics->new-partitions options]

   (let [topics->new-partitions' (reduce-kv (fn reduce-hmap [topics->new-partitions' topic partitions]
                                              (assoc topics->new-partitions'
                                                     topic
                                                     (K.-interop.java/new-partitions partitions)))
                                            {}
                                            topics->new-partitions)]
     (K.-interop.clj/create-partitions-result (if options
                                                (.createPartitions admin
                                                                   topics->new-partitions'
                                                                   (K.-interop.java/create-partitions-options options))
                                                (.createPartitions admin
                                                                   topics->new-partitions'))))))




;;;;;;;;;; Records


(defn delete-records-before

  "Requests the deletion of all records prior to the offset given for each [topic partition].

   Returns a map of [topic partition] -> future resolving to {:dvlopt.kafka/lowest-offset ...} of
   throwing in case of error.
  

   A map of options may be given :

     :dvlopt.kafka/timeout
      Cf. Namespace description


   Ex. (delete-records-before admin
                              {[\"some_topic\" 0] 62}
                              {:dvlopt.kafka/timeout [5 :seconds]})"

  ([admin topic-partition->offset]

   (delete-records-before admin
                          topic-partition->offset
                          nil))


  ([^AdminClient admin topic-partition->offset options]

   (let [topic-partition->offset' (reduce-kv (fn [topic-partition->offset' topic-partition offset]
                                               (assoc topic-partition->offset'
                                                      (K.-interop.java/topic-partition topic-partition)
                                                      (K.-interop.java/records-to-delete offset)))
                                             {}
                                             topic-partition->offset)]
   (K.-interop.clj/delete-records-result (if options
                                           (.deleteRecords admin
                                                           topic-partition->offset'
                                                           (K.-interop.java/delete-records-options options))
                                           (.deleteRecords admin
                                                           topic-partition->offset'))))))




;;;;;;;;;; Configuration


(defn configuration

  "Requests the current configuration of the given resources (brokers and/or topics).

   Each property points to a map containing :

     :dvlopt.kafka/default?
      Is this a default value ?

     :dvlopt.kafka/name
      Name of the property.

     :dvlopt.kafka/read-only? 
      Is this a read-only property ?

     :dvlopt.kafka/string-value
      Value presented as a string.

     ::sensitive?
      Is this a sensitive value that should be kept secret ?


   A map of options may be given :

     :dvlopt.kafka/timeout
      Cf. Namespace description


   Ex. (configuration admin
                      {:dvlopt.kafka/brokers #{\"0\"
                                               \"1\"}
                       :dvlopt.kafka/topics  #{\"topic-one\"
                                               \"topic-two\"}}
                      {:dvlopt.kafka/timeout [5 :seconds]})"

  ([admin resources]

   (configuration admin
                  resources
                  nil))


  ([^AdminClient admin resources options]
   (let [resources' (K.-interop.java/config-resources resources)]
     (K.-interop.clj/describe-configs-result (if options
                                               (.describeConfigs admin
                                                                 resources'
                                                                 (K.-interop.java/describe-configs-options options))
                                               (.describeConfigs admin
                                                                 resources'))))))




(defn configure

  "Requests modifications in the configuration of the given resources (brokers and/or topics).


   Cf. https://kafka.apache.org/documentation/#brokerconfigs
       https://kafka.apache.org/documentation/#topicconfigs


   Returns a map containing the involved brokers and topics, pointing to futures throwing an exception in case of failure.
   

   A map of options may be given :

     :dvlopt.kafka/timeout
      Cf. Namespace description
  

   Ex. (configure admin
                  {::configuration.brokers {\"0\" {\"delete.topic.enable\" true}}
                   ::configuration.topics  {\"my-topic\" {\"cleanup.policy\" \"compact\"}}}
                  {:dvlopt.kafka/timeout [5 :seconds]})"

  ([admin configurations]

   (configure admin
              configurations
              nil))


  ([^AdminClient admin configurations options]

   (let [configurations' (reduce-kv (fn by-type [configurations' configuration-type properties]
                                      (let [type' (K.-interop.java/config-resource$type (condp identical?
                                                                                               configuration-type
                                                                                          ::configuration.brokers ::K/brokers
                                                                                          ::configuration.topics  ::K/topics))]
                                                                                               
                                        (reduce-kv (fn reduce-resources [configurations'2 name entries]
                                                     (assoc configurations'2
                                                            (ConfigResource. type'
                                                                             name)
                                                            (K.-interop.java/config entries)))
                                                   configurations'
                                                   properties)))
                                    {}
                                    configurations)]
     (K.-interop.clj/alter-configs-result (if options
                                            (.alterConfigs admin
                                                           configurations'
                                                           (K.-interop.java/alter-configs-options options))
                                            (.alterConfigs admin
                                                           configurations'))))))




;;;;;;;;;; Consumer groups


(defn consumer-groups

  "Requests a map of current consumer group -> metadata.
  
   A map of options may be given :

     :dvlopt.kafka/timeout
      Cf. Namespace description"

  ([admin]

   (consumer-groups admin
                    nil))


  ([^AdminClient admin options]

   (K.-interop.clj/list-consumer-groups-result (if options
                                                 (.listConsumerGroups admin
                                                                      (K.-interop.java/list-consumer-groups-options options))
                                                 (.listConsumerGroups admin)))))




(defn describe-consumer-groups

  "Given a list of consumer groups to describe, requests a map of consumer group -> detailed metadata.
  
   A map of options may be given :

     :dvlopt.kafka/timeout
      Cf. Namespace description"

  ([admin group-ids]

   (describe-consumer-groups admin
                             group-ids
                             nil))


  ([^AdminClient admin group-ids options]

   (K.-interop.clj/describe-consumer-groups-result (if options
                                                     (.describeConsumerGroups admin
                                                                              group-ids
                                                                              (K.-interop.java/describe-consumer-groups-options options))
                                                     (.describeConsumerGroups admin
                                                                              group-ids)))))



(defn consumer-group-offsets

  "For the given consumer group, requests a map of [topic partition] -> map containing the current :dvlopt.kafka/offset.


   A map of options may be given :

     :dvlopt.kafka/timeout
      Cf. Namespace description

     :dvlopt.kafka/topic-partitions
      Restricts result to the requested [topic partition]'s."

  ([admin consumer-group]

   (consumer-group-offsets admin
                           consumer-group
                           nil))


  ([^AdminClient admin consumer-group options]

   (K.-interop.clj/list-consumer-group-offsets-result (if options
                                                        (.listConsumerGroupOffsets admin
                                                                                   consumer-group
                                                                                   (K.-interop.java/list-consumer-group-offsets-options options))
                                                        (.listConsumerGroupOffsets admin
                                                                                   consumer-group)))))




;;;;;;;;;; ACLs


(defn acls

  "Requests needed ACLs.

   Returns a future resolving to a list of ACLs or throwing in case of error.


   The ACL filter is a nilable map which may contain :

     ::operation
      Type of ACL, one of #{:all (default)
                            :alter
                            :alter-configuration
                            :any
                            :cluster-action
                            :create
                            :delete
                            :describe
                            :describe-configuration
                            :idempotent-write
                            :read
                            :write}.

    ::name-pattern
     [Filter-Type String] where Filter-Type is one of :

       :any (default)
        String unneeded as it maches anything.

       :exactly
        Will match the given string and nothing else.

       :match
        Like :exactly and :prefixed but also expands wildcards (ie. \"*\" like in globs).

       :prefixed
        Will match anything beginning with the given string.

     A nil value will match anything.

    ::permission
     One of #{:any (default)
              :allow
              :deny}.

    ::principal
     Who.


    ::resource-type
     One of #{:any (default)
              :cluster
              :consumer-group
              :topic
              :transactional-id}.

   :dvlopt.kafka/host
    Restricts to the given host or none if this option is not provided.


   A map of options may be given :

     :dvlopt.kafka/timeout
      Cf. Namespace description


   Ex. (acls admin
             {::permission    :allow
              ::operation     :create
              ::resource-type :topic
              ::name-pattern  [:match \"my_*_topic\"]}
             {:dvlopt.kafka/timeout [5 :seconds]})"

  ([admin]

   (acls admin
         nil
         nil))


  ([admin acl-filter]

   (acls admin
         acl-filter
         nil))


  ([^AdminClient admin acl-filter options]

   (let [acl-filter' (K.-interop.java/acl-binding-filter acl-filter)]
     (K.-interop.clj/describe-acls-result (if options
                                            (.describeAcls admin
                                                           acl-filter'
                                                           (K.-interop.java/describe-acls-options options))
                                            (.describeAcls admin
                                                           acl-filter'))))))




(defn create-acls

  "Requests the creation of new ACLs (a list of maps akin to filters described in `acls`).

   Returns a map of ACL -> future throwing in case of error.


   A map of options may be given :

     :dvlopt.kafka/timeout
      Cf. Namespace description
   

   Ex. (create-acls admin
                    [{::principal     \"some.user\"
                      ::permission    :allow
                      ::operation     :create
                      ::resource-type :topic
                      ::name-pattern  [:prefixed \"some_topics\"]}]
                    {:dvlopt.kafka/timeout [5 :seconds]})"

  ([admin acls]

   (create-acls admin
                nil))


  ([^AdminClient admin acls options]

   (let [acls' (map K.-interop.java/acl-binding
                    acls)]
     (K.-interop.clj/create-acls-result (if options
                                          (.createAcls admin
                                                       acls'
                                                       (K.-interop.java/create-acls-options options))
                                          (.createAcls admin
                                                       acls'))))))




(defn delete-acls

  "Requests the deletion of ACLs following a list of filters (cf. `acls`).

   Returns a map of ACL filter -> future resolving to a list of :

     [:acl ACL]         in case of success, where ACL matches the filter

     [:error Exception] in case of failure
 

   A map of options may be given :

     :dvlopt.kafka/timeout
      Cf. Namespace description


   Ex. (delete-acls admin
                    [{::principal     \"some.user\"
                      ::permission    :allow
                      ::operation     :create
                      ::resource-type :topic
                      ::pattern       [:prefixed \"some_topics\"]}]
                    {:dvlopt.kafka/timeout [5 :seconds]})"

  ([admin acl-filters]

   (delete-acls admin
                acl-filters
                nil))


  ([^AdminClient admin acl-filters options]

   (let [acl-filters' (map K.-interop.java/acl-binding-filter
                           acl-filters)]
     (K.-interop.clj/delete-acls-result (if options
                                          (.deleteAcls admin
                                                       acl-filters'
                                                       (K.-interop.java/delete-acls-options options))
                                          (.deleteAcls admin
                                                       acl-filters'))))))
