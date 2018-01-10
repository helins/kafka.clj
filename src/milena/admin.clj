(ns milena.admin
  
  "Manage and inspect topics, brockers, configurations and ACLs."

  {:author "Adam Helinski"}

  (:require [milena.interop      :as M.interop]
            [milena.interop.clj  :as M.interop.clj]
            [milena.interop.java :as M.interop.java])
  (:import java.util.concurrent.TimeUnit
           org.apache.kafka.common.TopicPartitionInfo
           org.apache.kafka.clients.admin.AdminClient))




;;;;;;;;;; Misc


(defn cluster

  "Gets information about the nodes in the cluster.

   @ client
     Admin client.

   @ opts (nilable)
     Cf. `milena.interop.java/describe-cluster-options`

   => Cf. `milena.interop.clj/describe-cluster-result`"

  ([client]

   (cluster client
            nil))


  ([^AdminClient client opts]

   (M.interop.clj/describe-cluster-result (if opts
                                            (.describeCluster client
                                                              (M.interop.java/describe-cluster-options opts))
                                            (.describeCluster client)))))




;;;;;;;;;; Topics and partitions


(defn topics

  "Gets the topics available in the cluster.

   @ client
     Admin client.

   @ opts (nilable)
     Cf. `milena.interop.java/list-topics-options`

   => A future
      Cf. `milena.interop.clj/topic-listings`"

  ([client]

   (topics client
           nil))


  ([^AdminClient client opts]

   (M.interop/future-proxy (.listings (if opts
                                        (.listTopics client
                                                     (M.interop.java/list-topics-options opts))
                                        (.listTopics client)))
                           M.interop.clj/topic-listings)))




(defn topics-describe

  "Describes some topics in the cluster.


   @ client
     Admin client.

   @ opts (nilable)
     Cf. `milena.interop.java/list-topics-options`

   => Cf. `milena.interop.clj/describe-topics-result`
  
  
   Ex. (topics-describe client
                        [\"some-topic\"
                         \"another-topic\"])"

  ([client topics]

   (topics-describe client
                    topics
                    nil))


  ([^AdminClient client topics opts]

   (M.interop.clj/describe-topics-result (if opts
                                           (.describeTopics client
                                                            topics
                                                            (M.interop.java/describe-topics-options opts))
                                           (.describeTopics client
                                                            topics)))))




(defn topics-create 

  "Creates new topics.
  
   This operation is supported by brokers with version 0.10.1.0 or higher.
  

   @ client
     Admin client.

   @ topics
     A map of topic-name to topic-args.
     Cf. `milena.interop.java/new-topic`

   @ opts (nilable)
     Cf. `milena.interop.java/create-topics-options`

   => `milena.interop.clj/create-topics-result`
  
  
   Ex. (topics-create client
                      {\"my-new-topic\" {:partitions         4
                                         :replication-factor 1
                                         :config             {:cleanup.policy \"compact\"}}})"

  ([client new-topics]
   
   (topics-create client
                  new-topics
                  nil))


  ([^AdminClient client new-topics opts]

   (let [topics' (map (fn map-topics [[topic topic-opts]]
                        (M.interop.java/new-topic topic
                                                  topic-opts))
                      new-topics)]
     (M.interop.clj/create-topics-result (if opts
                                           (.createTopics client
                                                          topics'
                                                          (M.interop.java/create-topics-options opts))
                                           (.createTopics client
                                                          topics'))))))




(defn topics-delete

  "Deletes topics.
  

   @ client
     Admin client.
  
   @ topics
     List of topic names.
  
   @ opts (nilable)
     Cf. `milena.interop.java/delete-topics-options`

   => `milena.interop.clj/delete-topics-result`


   Ex. (topics-delete client
                      [\"some-topic\"
                       \"another-topic\"])"

  ([client topics]

   (topics-delete client
                  topics
                  nil))


  ([^AdminClient client topics opts]

   (M.interop.clj/delete-topics-result (if opts
                                         (.deleteTopics client
                                                        topics
                                                        (M.interop.java/delete-topics-options opts))
                                         (.deleteTopics client
                                                        topics)))))




(defn partitions-create

  "Increases the number of partitions for the given topics.

   <!> Does not repartition existing topics and the partitioning of new records will be different <!>

   
   @ client
     Admin client.

   @ topics-to-new-partitions
     Cf. `milena.interop.java/topics-to-new-partitions`

   @ opts (nilable)
     Cf. `milena.interop.java.create-partitions-options

   => `milena.interop.clj/create-partitions-result`


   Ex. (partitions-create client
                          {:n           3
                           :assignments [[1 2]
                                         [2 3]
                                         [3 1]]})"

  ([client topics-to-new-partitions]

   (partitions-create client
                      topics-to-new-partitions
                      nil))


  ([^AdminClient client topics-to-new-partitions opts]

   (let [new-partitions (M.interop.java/topics-to-new-partitions topics-to-new-partitions)
         result         (if opts
                          (.createPartitions client
                                             new-partitions
                                             (M.interop.java/create-partitions-options opts))
                          (.createPartitions client
                                             new-partitions))]
     (M.interop.clj/create-partitions-result result))))




;;;;;;;;;; Configuration


(defn config

  "Gets the configuration of the given resources.

   
   @ client
     Admin client.

   @ resources
     Cf. `milena.interop.java/config-resources`

   @ opts (nilable)
     Cf. `milena.interop.java/describe-configs-options`

   => Cf. `milena.interop.clj/describe-configs-result`


   Ex. (config client
               {:topics #{\"some-topic\"
                          \"another-topic\"}
                :brokers #{\"0\"
                           \"1\"}})"

  ([client resources]

   (config client
           resources
           nil))


  ([^AdminClient client resources opts]
   (let [resources' (M.interop.java/config-resources resources)]
     (M.interop.clj/describe-configs-result (if opts
                                              (.describeConfigs client
                                                                resources'
                                                                (M.interop.java/describe-configs-options opts))
                                              (.describeConfigs client
                                                                resources'))))))




(defn config-alter 

  "Alter the configuration of the given resources.

   
   @ client
     Admin client.

   @ configs
     Resource configurations.
     Cf. `milena.interop.java/alter-configs`

   @ opts (nilable)
     Cf. `milena.interop.java/alter-configs-options`

   => Cf. `milena.interop.clj/alter-configs-result`
  
  
   Ex. (config-alter client
                     {:brokers {\"0\" {:delete.topic.enable true}}
                      :topics  {\"my-topic\" {:cleanup.policy \"compact\"}}})"

  ([client configs]

   (config-alter client
                 configs
                 nil))


  ([^AdminClient client configs opts]

   (let [configs' (M.interop.java/alter-configs configs)]
     (M.interop.clj/alter-configs-result (if opts
                                           (.alterConfigs client
                                                          configs'
                                                          (M.interop.java/alter-configs-options opts))
                                           (.alterConfigs client
                                                          configs'))))))




;;;;;;;;;; ACLs


(defn acls

  "Gets ACLs.


   @ client
     Admin client.

   @ acl-filter (nilable)
     ACL filter.
     Cf. `milena.interop.java/acl-binding-filter`

   @ opts (nilable)
     Cf. `M.interop.java/describe-acls-options`

   => Cf. `milena.interop.clj/describe-acls-result`

  
   Ex. (acls client)
  
       (acls client
             {:resource       {:name :topic
                               :type \"my-topic\"}
              :access-control {:permission :allow
                               :operation  :create}})"

  ([client]

   (acls client
         nil
         nil))


  ([client acl-filter]

   (acls client
         acl-filter
         nil))


  ([^AdminClient client acl-filter opts]

   (let [acl-filter' (M.interop.java/acl-binding-filter acl-filter)]
     (M.interop.clj/describe-acls-result (if opts
                                           (.describeAcls client
                                                          acl-filter'
                                                          (M.interop.java/describe-acls-options opts))
                                           (.describeAcls client
                                                          acl-filter'))))))




(defn acls-create 

  "Creates acls.
  
  
   @ client
     Admin client.

   @ acls
     List of ACLs.
     Cf. `milena.interop.java/acl-binding`

   @ opts (nilable)
     Cf. `milena.interop.java/create-acls-options`

   => Cf. `milena.interop.clj/create-acls-result`"

  ([client acls]

   (acls-create client
                nil))


  ([^AdminClient client acls opts]

   (let [acls' (map M.interop.java/acl-binding
                    acls)]
     (M.interop.clj/create-acls-result (if opts
                                         (.createAcls client
                                                      acls'
                                                      (M.interop.java/create-acls-options opts))
                                         (.createAcls client
                                                      acls'))))))




(defn acls-delete

  "Deletes acls.
  

   @ client
     Admin client.

   @ acl-filters
     List of acl filters.
     Cf. `milena.interop.java/acl-binding-filter`

   => Cf. `milena.interop.clj/delete-acls-result`


   Ex. (acls-delete client
                    [{:resource       {:name \"my-topic\"
                                       :type :topic}
                      :access-control {:permission :allow
                                       :operation  :alter}}])"

  ([client acl-filters]

   (acls-delete client
                acl-filters
                nil))


  ([^AdminClient client acl-filters opts]

   (let [acl-filters' (map M.interop.java/acl-binding-filter
                           acl-filters)]
     (M.interop.clj/delete-acls-result (if opts
                                         (.deleteAcls client
                                                      acl-filters'
                                                      (M.interop.java/delete-acls-options opts))
                                         (.deleteAcls client
                                                      acl-filters'))))))




;;;;;;;;;; Admin client


(defn close

  "Closes the admin client and releases all associated resources.

   @ client
     Admin client.

   @ timeout-ms
     A timeout in milliseconds can be provided to allow for ongoing operations to complete gracefully.
     New operations will NOT be accepted during this duration.
     Operations not yet completed at this time will throw org.apache.kafka.common.errors.TimeoutException.

   => nil"

  ([^AdminClient client]

   (.close client))


  ([^AdminClient client timeout-ms]

   (.close client
           (max timeout-ms
                0)
           TimeUnit/MILLISECONDS)))




(defn make

  "Builds a Kafka admin client.
  

   @ opts (nilable)
     {:nodes (nilable)
       A list of [host port].

      :config (nilable)
       Kafka configuration.
       Cf. https://kafka.apache.org/documentation/#adminclientconfigs}

  => org.apache.kafka.clients.admin.AdminClient


  Ex. (make {:nodes  [[\"some_host\" 9092]]
             :config {:client.id \"some_id\"}})"

  ([]

   (make nil))


  ([{:as   opts
     :keys [nodes
            config]
     :or   {nodes [["localhost" 9092]]}}]

   (AdminClient/create (M.interop/config config
                                         nodes))))
