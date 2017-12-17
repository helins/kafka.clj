(ns milena.admin
  
  "Manage and inspect topics, brockers, configurations and ACLs."

  {:author "Adam Helinski"}

  (:require [milena.interop      :as $.interop]
            [milena.interop.clj  :as $.interop.clj]
            [milena.interop.java :as $.interop.java])
  (:import (java.util.concurrent Future
                                 TimeUnit)
           org.apache.kafka.common.TopicPartitionInfo
           (org.apache.kafka.clients.admin AdminClient
                                           ListTopicsOptions
                                           TopicListing
                                           DescribeTopicsResult
                                           TopicDescription)))




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

   ($.interop.clj/describe-cluster-result (if opts
                                            (.describeCluster client
                                                              ($.interop.java/describe-cluster-options opts))
                                            (.describeCluster client)))))




;;;;;;;;;; Topics


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

   ($.interop/future-proxy (.listings (if opts
                                        (.listTopics client
                                                     ($.interop.java/list-topics-options opts))
                                        (.listTopics client)))
                           $.interop.clj/topic-listings)))




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

   ($.interop.clj/describe-topics-result (if opts
                                           (.describeTopics client
                                                            topics
                                                            ($.interop.java/describe-topics-options opts))
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
                        ($.interop.java/new-topic topic
                                                  topic-opts))
                      new-topics)]
     ($.interop.clj/create-topics-result (if opts
                                           (.createTopics client
                                                          topics'
                                                          ($.interop.java/create-topics-options opts))
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

   ($.interop.clj/delete-topics-result (if opts
                                         (.deleteTopics client
                                                        topics
                                                        ($.interop.java/delete-topics-options opts))
                                         (.deleteTopics client
                                                        topics)))))




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
   (let [resources' ($.interop.java/config-resources resources)]
     ($.interop.clj/describe-configs-result (if opts
                                              (.describeConfigs client
                                                                resources'
                                                                ($.interop.java/describe-configs-options opts))
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

   (let [configs' ($.interop.java/alter-configs configs)]
     ($.interop.clj/alter-configs-result (if opts
                                           (.alterConfigs client
                                                          configs'
                                                          ($.interop.java/alter-configs-options opts))
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
     Cf. `$.interop.java/describe-acls-options`

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

   (let [acl-filter' ($.interop.java/acl-binding-filter acl-filter)]
     ($.interop.clj/describe-acls-result (if opts
                                           (.describeAcls client
                                                          acl-filter'
                                                          ($.interop.java/describe-acls-options opts))
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

   (let [acls' (map $.interop.java/acl-binding
                    acls)]
     ($.interop.clj/create-acls-result (if opts
                                         (.createAcls client
                                                      acls'
                                                      ($.interop.java/create-acls-options opts))
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

   (let [acl-filters' (map $.interop.java/acl-binding-filter
                           acl-filters)]
     ($.interop.clj/delete-acls-result (if opts
                                         (.deleteAcls client
                                                      acl-filters'
                                                      ($.interop.java/delete-acls-options opts))
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


  Ex. (make {:?nodes  [[\"some_host\" 9092]]
             :?config {:client.id \"some_id\"}})"

  ([]

   (make nil))


  ([{:as   opts
     :keys [nodes
            config]
     :or   {nodes [["localhost" 9092]]}}]

   (AdminClient/create ($.interop/config config
                                         nodes))))
