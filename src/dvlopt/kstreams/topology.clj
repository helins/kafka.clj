(ns dvlopt.kstreams.topology

  "Kafka Streams low level API, rather imperative.
  
  
   This namespace is about building a topology of nodes manually. Three types of nodes exist :

     Sources
     -------

     A source act as one of the the entry points of a topology. It retrieves records from one or several topics and forwards
     them to processors or sinks.

     Processors
     ----------

     A processor process records from sources and other processors and forwards them to sinks or other processors as well.
     It can persist state by using a state store.

     Sinks
     -----

     A sink is always a terminal node. Its purpose is to persist records from sources and processors to a concrete topic.
  
  
   States stores are used in order to persist state in a fault-tolerant manner. By default, they are backed-up to a topic under
   the hood in order to do so. A regular state store is distributed following partitions. A global one always sources data from
   all partitions at the same time.
  
   Cf. `dvlopt.kstreams.store` for more"

  {:author "Adam Helinski"}

  (:require [dvlopt.kafka               :as K]
            [dvlopt.kafka.-interop      :as K.-interop]
            [dvlopt.kafka.-interop.java :as K.-interop.java]
            [dvlopt.kafka.-interop.clj  :as K.-interop.clj]
            [dvlopt.kstreams            :as KS]
            [dvlopt.void                :as void])
  (:import java.util.Collection
           java.util.regex.Pattern
           (org.apache.kafka.streams StreamsBuilder
                                     Topology)
           (org.apache.kafka.streams.kstream KStream
                                             KTable
                                             GlobalKTable)))




;;;;;;;;;;


(defn topology

  "Creates a new topology from scratch or from a builder.

   Cf. `dvlopt.kstreams.build/builder`"

  (^Topology

   []

   (Topology.))


  (^Topology

   [^StreamsBuilder builder]

   (.build builder)))




(defn describe

  "Describes the given topology.
  
   Returns a map of internal id -> subgraph, a map containing :

     :dvlopt.kstreams/subgraph-type
      Either :global-store or :subtopology.

     Subgraphs of type :global-store contains :

       :dvlopt.kafka/topic
        Where records are sourced from for populating the global store.
  
       :dvlopt.kstreams/processor.name
        Name of the processor node updating the global store.

       :dvlopt.kstreams/source.name
        Name of the source node fetching records from the topic.

       :dvlopt.kstreams.store/name
        Name of the global store.

     Subgraphs of type :subtopology contains

       :dvlopt.kstreams/nodes
        Map of node name -> map possibly containing :

          :dvlopt.kstreams/children
           Set of children nodes.

          :dvlopt.kstreams/parents
           Set of parent nodes."

  [^Topology topology]

  (K.-interop.clj/topology-description (.describe topology)))




(defn add-source

  "Adds a source (either a list of topics or regular expression) to the topology.

   It must be named uniquely in order to be later used by others nodes in the topology.
  

   A map of options may be given :

     :dvlopt.kafka/deserializer.key
     :dvlopt.kafka/deserializer.value
      Cf. `dvlopt.kafka` for description of deserializers.

     :dvlopt.kstreams/extract-timestamp
      Function accepting the previous timestamp of the last record and a record, and returning
      the timestamp chosen for the current record.

     :dvlopt.kstreams/offset-reset
      When a topic is consumed at first, it should start from the :earliest offset (default) or the :latest."

  (^Topology

   [topology source-name source]

   (add-source topology
               source-name
               source
               nil))


  (^Topology

   [^Topology topology ^String source-name source options]
 
   (let [offset-reset       (K.-interop.java/topology$auto-offset-reset (void/obtain ::KS/offset-reset
                                                                                     options
                                                                                     K/defaults))
         extract-timestamp  (some-> (::KS/extract-timestamp options)
                                    K.-interop.java/timestamp-extractor)
         deserializer-key   (K.-interop.java/extended-deserializer (void/obtain ::K/deserializer.key
                                                                                options
                                                                                K/defaults))
         deserializer-value (K.-interop.java/extended-deserializer (void/obtain ::K/deserializer.value
                                                                                options
                                                                                K/defaults))]
 
     (cond
       (coll? source)             (.addSource topology
                                              offset-reset
                                              source-name
                                              extract-timestamp
                                              deserializer-key
                                              deserializer-value
                                              ^"[Ljava.lang.String;" (into-array String
                                                                                 source))
       (K.-interop/regex? source) (.addSource topology
                                              offset-reset
                                              source-name
                                              extract-timestamp
                                              deserializer-key
                                              deserializer-value
                                              ^Pattern source)
       :else                      (throw (IllegalArgumentException. "Source must be a list of topics or a regular expression"))))
   topology))




(defn add-processor

  "Adds a processing node to the topology.

   Parents are names of sources and others processors streaming their data into this one.
  
   Just like parents, the processor must be named uniquely in order to be later refered to by others nodes in the topology.

   A processor is a map or a function producing a map containing any of those functions :

     :dvlopt.kstreams/processor.init  (fn [ctx])
      Given a context initializes processing.
      The returned value is considered as an in-memory state that will be passed to :dvlopt.kstreams/processor.on-record everytime.
      Cf. `dvlopt.kstreams.ctx`

     :dvlopt.kstreams/processor.on-record  (fn [ctx user-state record])
      Given a context and some user state produced by :dvlopt.kstreams/processor.init, processes a record.

     :dvlopt.kstreams/processor.close  (fn [])
      For producing a side-effect when the processor is shutting down, such as releasing resources.
      Resources such as state stores are already closed by the library.


   Ex. (add-processor topology
                      \"my-processor\"
                      [\"parent-1\" \"parent-2\"]
                      {:dvlopt.kstreams/processor.init      (fn [ctx]
                                                              (dvlopt.kstreams.ctx/kv-store ctx
                                                                                            \"my-store\"))
                       :dvlopt.kstreams/processor.on-record (fn [ctx my-store record]
                                                              ...)
                       :dvlopt.kstreams/processor.on-close  (fn []
                                                              (println \"Bye bye processor\"))})"

  ^Topology

  [^Topology topology processor-name parents processor]

  (.addProcessor topology
                 processor-name
                 (K.-interop.java/processor-supplier processor)
                 (into-array String
                             parents)))




(defn add-sink

  "Adds a sink topic to the topology.

   Parents are names of sources and others processors streaming their data into this one.

   Just like parents, the sink must be named uniquely in order to be later refered to by others nodes in the topology.

   A map of options may be given :

     :dvlopt.kafka/serializer.key
     :dvlopt.kafka/serializer.value
      Cf. `dvlopt.kafka` for description of serializers.

     :dvlopt.kstreams/select-partition
      Function called in order to determine the partition number the record belongs to.
      If missing, the record will be automatically partitioned by its key."

  (^Topology

   [topology sink-name parents topic]

   (add-sink topology
             sink-name
             parents
             topic
             nil))


  (^Topology

   [^Topology topology ^String sink-name parents ^String topic options]

   (.addSink topology
             sink-name
             topic
             (K.-interop.java/extended-serializer (void/obtain ::K/serializer.key
                                                               options
                                                               K/defaults))
             (K.-interop.java/extended-serializer (void/obtain ::K/serializer.value
                                                               options
                                                               K/defaults))
             (some-> (::KS/select-partition options)
                     K.-interop.java/stream-partitioner)
             ^"[Ljava.lang.String;" (into-array String
                                                parents))))




(defn add-store

  "Adds a state store to the topology which can later be used by the given processors.


   A map of options may be given, all options described in `dvlopt.kstreams.store`."

  (^Topology

   [^Topology topology processor-names]

   (add-store topology
              processor-names))


  (^Topology

   [^Topology topology processor-names options]

   (.addStateStore topology
                   (K.-interop.java/store-builder options)
                   (into-array String
                               processor-names))))




(defn add-global-store

  "Adds a global state store to the topology which, unlike a regular one, is directly connected to a topic and sources
   records from all its partitions.

   A processor is needed in order to update the global state.

   A map of options may be given, options for the store as described in `dvlopt.kstreams.store` as well as :
  
     :dvlopt.kstreams/extract-timestamp  (fn [last-timestamp record])
      Function accepting the previous timestamp of the last record and a record, and returning
      the timestamp chosen for the current record."

  (^Topology

   [^Topology topology source-name source-topic processor-name processor options]

   (.addGlobalStore topology
                    (K.-interop.java/store-builder options)
                    source-name
                    (some-> (::KS/extract-timestamp options)
                            K.-interop.java/timestamp-extractor)
                    (K.-interop.java/extended-deserializer (void/obtain ::K/deserializer.key
                                                                        options
                                                                        K/defaults))
                    (K.-interop.java/extended-deserializer (void/obtain ::K/deserializer.value
                                                                        options
                                                                        K/defaults))
                    source-topic
                    processor-name
                    (K.-interop.java/processor-supplier processor))
   topology))
