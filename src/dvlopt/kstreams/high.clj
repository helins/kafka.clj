(ns dvlopt.kstreams.high

  "High level API for Kafka Streams.
  
   Rather functional.
  

   Overview
   ========
  
   This API revolves mainly around these abstractions :

   
     Streams (`dvlopt.kstreams.stream`)
     ----------------------------------------

     A stream represent a sequence of records which need to be somehow transformed (mapped, filtered, etc). It is distributed by partitions.

     Often, some kind of aggregated values need to be computed. Fist, values need to be grouped by key to form a grouped stream. It is as if the stream is
     being divided into substreams, one for every key. Although useful, such a grouped stream does not have a notion of time. Hence, before applying any
     aggregation, a grouped stream can be windowed if needed. For instance, if keys are user names and values are clicks, we can group the stream by key,
     window per day, and then aggregate the values by counting the clicks. This would be for computing the number of clicks per user, per day.

     Grouped streams, windowed or not, are intermediary representations. Aggregating values always result in a table.

     Tables (`dvlopt.kstreams.table`)
     --------------------------------------

     A table associates a unique value to a key. For a given key, a new record represents an update. It can be created right away from a topic or be the
     result of an aggregation. It is backed-up by a key-value state store. Such a table is distributed by partitions. Hence, in order to be able to query
     alls keys, if needed, the application instances must be able to query each other. Typically, tables follow delete semantics (ie. a nil value for a
     key removes this key from the table).

     Akin to streams, tables can be re-grouped by another key. For instance, a table of users (keys) might be re-grouped into a table of countries (new keys,
     each user belongs to a country). Each country now has a list of values (the values of all the user belonging to that country) and those can be aggregated
     as well. The end result is naturally a new table.

     Global tables
     -------------

     A regular table is distributed by partitions. A global one sources its data from all the partitions of a topic at the same time. It is fine and useful
     as long as the data do not overwhelm a single instance of the Kafka Streams application.


   Aggregating
   ===========

   Values can be reduced by key. An aggregation is done much like in clojure itself : a reducing (fn [aggregated key value). Before processing the first record
   of a key, a function is called for obtaining a seed (ie. first aggregated value). The value is aggregated against the seed, thus bootstrapping the
   aggregation.


   State and repartioning
   ======================
   
   Just like in the low-level API, state stores are used for stateful operations. Those stores are typically created automatically and need not much more
   else than serializers and deserializers as described in `dvlopt.kafka`.
  
   Cf. `dvlopt.kstreams.store` for more about stores.

   Any operation acting on the keys of the records will result in a repartioning of data at some point, either right away or later. This means the library
   will persist the new records in an internal topic named '$APPLICATION_ID-$GENERATED_NAME-repartition'. This is needed because the way keys are partioned
   is very important for a lot of stateful operations such as joins. Operations which might lead to repartioning document this behavior.
  

   Joins and Co-partitioning
   =========================

   This API offers different kind of joining operations akin to SQL joins. For instance, a stream might be enriched by joining it with a table. Joins are
   always based on the keys of the records, hence the involved topics need to be co-partitioned. It means they must share the same number of partitions as
   well as the same partitioning strategy (eg. the default one). By doing so, the library can source records from the same partition numbers, in both joined
   topics, and be confident that each corresponding partition holds the same keys. 

   It is the responsability of the user to garantee the same number of partitions otherwise an exception will be thrown. For instance, if needed, a stream
   can be redirected to an adequate pre-created topic. It is easiest to use the default partitioning strategy. Other than that, a producer might decide the
   partition number of the records it is sending. In Kafka Streams, the partition number can be decided when writing to a topic by using the
   :dvlopt.kstreams/select-partition options. It is a function taking the total number of partitions of the topic a record is being sent to, as well as the
   key and the value of this record.

   All of this do not apply to joins with global tables as they sources data from all the available partitions."

  {:author "Adam Helinski"}

  (:require [dvlopt.kafka               :as K]
            [dvlopt.kafka.-interop      :as K.-interop]
            [dvlopt.kafka.-interop.java :as K.-interop.java]
            [dvlopt.void                :as void])
  (:import java.util.Collection
           java.util.regex.Pattern
           (org.apache.kafka.streams StreamsBuilder
                                     Topology)
           (org.apache.kafka.streams.kstream GlobalKTable
                                             KStream
                                             KTable)))




;;;;;;;;;; Public


(defn builder

  "A builder is used as a starting point for the functional API. It actually builds a topology under the hood."

  ^StreamsBuilder

  []

  (StreamsBuilder.))




(defn topology

  "Once ready, a builder can be transformed to a topology in order to make a Kafka Stream application or to add some low-level
   processing.
  
   Cf. `dvlopt.kstreams.topology` namespace."

  ^Topology

  [^StreamsBuilder builder]

  (.build builder))




(defn stream

  "Adds and returns a stream sourcing its data from a topic, a list of topics or a regular expression for topics.

   This stream can be used with the `dvlopt.kstreams.high.stream` namespace.
  

   A map of options may be given :

     :dvlopt.kafka/deserializer.key
     :dvlopt.kafka/deserializer.value
      Cf. `dvlopt.kafka` for description of deserializers.

     :dvlopt.kstreams/extract-timestamp
      Function accepting the previous timestamp of the last record and a record, and returning
      the timestamp chosen for the current record.

     :dvlopt.kstreams/offset-reset
      When a topic is consumed at first, it should start from the :earliest offset or the :latest."

  (^KStream

   [builder source]

   (stream builder
           source
           nil))


  (^KStream

   [^StreamsBuilder builder source options]

   (let [consumed (K.-interop.java/consumed options)]
     (cond
       (string? source)           (.stream builder
                                           ^String source
                                           consumed)
       (coll? source)             (.stream builder
                                           ^Collection source
                                           consumed)
       (K.-interop/regex? source) (.stream builder
                                          ^Pattern source
                                          consumed)))))




(defn table

  "Adds and returns a table which can be used with the `dvlopt.kstreams.table` namespace.

   A map of options may be given :

     :dvlopt.kafka/deserializer.key
     :dvlopt.kafka/deserializer.value
     :dvlopt.kafka/serializer.key
     :dvlopt.kafka/serializer.value
      Cf. `dvlopt.kafka` for description of serializers and deserializers.

     :dvlopt.kstreams/extract-timestamp
     :dvlopt.kstreams/offset-reset
      Cf. `stream`

     :dvlopt.kstreams.store/cache?
     :dvlopt.kstreams.store/name
     :dvlopt.kstreams.store/type
      Cf. `dvlopt.kstreams.store`
      The type is restricted to #{:kv.in-memory :kv.regular}.
      Other options related to stores are not needed. No changelog topic is required because the table is created
      directly from an original topic."

  (^KTable

   [builder topic]

   (table builder
          topic
          nil))


  (^KTable

   [^StreamsBuilder builder topic options]

   (.table builder
           topic
           (K.-interop.java/consumed options)
           (K.-interop.java/materialized--kv options))))




(defn global-table

  "Adds a global table which, unlike a regular one, will source its date from all the partitions of a topic at the
   same time.
  
   Cf. `table`"

  (^GlobalKTable

   [builder source]

   (global-table builder
                 source
                 nil))


  (^GlobalKTable

   [^StreamsBuilder builder source options]

   (.globalTable builder
                 source
                 (K.-interop.java/consumed options)
                 (K.-interop.java/materialized--kv options))))




(defn add-store

  "Manually adds a state store.

   Typically, stores are created automatically. However, this high-level API also offers a few operations akin to what
   can be find in the low-level API. Those might need an access to a manually created state store.


   A map of options may be given, all options described in `dvlopt.kstreams.store`."


  (^StreamsBuilder

   [builder]

   (add-store builder
              nil))


  (^StreamsBuilder

   [^StreamsBuilder builder options]

   (.addStateStore builder
                   (K.-interop.java/store-builder options))))




(defn add-global-store

  "Adds a global state store just like `dvlopt.kstreams.topology/add-global-store`."

  ^StreamsBuilder

  [^StreamsBuilder builder source-topic processor options]

  (.addGlobalStore builder
                   (K.-interop.java/store-builder options)
                   source-topic
                   (K.-interop.java/consumed options)
                   (K.-interop.java/processor-supplier processor)))
