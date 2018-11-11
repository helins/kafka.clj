(ns dvlopt.kstreams.table

  "Kafka Streams' abstraction of tables.
  
   Cf. `dvlopt.kstreams.builder` for the big picture and details


   A table can be transformed by various functions. Those functions always return a new table representing the transformation.
   Hence, a single table can be used for more than one transformation.

   A table can be re-grouped by other keys using the `map-and-group-by` function and then the values aggregated for each key,
   resulting in a new table.

   A table can also be joined with another table.
  
  
   A table is backed-up by a state store. As such, these options, called the standard options in this namespace, can very often
   be supplied :

     :dvlopt.kafka/deserializer.key
     :dvlopt.kafka/deserializer.value
     :dvlopt.kafka/serializer.key
     :dvlopt.kafka/serializer.value
      Cf. `dvlopt.kafka` for description of serializers and deserializers

     :dvlopt.kstreams.store/cache?
     :dvlopt.kstreams.store/changelog?
     :dvlopt.kstreams.store/configuration.changelog
     :dvlopt.kstreams.store/name
     :dvlopt.kstreams.store/type
      Exactly as described in `dvlopt.kstreams.store` but the type is restricted to #{:kv.in-memory :kv.regular}."

  {:author "Adam Helinsi"}

  (:require [dvlopt.kafka.-interop.java :as K.-interop.java]
            [dvlopt.kstreams.store      :as KS.store])
  (:import org.apache.kafka.streams.StreamsBuilder
           (org.apache.kafka.streams.kstream KStream
                                             KTable
                                             Materialized
                                             KGroupedTable)))




;;;;;;;;;; Transformations


(defn filter-kv

  "Returns a table filtering out key-values returning false on the given predicate.
  
   Keys with nil values are removed right away.


   Standard options can be provided (cf. namespace description).
  

   Ex. (filter-kv table
                  (fn [k v]
                    (>= (:age v)
                        18)))"

  (^KTable

   [table predicate]

   (filter-kv table
              predicate
              nil))


  (^KTable

   [^KTable table predicate standard-options]

   (.filter table
            (K.-interop.java/predicate predicate)
            (K.-interop.java/materialized--kv standard-options))))




(defn map-values

  "Returns a table mapping the value of each key-value.


   Standard options can be provided (cf. namespace description).
  

   Ex. (map-values table
                   (fn [k v]
                     (assoc v
                            :country
                            (country-from-ip (:ip v)))))"

  (^KTable

   [table f]

   (map-values table
               f
               nil))


  (^KTable

   [^KTable table f standard-options]

   (.mapValues table
               (K.-interop.java/value-mapper-with-key f)
               (K.-interop.java/materialized--kv standard-options))))




(defn process-values

  "Just like ´dvlopt.kstreams.stream/process-value` but with a table.
  
   
   Standard options can be provided (cf. namespace description)."

  (^KTable
    
   [table processor]

   (process-values table
                   processor
                   nil))


  (^KTable

   [^KTable table processor options]

   (.transformValues table
                     (K.-interop.java/value-transformer-with-key-supplier processor)
                     (K.-interop.java/materialized--kv options)
                     (into String
                           (::KS.store/names options)))))



;;;;;;;;; Joins


(defn join-with-table

  "Returns a table joining values from both tables when they share the same key.

   Records with nil values removes the corresponding key from the resulting table. Records with nil keys will be dropped.

   Cf. `dvlopt.kstreams.builder` for requirements related to joins

   
   Standard options can be provided (cf. namespace description).


   Ex. (join-with-table left-table
                        right-table
                        (fn [v-left v-right]
                          (merge v-left
                                 v-right)))"

  (^KTable

   [left-table right-table f]

   (join-with-table left-table
                    right-table
                    f
                    nil))


  (^KTable

   [^KTable left-table ^KTable right-table f standard-options]

   (.join left-table
          right-table
          (K.-interop.java/value-joiner f)
          (K.-interop.java/materialized--kv standard-options))))




(defn left-join-with-table

  "Exactly like `join-with-table` but the join is triggered even if the right table does not contain the key yet.
   In such case, the right value provided for the join is nil."

  (^KTable

   [left-table right-table f]

   (left-join-with-table left-table
                         right-table
                         f
                         nil))


  (^KTable

   [^KTable left-table ^KTable right-table f options]

   (.leftJoin left-table
              right-table
              (K.-interop.java/value-joiner f)
              (K.-interop.java/materialized--kv options))))




(defn outer-join-with-table

  "Exactly like `join-outer-with-table` but the join is triggered even if the other table does not contain the key yet.
   In such case, the value provided for this side of the join is nil."

  (^KTable

   [left-table right-table f]

   (outer-join-with-table left-table
                          right-table
                          f
                          nil))


  (^KTable

   [^KTable left-table ^KTable right-table f options]

   (.outerJoin left-table
               right-table
               (K.-interop.java/value-joiner f)
               (K.-interop.java/materialized--kv options))))




;;;;;;;;; Aggregations


(defn map-and-group-by

  "Returns a grouped table mapping key-values and then re-grouping them based on the new keys.

   Drops mapped records with nil keys.
  
   Because a new key is explicitly selected, the data is repartioned.
   Cf. `dvlopt.kstreams.builder` about repartioning.

   A map of options may be given :

     :dvlopt.kafka/deserializer.key
     :dvlopt.kafka/deserializer.value
     :dvlopt.kafka/serializer.key
     :dvlopt.kafka/serializer.value
      Cf. `dvlopt.kafka` for description of serializers and deserializers


   Ex. ;; Re-groups a table on countries while remembering from which user are the values.
  
       (regroup-by table
                   (fn [k v]
                     [(:country v)
                      (assoc v
                             :user-id
                             k)]))"

  (^KGroupedTable

   [table f]

   (map-and-group-by table
                     f
                     nil))


  (^KGroupedTable

   [^KTable table f options]

   (.groupBy table
             (K.-interop.java/key-value-mapper f)
             (K.-interop.java/serialized options))))




(defn reduce-values

  "Returns a new table aggregating the values for each key of the given grouped table.

   Let us take the example provided in `map-and-group-by` where users were re-grouped by country. Let us say each user has an :income and
   we want to sum all incomes for each country.

   A seed (sum of incomes) is created for each grouped key (country). Each new value (a user with an income) is aggregated using (reduce-add [agg k v])
   in order to increase the country sum. If the user is removed, then its income is removed from the country sum using (reduce-sub [agg k v]). If the
   value for an existing user is replaced, then both functions are called in undefined order for updating the country sum with the possibly new income.


   Ex. (reduce-values grouped-table
                      (fn reduce-add [country-income country user]
                        (+ country-income
                           (:income user)))
                      (fn reduce-sub [country-income country user]
                        (- country-income
                           (:income user)))
                      (fn seed []
                        0))"

  (^KTable

   [grouped-table fn-reduce-add fn-reduce-sub fn-seed]

   (reduce-values grouped-table
                  fn-reduce-add
                  fn-reduce-sub
                  fn-seed
                  nil))


  (^KTable

   [^KGroupedTable grouped-table f-add f-sub seed options]

   (.aggregate grouped-table
               (K.-interop.java/initializer seed)
               (K.-interop.java/aggregator f-add)
               (K.-interop.java/aggregator f-sub)
               (K.-interop.java/materialized--kv options))))




;;;;;;;;; Misc


(defn to-stream

  "Turns a table into a stream.
  
   A function can be provided for selecting new keys.


   Ex. ;; The new key is the length of the value.
  
       (to-stream table
                  (fn [k v]
                    (count v)))"

  (^KStream

   [^KTable table]

   (.toStream table))


  (^KStream

   [^KTable table f]

   (.toStream table
              (K.-interop.java/key-value-mapper--raw f))))




(defn store-name

  "Returns the name of the local underlying state store that can be used to query this table, or nil if the table cannot be
   queried."

  [^KTable table]

  (.queryableStoreName table))
