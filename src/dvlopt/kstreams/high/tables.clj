(ns dvlopt.kstreams.high.tables

  "Handling of tables.
  
   Cf. `dvlopt.kstreams.high` for the big picture and details


   A table can be transformed by various functions. Those functions always return a new table representing the transformation.
   Hence, a single table can be used for more than one transformation.

   A table can be re-grouped by other keys using the `map-and-group-by` function and then the values aggregated for each key,
   resulting in a new table.

   A table can also be joined with another table.
  
  
   A table is backed-up by a state store. As such, these options, called the standard options in this namespace, can very often
   be supplied :

     :dvlopt.kstreams.stores/cache?
     :dvlopt.kstreams.stores/changelog?
     :dvlopt.kstreams.stores/configuration.changelog
     :dvlopt.kstreams.stores/name
     :dvlopt.kstreams.stores/type
      Exactly as described in `dvlopt.kstreams.stores` but the type is restricted to #{:kv.in-memory :kv.regular}."

  {:author "Adam Helinsi"}

  (:require [dvlopt.kafka.-interop.java :as K.-interop.java]
            [dvlopt.kstreams.stores     :as KS.stores])
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
  

   Ex. (filter-kv (fn [k v]
                    (>= (:age v)
                        18))
                  table)"

  (^KTable

   [predicate table]

   (filter-kv predicate
              table
              nil))


  (^KTable

   [predicate ^KTable table standard-options]

   (.filter table
            (K.-interop.java/predicate predicate)
            (K.-interop.java/materialized--kv standard-options))))




(defn map-values

  "Returns a table mapping the value of each key-value.


   Standard options can be provided (cf. namespace description).
  

   Ex. (map-values (fn [k v]
                     (assoc v
                            :country
                            (country-from-ip (:ip v))))
                   table)"

  (^KTable

   [f table]

   (map-values f
               table
               nil))


  (^KTable

   [f ^KTable table standard-options]

   (.mapValues table
               (K.-interop.java/value-mapper-with-key f)
               (K.-interop.java/materialized--kv standard-options))))




(defn process-values

  "Just like ´dvlopt.kstreams.high.streams/process-value` but with a table.
  
   
   Standard options can be provided (cf. namespace description)."

  (^KTable
    
   [processor table]

   (process-values processor
                   table
                   nil))


  (^KTable

   [processor ^KTable table options]

   (.transformValues table
                     (K.-interop.java/value-transformer-with-key-supplier processor)
                     (K.-interop.java/materialized--kv options)
                     (into String
                           (::KS.stores/names options)))))



;;;;;;;;; Joins


(defn join-with-table

  "Returns a table joining values from both tables when they share the same key.

   Records with nil values removes the corresponding key from the resulting table. Records with nil keys will be dropped.

   Cf. `dvlopt.kstreams.high` for requirements related to joins

   
   Standard options can be provided (cf. namespace description).


   Ex. (join-with-table (fn [v-left v-right]
                          (merge v-left
                                 v-right))
                        left-table
                        right-table)"

  (^KTable

   [f left-table right-table]

   (join-with-table f
                    left-table
                    right-table
                    nil))


  (^KTable

   [f ^KTable left-table ^KTable right-table standard-options]

   (.join left-table
          right-table
          (K.-interop.java/value-joiner f)
          (K.-interop.java/materialized--kv standard-options))))




(defn left-join-with-table

  "Exactly like `join-with-table` but the join is triggered even if the right table does not contain the key yet.
   In such case, the right value provided for the join is nil."

  (^KTable

   [f left-table right-table]

   (left-join-with-table f
                         left-table
                         right-table
                         nil))


  (^KTable

   [f ^KTable left-table ^KTable right-table options]

   (.leftJoin left-table
              right-table
              (K.-interop.java/value-joiner f)
              (K.-interop.java/materialized--kv options))))




(defn outer-join-with-table

  "Exactly like `join-outer-with-table` but the join is triggered even if the other table does not contain the key yet.
   In such case, the value provided for this side of the join is nil."

  (^KTable

   [f left-table right-table]

   (outer-join-with-table f
                          left-table
                          right-table
                          nil))


  (^KTable

   [f ^KTable left-table ^KTable right-table options]

   (.outerJoin left-table
               right-table
               (K.-interop.java/value-joiner f)
               (K.-interop.java/materialized--kv options))))




;;;;;;;;; Aggregations


(defn map-and-group-by

  "Returns a grouped table mapping key-values and then re-grouping them based on the new keys.

   Drops mapped records with nil keys.
  
   Because a new key is explicitly selected, the data is repartioned.
   Cf. `dvlopt.kstreams.high` about repartioning.

   A map of options may be given :

     :dvlopt.kafka/deserializer.key
     :dvlopt.kafka/deserializer.value
     :dvlopt.kafka/serializer.key
     :dvlopt.kafka/serializer.value
      Cf. `dvlopt.kafka` for description of serializers and deserializers


   Ex. ;; Re-groups a table on countries while remembering from which user are the values.
  
       (regroup-by (fn [k v]
                     [(:country v)
                      (assoc v
                             :user
                             k)])
                   table)"

  (^KGroupedTable

   [f table]

   (map-and-group-by f
                     table
                     nil))


  (^KGroupedTable

   [f ^KTable table f options]

   (.groupBy table
             (K.-interop.java/key-value-mapper f)
             (K.-interop.java/serialized options))))




(defn reduce-values

  "Returns a new table aggregating the values for each key of the given grouped table.

   Records with nil keys are ignored.

   When the first non-nil value is received for a key, it is aggregated using the `fn-reduce-add` aggregating function.
   When subsequent non-nil values are received for a key, `fn-reduce-sub` is also called with the current aggregated value
   and the old value as stored in the table. The order of those 2 operations is undefined.


   Ex. (reduce-values (fn reduce-add [sum k v]
                        (
  "

  ;; TODO. Docstring.

  (^KTable

   [fn-reduce-add fn-reduce-sub fn-seed grouped-table]

   (reduce-values fn-reduce-add
                  fn-reduce-sub
                  fn-seed
                  grouped-table
                  nil))


  (^KTable

   [f-add f-sub seed ^KGroupedTable grouped-table options]

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
  
       (to-stream (fn [k v]
                    (count v)
                  table))"

  (^KStream

   [^KTable table]

   (.toStream table))


  (^KStream

   [f ^KTable table]

   (.toStream table
              (K.-interop.java/key-value-mapper--raw f))))




(defn store-name

  "Returns the name of the local underlying state store that can be used to query this table, or nil if the table cannot be
   queried."

  [^KTable table]

  (.queryableStoreName table))
