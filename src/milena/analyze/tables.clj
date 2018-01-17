( ns milena.analyze.tables

  ""

  {:author "Adam Helinsi"}

  (:refer-clojure :exclude [filter
                            map
                            group-by])
  (:require [milena.interop.java :as M.interop.java])
  (:import org.apache.kafka.streams.StreamsBuilder
           (org.apache.kafka.streams.kstream KStream
                                             KTable
                                             Materialized
                                             KGroupedTable)))




;;;;;;;;;;


(defn filter

  ""

  (^KTable

   [^KTable table pred?]

   (.filter table
            (M.interop.java/predicate pred?)))


  (^KTable

   [^KTable table pred? opts]

   (.filter table
            (M.interop.java/predicate pred?)
            (M.interop.java/materialized--kv (merge opts
                                                    (:store opts))))))




(defn map-vals

  ""

  (^KTable

   [^KTable table f]

   (.mapValues table
               (M.interop.java/value-mapper f)))


  (^KTable

   [^KTable table f opts]

   (.mapValues table
               (M.interop.java/value-mapper f)
               (M.interop.java/materialized--kv (merge opts
                                                       (:store opts))))))




(defn group-by

  ""

  (^KGroupedTable

   [^KTable table f]

   (.groupBy table
             (M.interop.java/key-value-mapper f)))


  (^KGroupedTable

   [^KTable table f opts]

   (.groupBy table
             (M.interop.java/key-value-mapper f)
             (M.interop.java/serialized opts))))





(defn join-with-table

  ""

  (^KTable

   [^KTable table-left ^KTable table-right f]

   (.join table-left
          table-right
          (M.interop.java/value-joiner f)))


  (^KTable

   [^KTable table-left ^KTable table-right f opts]

   (.join table-left
          table-right
          (M.interop.java/value-joiner f)
          (M.interop.java/materialized--kv (merge opts
                                                  (:store opts))))))




(defn join-left-with-table

  ""

  (^KTable

   [^KTable table-left ^KTable table-right f]

   (.leftJoin table-left
              table-right
              (M.interop.java/value-joiner f)))


  (^KTable

   [^KTable table-left ^KTable table-right f opts]

   (.leftJoin table-left
              table-right
              (M.interop.java/value-joiner f)
              (M.interop.java/materialized--kv (merge opts
                                                      (:store opts))))))




(defn join-outer-with-table

  ""

  (^KTable

   [^KTable table-left ^KTable table-right f]

   (.outerJoin table-left
               table-right
               (M.interop.java/value-joiner f)))


  (^KTable

   [^KTable table-left ^KTable table-right f opts]

   (.outerJoin table-left
               table-right
               (M.interop.java/value-joiner f)
               (M.interop.java/materialized--kv (merge opts
                                                       (:store opts))))))




(defn into-stream

  ""

  (^KStream

   [^KTable table]

   (.toStream table))


  (^KStream

   [^KTable table f]

   (.toStream table
              (M.interop.java/key-value-mapper--raw f))))
