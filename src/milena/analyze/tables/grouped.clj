(ns milena.analyze.tables.grouped

  ""

  {:author "Adam Helinski"}

  (:refer-clojure :exclude [reduce
                            count])
  (:require [milena.interop.java :as M.interop.java])
  (:import (org.apache.kafka.streams.kstream KTable
                                             KGroupedTable)))




;;;;;;;;;;


(defn aggregate

  ""

  (^KTable

   [^KGroupedTable grouped-table seed f-add f-sub]

   (.aggregate grouped-table
               (M.interop.java/initializer seed)
               (M.interop.java/aggregator f-add)
               (M.interop.java/aggregator f-sub)))


  (^KTable

   [^KGroupedTable grouped-table seed f-add f-sub opts]

   (.aggregate grouped-table
               (M.interop.java/initializer seed)
               (M.interop.java/aggregator f-add)
               (M.interop.java/aggregator f-sub)
               (M.interop.java/materialized--kv (merge opts
                                                       (:store opts))))))




(defn reduce

  ""

  (^KTable

   [^KGroupedTable grouped-table f-add f-sub]

   (.reduce grouped-table
            (M.interop.java/reducer f-add)
            (M.interop.java/reducer f-sub)))


  (^KTable

   [^KGroupedTable grouped-table f-add f-sub opts]

   (.reduce grouped-table
            (M.interop.java/reducer f-add)
            (M.interop.java/reducer f-sub)
            (M.interop.java/materialized--kv (merge opts
                                                    (:store opts))))))




(defn count

  ""

  (^KTable

   [^KGroupedTable grouped-table]

   (.count grouped-table))


  (^KTable

   [^KGroupedTable grouped-table opts]

   (.count grouped-table
           (M.interop.java/materialized--kv (merge opts
                                                   (:store opts))))))
