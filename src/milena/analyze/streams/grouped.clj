(ns milena.analyze.streams.grouped

  ""

  {:author "Adam Helinski"}

  (:refer-clojure :exclude [reduce
                            count])
  (:require [milena.interop.java :as M.interop.java])
  (:import (org.apache.kafka.streams.kstream KGroupedStream
                                             TimeWindowedKStream
                                             SessionWindowedKStream)))




;;;;;;;;;;


(defn window-by-interval

  ""

  ^TimeWindowedKStream

  [^KGroupedStream grouped-stream opts]

  (.windowedBy grouped-stream
               (M.interop.java/time-windows opts)))




(defn window-by-session

  ""

  ^SessionWindowedKStream

  [^KGroupedStream grouped-stream opts]

  (.windowedBy grouped-stream
               (M.interop.java/session-windows opts)))




;;;;;;;;;;


(defprotocol IGrouped

  ""

  (^KTable

   aggregate [grouped seed f]
             [grouped seed f opts]

    "")


  (^KTable
    
   reduce [grouped f]
          [grouped f opts]

    "")


  (^KTable
     
   count [grouped]
         [grouped opts]

    ""))




(extend-type KGroupedStream

  IGrouped

    (aggregate [grouped-stream seed f]
      (.aggregate grouped-stream
                  (M.interop.java/initializer seed)
                  (M.interop.java/aggregator f)))


    (aggregate [grouped-stream seed f opts]
      (.aggregate grouped-stream
                  (M.interop.java/initializer seed)
                  (M.interop.java/aggregator f)
                  (M.interop.java/materialized--kv (merge opts
                                                          (:store opts)))))


    (reduce [grouped-stream f]
      (.reduce grouped-stream
               (M.interop.java/reducer f)))


    (reduce [grouped-stream f opts]
      (.reduce grouped-stream
               (M.interop.java/reducer f)
               (M.interop.java/materialized--kv (merge opts
                                                       (:store opts)))))


    (count [grouped-stream]
      (.count grouped-stream))


    (count [grouped-stream opts]
      (.count grouped-stream
              (M.interop.java/materialized--kv (merge opts
                                                      (:store opts))))))




(extend-type TimeWindowedKStream

  IGrouped

    (aggregate [tw-stream seed f]
      (.aggregate tw-stream
                  (M.interop.java/initializer seed)
                  (M.interop.java/aggregator f)))


    (aggregate [tw-stream seed f opts]
      (.aggregate tw-stream
                  (M.interop.java/initializer seed)
                  (M.interop.java/aggregator f)
                  (M.interop.java/materialized--by-name (merge opts
                                                               (:store opts)))))


    (reduce [tw-stream f]
      (.reduce tw-stream
               (M.interop.java/reducer f)))


    (reduce [tw-stream f opts]
      (.reduce tw-stream
               (M.interop.java/reducer f)
               (M.interop.java/materialized--by-name (merge opts
                                                            (:store opts)))))


    (count [tw-stream]
      (.count tw-stream))


    (count [tw-stream opts]
      (.count tw-stream
              (M.interop.java/materialized--by-name (merge opts
                                                           (:store opts))))))




(extend-type SessionWindowedKStream

  IGrouped

    (aggregate [sw-stream seed f]
      (.aggregate sw-stream
                  (M.interop.java/initializer seed)
                  (M.interop.java/aggregator f)))


    (aggregate [sw-stream seed f opts]
      (.aggregate sw-stream
                  (M.interop.java/initializer seed)
                  (M.interop.java/aggregator f)
                  (M.interop.java/materialized--by-name (merge opts
                                                               (:store opts)))))


    (reduce [sw-stream f]
      (.reduce sw-stream
               (M.interop.java/reducer f)))


    (reduce [sw-stream f opts]
      (.reduce sw-stream
               (M.interop.java/reducer f)
               (M.interop.java/materialized--by-name (merge opts
                                                            (:store opts)))))


    (count [sw-stream]
      (.count sw-stream))


    (count [sw-stream opts]
      (.count sw-stream
              (M.interop.java/materialized--by-name (merge opts
                                                           (:store opts))))))
