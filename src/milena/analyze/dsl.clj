(ns milena.analyze.dsl

  ""

  {:author "Adam Helinski"}

  (:require [milena.interop      :as M.interop]
            [milena.interop.java :as M.interop.java])
  (:import java.util.Collection
           java.util.regex.Pattern
           (org.apache.kafka.streams StreamsBuilder
                                     Topology)
           (org.apache.kafka.streams.kstream KStream
                                             KTable
                                             GlobalKTable)
           )
  )




;;;;;;;;;;


(defn builder

  ""

  ^StreamsBuilder

  []

  (StreamsBuilder.))




(defn stream

  ""

  (^KStream

   [builder source]

   (stream builder
           source
           nil))


  (^KStream

   [^StreamsBuilder builder source opts]

   (let [consumed (M.interop.java/consumed opts)]
     (cond
       (string? source)          (.stream builder
                                          ^String source
                                          consumed)
       (coll? source)            (.stream builder
                                          ^Collection source
                                          consumed)
       (M.interop/regex? source) (.stream builder
                                          ^Pattern source
                                          consumed)))))




(defn table

  ""

  (^KTable

   [builder source]

   (table builder
          source
          nil))


  (^KTable

   [^StreamsBuilder builder source opts]

   (.table builder
           source
           (M.interop.java/consumed opts)
           (M.interop.java/materialized--kv (merge opts
                                                   (:store opts))))))




(defn global-table

  ""

  (^GlobalKTable

   [builder source]

   (global-table builder
                 source
                 nil))


  (^GlobalKTable

   [^StreamsBuilder builder source opts]

   (.globalTable builder
                 source
                 (M.interop.java/consumed opts)
                 (M.interop.java/materialized--kv (merge opts
                                                         (:store opts))))))




(defn add-store

  ""

  ^StreamsBuilder

  [^StreamsBuilder builder store]

  (.addStateStore builder
                  (M.interop.java/store-builder store)))




(defn topology

  ""

  ^Topology

  [^StreamsBuilder builder]

  (.build builder))
