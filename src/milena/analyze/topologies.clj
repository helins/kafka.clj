(ns milena.analyze.topologies

  ""

  {:author "Adam Helinski"}

  (:require [milena.interop      :as M.interop]
            [milena.interop.java :as M.interop.java]
            [milena.interop.clj  :as M.interop.clj]
            [milena.serialize    :as M.serialize]
            [milena.deserialize  :as M.deserialize])
  (:import java.util.Collection
           java.util.regex.Pattern
           (org.apache.kafka.streams StreamsBuilder
                                     Topology)
           (org.apache.kafka.streams.kstream KStream
                                             KTable
                                             GlobalKTable)))




;;;;;;;;;;


(defn topology

  ""

  ^Topology

  []

  (Topology.))




(defn describe

  ""

  [^Topology topology]

  (M.interop.clj/topology-description (.describe topology)))




(defn add-source

  ""

  (^Topology

   [topology name source]

   (add-source topology
               name
               source
               nil))


  (^Topology

   [^Topology topology ^String name source {:as   opts
                                            :keys [deserializer
                                                   deserializer-key
                                                   deserializer-value
                                                   offset-reset
                                                   extract-timestamp]}]
 
   (let [offset-reset'       (M.interop.java/topology$auto-offset-reset offset-reset)
         extract-timestamp'  (some-> extract-timestamp
                                     M.interop.java/timestamp-extractor)
         deserializer-key'   (M.deserialize/make (or deserializer-key
                                                     deserializer))
         deserializer-value' (M.deserialize/make (or deserializer-value
                                                     deserializer))]
 
     (cond
       (string? source)          (.addSource topology
                                             offset-reset'
                                             name
                                             extract-timestamp'
                                             deserializer-key'
                                             deserializer-value'
                                             ^"[Ljava.lang.String;" (into-array String
                                                                                [source]))
       (coll? source)            (.addSource topology
                                             offset-reset'
                                             name
                                             extract-timestamp'
                                             deserializer-key'
                                             deserializer-value'
                                             ^"[Ljava.lang.String;" (into-array String
                                                                                source))
       (M.interop/regex? source) (.addSource topology
                                             offset-reset'
                                             name
                                             extract-timestamp'
                                             deserializer-key'
                                             deserializer-value'
                                             ^Pattern source)))))




(defn add-processor

  ""

  ^Topology

  [^Topology topology name parents processor]

  (.addProcessor topology
                 name
                 (M.interop.java/processor-supplier processor)
                 (into-array String
                             parents)))




(defn add-sink

  ""

  ^Topology

  [^Topology topology name parents topic {:as   opts
                                          :keys [serializer
                                                 serializer-key
                                                 serializer-value
                                                 partition-stream]}]

  (.addSink topology
            name
            topic
            (M.serialize/make (or serializer-key
                                  serializer))
            (M.serialize/make (or serializer-value
                                  serializer))
            (some-> partition-stream
                    M.interop.java/stream-partitioner)
            (into-array String
                        parents)))




(defn add-store

  ""

  (^Topology

   [^Topology topology opts]

   (add-store topology
              nil
              opts))


  (^Topology

   [^Topology topology processor-names opts]

   (.addStateStore topology
                   (M.interop.java/store-builder opts)
                   (into-array String
                               processor-names))))
