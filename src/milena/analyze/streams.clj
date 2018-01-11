(ns milena.analyze.streams

  ""

  {:author "Adam Helinski"}

  (:refer-clojure :exclude [filter
                            map
                            group-by])
  (:require [clojure.core        :as clj]
            [milena.interop.java :as M.interop.java])
  (:import (org.apache.kafka.streams.kstream KStream
                                             KTable
                                             GlobalKTable
                                             KGroupedStream
                                             Predicate
                                             KeyValueMapper)))




;;;;;;;;;;


(defn branch

  ""

  ^{:tag "[Lorg.apache.kafka.streams.kstream.KStream;"}

  [^KStream stream preds?]

  (.branch stream
           (into-array Predicate
                       (clj/map M.interop.java/predicate
                                preds?))))




(defn filter

  ""

  ^KStream

  [^KStream stream pred?]

  (.filter stream
           (M.interop.java/predicate pred?)))





(defn map

  ""

  ^KStream

  [^KStream stream f]

  (.map stream
        (M.interop.java/key-value-mapper f)))




(defn map-keys

  ""

  ^KStream

  [^KStream stream f]

  (.selectKey stream
              (M.interop.java/key-value-mapper--key f)))




(defn map-vals

  ""

  ^KStream

  [^KStream kstream f]

  (.mapValues kstream
              (M.interop.java/value-mapper f)))




(defn fmap

  ""

  ^KStream

  [^KStream kstream f]

  (.flatMap kstream
            (M.interop.java/key-value-mapper--flat f)))




(defn fmap-vals

  ""

  ^KStream

  [^KStream kstream f]

  (.flatMapValues kstream
                  (M.interop.java/value-mapper f)))




(defn group-by

  ""

  ^KGroupedStream

  [^KStream kstream f opts]

  (.groupBy kstream
            (M.interop.java/key-value-mapper--raw f)
            (M.interop.java/serialized opts)))




(defn group-by-key

  ""

  (^KGroupedStream

   [^KStream kstream]

   (.groupByKey kstream))


  (^KGroupedStream

   [^KStream kstream opts]

   (.groupByKey kstream
                (M.interop.java/serialized opts))))




(defn join-with-stream

  ""

  (^KStream

   [^KStream stream-left ^KStream stream-right window-ms f]

   (.join stream-left
          stream-right
          (M.interop.java/value-joiner f)
          (M.interop.java/join-windows window-ms)))


  (^KStream

   [^KStream stream-left ^KStream stream-right window-ms f opts]

   (.join stream-left
          stream-right
          (M.interop.java/value-joiner f)
          (M.interop.java/join-windows window-ms)
          (M.interop.java/joined opts))))




(defn join-with-table

  ""

  (^KStream

   [^KStream stream-left ^KTable table-right f]

   (.join stream-left
          table-right
          (M.interop.java/value-joiner f)))


  (^KStream

   [^KStream stream-left ^KTable table-right f opts]

   (.join stream-left
          table-right
          (M.interop.java/value-joiner f)
          (M.interop.java/joined opts))))




(defn join-with-global-table

  ""

  ^KStream

  ;; TODO why no arity with Joined in the java lib ?
  ;; TODO why `f-map` for gtables and not for another kinds of join ?

  [^KStream stream-left ^GlobalKTable gtable-right f-map f-join]

  (.join stream-left
         gtable-right
         (M.interop.java/key-value-mapper--raw f-map)
         (M.interop.java/value-joiner f-join)))




(defn join-left-with-stream

  ""

  (^KStream

   [^KStream stream-left ^KStream stream-right window-ms f]

   (.leftJoin stream-left
              stream-right
              (M.interop.java/value-joiner f)
              (M.interop.java/join-windows window-ms)))


   (^KStream

    [^KStream stream-left ^KStream stream-right window-ms f opts]

    (.leftJoin stream-left
               stream-right
               (M.interop.java/value-joiner f)
               (M.interop.java/join-windows window-ms)
               (M.interop.java/joined opts))))




(defn join-left-with-table

  ""

  (^KStream

   [^KStream stream-left ^KTable table-right f]

   (.leftJoin stream-left
              table-right
              (M.interop.java/value-joiner f)))


  (^KStream

   [^KStream stream-left ^KTable table-right f opts]

   (.leftJoin stream-left
              table-right
              (M.interop.java/value-joiner f)
              (M.interop.java/joined opts))))




(defn join-left-with-global-table

  ""

  ^KStream

  ;; TODO cf. `join-with-global-table` for arity and `f-map`

  [^KStream stream-left ^GlobalKTable gtable-right f-map f-join]

  (.leftJoin stream-left
             gtable-right
             (M.interop.java/key-value-mapper--raw f-map)
             (M.interop.java/value-joiner f-join)))




(defn join-outer-with-stream

  ""

  (^KStream

   [^KStream stream-left ^KStream stream-right window-ms f]

   (.outerJoin stream-left
               stream-right
               (M.interop.java/value-joiner f)
               (M.interop.java/join-windows window-ms)))


  (^KStream

   [^KStream stream-left ^KStream stream-right window-ms f opts]

   (.outerJoin stream-left
               stream-right
               (M.interop.java/value-joiner f)
               (M.interop.java/join-windows window-ms)
               (M.interop.java/joined opts))))




(defn through

  ""

  (^KStream

   [^KStream stream topic]

   (.through stream
             topic))


  (^KStream

   [^KStream stream ^String topic opts]

   (.through stream
             topic
             (M.interop.java/produced opts))))




(defn process

  ""

  ^KStream

  [^KStream stream store-names processor]

  (.process stream
            (M.interop.java/processor-supplier processor)
            (into-array String
                        store-names)))




(defn side-effect

  ""

  ^KStream

  [^KStream stream f]

  (.peek stream
         (M.interop.java/foreach-action f)))




(defn sink-topic

  ""

  ([^KStream kstream topic]

   (.to kstream
        topic))


  ([^KStream kstream ^String topic opts]

   (.to kstream
        topic
        (M.interop.java/produced opts))))




(defn sink

  ""

  [^KStream kstream f]

  (.foreach kstream
            (M.interop.java/foreach-action f)))
