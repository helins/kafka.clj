(ns milena.analyze.ctx

  ""

  {:author "Adam Helinski"}

  (:refer-clojure :exclude [partition])
  (:require [milena.interop.java :as M.interop.java])
  (:import org.apache.kafka.streams.processor.ProcessorContext
           (org.apache.kafka.streams.state KeyValueStore
                                           WindowStore
                                           SessionStore)))




;;;;;;;;;;


(defn commit

  ""

  ^ProcessorContext

  [^ProcessorContext ctx]

  (.commit ctx)
  ctx)




(defn forward

  ""

  (^ProcessorContext

   [^ProcessorContext ctx k v]

   (.forward ctx
             k
             v)
   ctx)


  (^ProcessorContext

   [^ProcessorContext ctx ^String child-name k v]

   (.forward ctx
             k
             v
             child-name)
   ctx))




(defn schedule

  ""

  ^ProcessorContext

  [^ProcessorContext ctx time-type interval-ms f]

  (.schedule ctx
             interval-ms
             (M.interop.java/punctuation-type time-type)
             (M.interop.java/punctuator f))
  ctx)




(defn store-kv

  ""

  ^KeyValueStore

  [^ProcessorContext ctx name]

  (.getStateStore ctx
                  name))




(defn store-windows

  ""

  ^WindowStore

  [^ProcessorContext ctx name]

  (.getStateStore ctx
                  name))




(defn store-sessions

  ""

  ^SessionStore

  [^ProcessorContext ctx name]

  (.getStateStore ctx
                  name))
