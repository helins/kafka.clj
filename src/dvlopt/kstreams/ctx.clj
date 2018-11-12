(ns dvlopt.kstreams.ctx

  "Contexts are needed for low-level processors."

  {:author "Adam Helinski"}

  (:refer-clojure :exclude [partition])
  (:require [dvlopt.kafka               :as K]
            [dvlopt.kafka.-interop.clj  :as K.-interop.clj]
            [dvlopt.kafka.-interop.java :as K.-interop.java]
            [dvlopt.kstreams            :as KS])
  (:import (org.apache.kafka.streams.processor ProcessorContext
                                               To)
           (org.apache.kafka.streams.state KeyValueStore
                                           WindowStore
                                           SessionStore)))




;;;;;;;;;;


(defn commit

  "Manually requests a commit of the current state.

   Commits are handle automatically by the library. This function request the next commit to happen as soon
   as possible."

  ^ProcessorContext

  [^ProcessorContext ctx]

  (.commit ctx)
  ctx)




(defn forward

  "Forwards a key-value to child nodes during processing.
  
   A map of options may be given :

     :dvlopt.kafka/key
      Unserialized key.

     :dvlopt.kafka/timestamp
      Chosen timestamp.
      If missing, selects the timestamp of the record being processed.

     :dvlopt.kafka/value
      Unserialized value.

     :dvlopt.streams/child
      Name of a specific child node.
      If missing, forwards to all children."

  ^ProcessorContext

  [^ProcessorContext ctx options]

  (let [to (if-let [child (::KS/child options)]
             (To/child child)
             (To/all))]
    (some->> (::K/timestamp options)
             (.withTimestamp to))
    (.forward ctx
              (::K/key options)
              (::K/value options)
              to))
  ctx)




(defn schedule

  "Schedules a periodic operation for processors (may be used during :dvlopt.kstreams/processor.init and/or
   :dvlopt.kstreams/processor.on-record).

   The time interval must be as described in `dvlopt.kafka` (best effort for millisecond precision).

   The time type is either :

     :stream-time
      Time advances following timestamps extracted from records.
      An operation is skipped if stream time advances more than the interval.

     :wall-clock-time
      Times advances following system time (best effort).
      An operation is skipped if garbage-collection halts the world for too long or if the current operation
      takes more time to complete than the interval.

   The callback accepts only 1 argument, the timestamp of \"when\" it is called (depending on time type).


   Returns a no-op function for cancelling the operation.
  

   Ex. (schedule ctx
                 [5 :seconds]
                 :stream-time
                 (fn callback [timestamp]
                   (do-stuff-like-forwarding-records ...)))"

  ^ProcessorContext

  [^ProcessorContext ctx interval time-type callback]

  (K.-interop.clj/cancellable (.schedule ctx
                                         (K.-interop.java/to-milliseconds interval)
                                         (K.-interop.java/punctuation-type time-type)
                                         (K.-interop.java/punctuator callback))))




(defn kv-store

  "Retrieves a writable key-value store.
  
   Cf. `dvlopt.kstreams.store`"

  ^KeyValueStore

  [^ProcessorContext ctx store-name]

  (.getStateStore ctx
                  store-name))




(defn window-store

  "Retrieves a writable window store.

   Cf. `dvlopt.kstreams.store`"

  ^WindowStore

  [^ProcessorContext ctx store-name]

  (.getStateStore ctx
                  store-name))




(defn session-store

  "Retrieves a writable session store.

   Cf. `dvlopt.kstreams.store`"

  ^SessionStore

  [^ProcessorContext ctx store-name]

  (.getStateStore ctx
                  store-name))
