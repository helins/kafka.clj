(ns milena.analyze.stores

  ""

  {:author "Adam Helinski"}

  (:refer-clojure :exclude [flush])
  (:require [milena.interop.java :as M.interop.java]
            [milena.interop.clj  :as M.interop.clj])
  (:import org.apache.kafka.streams.processor.StateStore
           (org.apache.kafka.streams.state ReadOnlyKeyValueStore
                                           ReadOnlyWindowStore
                                           ReadOnlySessionStore
                                           KeyValueStore
                                           WindowStore
                                           SessionStore))
  )




;;;;;;;;;; Misc


(defn close

  ""

  [^StateStore store]

  (.close store))




(defn flush

  ""

  [^StateStore store]

  (.flush store))




(defn persistent?

  ""

  [^StateStore store]

  (.persistent store))




;;;;;;;;;; Key value stores


(defn kv-count

  ""

  [^ReadOnlyKeyValueStore store]

  (.approximateNumEntries store))




(defn kv-get

  ""

  ([^ReadOnlyKeyValueStore store]

   (M.interop.clj/key-value-iterator (.all store)))


  ([^ReadOnlyKeyValueStore store k]

   (.get store
         k))


  ([^ReadOnlyKeyValueStore store k-from k-to]

   (M.interop.clj/key-value-iterator (.range store
                                             k-from
                                             k-to))))




(defn kv-put

  ""

  (^KeyValueStore

   [^KeyValueStore store kvs]

   (.putAll store
            (map M.interop.java/key-value
                 kvs))
   store)


  (^KeyValueStore

   [^KeyValueStore store k v]

   (.put store
         k
         v)
   store))




(defn kv-offer

  ""

  [^KeyValueStore store k v]

  (.putIfAbsent store
                k
                v))



(defn kv-remove

  ""

  [^KeyValueStore store k]

  (.delete store
           k))




;;;;;;;;;; Window stores


(defn ws-get

  ""

  ([^ReadOnlyWindowStore store-w ts-from ts-to k]

   (M.interop.clj/window-store-iterator (.fetch store-w
                                                k
                                                ts-from
                                                ts-to)))


  ([^ReadOnlyWindowStore store-w ts-from ts-to k-from k-to]

   (M.interop.clj/key-value-iterator--windowed (.fetch store-w
                                                       k-from
                                                       k-to
                                                       ts-from
                                                       ts-to))))




(defn ws-put


  ""

  (^WindowStore

   [^WindowStore store k v]

   (.put store
         k
         v)
   store)


  (^WindowStore

   [^WindowStore store k v timestamp]

   (.put store
         k
         v
         timestamp)))




;;;;;;;;;; Session stores


(defn ss-get

  ""

  ([^ReadOnlySessionStore store-ss k]

   (M.interop.clj/key-value-iterator--windowed (.fetch store-ss
                                                       k)))


  ([^ReadOnlySessionStore store-ss k-from k-to]

   (M.interop.clj/key-value-iterator--windowed (.fetch store-ss
                                                       k-from
                                                       k-to))))




(defn ss-put

  ""

  ^SessionStore

  [^SessionStore store k start end v]

  (.put store
        (M.interop.java/windowed k
                                 start
                                 end)
        v)
  store)




(defn ss-remove

  ""

  ^SessionStore

  [^SessionStore store k start end]

  (.remove store
           (M.interop.java/windowed k
                                    start
                                    end))
  store)




(defn ss-sessions

  ""

  ([^SessionStore store ts-from ts-to k]

   (M.interop.clj/key-value-iterator--windowed k
                                               ts-from
                                               ts-to))


  ([^SessionStore store ts-from ts-to k-from k-to]

   (M.interop.clj/key-value-iterator--windowed k-from
                                               k-to
                                               ts-from
                                               ts-to)))
