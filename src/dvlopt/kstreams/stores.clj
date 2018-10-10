(ns dvlopt.kstreams.stores

  "Handling of Kafka Streams state stores.

   Streaming applications often need to store some kind of state. This is the purposes of state stores which are typically backed-up
   to compacted Kafka topics called changelogs in order to be fault-tolerant. Those changelogs topics are named '$APPLICATION_ID-$GENERATED_NAME-changelog`.
  
   Three type of stores exist, typically persistent by using RocksDB under the hood :
  

     Key-value stores
     ----------------

     Behave like regular tables or, when decided, like LRU ones. A regular key-value store can also be in-memory rather than persistent,
     which is great for cloud environments where applications are started from scratch everytime. However, an in-memory key-value
     store will have to catch up with its corresponding changelog everytime the application is restarted.

     Persistentwindow stores
     -----------------------------

     In window stores, each value is associated with a key but also a timestamp. They are used for computing over fixed time intervals.

     Persistent session stores
     -------------------------

     Sessions are non-fixed intervals of activity organized around fixed intervals of inactivity.
    
     For the same key, with an interval of 5 seconds, let us suppose we receive values at time 10, 12 and 20. There is a gap of more than
     5 seconds between event 2 and 3. Hence, event 3 would be part of another session. Now let us suppose a 4th event arrives out of order
     with a timestamp of 16. Now, all 4 events would merge into the same session as no event is more than 5 seconds away from another one.

  
   In the high-level API, stores are often created automatically but can be tweaked to some extent. When a store needs to be created manually,
   these options might be provided :

    :dvlopt.kafka/deserializer.key
    :dvlopt.kafka/deserializer.value
    :dvlopt.kafka/serializer.key
    :dvlopt.kafka/serializer.value
     Cf. `dvlopt.kafka` for description of serializers and deserializers.

    ::cache?
     Caching tries to minimize the number of updates. For instance, if 2 subsequent values share the same key, only the second one
     will be persisted. It improves performance and IO but it means records will not be persisted as soon as possible.
     True by default, should be false only for testing and debugging.

    ::changelog?
     In order for state stores to be fault tolerant, they are continuously backed up to a changelog topic behind the scenes.
     Default is true and this option should not be disabled unless needed.

    ::configuration.changelog
     Map of Kafka topic properties for configuring the changelog topic.
     Cf. https://kafka.apache.org/documentation/#topicconfigs

    ::name
     Generated in the form of 'dvlopt.kafka-store-8_DIGIT_NUMBER' when not supplied.
  
    ::type
     Determines the type of the store, one of :

       :kv.in-memory
       :kv.lru
       :kv.regular
       :session
       :window

    LRU key-value stores have this additional option :

       ::lru-size
         Maximum number of items in the LRU store.

    Session and window stores have these additional options :

      ::retention 
       Time period for which the state store will retain historical data, cannot be smaller than the chosen interval.
       During any kind of stream processing, it is common that data arrives late or out of order and instead of dropping this data,
       it is better to update past time windows. However, because disks are not unlimited, one cannot keep the data for all time windows
       just in case. Hence the need for this option. The higher the retention period and the later can data arrive.
       Default is [1 :days].

    Window stores have these additional options and mandatory arguments :

      ::duplicate-keys?
       Whether or not to retain duplicate keys, akin to caching.
       Default is false.

      ::interval (mandatory)
       Fixed interval of each window.
       Cf. `dvlopt.kafka` for description of time intervals

      ::segments
       Number of database segments (must be >= 2).
       Default is 2."

  {:author "Adam Helinski"}

  (:refer-clojure :exclude [flush])
  (:require [dvlopt.kafka.-interop.clj  :as K.-interop.clj]
            [dvlopt.kafka.-interop.java :as K.-interop.java])
  (:import org.apache.kafka.streams.processor.StateStore
           (org.apache.kafka.streams.state KeyValueStore
                                           ReadOnlyKeyValueStore
                                           ReadOnlySessionStore
                                           ReadOnlyWindowStore
                                           SessionStore
                                           WindowStore)))




;; TODO. Somes functions returns sequences based on stateful iterators (bad).
;; TODO. Docstrings




;;;;;;;;;; Misc


(defn close

  "Closes the storage engine.

   Typically, a store is closed automatically when the Kafka Streams application is shutdown."

  [^StateStore store]

  (.close store))




(defn flush

  "Flushes any cached data."

  [^StateStore store]

  (.flush store))




(defn open?

  "Is this store open for IO ?"

  [^StateStore store]

  (.isOpen store))




(defn persistent?

  "Is this store persistent ? Rather than in-memory ?"

  [^StateStore store]

  (.persistent store))




(defn store-name

  "Returns the name of the given store which might be automatically generated."

  [^StateStore store]

  (.name store))




;;;;;;;;;; Key value stores


(defn kv-count

  "Returns the approximate number of entries in the key-value store."

  [^ReadOnlyKeyValueStore kv-store]

  (.approximateNumEntries kv-store))




(defn kv-get

  "Returns the value mapped to the given key in the key-value store."

  [^ReadOnlyKeyValueStore kv-store k]

  (.get kv-store
        k))




(defn kv-range

  ""

  ([^ReadOnlyKeyValueStore kv-store]

   (K.-interop.clj/key-value-iterator (.all kv-store)))


  ([^ReadOnlyKeyValueStore kv-store from-key to-key]

   (K.-interop.clj/key-value-iterator (.range kv-store
                                              from-key
                                              to-key))))




(defn kv-put

  "Adds the key-value to the key-value store.

   A list of [key value]'s can be provided."

  (^KeyValueStore

   [^KeyValueStore kv-store kvs]

   (.putAll kv-store
            (map K.-interop.java/key-value
                 kvs))
   kv-store)


  (^KeyValueStore

   [^KeyValueStore kv-store k v]

   (.put kv-store
         k
         v)
   kv-store))




(defn kv-offer

  "Adds the key-value to the key-value store only if the key does not exist yet."

  [^KeyValueStore kv-store k v]

  (.putIfAbsent kv-store
                k
                v))



(defn kv-remove

  "Removes the key from the key-value store."

  [^KeyValueStore store k]

  (.delete store
           k))




;;;;;;;;;; Window stores


(defn ws-get

  ""

  [^ReadOnlyWindowStore window-store k timestamp]

  (.fetch window-store
          k
          timestamp))




(defn ws-range

  ""

  ([window-store k]

   (ws-range window-store
             k
             nil))


  ([^ReadOnlyWindowStore window-store k options]

   (K.-interop.clj/window-store-iterator (.fetch window-store
                                                 k
                                                 (or (::timestamp.from options)
                                                     0)
                                                 (or (::timestamp.to options)
                                                     (System/currentTimeMillis))))))




(defn ws-multi-range

  ""

  ([window-store]

   (ws-multi-range window-store
                   nil))


  ([^ReadOnlyWindowStore window-store options]

   (K.-interop.clj/key-value-iterator--windowed (.fetchAll window-store
                                                           (or (::timestamp.from options)
                                                               0)
                                                           (or (::timestamp.to options)
                                                               (System/currentTimeMillis)))))


  ([window-store from-key to-key]

   (ws-multi-range window-store
                   from-key
                   to-key))


  ([^ReadOnlyWindowStore window-store from-key to-key options]

   (K.-interop.clj/key-value-iterator--windowed (.fetch window-store
                                                        from-key
                                                        to-key
                                                        (or (::timestamp.from options)
                                                            0)
                                                        (or (::timestamp.to options)
                                                            (System/currentTimeMillis))))))




(defn ws-put

  "Adds a key to the window store.
  
   If none is provided, the timestamp will be the system time."

  (^WindowStore

   [^WindowStore window-store k v]

   (.put window-store
         k
         v)
   window-store)


  (^WindowStore

   [^WindowStore window-store k v timestamp]

   (.put window-store
         k
         v
         timestamp)
   window-store))




;;;;;;;;;; Session stores


(defn ss-get

  ""

  ([^ReadOnlySessionStore session-store k]

   (K.-interop.clj/key-value-iterator--windowed (.fetch session-store
                                                        k)))


  ([^ReadOnlySessionStore session-store from-key to-key]

   (K.-interop.clj/key-value-iterator--windowed (.fetch session-store
                                                        from-key
                                                        to-key))))




(defn ss-sessions

  ""

  ([session-store k]

   (ss-sessions session-store
                k
                nil))

  ([^SessionStore session-store k options]

   (K.-interop.clj/key-value-iterator--windowed (.findSessions session-store
                                                               k
                                                               (or (::timestamp.from options)
                                                                   0)
                                                               (or (::timestamp.to options)
                                                                   (System/currentTimeMillis))))))




(defn ss-multi-sessions

  ""

  ([session-store from-key to-key]

   (ss-multi-sessions session-store
                      from-key
                      to-key
                      nil))


  ([^SessionStore session-store from-key to-key options]

   (K.-interop.clj/key-value-iterator--windowed (.findSessions session-store
                                                               from-key
                                                               to-key
                                                               (or (::timestamp.from options)
                                                                   0)
                                                               (or (::timestamp.to options)
                                                                   (System/currentTimeMillis))))))




(comment

  ;; TODO. Ultimately, these functions needs to instantiate a Window which is an abstract class (weird).


  (defn ss-put

    "Adds a key-value to a session in a session store."

    ^SessionStore

    [^SessionStore store from-timestamp to-timestamp k v]

    (.put store
          (K.-interop.java/windowed k
                                    from-timestamp
                                    to-timestamp)
          v)
    store)




  (defn ss-remove

    "Removes a key-value from a session in a session store."

    ^SessionStore

    [^SessionStore store k timestamp-start timestamp-end]

    (.remove store
             (K.-interop.java/windowed k
                                       timestamp-start
                                       timestamp-end))
    store))
