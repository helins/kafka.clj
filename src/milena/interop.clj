(ns milena.interop

  "Miscellaneous interop utilities."

  {:author "Adam Helinski"}

  (:require [clojure.string :as string])
  (:import java.util.Map
           java.util.regex.Pattern
           java.util.concurrent.Future
           clojure.lang.Named
           org.apache.kafka.streams.StreamsConfig))




;;;;;;;;;;


(defn regex?

  "Is `x` a regular expression ?"

  [x]

  (instance? Pattern
             x))




(defn named?

  "Is `x` an instance of Named ?"

  [x]

  (instance? Named
             x))




(defn nodes-string

  "Produces a string of Kafka nodes for various configurations.

   @ nodes
     List of [host port] representing Kafka nodes.

  
   Ex. (nodes-string [[\"localhost\"    9092]
                      [\"another_host\" 9092]])"

  [nodes]

  (if (string? nodes)
    nodes
    (string/join ","
                 (map (fn host-port [[host port]]
                        (str host ":" port))
                      nodes))))




(defn config-key

  ""

  ;; TODO docstring

  [k]

  (cond
    (string? k) k
    (named? k)  (name k)
    :else       (str  k)))




(defn config-prefixed-key

  ""

  [k]

  ;; TODO docstring

  (cond
    (string? k)      k
    (instance? Named
               k)    (let [namesp (namespace k)
                           nm     (name      k)]
                       (case namesp
                         nil        nm
                         "producer" (StreamsConfig/producerPrefix nm)
                         "consumer" (StreamsConfig/consumerPrefix nm)
                         "topic"    (StreamsConfig/topicPrefix    nm)))
    :else            (str k)))




(defn stringify-keys

  "Stringifies keys in a map.

   In case of a named key, takes into account only the name and not the namespace."

  ^Map

  [hmap]

  (reduce-kv (fn stringify-key [hmap' k v]
               (assoc hmap'
                      (config-key k)
                      v))
             {}
             hmap))




(defn stringify-prefixed-keys

  "Stringifies keys in a map.

   In case of a named key, takes into account the namespace for prefixing the name as needed.

   Cf. `config-prefixed-key`"

  ^Map

  [hmap]

  (reduce-kv (fn stringify-key [hmap' k v]
               (assoc hmap'
                      (config-prefixed-key k)
                      v))
             {}
             hmap))




(defn keywordize-keys

  "Stringifies string keys in a map."

  ^Map

  [hmap]

  (reduce (fn keywordize-key [hmap' [k v]]
            (assoc hmap'
                   (keyword k)
                   v))
          {}
          hmap))




(defn config

  "Given a Kakfa configuration map, stringifies keys and adds Kafka nodes.
  
   Cf. https://kafka.apache.org/documentation/#configuration"

  ^Map

  [config nodes]

  (assoc (stringify-keys config)
         "bootstrap.servers"
         (nodes-string nodes)))




(defn future-proxy

  "Given a future, makes a future returning (transform @f*) on deref."

  ^Future

  [^Future f* transform]

  (reify
    
    Future

      (cancel [_ interrupt?]
        (.cancel f*
                 interrupt?))


      (isCancelled [_]
        (.isCancelled f*))


      (isDone [_]
        (.isDone f*))


      (get [_]
        (transform (.get f*)))


      (get [_ timeout unit]
        (transform (.get f*
                         timeout
                         unit)))))
