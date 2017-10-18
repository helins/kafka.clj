(ns milena.interop

  "Miscellaneous interop utilities"

  {:author "Adam Helinski"}

  (:require [clojure.string :as string])
  (:import java.util.Map
           java.util.concurrent.Future))




;;;;;;;;;;


(defn nodes-string

  "Produce a string of Kafka nodes for admins, producers, and consumers.

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




(defn stringify-keys

  "Stringifies nameable keys (symbols or keywords) in a map."

  [hmap]

  (reduce-kv (fn stringify-key [hmap' k v]
               (assoc hmap'
                      (if (string? k)
                        k
                        (name k))
                      v))
             {}
             hmap))




(defn keywordize-keys

  "Stringifies string keys in a map."

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
