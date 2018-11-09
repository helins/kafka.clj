(ns dvlopt.kafka.-interop

  ;; Miscellaneous interop utilities.
  ;;
  ;; Not meant to be directly used by the user.

  {:author "Adam Helinski"}

  (:require [clojure.string :as string])
  (:import java.util.Map
           java.util.regex.Pattern
           java.util.concurrent.Future
           clojure.lang.Named
           org.apache.kafka.streams.StreamsConfig))




;;;;;;;;;;


(defn future-proxy

  ;; Given a future, makes a future returning (transform @f*) on deref."

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




(defn named?

  ;; Is this an Named thing ?

  [x]

  (instance? Named
             x))




(defn nodes-string

  ;; Produces a string of Kafka nodes for various configurations from a list of [host port].

  [nodes]

  (if (string? nodes)
    nodes
    (string/join ","
                 (map (fn host-port [[host port]]
                        (str host ":" port))
                      nodes))))




(defn regex?

  ;; Is this a regular expression pattern ?

  [x]

  (instance? Pattern
             x))




(defn resource-configuration

  ;; Prepares a configuration map for resources such as a consumer or a producer.
  ;;
  ;;  Cf. https://kafka.apache.org/documentation/#configuration

  ^Map

  [configuration nodes]

  (assoc configuration
         "bootstrap.servers"
         (nodes-string nodes)))
