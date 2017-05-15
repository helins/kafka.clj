(ns milena.shared

  "Utilities common to producers and consumers"

  {:author "Adam Helinski"}

  (:require [clojure.string    :as string]
            [milena.converters :as convert])
  (:import java.util.concurrent.TimeUnit
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.consumer.KafkaConsumer
           (org.apache.kafka.common Metric
                                    MetricName)))





(defmacro try-nil

  "Wrap forms in a try catch and return nil if an exception is throw"

  [& forms]

  `(try ~@forms
        (catch Throwable _#
          nil)))




(defmacro try-bool

  "Wrap forms in a try catch and return true if everything is okay,
   false otherwise"

  [& forms]

  `(try ~@forms
        true
        (catch Throwable _#
          false)))




(defn nodes-string

  "Produce a string of Kafka nodes for producers and consumers.

   nodes : a list of [host port] representing Kafka nodes
         | an already prepared connection string"

  [nodes]

  (if (string? nodes)
      nodes
      (string/join ","
                   (map (fn [[host port]] (str host ":" port))
                        nodes))))




(defn stringify-keys

  "Simply stringify symbol or keyword keys in a map"

  [hmap]

  (reduce-kv (fn [hmap' k v]
               (assoc hmap'
                      (name k)
                      v))
             {}
             hmap))




(defprotocol IWrapped

  "A wrapper for managing sanely Kafka producers and consumers"


  (closed? [this]

    "Is this Kafka producer/consumer closed ?")


  (close [this]
         [this timeout-ms]

  "Close a producer or a consumer.

   cf. milena.produce/close
       milena.consume/close")


  (raw [this]

    "Unwrap this producer/consumer and get the raw Kafka object.
    
     At your own risks."))




(defn- -timeout-micros

  [timeout-ms]

  (long (* 1000
           (max 0
                timeout-ms))))



(extend-protocol IWrapped

  KafkaProducer

    (close
      ([this]
       (try-bool (.close this)))

      ([this timeout-ms]
       (try-bool (.close this
                         (-timeout-micros timeout-ms)
                         TimeUnit/MICROSECONDS))))


     (raw [this]
       this)


  KafkaConsumer

    (close
      ([this]
       (try-bool (.close this)))

      ([this timeout-ms]
       (try-bool (.close this
                         (-timeout-micros timeout-ms)
                         TimeUnit/MICROSECONDS))))


    (raw [this]
      this)


  Object

    (closed? [_]
      nil)

    (raw [this]
      this))




(deftype Wrapper [*closed?
                  p|c]

  IWrapped

    (closed? [_]
      @*closed?)


    (close [_]
      (or @*closed?
          (do (reset! *closed?
                      true)
              (reset! *closed?
                      (close p|c)))))
    

    (close [_ timeout-ms]
      (or @*closed?
          (do (reset! *closed?
                      true)
              (reset! *closed?
                      (close p|c
                             timeout-ms)))))


    (raw [_]
      p|c))




(defn wrap

  "Wrap a Kafka consumer or producer for this library"

  [p|c]

  (Wrapper. (atom false)
            p|c))




(defn partitions
  
  "Using a producer or a consumer, get a list of partitions
   for a given topic.

   <!> Producer will block for ever is the topic doesn't exist
       and dynamic creation has been disabled. Consumer will
       return an empty list."

  [p|c ktopic]

  (when-not (closed? p|c)
    (try-nil (map convert/PartitionInfo->hmap
                  (.partitionsFor (raw p|c)
                                  ktopic)))))




(defn metrics

  "Get the metrics of a producer or a consumer as a map"

  [p|c]

  (try-nil
    (reduce (fn [metrics' [^MetricName m-name ^Metric metric]]
              (assoc metrics'
                     (keyword (.group m-name)
                              (.name  m-name))
                     {:description (.description m-name)
                      :tags        (reduce (fn [tags [k v]] (assoc tags (keyword k) v))
                                           {}
                                           (.tags m-name))
                      :value       (.value metric)}))
            {}
            (.metrics (raw p|c)))))
