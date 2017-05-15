(ns milena.produce

  "Everything related to Kafka producers"

  {:author "Adam Helinski"}

  (:refer-clojure :exclude [deref
                            flush])
  (:require [clojure.core      :as clj]
            [milena.shared     :as shared]
            [milena.converters :as convert])
  (:import milena.shared.Wrapper
           org.apache.kafka.clients.producer.KafkaProducer
           (org.apache.kafka.common.serialization Serializer
                                                  ByteArraySerializer
                                                  ByteBufferSerializer
                                                  DoubleSerializer
                                                  IntegerSerializer
                                                  LongSerializer
                                                  StringSerializer)))





(def serializers

  "Basic serializers provided by Kafka"

  {:byte-array  (ByteArraySerializer.)
   :byte-buffer (ByteBufferSerializer.)
   :double      (DoubleSerializer.)
   :int         (IntegerSerializer.)
   :long        (LongSerializer.)
   :string      (StringSerializer.)
   })
  



(defn make-serializer
  
  "Given a fn that takes a topic and data to serialize,
   make a Kafka serializer for producers"

  [f]

  (reify Serializer
    
    (serialize [_ ktopic data] (f ktopic
                                  data))

    (close [_] nil)

    (configure [_ _ _] nil)))




(defn serialize

  "Serialize data using a Kafka serializer"

  [^Serializer serializer ktopic data]

  (.serialize serializer
              ktopic
              data))




(defn make

  "Build a Kafka producer.

   config :
     config : a configuration map for the producer as described in
              Kafka documentation (keys can be keywords)
     nodes : a connection string to Kafka nodes
           | a list of [host port]
     serializer... : cf. serializers 
                         (make-serializer)

   Producer are thread-safe and it is more efficient to share
   one amongst multiple threads."

  [& [{:as   opts
       :keys [config
              nodes
              serializer
              serializer-key
              serializer-value]
       :or   {nodes            [["localhost" 9092]]
              serializer       (serializers :byte-array)
              serializer-key   serializer
              serializer-value serializer}}]]

  (shared/wrap (KafkaProducer. (assoc (shared/stringify-keys config)
                                      "bootstrap.servers"
                                      (shared/nodes-string nodes))
                               serializer-key
                               serializer-value)))




(defn closed?

  "Is this producer closed ?"

  [producer]

  (shared/closed? producer))




(defn close

  "Try to close the producer cleanly.

   Will block until all sends are done or the optional timeout is elapsed."

  ([producer]

   (shared/close producer))


  ([producer timeout-ms]

   (shared/close producer
                 timeout-ms)))




(defn raw

  "Unwrap this consumer and get the raw Kafka object.
  
   At your own risks."

  [consumer]

  (shared/raw consumer))




(defn producer?

  "Is this a producer ?"

  [x]

  (instance? KafkaProducer
             (shared/raw x)))




(defn partitions
  
  "Get a list of partitions for a given topic.

   <!> Will block for ever is the topic doesn't exist and dynamic
       creation has been disabled."

  [producer ktopic]

  (shared/partitions producer
                     ktopic))




(defn- -commit

  "Helper for (commit).

   Effectively commit a message"

  [producer message & [f]]

  (.send ^KafkaProducer (raw producer)
         (convert/hmap->ProducerRecord message)
         (some-> f
                 convert/f->Callback)))




(defn commit

  "Asynchronously commit a message to Kafka via a producer and
   call the optional callback on acknowledgment.

   message : a map containing :topic
                              :partition
                              :timestamp
                              :key
                              :value
             where only :topic is mandatory
             (cf. milena.converters/hmap->ProducerRecord)
   callback : an optional callback invoked on completion and taking as
              arguments an exception and metadata about the sent record,
              only one being non-nil whether an exception was thrown or not
              (cf. milena.converters/f->Callback)
   metadata : a map containing :topic
                               :partition
                               :timestamp
                               :offset
                               :checksum

   Without a callback, this fn might throw if something goes wrong.

   Returns a future resolving to a raw metadata Kafka object. It is advised to
   use (deref) to deref and convert it."

  ([producer message]

   (when (and message
              (not (closed? producer)))
     (-commit producer
              message)))


  ([producer message callback]

   (when message
     (if (closed? producer)
         (do (callback (IllegalStateException. "Cannot send after the producer is closed")
                       nil)
             nil)
         (try (-commit producer
                       message
                       callback)
              (catch Throwable e
                (callback e
                          nil)
                nil))))))




(defn deref

  "Deref a future returned by (commit) and convert the metadata
   the sent record to a map."

  ([kfuture]

   (when kfuture
     (convert/RecordMetadata->hmap (clj/deref kfuture))))


  ([kfuture timeout-ms timeout-val]

   (when kfuture
     (convert/RecordMetadata->hmap (clj/deref kfuture
                                              timeout-ms
                                              timeout-val)))))




(defn flush

  "Flush the producer.
  
   Sends all buffered messages immediately, even is 'linger.ms' is greater than 0,
   and blocks until completion. Other thread can continue sending messages but no
   garantee is made they will be part of the flush.

   Returns true or false whether the flush succeeded or not."

  [producer]

  (shared/try-bool (.flush ^KafkaProducer (raw producer))))




(defn metrics

  "Get metrics about this producer"

  [producer]

  (shared/metrics producer))


