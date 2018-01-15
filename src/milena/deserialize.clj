(ns milena.deserialize

  "Kafka deserializers."

  {:author "Adam Helinski"}

  (:refer-clojure :exclude [int
                            long
                            double
                            byte-array])
  (:import (org.apache.kafka.common.serialization Deserializer
                                                  IntegerDeserializer
                                                  LongDeserializer
                                                  DoubleDeserializer
                                                  ByteArrayDeserializer
                                                  ByteBufferDeserializer
                                                  StringDeserializer)))




;;;;;;;;;; Basic deserializer


(def ^Deserializer int

  "Deserializer for ints."

  (IntegerDeserializer.))




(def ^Deserializer long

  "Deserializer for longs."

  (LongDeserializer.))




(def ^Deserializer double

  "Deserializer for doubles."

  (DoubleDeserializer.))




(def ^Deserializer byte-array

  "Deserializer for byte arrays."

  (ByteArrayDeserializer.))




(def ^Deserializer byte-buffers

  "Deserializer for byte buffers."

  (ByteBufferDeserializer.))


(def ^Deserializer string

  "Deserializer for strings."

  (StringDeserializer.))




;;;;;;;;;;


(defn- -deserializer

  "@ f
     A function taking a topic and data, and deserializing the data.
  
   => org.apache.kafka.common.serialization.Deserializer"

  [f]

  (reify Deserializer
    
    (deserialize [_ topic data]
      (f topic
         data))

    (close [_] nil)

    (configure [_ _ _] nil)))




(defn make

  "Given a fn, creates a Kafka deserializer.

   Ex. (make (fn my-deserializer [topic data]
               (nippy/thaw data)))"

  ^Deserializer

  [f]

  (if (fn? f)
    (-deserializer f)
    f))




(defn deserialize

  "Deserializes data using a Kafka deserializer."

  [^Deserializer deserializer topic data]

  (.deserialize deserializer
                topic
                data))
