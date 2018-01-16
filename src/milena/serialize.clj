(ns milena.serialize

  "Kafka serializers."

  {:author "Adam Helinski"}

  (:refer-clojure :exclude [boolean
                            int
                            long
                            double
                            byte-array])
  (:import (org.apache.kafka.common.serialization Serializer
                                                  IntegerSerializer
                                                  LongSerializer
                                                  DoubleSerializer
                                                  ByteArraySerializer
                                                  ByteBufferSerializer
                                                  StringSerializer)))




;;;;;;;;;; Misc


(defn- -serializer

  "@ f
     A function accepting a topic and data, and serializing the data.
  
   => org.apache.kafka.common.serialization.Serializer"

  [f]

  (reify Serializer
    
    (serialize [_ topic data]
      (f topic
         data))

    (close [_]
      nil)

    (configure [_ _ _]
      nil)))




(defn make
  
  "Given a fn, creates a Kafka serializer.

   Ex. (make (fn my-serializer [topic data]
               (nippy/freeze data)))"

  ^Serializer
  
  [f]

  (if (fn? f)
    (-serializer f)
    f))




(defn serialize

  "Serializes data using a Kafka serializer."

  ^bytes

  [^Serializer serializer topic data]

  (.serialize serializer
              topic
              data))




;;;;;;;;;; Basic serializers


(def ^Serializer boolean

  "Serializer for booleans."

  (-serializer (fn serialize-bool [_ bool?]
                 (if (nil? bool?)
                   nil
                   (let [ba (clojure.core/byte-array 1)]
                    (when bool?
                        (aset-byte ba
                                  0
                                  1))
                     ba)))))




(def ^Serializer int

  "Serializer for ints."

  (IntegerSerializer.))




(def ^Serializer long

  "Serializer for longs."

  (LongSerializer.))




(def ^Serializer double

  "Serializer for doubles."

  (DoubleSerializer.))




(def ^Serializer byte-array

  "Serializer for byte arrays."

  (ByteArraySerializer.))




(def ^Serializer byte-buffer

  "Serializer for byte-buffers."

  (ByteBufferSerializer.))


(def ^Serializer string

  "Serializer for strings."

  (StringSerializer.))
