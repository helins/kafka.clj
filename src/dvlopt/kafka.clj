(ns dvlopt.kafka

  "Start here.
  
   This library aims to be clojure idiomatic while not being too smart about wrapping the java libraries, so that upgrading
   will not be laborious in the future.

   It is organized by specific namespaces. Producers (dvlopt.kafka.out) produce records, consumers (dvlopt.kafka.in)
   consume records, and administrators can alter Kafka (dvlopt.kafka.admin). For anything related to Kafka Streams, see the
   `dvlopt.kstreams` namespace.


   Records
   =======

   A record is a map containing at least a ::topic. It might also hold :

     ::headers
       List of [Key Value] where the `Key` is an arbitrary string and `Value` is an arbitrary byte array which can be missing.
       Keys can appear more than once.

     ::offset
       Offset of the record in the topic-partition.

     ::key
       Key of the record, serialized or deserialized. Can be nil.

     ::timestamp
       Timestamp of the record.

     ::timestamp.type
       A timestamp can refer to when the record was created (:create) or sent (:log-append).

     ::value
       Value of the record, serializer of deserializer. Can be nil

   Obviously, the offset cannot be decided by the user when sending the record. Headers allow records to have metadata
   clearly distinct from the value. They exerce the same role as in other protocols such as HTTP. 


   Connecting to nodes
   ===================

   Opening a resource such as a producer requires a list of nodes (ie. list of [host port]). If not provided, the library
   tries to reach [[\"localhost\" 9092]]. When described, a node is a map containing ::host, ::port, ::id (numerical
   identification), and ::rack when one is assigned. A replica node also contains the attribute ::synced?.


   Ser/de
   ======

   Within Kafka, records are kept as transparent byte arrays. When sending records, the key and the value need to be
   serialized and when consuming some, deserialized. 

   A serializer is a function mapping some data to a byte array or nil. It is needed for producing records.

   Ex. (fn serializer [data metadata]
         (some-> v
                 nippy/freeze))

   A deserializer does the opposite, it maps a byte array (or nil) to a value. It is needed for consuming records.

   Ex. (fn deserializer [ba metadata]
         (some-> ba
                 nippy/thaw))


   Both type of functions are often used throughout the library. The provided metadata might help the user to decide on
   how to ser/de the data. It is a map containing the ::topic involved as well as ::headers when there are some.
  
   Built-in deserializers and serializers are available at `deserializers` and  `serializers` respectively. Because ser/de
   is so common, a key word specifying one of the built-in functions can be provided :

     :boolean
     :byte-array
     :byte-buffer
     :double
     :integer
     :keyword
     :long
     :string
  
   If a ser/de is not provided by the user when needed, the default is :byte-array.


   Time
   ====
  
   All time intervals such as timeouts, throughout all this library, are expressed as [Duration Unit] where
   Duration is coerced to an integer and Unit is one of :

     :nanoseconds
     :microseconds
     :milliseconds
     :seconds
     :minutes
     :hours
     :days

   Ex. [5 :seconds]"

  {:author "Adam Helinski"}

  (:import (org.apache.kafka.common.serialization ByteArrayDeserializer
                                                  ByteArraySerializer
                                                  ByteBufferDeserializer
                                                  ByteBufferSerializer
                                                  Deserializer
                                                  DoubleDeserializer
                                                  DoubleSerializer
                                                  IntegerDeserializer
                                                  IntegerSerializer
                                                  LongDeserializer
                                                  LongSerializer
                                                  Serializer
                                                  StringDeserializer
                                                  StringSerializer)))




;;;;;;;;;; Serde


(def deserializers

  "Built-in deserializers."

  (reduce-kv (fn prepare-deserializer [deserializers' kw-type ^Deserializer original-deserializer]
               (assoc deserializers'
                      kw-type
                      (with-meta (fn deserialize
                                   ([x]
                                    (deserialize x
                                                 nil))
                                   ([x metadata]
                                    (.deserialize original-deserializer
                                                  nil
                                                  x)))
                                 {::original-deserializer original-deserializer})))
             {}
             {:boolean     (reify Deserializer

                             (close [_]
                               nil)

                             (configure [_ _ _]
                               nil)

                             (deserialize [_ _topic ba]
                               (if (nil? ba)
                                 nil
                                 (not (zero? (aget ba
                                                   0))))))
              :byte-array  (ByteArrayDeserializer.)
              :byte-buffer (ByteBufferDeserializer.)
              :double      (DoubleDeserializer.)
              :integer     (IntegerDeserializer.)
              :keyword     (reify Deserializer

                             (close [_]
                               nil)

                             (configure [_ _ _]
                               nil)

                             (deserialize [_ _topic ba]
                               (keyword ((deserializers :string) ba))))
              :long        (LongDeserializer.)
              :string      (StringDeserializer.)}))




(def serializers

  "Built-in serializers."

  (reduce-kv (fn prepare-serializer [serializers' kw-type ^Serializer original-serializer]
               (assoc serializers'
                      kw-type
                      (with-meta (fn serialize
                                   ([x]
                                    (serialize x
                                               nil))
                                   ([x metadata]
                                    (.serialize original-serializer
                                                nil
                                                x)))
                                 {::original-serializer original-serializer})))
             {}
             {:boolean     (reify Serializer

                             (close [_]
                               nil)

                             (configure [_ _ _]
                               nil)

                             (serialize [_ _topic bool]
                               (if (nil? bool)
            	      		     nil
            	      		     (let [ba (byte-array 1)]
            	      		       (when bool
            	      		         (aset-byte ba
            	      		                    0
            	      		                    1))
            	      		       ba))))
              :byte-array  (ByteArraySerializer.)
              :byte-buffer (ByteBufferSerializer.)
              :double      (DoubleSerializer.)
              :integer     (IntegerSerializer.)
              :keyword     (reify Serializer

                             (close [_]
                               nil)

                             (configure [_ _ _]
                               nil)

                             (serialize [_ _topic kw]
                               (when kw
                                 ((serializers :string) (let [name-str (name kw)]
                                                          (if-let [namespace-str (namespace kw)]
                                                            (str namespace-str "/" name-str)
                                                            name-str))))))
              :long        (LongSerializer.)
              :string      (StringSerializer.)}))




;;;;;;;;;; Misc


(def defaults

  "Default values and options used throughout this library."

  {::deserializer.key                          :byte-array
   ::deserializer.value                        :byte-array
   ::internal?                                 false
   ::nodes                                     [["localhost" 9092]]
   ::serializer.key                            :byte-array
   ::serializer.value                          :byte-array
   :dvlopt.kafka.admin/name-pattern            [:any]
   :dvlopt.kafka.admin/number-of-partitions    1
   :dvlopt.kafka.admin/operation               :all
   :dvlopt.kafka.admin/permission              :any
   :dvlopt.kafka.admin/replication-factor      1
   :dvlopt.kafka.admin/resource-type           :any
   :dvlopt.kafka.in.mock/offset-reset-strategy :latest
   :dvlopt.kafka.out.mock/auto-complete?       true
   :dvlopt.kstreams/offset-reset               :earliest
   :dvlopt.kstreams.mock/clock                 0
   :dvlopt.kstreams.store/cache?               true
   :dvlopt.kstreams.store/changelog?           true
   :dvlopt.kstreams.store/duplicate-keys?      false
   :dvlopt.kstreams.store/lru-size             0
   :dvlopt.kstreams.store/retention            [1 :days]
   :dvlopt.kstreams.store/segments             2
   :dvlopt.kstreams.store/type                 :kv.regular})
