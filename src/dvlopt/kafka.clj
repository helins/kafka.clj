(ns dvlopt.kafka

  "This library aims to be clojure idiomatic while not being too smart about wrapping the java libraries, so that upgrading
   will not be too laborious in the future.

   It is organized by specific namespaces. Producers (dvlopt.kafka.produce) produce records, consumers (dvlopt.kafka.consume)
   consume records, and administrators can alter Kafka (dvlopt.kafka.admin). For anything related to Kafka Streams, see the
   `dvlopt.kstreams` namespace.


   Records
   =======

   A record is a map containing at least a ::topic. It might also hold :

     ::headers
     ::offset
     ::key
     ::timestamp
     ::value

   Obviously, the offset cannot be decided by the user when sending the record. Headers allow records to have metadata
   clearly distinct from the value. They exerce the same role as in other protocols such as HTTP. They consist of a list
   of key-values where the order might be important. Keys are arbitrary strings and can appear more than once. Values are
   arbitrary byte arrays or may be missing.


   Connecting to nodes
   ===================

   Opening a resource such as a producer requires a list of nodes (ie. list of [host port]). If not provided, the library
   tries to reach [[\"localhost\" 9092]]. When described, a node is a map containing ::host, ::port, ::id (numerical
   identification), and ::rack when one is assigned. A replica node also contains the attribute ::synced?.


   Ser/de
   ======

   Within Kafka, records are kept as transparent byte arrays. When sending records, the key and the value need to be
   serialized and when consuming some, deserialized. 

   A serializer is a function mapping some data to a byte array or nil. It is needed for producing records. It receives

   Ex. (fn [data metadata]
         (some-> v
                 nippy/freeze))

   A deserializer does the opposite, it maps a byte array (or nil) to a value. It is needed for consuming records.

   Ex. (fn [ba metadata]
         (some-> ba
                 nippy/thaw))


   Both type of functions are often used throughout the library. The provided metadata might help the user to decide on
   how to ser/de the data. It is a map containing the ::topic involved as well as ::headers when there are some.
  
   Instead of providing a function, the user can rely on built-in ser/de functions by providing one of those keywords :

     :boolean
     :byte-array
     :byte-buffer
     :double
     :integer
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

  {:author "Adam Helinski"})




;;;;;;;;;; Misc


(def defaults

  "Default values and options used throughout this library."

  {::deserializer.key                       :byte-array
   ::deserializer.value                     :byte-array
   ::internal?                              false
   ::nodes                                  [["localhost" 9092]]
   ::serializer.key                         :byte-array
   ::serializer.value                       :byte-array
   :dvlopt.kafka.admin/name-pattern         [:any]
   :dvlopt.kafka.admin/number-of-partitions 1
   :dvlopt.kafka.admin/operation            :all
   :dvlopt.kafka.admin/permission           :any
   :dvlopt.kafka.admin/replication-factor   1
   :dvlopt.kafka.admin/resource-type        :any
   :dvlopt.kstreams/offset-reset            :earliest
   :dvlopt.kstreams.stores/cache?           true
   :dvlopt.kstreams.stores/changelog?       true
   :dvlopt.kstreams.stores/duplicate-keys?  false
   :dvlopt.kstreams.stores/lru-size         0
   :dvlopt.kstreams.stores/retention        [1 :days]
   :dvlopt.kstreams.stores/segments         2
   :dvlopt.kstreams.stores/type             :kv.regular})
