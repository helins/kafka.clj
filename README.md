# dvlopt.kafka

[![Clojars
Project](https://img.shields.io/clojars/v/dvlopt/kafka.svg)](https://clojars.org/dvlopt/kafka)

This [Apache Kafka](https://kafka.apache.org/) client library is a Clojure
wrapper for the [official java libraries](https://kafka.apache.org/). It strives
for a balance between being idiomatic but not too clever. Users used to the java
libraries will be right at home, although it is not a prerequisite.

It provides namespaces for handling consumers, producers, and doing some
administration. Also, we have the pleasure to announce that Kafka Streams is
fully supported.

## Usage

First, read the fairly detailed
[API](https://dvlopt.github.io/doc/clojure/dvlopt/kafka/index.html). Specially
if you have not used the java libraries and its concepts.

Then, have a look at the following examples. Just so we are prepared, let us 
require all namespaces involved.

```clj
;; For Kafka :

(require '[dvlopt.kafka       :as K]
         '[dvlopt.kafka.admin :as K.admin]
         '[dvlopt.kafka.in    :as K.in]
         '[dvlopt.kafka.out   :as K.out])


;; For Kafka Streams :

(require '[dvlopt.kstreams          :as KS]
         '[dvlopt.kstreams.topology :as KS.topology]
         '[dvlopt.kstreams.ctx      :as KS.ctx]
         '[dvlopt.kstreams.store    :as KS.store]
         '[dvlopt.kstreams.builder  :as KS.builder]
         '[dvlopt.kstreams.stream   :as KS.stream]
         '[dvlopt.kstreams.table    :as KS.table])
```

### Administration

Creating topic "my-topic" using the `dvlopt.kafka.admin` namespace.

```clj
(with-open [admin (K.admin/admin)]
  (K.admin/create-topics admin
                         {"my-topic" {::K.admin/number-of-partitions 4
                                      ::K.admin/replication-factor   3
                                      ::K.admin/configuration        {"cleanup.policy" "compact"}}})
  (println "Existing topics : " (keys @(K.admin/topics admin
                                                       {::K/internal? false}))))
```

### Producing records

Sending 25 records to "my-topic" using the `dvlopt.kafka.out` namespace.

```clj
(with-open [producer (K.out/producer {::K/nodes             [["localhost" 9092]]
                                      ::K/serializer.key    (K/serializers :long)
                                      ::K/serializer.value  :long
                                      ::K.out/configuration {"client.id" "my-producer"}})]
  (doseq [i (range 25)]
    (K.out/send producer
                {::K/topic "my-topic"
                 ::K/key   i
                 ::K/value (* 100 i)}
                (fn callback [exception metadata]
                  (println (format "Record %d : %s"
                                   i
                                   (if exception
                                     "FAILURE"
                                     "SUCCESS")))))))
```

### Consuming records

Reding a batch of records from "my-topic" and manually commit the offset of
where we are using the `dvlopt.kafka.in` namespace.

```clj
(with-open [consumer (K.in/consumer {::K/nodes              [["localhost" 9092]]
                                     ::K/deserializer.key   :long
                                     ::K/deserializer.value :long
                                     ::K.in/configuration   {"auto.offset.reset" "earliest"
                                                             "enable.auto.commit" false
                                                             "group.id"           "my-group"}})]
  (K.in/register-for consumer
                     ["my-topic"])
  (doseq [record (K.in/poll consumer
                            {::K/timeout [5 :seconds]})]
    (println (format "Record %d @%d - Key = %d, Value = %d"
                     (::K/offset record)
                     (::K/timestamp record)
                     (::K/key record)
                     (::K/value record))))
  (K.in/commit-offsets consumer))
```

### Kafka Streams low-level API

Useless but simple example of grouping records in two categories based on their
key, "odd" and "even", and continuously summing values in each category.

First, we create a topology. We then add a source node fetching records from
"my-topic". Those records are processed by "my-processor" which needs
"my-store" in order to persist the current sum for each category. Finally, a
sink node receives processed records and sends them to "my-topic-2".


```clj
(def topology
     (-> (KS.topology/topology)
         (KS.topology/add-source "my-source"
                                 ["my-topic"]
                                 {::K/deserializer.key   :long
                                  ::K/deserializer.value :long
                                  ::KS/offset-reset      :earliest})
         (KS.topology/add-processor "my-processor"
                                    ["my-source"]
                                    {::KS/processor.init      (fn [ctx]
                                                                (KS.ctx/kv-store ctx
                                                                                 "my-store"))
                                     ::KS/processor.on-record (fn [ctx my-store record]
                                                                (println "Processing record : " record)
                                                                (let [key' (if (odd? (::K/key record))
                                                                             "odd"
                                                                             "even")
                                                                      sum  (+ (or (KS.store/kv-get my-store
                                                                                                   key')
                                                                                  0)
                                                                              (::K/value record))]
                                                                  (KS.store/kv-put my-store
                                                                                   key'
                                                                                   sum)
                                                                  (KS.ctx/forward ctx
                                                                                  {::K/key   key'
                                                                                   ::K/value sum})))})
         (KS.topology/add-store ["my-processor"]
                                {::K/deserializer.key   :string
                                 ::K/deserializer.value :long
                                 ::K/serializer.key     :string
                                 ::K/serializer.value   :long
                                 ::KS.store/name        "my-store"
                                 ::KS.store/type        :kv.in-memory
                                 ::KS.store/cache?      false})
         (KS.topology/add-sink "my-sink"
                               ["my-processor"]
                               "my-topic-2"
                               {::K/serializer.key   :string
                                ::K/serializer.value :long})))


(def app
     (KS/app "my-app-1"
             topology
             {::K/nodes         [["localhost" 9092]]
              ::KS/on-exception (fn [exception _thread]
                                  (println "Exception : " exception))}))


(KS/start app)
```


### Kafka Streams high-level API

Same example as previously but in a more functional style. In addition, values
are aggregated in 2 seconds windows (it is best to run the producer example a
few times first).

First, we need a builder. Then we add a stream fetching records from "my-topic".
Records are then grouped into our categories and then each category is windowed
in 2 seconds windows. Each window is then reduced for computing a sum. Then we
are ready and we can build a topology out of our builder. It is always a good
idea to have a look at the description of the built topology to have a better
idea of what is created by the high-level API.

A window store is then retrieved and then each window for each category is
printed. 

```clj
(def topology
     (let [builder (KS.builder/builder)]
       (-> builder
           (KS.builder/stream ["my-topic"]
                              {::K/deserializer.key   :long
                               ::K/deserializer.value :long
                               ::KS/offset-reset      :earliest})
           (KS.stream/group-by (fn [k v]
                                 (println (format "Grouping [%d %d]"
                                                  k
                                                  v))
                                 (if (odd? k)
                                   "odd"
                                   "even"))
                               {::K/deserializer.key   :string
                                ::K/deserializer.value :long
                                ::K/serializer.key     :string
                                ::K/serializer.value   :long})
           (KS.stream/window [2 :seconds])
           (KS.stream/reduce-windows (fn reduce-window [sum k v]
                                       (println (format "Adding value %d to sum %s for key '%s'"
                                                        v
                                                        sum
                                                        k))
                                       (+ sum
                                          v))
                                     (fn seed []
                                       0)
                                     {::K/deserializer.key   :string
                                      ::K/deserializer.value :long
                                      ::K/serializer.key     :string
                                      ::K/serializer.value   :long
                                      ::KS.store/name        "my-store"
                                      ::KS.store/type        :kv.in-memory
                                      ::KS.store/cache?      false}))
       (KS.topology/topology builder)))


;; Always interesting to see what is the actual topology.

(KS.topology/describe topology)


(def app
     (KS/app "my-app-2"
             topology
             {::K/nodes         [["localhost" 9092]]
              ::KS/on-exception (fn [exception _thread]
                                  (println "Exception : " exception))}))


(KS/start app)


(def my-store
     (KS/window-store app
                      "my-store"))


(with-open [cursor (KS.store/ws-multi-range my-store)]
  (doseq [db-record (iterator-seq cursor)]
    (println (format "Aggregated key = '%s', time windows = [%d;%d), value = %d"
                     (::K/key db-record)
                     (::K/timestamp.from db-record)
                     (::K/timestamp.to db-record)
                     (::K/value db-record)))))
```

## License

Copyright © 2017-2018 Adam Helinski

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
