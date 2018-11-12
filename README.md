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
[API](https://dvlopt.github.io/doc/clojure/dvlopt/kafka/index.html).

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

We will simply create a topic using the `dvlopt.kafka.admin` namespace.

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

We will send 25 records to the previously created topic using the
`dvlopt.kafka.out` namespace.

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

We will read a batch of records from our test topic and manually commit the
offset of where we are using the `dvlopt.kafka.in` namespace.

```clj
(with-open [consumer (K.in/consumer {::K/nodes              [["localhost" 9092]]
                                     ::K/deserializer.key   :long
                                     ::K/deserializer.value :long
                                     ::K.in/configuration   {"group.id"           "my-group"
                                                             "enable.auto.commit" false}})]
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

## License

Copyright © 2017-2018 Adam Helinski

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
