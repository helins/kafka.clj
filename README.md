# Milena

![alt text](https://s-media-cache-ak0.pinimg.com/600x315/a4/11/25/a411251488b63fb207751b1545aeb551.jpg "Milena")

Franz Kafka and Milena Jesenska wrote passionate letters to each other.

This library allows the user to exchange records with Kafka while speaking
clojure.

The user should be using almost always these namespaces :

- `milena.admin`
  For managing topics, configurations and ACLs.

- `milena.serialize`
  `milena.produce`
  For serializing and sending records.

- `milena.deserialize`
  `milena.consume`
  For deserializing and consuming records.


Although not strictly needed, it is best for the user to be familiar with the
original [java library](https://kafka.apache.org/documentation/#api). The goal
of this clojure wrapper is to avoid clunky java interop while being up to date.
The [API](https://dvlopt.github.io/doc/milena/) translates clojure data
structures to java objects and vice-versa for the user. As such, documentation
often refers to those namespaces :

- `milena.interop.clj`
  For translating clojure data structures to java objects.

- `milena.interop.java`
  For translating java objects to clojure data structures.

The user should never need to use those functions directly but can at least
understand what is going on.

## Status

While being already used in production, this library is still alpha (soon to be
beta). The main reason being that all the java admin stuff is itself brand new
and might change in the future.

Otherwise, after quite a few breaking changes, other namespaces should now be
quite stable.

## Usage

Everything is commented as clearly as possible and often with examples.

[Read the full API](https://dvlopt.github.io/doc/milena/)

As a convention, :keywords and symbols starting with a "?" are nilable.

### Basics

```clj
;; We need a few things
(require '[milena.admin       :as admin]
         '[milena.produce     :as produce]
         '[milena.consume     :as consume]
         '[milena.serialize   :as serialize]
         '[milena.deserialize :as deserialize])


;; First, we are going to create a topic.
;; We need an admin client.
(def A
     (admin/make {:?nodes [["localhost"] 9092]}))


;; Let's create the topic.
(admin/topics-create A
                     {"my-topic" {:?partitions         1
                                  :?replication-factor 1
                                  :?config             {:cleanup.policy "compact"}}})


;; There it is, amongst other topics.
(admin/topics A)
;; => <Future {"my-topic" {:internal? false}
               ...}>


;; Now, let's send some records.
;; We need a producer.
(def P
     (produce/make {:?nodes            [["localhost" 9092]]
                    :?serializer-key   serialize/string
                    :?serializer-value serialize/long}))


;; Let's send 5 records to our new topic on partition 0.
;; We'll provide an optional callback.
(dotimes [i 5]
  (produce/commit P
                  {:topic      "my-topic"
                   :?partition 0
                   :?key       (format "message-%d"
                                       i)
                   :?value     i}
                  (fn callback [?exception ?meta]
                    (println i :okay? (boolean ?exception)))))


;; Okay, time to consume some records !
;; We need a consumer (with a little bit of optional configuration).
(def C
     (consume/make {:?nodes              [["localhost" 9092]]
                    :?deserializer-key   deserialize/string
                    :?deserializer-value deserialize/long
                    :?config             {:group.id           "my_test"
                                          :enable.auto.commit false}}))



;; The consumer needs to be assigned.
(consume/listen C
                [["my-topic" 0]])


;; Time to get a batch of records.
(consume/poll C)


;; Wait, let's do something different !
;; We need to rewind the partition to the beginning.
(consume/rewind C
                "my-topic"
                0)


;; So, we'd like to sum all the available values.
;; At the end of the partition, we'll wait for up to 500 milliseconds for new records.
(reduce (fn [sum record]
          (+ sum
             (:value record)))
        0
        (consume/poll-seq C
                          500))


;; The job is done.
;; Let's commit the offsets so next time we won't forget where we ended.
;; This will commit the offset of the last record from the last time we polled records.
(consume/commit-sync C)


;; That's it for now !
;; Check the full API, there is more you can do.
;; Oh, and don't forget to close your resources, it's cleaner.
(admin/close   A)
(produce/close P)
(consume/close C)
```

## License

Copyright © 2017 Adam Helinski

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
