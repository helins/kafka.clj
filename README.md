# Milena

![alt text](https://s-media-cache-ak0.pinimg.com/600x315/a4/11/25/a411251488b63fb207751b1545aeb551.jpg "Milena")

A straightforward clojure client for Kafka. Doesn't try to be too clever, simply
provides clear functions using simple clojure data structures.

Franz Kafka and Milena Jesenska wrote passionate letters to each other. Isn't that
relevant ?

## Status

Still in alpha for the time being, explaining why it isn't available on clojars yet.
Expected to evolve.

## Usage

Everything is commented as clearly as possible. Hence :
```
lein codox
```

### Basic

```clj
(require '[milena.produce :as mp]
         '[milena.consume :as mc])


;; make a producer
(def p (mp/make {:nodes            [["localhost" 9092]
                                    ["localhost" 9093]]
                 :serializer-key   (mp/serializers :string)
                 :serializer-value (mp/serializers :long)}))

;; make a consumer and assign it to topic "t1" partition 0
;;                                        "t2" partition 0
(def c (mc/make {:nodes            [["localhost" 9092]
                                    ["localhost" 9093]]
                 :serializer-key   (mc/deserializers :string)
                 :serializer-value (mc/deserializers :long)}
                 :config           {:group.id           "foo.bar"
                                    :enable.auto.commit false}
                 :listen           [["t1" 0]
                                    ["t2" 0]]}))

;; pause "t2"
(mc/pause c
          [["t2" 0]])

(mc/paused c)
;; => #{["t2" 0]}

(mc/paused? c
            [["t2" 0]])
;; => true

;; what is this consumer "listening" to ?
(mc/listening c)

(mc/listening? c
               [["t2" 0]])
;; => true, although it is paused


;; send 2500 messages with callback on completion
;; printing any failure
(dotimes [i 2500]
  (mp/commit p
             {:topic     "t1"
              :partition 0
              :key       "count"
              :value     i}
             (fn [exception metadata]
               (when exception
                 (println [:fail i])))))


;; get the first offsets on topic "t1" partition 0
(mc/first c
          "t1"
          0)

;; set position to the first available offset on topic "t1" p0
(mc/rewind c
           [["t1" 0]])

(mc/position c
             "t1"
             0)
;; => 0

;; sum all numbers in "t1" p0 but stop polling when
;; the sum is > 1000
(mc/poll-reduce (fn [sum msg]
                  (let [sum' (+ sum
                                (:value msg))]
                    (if (> sum'
                           1000)
                        (reduced sum')
                        sum')))
                0
                c
                200)

;; seek to offset 100 on topic "t1" p0 and poll from there
;; for 500 ms
(mc/seek c
         [["t1" 0]]
         100)

(mc/poll c
         500)

;; synchronously commit offsets from last poll
(mc/commit c)

;; like a few other fns, ml/seek accepts either
;; 'topic' 'partition' or [['topic' 'partition']]
(mc/seek c
         "t1"
         0)


;; resume polling on previously paused topic "t2" p0
(mc/resume c
           "t2"
           0)

;; get metrics for the producer
(mp/metrics p)

;; close producer and consumer
(mp/close p)
(mc/close c)
```

### Serde

If we had cheshire, for instance, we could do the following and use those for
(mp/make) or (mc/make) :

```clj
(def serializer-json
     (mp/make-serializer (fn [topic data]
                           (.getBytes (chesire/generate-string data)))))

(def deserializer-json
     (mc/make-deserializer (fn [topic data]
                             (chesire/parse-string (String. data)))))
```

## License

Copyright © 2017 Adam Helinski

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
