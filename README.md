# Milena

A straightforward clojure client for Kafka. Doesn't try to be too clever, simply
provides clear functions using simple clojure data structures.

## Status

Still in alpha for the time being, explaining why it isn't available on clojars yet.
Expected to evolve.

## Usage

Everything is commented as clearly as possible. Hence :
```
lein codox
```

# Basic

```clj
(require '[milena.core :as ml])


;; make a producer
(def p (ml/producer {:nodes            [["localhost" 9092]
                                        ["localhost" 9093]]
                     :serializer-key   (ml/serializers :string)
                     :serializer-value (ml/serializers :long)}))

;; make a consumer and assign it to topic "t1" partition 0
                                          "t2" partition 0
(def p (ml/consumer {:nodes            [["localhost" 9092]
                                        ["localhost" 9093]]
                     :serializer-key   (ml/serializers :string)
                     :serializer-value (ml/serializers :long)}
                     :listen           [["t1" 0]
                                        ["t2" 0]]}))

;; pause "t2"
(ml/pause c
          [["t2" 0]])

(ml/paused c)
;; => #{["t2" 0]}

(ml/paused? c
            [["t2" 0]])
;; => true

;; what is this consumer "listening" to ?
(ml/listening c)

(ml/listening? c
               [["t2" 0]])
;; => true, although it is paused


;; send 2500 messages with callback on completion
;; printing any failure
(dotimes [i 2500]
  (ml/psend p
            {:topic     "t1"
             :partition 0
             :key       "count"
             :value     i}
            (fn [exception metadata]
              (when exception
                (println [:fail i])))))


;; get the last offsets on topic "t1" partition 0
(ml/offsets-last c
                 "t1"
                 0)

;; set position to the first available offset on topic "t1" p0
(ml/rewind c
           [["t1" 0]])

(ml/position c
             "t1"
             0)
;; => 0

;; sum all numbers in "t1" p0 but stop polling when
;; the sum is > 1000
(ml/poll-reduce c
                200
                (fn [sum msg]
                  (let [sum' (+ sum
                                (:value msg))]
                    (if (> sum'
                           1000)
                        (reduced sum')
                        sum'))))

;; seek to offset 100 on topic "t1" p0 and poll from there
;; for 500 ms
(ml/seek c
         [["t1" 0]]
         100)

(ml/poll c
         500)

;; like a few other fns, ml/seek accepts either
;; 'topic' 'partition' or [['topic' 'partition']]
(ml/seek c
         "t1"
         0)


;; resume polling on previously paused topic "t2" p0
(ml/resume c
           "t2"
           0)

;; get metrics for the producer
(ml/metrics p)

;; close producer and consumer
(ml/close p)
(ml/close c)
```

# Serde

If we had cheshire, for instance, we could do the following and use those for
ml/producer or ml/consumer :

```clj
(def serializer-json
     (ml/make-serializer (fn [topic data]
                           (.getBytes (chesire/generate-string data)))))

(def deserializer-json
     (ml/make-deserializer (fn [topic data]
                             (chesire/parse-string (String. data)))))
```

## License

Copyright © 2017 Adam Helinski

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
