(ns milena.examples.core
  (:require [milena.core :as ml]))




;; Create a producer
(def p (ml/producer "localhost:9092"
                    (ml/serializer :string)
                    (ml/serializer :long)))

;; Create a consumer
(def c (ml/consumer "localhost:9092"
                    "my-group-id"
                    (ml/deserializer :string)
                    (ml/deserializer :long)))

;; Get a list of all partitions by topics the consumer is authorized to consume
(ml/topics c)

;; Assign consumer to partition 0 of topic "t1" and "t2"
(ml/listen c
           [["t1" 0]
            ["t2" 0]])

;; Pause "t2", we won't need it for now
(ml/pause c
          "t2"
          0)

;; What are we consuming ?
(ml/listening c)

;; Should be true, although it is paused
(ml/listening? c
               ["t2" 0])

;; ["t2" 0] should be there
(ml/paused c)

;; true
(ml/paused? c
            "t2"
            0)


;; Send 10 values with a callback on completion
(dotimes [i 10]
  (ml/ksend p
            {:topic "t1"
             :key   "count"
             :value i}
            (fn [exception metadata]
              (println (if exception
                           [:fail]
                           metadata)))))

;; Get the last offset of "t1" partition 0
(ml/offsets-latest c
                   "t1"
                   0)

;; Poll messages with consumer for at most 200ms (probably nil for now)
(ml/poll c
         200)

;; Get the current position of the consumer on partition 0 of topic "t1"
(ml/position c
             "t1"
             0)

;; Get the committed offset on partition 0 of topic "t1"
(ml/committed c
              "t1"
              0)

;; Seek consumer to position 3 on partition 0 of topic "t1"
(ml/seek c
        "t1"
        0
        3)

;; Poll again (should returns records)
(ml/poll c
         200)


;; Rewind consumer
(ml/rewind c
           "t1"
           0)

;; Sum all the values
(ml/poll-reduce c
                200
                (fn [sum i] (+ sum i))
                0)


;; Resume consumption of partition 0 of "t2"
(ml/resume c
           "t2"
           0)


;; Get metrics for the producer
(ml/metrics p)


;; Close the producer and the consumer
(ml/close p)
(ml/close c)




;;; Serialization
;;; If cheshire was required, we could do this

(def serializer-json (ml/make-serializer (fn [topic data]
                                           (-> data
                                               cheshire/generate-string
                                               .getBytes))))

(def deserializer-json (ml/make-deserializer (fn [topic data]
                                               (-> data
                                                   String.
                                                   cheshire/parse-string))))
