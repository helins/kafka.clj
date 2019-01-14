(ns dvlopt.kafka.in.mock

  "Mocking Kafka consumers."

  {:author "Adam Helinski"}

  (:require [dvlopt.kafka               :as K]
            [dvlopt.kafka.-interop.java :as K.-interop.java]
            [dvlopt.void                :as void])
  (:import (org.apache.kafka.clients.consumer Consumer
                                              MockConsumer)))




;;;;;;;;;;


(defn mock-consumer

  "Builds a mock Kafka consumer for testing purposes. It can use the `dvlopt.kafka.in` API like a regular consumer."

  (^MockConsumer
    
   []

   (mock-consumer nil))


  (^MockConsumer

   [options]

   (MockConsumer. (K.-interop.java/offset-reset-strategy (void/obtain ::offset-reset-strategy
                                                                      options
                                                                      K/defaults)))))
