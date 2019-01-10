(ns dvlopt.kafka.out.mock

  "Mocking Kafka producers."

  {:author "Adam Helinski"}

  (:require [dvlopt.kafka               :as K]
            [dvlopt.kafka.-interop.java :as K.-interop.java]
            [dvlopt.void                :as void])
  (:import org.apache.kafka.clients.producer.MockProducer))




;;;;;;;;;;


(defn mock-producer

  "Builds a mock Kafka producer for testing purposes. It can use the `dvlopt.kafka.out` API.
  
   A map of options may be given :

     :dvlopt.kafka/serializer.key
     :dvlopt.kafka/serializer.value
      Cf. `dvlopt.kafka` for description of serializers.

     ::auto-complete?
      By default, each send operation is completed synchronously. Set this option to false in order to keep control."

  (^MockProducer

   []

   (mock-producer nil))


  (^MockProducer

   [options]

   (MockProducer. (void/obtain ::auto-complete?
                               options
                               K/defaults)
                  (K.-interop.java/extended-serializer (void/obtain ::K/serializer.key
                                                                    options
                                                                    K/defaults))
                  (K.-interop.java/extended-serializer (void/obtain ::K/serializer.value
                                                                    options
                                                                    K/defaults)))))
