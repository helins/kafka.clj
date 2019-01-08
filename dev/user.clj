(ns user

  "For daydreaming in the repl"

  (:require [clojure.spec.alpha                   :as s]
            [clojure.spec.gen.alpha               :as gen]
            [clojure.spec.test.alpha              :as stest]
            [criterium.core                       :as ct]
            [dvlopt.kafka                         :as K]
            [dvlopt.kafka.-interop                :as K.-interop]
            [dvlopt.kafka.-interop.clj            :as K.-interop.clj]
            [dvlopt.kafka.-interop.java           :as K.-interop.java]
            [dvlopt.kafka.admin                   :as K.admin]
            [dvlopt.kafka.in                      :as K.in]
            [dvlopt.kafka.out                     :as K.out]
            [dvlopt.kstreams                      :as KS]
            [dvlopt.kstreams.builder              :as KS.builder]
            [dvlopt.kstreams.ctx                  :as KS.ctx]
            [dvlopt.kstreams.store                :as KS.store]
            [dvlopt.kstreams.stream               :as KS.stream]
            [dvlopt.kstreams.table                :as KS.table]
            [dvlopt.kstreams.topology             :as KS.topology]
            [dvlopt.void                          :as void]
            [taoensso.nippy                       :as nippy]))




;;;;;;;;;;


(defn snd

  ;; Sends a bunch of messages.

  ([producer topic n]

   (snd producer topic
        n
        false))


  ([producer topic n print?]

   (dotimes [i n]
     (K.out/send producer
                 {::K/topic     topic
                  ::K/partition 0
                  ::K/key       i
                  ::K/value     i}
                 (fn [e mta]
                   (when print?
                     (println :commit e mta)))))))




;;;;;;;;;;


(comment
  

    (def a
         (K.admin/admin))


    )
