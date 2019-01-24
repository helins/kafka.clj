(ns user

  "For daydreaming at the repl."

  (:require [clojure.spec.alpha         :as s]
            [clojure.spec.gen.alpha     :as gen]
            [clojure.spec.test.alpha    :as stest]
            [criterium.core             :as ct]
            [dvlopt.kafka               :as K]
            [dvlopt.kafka.-interop      :as K.-interop]
            [dvlopt.kafka.-interop.clj  :as K.-interop.clj]
            [dvlopt.kafka.-interop.java :as K.-interop.java]
            [dvlopt.kafka.admin         :as K.admin]
            [dvlopt.kafka.in            :as K.in]
            [dvlopt.kafka.in.mock       :as K.in.mock]
            [dvlopt.kafka.out           :as K.out]
            [dvlopt.kafka.out.mock      :as K.out.mock]
            [dvlopt.kstreams            :as KS]
            [dvlopt.kstreams.mock       :as KS.mock]
            [dvlopt.kstreams.builder    :as KS.builder]
            [dvlopt.kstreams.ctx        :as KS.ctx]
            [dvlopt.kstreams.store      :as KS.store]
            [dvlopt.kstreams.stream     :as KS.stream]
            [dvlopt.kstreams.table      :as KS.table]
            [dvlopt.kstreams.topology   :as KS.topology]
            [dvlopt.void                :as void]
            [taoensso.nippy             :as nippy]))




;;;;;;;;;;


(comment
  

    (def a
         (K.admin/admin))


    )
