(ns dvlopt.kstreams.stream

  "Handling of streams.

   Cf. `dvlopt.kstreams.high` for the big picture and details


   A stream can be transformed by various functions. Those functions returns themselves a new stream representing the transformation.
   Hence, a single stream can be used for the base of more than one transformation.

   The values of a stream can be aggregated. Prior to this, it needs to be grouped by using the `group-by` or `group-by-key` function from
   this namespace. If needed, a grouped stream can then be windowed by fixed time intervals or by sessions. Then, an appropriate `reduce-*`
   function can be used to perform the aggregation which always result in a table.

   A whole variety of joins between a stream and anything else are available."

  {:author "Adam Helinski"}

  (:refer-clojure :exclude [group-by])
  (:require [dvlopt.kafka.-interop.java :as K.-interop.java]
            [dvlopt.kstreams.store      :as KS.store])
  (:import (org.apache.kafka.streams.kstream GlobalKTable
                                             KeyValueMapper
                                             KGroupedStream
                                             KStream
                                             KTable
                                             Predicate
                                             SessionWindowedKStream
                                             TimeWindowedKStream)))




;;;;;;;;;; Transformations


(defn branch

  "Given a list of predicate functions, returns a vector of new corresponding streams such that as soon as a key-value from
   the original stream returns true on a predicate, it is send to the corresponding stream (and only that one).

   A record is dropped if it matches no predicate.


   Ex. ;; Divives a stream into 3 streams containing respectively :red values, :black ones, and any other ones.
  
       (branch stream
               [(fn only-red [k v]
                  (= v
                     :red))
                (fn only-black [k v]
                  (= v
                     :black))
                (fn other [k v]
                  true)])"

  [^KStream stream predicates]

  (into []
        (.branch stream
                 (into-array Predicate
                             (map K.-interop.java/predicate
                                  predicates)))))




(defn filter-kv

  "Returns a new stream filtering out key-values from the given one.
  

   Ex. ;; Keeps values greater than or equal to 42.
  
       (filter-kv stream
                  (fn [k v]
                    (>= v
                        42)))"

  ^KStream

  [^KStream stream predicate]

  (.filter stream
           (K.-interop.java/predicate predicate)))




(defn map-kv

  "Returns a new stream mapping key-values from the given one.

   Marks the resulting stream for repartioning.
   Cf. `dvlopt.kstreams.high`

   
   Ex. ;; The key is an ip address mapped to a country and the value is a collection mapped to its
          number of items.
  
       (map-kv stream
               (fn [k v]
                 [(country-from-ip k)
                  (count v)]))"

  ^KStream

  [^KStream stream f]

  (.map stream
        (K.-interop.java/key-value-mapper f)))




(defn map-keys

  "Returns a new stream efficiently mapping keys from the given one.

   Marks the resulting stream for repartioning.
   Cf. `dvlopt.kstreams.high`

   
   Ex. ;; The key is an ip address mapped to a country.
  
       (map-keys stream
                 (fn [k v]
                   (country-from-ip k)))"

  ^KStream

  [^KStream stream f]

  (.selectKey stream
              (K.-interop.java/key-value-mapper--raw f)))




(defn map-values

  "Returns a new stream efficiently mapping values from the given one.

   Unlike other mapping functions, does not mark the resulting stream for repartioning as the keys remain intact.
  
   
   Ex. ;; The value is a collection mapped to its number of items.
  
       (map-values stream
                   (fn [k v]
                     (count v)))"

  ^KStream

  [^KStream kstream f]

  (.mapValues kstream
              (K.-interop.java/value-mapper-with-key f)))




(defn fmap-kv

  "Returns a new stream mapping key-values from the given one into a collection of [key value]'s (or nil).
  
   Marks the resulting stream for repartioning.
   Cf. `dvlopt.kstreams.high`

  
   Ex. ;; The value is a list of tokens and we want to count them individually so that we end up with
          a collection where the key is a unique token and the value is its count.
  
       (fmap-kv stream
                (fn [k v]
                  (reduce (fn [hmap token]
                            (assoc hmap
                                   token
                                   (or (some-> (get hmap
                                                    token)
                                               inc)
                                       1)))
                          {}
                          v)))"

  ^KStream

  [^KStream kstream f]

  (.flatMap kstream
            (K.-interop.java/key-value-mapper--flat f)))




(defn fmap-values

  "Returns a new stream mapping values from the given one into a collection of values (or nil).
  
   Unlike `fmap-kv`, does not mark the resulting stream for repartioning as the keys remain intact.


   Ex. ;; The value is a list we want to split into individual items while filtering out nils.

       (fmap-values stream
                    (fn [k v]
                      (filter some?
                              v)))"

  ^KStream

  [^KStream kstream f]

  (.flatMapValues kstream
                  (K.-interop.java/value-mapper-with-key f)))




(defn process

  "Returns a new stream processing the records from the given one using a low-level processor.

   For when the high-level API is not enough. It is typically used for an `fmap-kv` like behavior involving some state.


   Cf. `dvlopt.kstreams.topology/add-processor`
       :dvlopt.kstreams/processor.on-record may be used to explicitly forward records using the associated context.

   Marks the topic for repartioning.
   Cf. `dvlopt.kstreams.high`


   A map of options may be given :

     :dvlopt.kstreams.store/names
      List of state store names previously added to the underlying builder this processor need to access.
      Cf. `dvlopt.kstreams.high/add-store`"

  (^KStream

   [stream processor]

   (process stream
            processor
            nil))


  (^KStream

   [^KStream stream processor options]

   (.transform stream
               (K.-interop.java/transformer-supplier processor)
               (into-array String
                           (::KS.store/names options)))))




(defn process-values 

  "Returns a new stream efficiently proccessing the values of the given one using a low-level processor.

   Unlike `process`, does not mark the resulting stream for repartioning as the keys remain intact. Also, because this
   function is about processing values, there is no point forwarding records using the associated context so doing so
   will throw an exception. Rather, the library maps the original value of the records to the value returned by
   :dvlopt.kstreams/processor.on-record (which may be nil).

   It is typically used when some state is needed.


   A map of options may be given, just like in `process`." 

  (^KStream

   [stream processor]

   (process-values stream
                   processor
                   nil))

  (^KStream

   [^KStream stream options processor]

   (.transformValues stream
                     (K.-interop.java/value-transformer-with-key-supplier processor)
                     ^"[Ljava.lang.String;" (into-array String
                                                        (::KS.store/names options)))))




;;;;;;;;;; Joins


(defn join-with-stream

  "Returns a new stream inner joining values from both given streams every time :

     a) They shares the same key.

     b) The difference in the timestamps of both records is <= the given interval.
        Cf. `dvlopt.kafka` for descriptions of time intervals.

   Hence, for the same key, several joins might happen within a single interval.

   Records with a nil key or nil value are ignored.

   Input streams are repartitioned if they were marked for repartitioning and a store will be generated for
   each of them.

   Cf. `dvlopt.kstreams.high` for requirements related to joins


   A map of options may be given :

     :dvlopt.kafka/deserializer.key
     :dvlopt.kafka/serializer.key

     :dvlopt.kstreams/left
     :dvlopt.kstreams/right
      Maps which containing value ser/de :

        :dvlopt.kafka/deserializer.value
        :dvlopt.kafka/serializer.value

      Cf. `dvlopt.kafka` for description of serializers and deserializers.

     :dvlopt.kstreams.store/retention
      Cf. `dvlopt.kstreams.store`


   Ex. (join-with-stream left-stream
                         right-stream
                         (fn [v-left v-right]
                           (str \"left = \" v-left \"and right = \" v-right))
                         [2 :seconds])"

  (^KStream

   [^KStream left-stream ^KStream right-stream f interval]

   (join-with-stream left-stream
                     right-stream
                     f
                     interval
                     nil))


  (^KStream

   [^KStream left-stream ^KStream right-stream f interval options]

   (.join left-stream
          right-stream
          (K.-interop.java/value-joiner f)
          (K.-interop.java/join-windows interval)
          (K.-interop.java/joined options))))




(defn left-join-with-stream

  "Exactly like `join-with-stream` but the join is triggered even if there is no record with the same key yet in
   the right stream for a given time window. In such case, the right value provided for the join is nil."

  (^KStream

   [^KStream left-stream ^KStream right-stream f interval]

   (left-join-with-stream left-stream
                          right-stream
                          f
                          interval
                          nil))


   (^KStream

    [^KStream left-stream ^KStream right-stream f interval options]

    (.leftJoin left-stream
               right-stream
               (K.-interop.java/value-joiner f)
               (K.-interop.java/join-windows interval)
               (K.-interop.java/joined options))))




(defn outer-join-with-stream

  "Exactly like `join-with-stream` but the join is triggered even if there is no record with the same key yet in
   the other stream for a given time window. In such case, the value provided for this side of the join is nil."

  (^KStream

   [^KStream left-stream ^KStream right-stream f interval]

   (outer-join-with-stream left-stream
                           right-stream
                           f
                           interval
                           nil))


  (^KStream

   [^KStream left-stream ^KStream right-stream f interval options]

   (.outerJoin left-stream
               right-stream
               (K.-interop.java/value-joiner f)
               (K.-interop.java/join-windows interval)
               (K.-interop.java/joined options))))




(defn join-with-table

  "Returns a new stream inner joining the given stream with the given table. The join is triggered everytime
   a new record arrives in the stream and the table contains the key of this record. This is very useful for
   enriching a stream with some up-to-date information.

   Records from the input stream with a nil key or value are ignored. Records from the input table with a nil
   value removes the corresponding key from this table.

   The input stream is repartioned if it was marked for repartioning.

 
   A map of options may be given, just like in `join-with-stream`. 


   Ex. (join-with-table stream
                        table
                        (fn [v-stream v-table]
                          (assoc v-stream
                                 :last-known-location
                                 (:location v-table))))"

  (^KStream

   [^KStream left-stream ^KTable right-table f]

   (join-with-table left-stream
                    right-table
                    f
                    nil))


  (^KStream

   [^KStream left-stream ^KTable right-table f options]

   (.join left-stream
          right-table
          (K.-interop.java/value-joiner f)
          (K.-interop.java/joined options))))




(defn left-join-with-table

  "Exactly like `join-with-table` but the join is triggered even if the table does contain the key. In such case, the
   right value supplied during the join in nil."
 
  (^KStream

   [^KStream left-stream ^KTable right-table f]

   (left-join-with-table left-stream
                         right-table
                         f
                         nil))


  (^KStream

   [^KStream left-stream ^KTable right-table f options]

   (.leftJoin left-stream
              right-table
              (K.-interop.java/value-joiner f)
              (K.-interop.java/joined options))))




(defn join-with-global-table

  "Similar to `join-with-table` but offers the following :
  
     - Data need not to be co-partitioned.
     - More efficient when a regular table is skewed (some partitions are heavily populated, some not).
     - Typically more efficient that performing several joins in order to reach the same result.
     - Allows for joining on a different key.

   Each record of the input stream is mapped to a new key which triggers a join if the global table contains that mapped
   key.  The value resulting from the join is associated with the key of the original record.

   Records from the input stream with a nil key or value are ignored. Records from the global table with a nil value
   removes the corresponding key from this global table.

   The intput stream is repartitioned if it was marked for repartitioning.


   Ex. ;; Enrich a stream of users by adding the most popular song of the country they are from.

       (join-with-global-table stream
                               global-table
                               (fn map-k [k v]
                                 (:country v))
                               (fn join [v-stream v-global-table]
                                 (assoc v-stream
                                        :song
                                        (first (:top-10 v-global-table)))))"

  ^KStream

  [^KStream left-stream ^GlobalKTable right-global-table f-map-k f-join]

  (.join left-stream
         right-global-table
         (K.-interop.java/key-value-mapper--raw f-map-k)
         (K.-interop.java/value-joiner f-join)))




(defn join-left-with-global-table

  "Exactly like `join-with-global-table` but the join is triggered even if the global table does not contain the mapped
   key. In such case, the right value supplied during the join is nil."

  ^KStream

  [^KStream left-stream ^GlobalKTable right-global-table f-map-k f-join]

  (.leftJoin left-stream
             right-global-table
             (K.-interop.java/key-value-mapper--raw f-map-k)
             (K.-interop.java/value-joiner f-join)))




;;;;;;;;; Grouping, windowing and aggregating


(defn group-by

  "Returns a new grouped stream grouping values based on a chosen key.

   Drops records leading to a nil chosen key.
  
   Because a new key is explicitly selected, the data is repartioned.
   Cf. `dvlopt.kstreams.high` about repartioning.

   A map of options may be given :

     :dvlopt.kafka/deserializer.key
     :dvlopt.kafka/deserializer.value
     :dvlopt.kafka/serializer.key
     :dvlopt.kafka/serializer.value
      Cf. `dvlopt.kafka` for description of deserializers


   Ex. (group-by stream
                 (fn by-country [k v]
                   (:country v)))"

  (^KGroupedStream
    
   [stream f]

   (group-by stream
             f
             nil))


  (^KGroupedStream
    
   [^KStream kstream f options]

   (.groupBy kstream
             (K.-interop.java/key-value-mapper--raw f)
             (K.-interop.java/serialized options))))




(defn group-by-key

  "Exactly like `group-by` but groups directly by the original key than a selected one.

   Unless the stream was marked for repartioning, this function will not repartition it as keys remain intact."

  (^KGroupedStream

   [^KStream stream]

   (group-by-key stream
                 nil))


  (^KGroupedStream

   [^KStream stream options]

   (.groupByKey stream
                (K.-interop.java/serialized options))))




(defn window

  "Returns a new time windowed stream windowing values by a fixed time interval for each key of the given grouped stream.
  

   A map of options may be given :

     :dvlopt.kstreams.store/retention
      Cf. `dvlopt.kstreams.store`

     ::interval.type
      Fixed time windows can behave in 3 fashions :

        :hopping
         Hopping windows of interval I advance by sub-interval J <= I and are aligned to the epoch, meaning they
         always start at time 0. The lower bound is inclusive and upper bound is exclusive. 
           
           Ex. I = 5 seconds, J = 3 seconds (thus every window overlaps with its neighbor by 2 seconds)

               [0;5), [3;8), [6;11), ...

        :tumbling (default)
         Tumbling windows are simply non-overlapping hopping windows (ie. I = J).

           Ex. [0;5), [5;10), [10;15), ...

        :sliding
         Sliding windows are aligned to the timestamps of the records and not the epoch. Two records are said to
         be part of the same sliding window if the difference of their timestamps <= I, where both the lower and
         upper bound are inclusive.

   Windows of type :hopping accepts the following additional option :

     ::advance-by 
      The J sub-interval.


   Cf. `dvlopt.kafka` for description of time intervals.
       `dvlopt.kstreams.store` for description of time windows.


   Ex. ;; Windows of 5 seconds advancing by 3 seconds (thus overlapping 2 seconds).

       (window grouped-stream
               [5 :seconds]
               {::interval.type :hopping
                ::advance-by    [3 :seconds]})"

  (^TimeWindowedKStream

   [grouped-stream interval]

   (window grouped-stream
           interval
           nil))


  (^TimeWindowedKStream

   [^KGroupedStream grouped-stream interval options]

   (.windowedBy grouped-stream
                (K.-interop.java/time-windows interval
                                              options))))




(defn window-by-session

  "Returns a new session windowed stream windowing values by sessions for each key of the given grouped stream.

   Sessions are non-fixed intervals of activity between fixed intervals of inactivity.

   Cf. `dvlopt.kafka` for description of time intervals
   Cf. `dvlopt.kstreams.store` for description of sessions
  

   A map of options may be given :

     :dvlopt.kstreams.store/retention
      Cf. `dvlopt.kstreams.store`"

  (^SessionWindowedKStream

   [grouped-stream interval]

   (window-by-session grouped-stream
                      interval
                      nil))


  (^SessionWindowedKStream

   [^KGroupedStream grouped-stream interval options]

   (.windowedBy grouped-stream
                (K.-interop.java/session-windows interval
                                                 options))))




(defn reduce-values

  "Returns a new table aggregating values for each key of the given grouped stream.

   
   A map of standard table options may be given (cf. `dvlopt.kstreams.table`).


   Ex. (reduce-values grouped-stream
                      (fn reduce [aggregated k v]
                        (+ aggregated
                           v))
                      (fn seed []
                        0))"

  (^KTable

   [grouped-stream fn-reduce fn-seed]

   (reduce-values grouped-stream
                  fn-reduce
                  fn-seed
                  nil))


  (^KTable

   [^KGroupedStream grouped-stream fn-reduce fn-seed options]

   (.aggregate grouped-stream
               (K.-interop.java/initializer fn-seed)
               (K.-interop.java/aggregator fn-reduce)
               (K.-interop.java/materialized--kv options))))




(defn reduce-windows

  "Returns a new table aggregating values for each time window of each key of the given time-windowed stream.
  

   A map of standard table options may be given (cf. `dvlopt.kstreams.table`)."

  (^KTable

   [time-windowed-stream fn-reduce fn-seed]

   (reduce-windows time-windowed-stream
                   fn-reduce
                   fn-seed
                   nil))


  (^KTable

   [^TimeWindowedKStream time-windowed-stream fn-reduce fn-seed options]

   (.aggregate time-windowed-stream
               (K.-interop.java/initializer fn-seed)
               (K.-interop.java/aggregator fn-reduce)
               (K.-interop.java/materialized--by-name options))))




(defn reduce-sessions

  "Returns a new table aggregating values for each session of each key for the given session-windowed stream.
  
   Sessions might merge, hence the need for a function being able to do so.


   A map of standard table options may be given (cf. `dvlopt.kstreams.table`).


   Ex. (reduce-sessions grouped-stream
                        (fn reduce [aggregated k v]
                          (+ aggregated
                             v))
                        (fn merge [aggregated-session-1 aggregated-session-2 k]
                          (+ aggregated-session-1
                             aggregated-session-2))
                        (fn seed []
                          0))"

  (^KTable

   [session-windowed-stream fn-reduce fn-merge fn-seed]

   (reduce-sessions session-windowed-stream
                    fn-reduce
                    fn-merge
                    fn-seed
                    nil))


  (^KTable

   [^SessionWindowedKStream session-windowed-stream fn-reduce fn-merge fn-seed options]

   (.aggregate session-windowed-stream
               (K.-interop.java/initializer fn-seed)
               (K.-interop.java/aggregator fn-reduce)
               (K.-interop.java/merger fn-merge)
               (K.-interop.java/materialized--by-name options))))




;;;;;;;;;; Transitions


(defn through-topic

  "Sends each record of the given stream to a given topic and then continue streaming.

   Useful when the stream was marked for repartioning. Streaming to a chosen topic before a stateful operation will avoid
   automatic repartioning.
  
   A map of options may be given :

     :dvlopt.kafka/deserializer.key
     :dvlopt.kafka/deserializer.value
     :dvlopt.kafka/serializer.key
     :dvlopt.kafka/serializer.value
      Cf. `dvlopt.kafka` for description of serializers and deserializers.

     :dvlopt.kstreams/select-partition
      Cf. `dvlopt.kstreams.high`"

  (^KStream

   [stream topic]

   (through-topic stream
                  topic
                  nil))


  (^KStream

   [^KStream stream ^String topic options]

   (.through stream
             topic
             (K.-interop.java/produced options))))




(defn do-kv

  "For each key-value, do a side-effect and then continue streaming.

   Side effects cannot be tracked by Kafka, hence they do not benefit from Kafka's processing garantees.
  
   
   Ex. (do-kv stream
              (fn [k v]
                (println :key k :value v)))"

  ^KStream

  [^KStream stream f]

  (.peek stream
         (K.-interop.java/foreach-action f)))




;;;;;;;;;; Terminating streams


(defn sink-process

  "Marks the end of the given stream by processing each record with a low-level processor.

   Returns nil.

   
   A map of options may be given, just like in `process`."

  (^KStream

   [stream processor]

   (process stream
            processor
            nil))


  (^KStream

   [^KStream stream processor options]

   (.process stream
             (K.-interop.java/processor-supplier processor)
             (into-array String
                         (::KS.store/names options)))))




(defn sink-topic

  "Marks the end of the given stream by sending each key-value to a chosen topic.

   In this case, topic might be either :

     - [:always TopicName]
       Where TopicName is the name of the unique topic the records will always be sent to.

     - [:select (fn [record])]
       Where the function is used to dynamically select a topic. All the values in the provided record refer
       to what is known about the original record sourced from Kafka. As such, even ::topic might be missing.
  
   Returns nil.
  
  
   A map of options may be given, the same one as for `through-topic`."

  ([stream topic]

   (sink-topic stream
               topic
               nil))


  ([^KStream stream topic options]
   
   (let [options' (K.-interop.java/produced options)
         [type
          target] topic]
     (condp identical?
            type
       :always  (.to stream
                     ^String target
                     options')
       :select (.to stream
                    (K.-interop.java/topic-name-extractor target)
                    options')))))




(defn sink-do

  "Marks the end of the given stream by doing a side-effect for each key-value.
  
   Side effects cannot be tracked by Kafka, hence they do not benefit from Kafka's processing garantees.
   Returns nil.


   Ex. (sink-do stream
                (fn [k v]
                  (println :key k :value v)))"

  [^KStream stream f]

  (.foreach stream
            (K.-interop.java/foreach-action f)))
