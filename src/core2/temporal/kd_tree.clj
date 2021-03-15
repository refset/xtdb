(ns core2.temporal.kd-tree
  (:import [java.util ArrayDeque ArrayList Arrays Collection Comparator Deque List Random Spliterator Spliterator$OfInt Spliterators]
           [java.util.function Consumer IntConsumer Function Predicate]
           [java.util.stream Collectors StreamSupport]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BigIntVector VectorSchemaRoot]))

(set! *unchecked-math* :warn-on-boxed)

(defrecord Node [^longs location left right])

(defmacro ^:private next-axis [axis k]
  `(let [next-axis# (unchecked-inc-int ~axis)]
     (if (= ~k next-axis#)
       (int 0)
       next-axis#)))

(def ^:private ^Class longs-class (Class/forName "[J"))

(defn- ->longs ^longs [xs]
  (if (instance? longs-class xs)
    xs
    (long-array xs)))

(defn ->kd-tree
  (^core2.temporal.kd_tree.Node [points]
   (->kd-tree (mapv ->longs points) 0))
  (^core2.temporal.kd_tree.Node [points ^long axis]
   (when-let [points (not-empty points)]
     (let [k (alength ^longs (first points))
           points (vec (sort-by #(aget ^longs % axis) points))
           median (quot (count points) 2)
           axis (next-axis axis k)]
       (->Node (nth points median)
               (->kd-tree (subvec points 0 median) axis)
               (->kd-tree (subvec points (inc median)) axis))))))

(defn kd-tree-insert ^core2.temporal.kd_tree.Node [^Node kd-tree location]
  (let [location (->longs location)
        k (alength location)]
    (loop [axis 0
           node kd-tree
           build-fn identity]
      (if-not node
        (build-fn (Node. location nil nil))
        (let [^longs location-node (.location node)
              location-axis (aget location-node axis)]
          (cond
            (Arrays/equals location location-node)
            (build-fn node)

            (< (aget location axis) location-axis)
            (recur (next-axis axis k) (.left node) (comp build-fn (partial assoc node :left)))

            (<= location-axis (aget location axis))
            (recur (next-axis axis k) (.right node) (comp build-fn (partial assoc node :right)))))))))

(deftype NodeStackEntry [^Node node ^int axis])

(defmacro ^:private in-range? [mins xs maxs]
  `(let [mins# ~mins
         xs# ~xs
         maxs# ~maxs
         len# (alength xs#)]
     (loop [n# (int 0)]
       (if (= n# len#)
         true
         (let [x# (aget xs# n#)]
           (if (and (<= (aget mins# n#) x#)
                    (<= x# (aget maxs# n#)))
             (recur (inc n#))
             false))))))

(deftype NodeRangeSearchSpliterator [^longs min-range ^longs max-range ^int k ^Deque stack]
  Spliterator
  (tryAdvance [_ c]
    (loop []
      (if-let [^NodeStackEntry entry (.poll stack)]
        (let [^Node node (.node entry)
              axis (.axis entry)
              ^longs location (.location node)
              location-axis (aget location axis)
              min-match? (<= (aget min-range axis) location-axis)
              max-match? (<= location-axis (aget max-range axis))
              axis (next-axis axis k)]
          (when-let [right (when max-match?
                             (.right node))]
            (.push stack (NodeStackEntry. right axis)))
          (when-let [left (when min-match?
                            (.left node))]
            (.push stack (NodeStackEntry. left axis)))

          (if (and min-match?
                   max-match?
                   (in-range? min-range location max-range))
            (do (.accept c location)
                true)
            (recur)))
        false)))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE Spliterator/NONNULL))

  (estimateSize [_]
    Long/MAX_VALUE)

  (trySplit [_]))

(defn kd-tree-range-search ^java.util.Spliterator [^Node kd-tree min-range max-range]
  (let [min-range (->longs min-range)
        max-range (->longs max-range)
        k (count (some-> kd-tree (.location)))
        stack (doto (ArrayDeque.)
                (.push (NodeStackEntry. kd-tree 0)))]
    (->NodeRangeSearchSpliterator min-range max-range k stack)))

(deftype ColumnStackEntry [^int start ^int end ^int axis])

(defn ->column-kd-tree ^org.apache.arrow.vector.VectorSchemaRoot [^BufferAllocator allocator points]
  (let [points (object-array (mapv ->longs points))
        n (alength points)
        k (alength ^longs (aget points 0))
        columns (VectorSchemaRoot. ^List (repeatedly k #(doto (BigIntVector. "" allocator)
                                                          (.setInitialCapacity n)
                                                          (.allocateNew))))
        stack (doto (ArrayDeque.)
                (.push (ColumnStackEntry. 0 (alength points) 0)))]
    (loop []
      (when-let [^ColumnStackEntry entry (.poll stack)]
        (let [start (.start entry)
              end (.end entry)
              axis (.axis entry)]
          (Arrays/sort points start end (reify Comparator
                                          (compare [_ x y]
                                            (Long/compare (aget ^longs x axis)
                                                          (aget ^longs y axis)))))
          (let [median (quot (+ start end) 2)
                axis (next-axis axis k)]
            (when (< (inc median) end)
              (.push stack (ColumnStackEntry. (inc median) end axis)))
            (when (< start median)
              (.push stack (ColumnStackEntry. start median axis)))))
        (recur)))
    (dotimes [n k]
      (let [^BigIntVector v (.getVector columns n)]
        (dotimes [m (alength points)]
          (.set v m (aget ^longs (aget points m) n)))))
    (doto columns
      (.setRowCount n))))

(defmacro ^:private in-range-column? [mins xs k idx maxs]
  (let [col-sym (with-meta (gensym "col") {:tag `BigIntVector})]
    `(let [idx# ~idx
           mins# ~mins
           xs# ~xs
           maxs# ~maxs
           len# ~k]
       (loop [n# (int 0)]
         (if (= n# len#)
           true
           (let [~col-sym (.getVector xs# n#)
                 x# (.get ~col-sym idx#)]
             (if (and (<= (aget mins# n#) x#)
                      (<= x# (aget maxs# n#)))
               (recur (inc n#))
               false)))))))

(deftype ColumnRangeSearchSpliterator [^VectorSchemaRoot kd-tree ^longs min-range ^longs max-range ^int k ^Deque stack]
  Spliterator$OfInt
  (^boolean tryAdvance [_ ^IntConsumer c]
    (loop []
      (if-let [^ColumnStackEntry entry (.poll stack)]
        (let [start (.start entry)
              end (.end entry)
              axis (.axis entry)
              median (quot (+ start end) 2)
              ^BigIntVector axis-column (.getVector kd-tree axis)
              axis-value (.get axis-column median)
              min-match? (<= (aget min-range axis) axis-value)
              max-match? (<= axis-value (aget max-range axis))
              axis (next-axis axis k)]

          (when (and max-match? (< (inc median) end))
            (.push stack (ColumnStackEntry. (inc median) end axis)))
          (when (and min-match? (< start median))
            (.push stack (ColumnStackEntry. start median axis)))

          (if (and min-match?
                   max-match?
                   (in-range-column? min-range kd-tree k median max-range))
            (do (.accept c median)
                true)
            (recur)))
        false)))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE Spliterator/NONNULL))

  (estimateSize [_]
    Long/MAX_VALUE)

  (trySplit [_]))

(defn column-kd-tree-range-search ^java.util.Spliterator$OfInt [^VectorSchemaRoot kd-tree min-range max-range]
  (let [min-range (->longs min-range)
        max-range (->longs max-range)
        k (alength min-range)
        stack (doto (ArrayDeque.)
                (.push (ColumnStackEntry. 0 (.getRowCount kd-tree) 0)))]
    (->ColumnRangeSearchSpliterator kd-tree min-range max-range k stack)))

;; TODO:
;; Sanity check counts via stream count.
;; Try different implicit orders for implicit/column/ist.
(defn- run-test []
  (with-open [allocator (RootAllocator.)]
    (assert (= (-> (->kd-tree [[7 2] [5 4] [9 6] [4 7] [8 1] [2 3]])
                   (kd-tree-range-search [0 0] [8 4])
                   (StreamSupport/stream false)
                   (.toArray)
                   (->> (mapv vec)))

               (-> (reduce
                    kd-tree-insert
                    nil
                    [[7 2] [5 4] [9 6] [4 7] [8 1] [2 3]])
                   (kd-tree-range-search [0 0] [8 4])
                   (StreamSupport/stream false)
                   (.toArray)
                   (->> (mapv vec)))

               (with-open [column-kd-tree (->column-kd-tree allocator [[7 2] [5 4] [9 6] [4 7] [8 1] [2 3]])]
                 (-> column-kd-tree
                     (column-kd-tree-range-search [0 0] [8 4])
                     (StreamSupport/intStream false)
                     (.toArray)
                     (->> (mapv #(mapv (fn [^BigIntVector col]
                                         (.get col %))
                                       (.getFieldVectors column-kd-tree)))))))
            "wikipedia-test"))

  (with-open [allocator (RootAllocator.)]
    (doseq [k (range 2 4)]
      (let [rng (Random. 0)
            _ (prn :k k)
            ns 100000
            qs 10000
            ts 3
            _ (prn :gen-points ns)
            points (time
                    (vec (for [n (range ns)]
                           (long-array (repeatedly k #(.nextLong rng))))))

            _ (prn :gen-queries qs)
            queries (time
                     (vec (for [n (range qs)
                                :let [min+max-pairs (repeatedly k #(sort [(.nextLong rng)
                                                                          (.nextLong rng)]))]]
                            [(long-array (map first min+max-pairs))
                             (long-array (map second min+max-pairs))])))]

        (prn :range-queries-scan qs)
        (time
         (doseq [[^longs min-range ^longs max-range] queries]
           (-> (.stream ^Collection points)
               (.filter (reify Predicate
                          (test [_ location]
                            (in-range? min-range ^longs location max-range))))
               (.count))))

        (prn :build-kd-tree-insert ns)
        (let [kd-tree (time
                       (reduce
                        kd-tree-insert
                        nil
                        points))]

          (prn :range-queries-kd-tree-insert qs)
          (dotimes [_ ts]
            (time
             (doseq [[min-range max-range] queries]
               (-> (kd-tree-range-search kd-tree min-range max-range)
                   (StreamSupport/stream false)
                   (.count))))))


        (prn :build-kd-tree-bulk ns)
        (let [kd-tree (time
                       (->kd-tree points))]

          (prn :range-queries-kd-tree-bulk qs)
          (dotimes [_ ts]
            (time
             (doseq [[min-range max-range] queries]
               (-> (kd-tree-range-search kd-tree min-range max-range)
                   (StreamSupport/stream false)
                   (.count))))))

        (prn :build-column-kd-tree ns)
        (with-open [kd-tree (time
                             (->column-kd-tree allocator points))]
          (prn :range-queries-column-kd-tree qs)
          (dotimes [_ ts]
            (time
             (doseq [[min-range max-range] queries]
               (-> (column-kd-tree-range-search kd-tree min-range max-range)
                   (StreamSupport/intStream false)
                   (.count))))))))))
