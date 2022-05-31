(ns xtdb.kv.mutable-kv
  (:require [xtdb.kv :as kv]
            [xtdb.memory :as mem])
  (:import clojure.lang.ISeq
           java.io.Closeable
           [java.util NavigableMap TreeMap]))

(deftype MutableKvIterator [^NavigableMap db, !tail-seq]
  kv/KvIterator
  (seek [_ k]
    (some-> (reset! !tail-seq (->> (.tailMap db (mem/as-buffer k) true)
                                   (filter val)))
            first
            key))

  (seek-to-last [_]
    (when-not (.isEmpty db)
      (some-> (reset! !tail-seq (->> (.tailMap db (.lastKey db) true)
                                     (filter val)))
              first
              key)))

  (next [_]
    (some-> (swap! !tail-seq rest) first key))

  (prev [_]
    (loop []
      (when-let [[[k _] :as tail-seq] (seq @!tail-seq)]
        (when-let [[k v :as lower-entry] (.lowerEntry db k)]
          (reset! !tail-seq (cons lower-entry tail-seq))
          (if v
            k
            (recur))))))

  (value [_]
    (some-> (first @!tail-seq) val))

  Closeable
  (close [_]))

(deftype MutableKvSnapshot [^NavigableMap db]
  kv/KvSnapshot
  (new-iterator [_] (->MutableKvIterator db (atom nil)))
  (get-value [_ k] (get db (mem/as-buffer k)))

  ISeq
  (seq [_] (seq db))

  Closeable
  (close [_]))

(deftype MutableKvTxSnapshot [db tx-db]
  kv/KvSnapshot
  (new-iterator [_]
    (kv/->MergedKvIterator (kv/new-iterator db)
                           (kv/new-iterator tx-db)
                           (atom nil)
                           (atom nil)
                           (atom nil)
                           (atom nil)
                           (atom nil)))
  (get-value [_ k]
    (or (kv/get-value tx-db (mem/as-buffer k))
        (kv/get-value db (mem/as-buffer k))))

  ISeq
  (seq [_] (concat db tx-db))

  Closeable
  (close [_]))

(declare ->mutable-kv-store)

(defrecord MutableKvStoreTx [kv-store tx-kv-store]
  kv/KvStore
  (new-snapshot ^java.io.Closeable [_]
    (->MutableKvTxSnapshot (kv/new-snapshot kv-store)
                           (kv/new-snapshot tx-kv-store)))

  (store [_ kvs] (kv/store tx-kv-store kvs))
  (fsync [_])
  (compact [_])
  (count-keys [_] (+ (kv/count-keys kv-store) (kv/count-keys tx-kv-store)))
  (db-dir [_])
  (kv-name [this] (str (class this)))
  (begin-kv-tx [this] (->MutableKvStoreTx this (->mutable-kv-store)))

  kv/KvStoreTx
  (abort-kv-tx [_])

  (commit-kv-tx [_]
    (when-let [s (seq (kv/new-snapshot tx-kv-store))]
      (kv/store kv-store s))))

(deftype MutableKvStore [^NavigableMap db]
  kv/KvStore
  (new-snapshot ^java.io.Closeable [_]
    (->MutableKvSnapshot db))

  (store [_ kvs]
    (doseq [[k v] kvs]
      (.put db (mem/as-buffer k) (some-> v mem/as-buffer))))

  (fsync [_])
  (compact [_])
  (count-keys [_] (count db))
  (db-dir [_])
  (kv-name [this] (str (class this)))
  (begin-kv-tx [this] (->MutableKvStoreTx this (->mutable-kv-store))))

(defn ->mutable-kv-store
  ([] (->mutable-kv-store nil))
  ([_] (->MutableKvStore (TreeMap. mem/buffer-comparator))))

(comment
  (let [m (->mutable-kv-store)
        t (kv/begin-kv-tx m)
        t2 (kv/begin-kv-tx t)
        ]
    (kv/store t2 [[(byte-array [(byte 1)]) (byte-array [(byte 4)])]])
;;    (.getByte  (mem/as-buffer (kv/get-value (kv/new-snapshot t) (byte-array [(byte 1)]))) 0)

    (kv/commit-kv-tx t2)
    (kv/commit-kv-tx t)
    (kv/get-value (kv/new-snapshot m) (byte-array [(byte 1)]))
    )
  )
