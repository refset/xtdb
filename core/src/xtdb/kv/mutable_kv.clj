(ns xtdb.kv.mutable-kv
  (:require [xtdb.kv :as kv]
            [xtdb.memory :as mem])
  (:import clojure.lang.ISeq
           java.io.Closeable
           [java.util NavigableMap TreeMap Vector]))

(deftype MutableKvIterator [^NavigableMap db, !tail-seq]
  kv/KvIterator
  (seek [this k]
    (some-> (reset! !tail-seq (->> (.tailMap db (mem/as-buffer k) true)
                                   (filter val)))
            first
            key))

  (next [this]
    (some-> (swap! !tail-seq rest) first key))

  (prev [this]
    (loop []
      (when-let [[[k _] :as tail-seq] (seq @!tail-seq)]
        (when-let [[k v :as lower-entry] (.lowerEntry db k)]
          (reset! !tail-seq (cons lower-entry tail-seq))
          (if v
            k
            (recur))))))

  (value [this]
    (some-> (first @!tail-seq) val))

  Closeable
  (close [_]))

(deftype MutableKvSnapshot [^NavigableMap db db-seq]
  kv/KvSnapshot
  (new-iterator [this] (->MutableKvIterator db (atom nil)))
  (get-value [this k] (get db (mem/as-buffer k)))

  ISeq
  (seq [_] (seq db-seq))

  Closeable
  (close [_]))

(deftype MutableKvStore [^NavigableMap db ^Vector db-seq]
  kv/KvStore
  (new-snapshot ^java.io.Closeable [this]
    (->MutableKvSnapshot db db-seq))

  (store [this kvs]
    (doseq [[k v] kvs]
      (.put db (mem/as-buffer k) (some-> v mem/as-buffer))
      (.add db-seq [(mem/as-buffer k) (some-> v mem/as-buffer)])))

  (fsync [this])
  (compact [this])
  (count-keys [this] (count db))
  (db-dir [this])
  (kv-name [this] (str (class this))))

(defn ->mutable-kv-store
  ([] (->mutable-kv-store nil nil))
  ([_ _] (->MutableKvStore (TreeMap. mem/buffer-comparator) (Vector.))))
