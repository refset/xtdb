(ns xtdb.kv.codec-aware-lazy-mutable-kv
  (:require [xtdb.kv :as kv]
            [xtdb.codec :as c]
            [xtdb.memory :as mem]
            [xtdb.kv.mutable-kv :as mutable-kv]
            [xtdb.kv.lazy-mutable-kv :as lazy-kv])
  (:import clojure.lang.ISeq
           java.io.Closeable))

(deftype CodecAwareLazyMutableKvIterator [^xtdb.kv.KvStore kv ^xtdb.kv.KvStore as-of-kv !i]
  kv/KvIterator
  (seek [_ k]
    (let [snapshot (if (#{c/entity+vt+tt+tx-id->content-hash-index-id
                          c/entity+z+tx-id->content-hash-index-id}
                        (aget (mem/->on-heap k) 0))
                     (kv/new-snapshot as-of-kv)
                     (kv/new-snapshot kv))]
      (reset! !i (kv/new-iterator snapshot))
      (kv/seek @!i k)))

  (next [_]
    (kv/next @!i))

  (prev [_]
    (kv/prev @!i))

  (value [_]
    (kv/value @!i))

  Closeable
  (close [_]))

(deftype CodecAwareLazyMutableKvSnapshot [^xtdb.kv.KvStore kv ^xtdb.kv.KvStore as-of-kv]
  kv/KvSnapshot

  (new-iterator [_]
    (->CodecAwareLazyMutableKvIterator kv as-of-kv (atom nil)))

  (get-value [_ k]
    (kv/get-value (kv/new-snapshot kv) k))

  ISeq
  (seq [_] (seq (kv/new-snapshot kv)))

  Closeable
  (close [_]))

(deftype CodecAwareLazyKvStore [^xtdb.kv.KvStore kv ^xtdb.kv.KvStore as-of-kv]
  kv/KvStore
  (new-snapshot ^java.io.Closeable [_]
    (->CodecAwareLazyMutableKvSnapshot kv as-of-kv))

  (store [_ kvs]
    (kv/store kv kvs)
    (kv/store as-of-kv
              (into []
                    (filter #(case (.get (mem/direct-byte-buffer (.key ^clojure.lang.MapEntry %)) 0)
                               c/entity+vt+tt+tx-id->content-hash-index-id
                               true
                               c/entity+z+tx-id->content-hash-index-id
                               true
                               false))
                    kvs)))

  (fsync [_])
  (compact [_])
  (count-keys [_]
    (kv/count-keys kv))
  (db-dir [_])
  (kv-name [this] (str (class this))))

(defn ->codec-aware-lazy-mutable-kv-store []
  (->CodecAwareLazyKvStore (lazy-kv/->lazy-mutable-kv-store) (mutable-kv/->mutable-kv-store)))

(comment
  (let [kv (->codec-aware-lazy-mutable-kv-store)]
    (kv/store kv [[(.getBytes "9sad1") (.getBytes "sadv")]
                  [(.getBytes "sad2") (.getBytes "sadx")]])
    (kv/get-value (kv/new-snapshot kv)(.getBytes "9sad1"))
    (kv/count-keys kv))

  )
