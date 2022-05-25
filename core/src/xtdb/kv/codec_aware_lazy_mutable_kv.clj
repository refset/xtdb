(ns xtdb.kv.codec-aware-lazy-mutable-kv
  (:require [xtdb.kv :as kv]
            [xtdb.codec :as c]
            [xtdb.kv.mutable-kv :as mutable-kv]
            [xtdb.kv.lazy-mutable-kv :as lazy-kv])
  (:import clojure.lang.ISeq
           java.io.Closeable
           org.agrona.DirectBuffer))

(deftype CodecAwareLazyMutableKvIterator [kv-snapshot as-of-kv-snapshot !i]
  kv/KvIterator
  (seek [_ k]
    (let [snapshot (condp = (.getByte ^DirectBuffer k 0)
                     c/entity+vt+tt+tx-id->content-hash-index-id
                     as-of-kv-snapshot
                     c/entity+z+tx-id->content-hash-index-id
                     as-of-kv-snapshot
                     kv-snapshot)]
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

(deftype CodecAwareLazyMutableKvSnapshot [kv-snapshot as-of-kv-snapshot]
  kv/KvSnapshot

  (new-iterator [_]
    (->CodecAwareLazyMutableKvIterator kv-snapshot as-of-kv-snapshot (atom nil)))

  (get-value [_ k]
    (kv/get-value kv-snapshot k))

  ISeq
  (seq [_] (seq kv-snapshot))

  Closeable
  (close [_]))

(deftype CodecAwareLazyKvStore [^xtdb.kv.KvStore kv ^xtdb.kv.KvStore as-of-kv]
  kv/KvStore
  (new-snapshot ^java.io.Closeable [_]
    (->CodecAwareLazyMutableKvSnapshot (kv/new-snapshot kv) (kv/new-snapshot as-of-kv)))

  (store [_ kvs]
    (kv/store kv kvs)
    (when (condp = (.getByte ^DirectBuffer (.key ^clojure.lang.MapEntry (first kvs)) 0)
            c/entity+vt+tt+tx-id->content-hash-index-id
            true
            c/entity+z+tx-id->content-hash-index-id
            true
            false)
      (kv/store as-of-kv kvs)))

  (fsync [_])
  (compact [_])
  (count-keys [_]
    (kv/count-keys kv))
  (db-dir [_])
  (kv-name [this] (str (class this))))

(defn ->codec-aware-lazy-mutable-kv-store []
  (->CodecAwareLazyKvStore (lazy-kv/->lazy-mutable-kv-store) (mutable-kv/->mutable-kv-store)))
