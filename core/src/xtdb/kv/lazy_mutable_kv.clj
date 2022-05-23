(ns xtdb.kv.lazy-mutable-kv
  (:require [xtdb.kv :as kv]
            [xtdb.memory :as mem]
            [xtdb.kv.mutable-kv :as mutable-kv])
  (:import clojure.lang.ISeq
           java.io.Closeable
           [java.util Vector]))

(defn- sync! [^xtdb.kv.KvStore kv offset ^Vector kvv]
  (let [size (.size kvv)]
    (kv/store kv (.subList kvv @offset size))
    (reset! offset size)))

(deftype LazyMutableKvSnapshot [^xtdb.kv.KvStore kv offset ^Vector kvv]
  kv/KvSnapshot

  (new-iterator [_]
    (sync! kv offset kvv)
    (kv/new-iterator (kv/new-snapshot kv)))

  (get-value [_ k]
    (sync! kv offset kvv)
    (kv/get-value (kv/new-snapshot kv) k))

  ISeq
  (seq [_] (seq kvv))

  Closeable
  (close [_]))

(deftype LazyKvStore [^xtdb.kv.KvStore kv offset ^Vector kvv]
  kv/KvStore
  (new-snapshot ^java.io.Closeable [_]
    (->LazyMutableKvSnapshot kv offset kvv))

  (store [_ kvs]
    (doseq [^clojure.lang.MapEntry e kvs]
      (.add kvv e;[(mem/as-buffer (.key e)) (.val e)]
            )))

  (fsync [_])
  (compact [_])
  (count-keys [_]
    (sync! kv offset kvv)
    (kv/count-keys kv))
  (db-dir [_])
  (kv-name [this] (str (class this))))

(defn ->lazy-mutable-kv-store []
  (->LazyKvStore (mutable-kv/->mutable-kv-store) (atom 0) (Vector.)))
