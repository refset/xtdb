(ns ^:no-doc xtdb.kv
  "Protocols for KV backend implementations."
  (:refer-clojure :exclude [next])
  (:require [xtdb.io :as xio]
            [xtdb.memory :as mem]
            [xtdb.status :as status]
            [xtdb.system :as sys])
  (:import java.io.Closeable))

(defprotocol KvIterator
  (seek [this k])
  (next [this])
  (prev [this])
  (value [this]))

(defprotocol KvSnapshot
  (new-iterator ^java.io.Closeable [this])
  (get-value [this k]))

(defprotocol KvStoreTx
  (abort-kv-store-tx [this])
  (commit-kv-store-tx [this]))

;; tag::KvStore[]
(defprotocol KvStore
  (new-snapshot ^java.io.Closeable [this])
  (store [this kvs])
  (fsync [this])
  (compact [this])
  (count-keys [this])
  (db-dir [this])
  (kv-name [this])
  (begin-kv-tx [this]))
;; end::KvStore[]

(def args
  {:db-dir {:doc "Directory to store K/V files"
            :required? false
            :spec ::sys/path}
   :sync? {:doc "Sync the KV store to disk after every write."
           :default false
           :spec ::sys/boolean}})

(extend-protocol status/Status
  xtdb.kv.KvStore
  (status-map [this]
    {:xtdb.kv/kv-store (kv-name this)
     :xtdb.kv/estimate-num-keys (count-keys this)
     :xtdb.kv/size (some-> (db-dir this) (xio/folder-size))}))

(deftype MergedKvIterator [db-iterator tx-db-iterator !db-iterated-last !last-seek-other-k]
  KvIterator
  (seek [_ k]
    (let [db-k (seek db-iterator k)
          tx-db-k (seek tx-db-iterator k)
          cmp (.compare mem/buffer-comparator db-k tx-db-k)]
      (cond
        (neg? cmp) (do
                     (reset! !db-iterated-last true)
                     (reset! !last-seek-other-k tx-db-k)
                     db-k)
        (zero? cmp) (do
                      (reset! !db-iterated-last false)
                      (reset! !last-seek-other-k db-k)
                      tx-db-k)
        :else (do
                (reset! !db-iterated-last false)
                (reset! !last-seek-other-k db-k)
                tx-db-k))))

  (next [_]
    (if @!db-iterated-last
      (let [db-next-k (next db-iterator)
            tx-db-k @!last-seek-other-k
            cmp (.compare mem/buffer-comparator db-next-k tx-db-k)]
        (cond
          (neg? cmp) (do
                       (reset! !db-iterated-last true)
                       db-next-k)
          (zero? cmp) (do
                        (reset! !db-iterated-last false)
                        (reset! !last-seek-other-k db-next-k)
                        tx-db-k)
          :else (do
                  (reset! !db-iterated-last false)
                  (reset! !last-seek-other-k db-next-k)
                  tx-db-k)))
      (let [tx-db-next-k (next tx-db-iterator)
            db-k @!last-seek-other-k
            cmp (.compare mem/buffer-comparator db-k tx-db-next-k)]
        (cond
          (neg? cmp) (do
                       (reset! !db-iterated-last true)
                       db-k)
          (zero? cmp) (do
                        (reset! !db-iterated-last false)
                        (reset! !last-seek-other-k db-k)
                        tx-db-next-k)
          :else (do
                  (reset! !db-iterated-last false)
                  (reset! !last-seek-other-k db-k)
                  tx-db-next-k)))))

  (prev [_]
    (if @!db-iterated-last
      (let [db-prev-k (prev db-iterator)
            tx-db-k @!last-seek-other-k
            cmp (.compare mem/buffer-comparator db-prev-k tx-db-k)]
        (cond
          (neg? cmp) (do
                       (reset! !db-iterated-last true)
                       db-prev-k)
          (zero? cmp) (do
                        (reset! !db-iterated-last false)
                        (reset! !last-seek-other-k db-prev-k)
                        tx-db-k)
          :else (do
                  (reset! !db-iterated-last false)
                  (reset! !last-seek-other-k db-prev-k)
                  tx-db-k)))
      (let [tx-db-prev-k (prev tx-db-iterator)
            db-k @!last-seek-other-k
            cmp (.compare mem/buffer-comparator db-k tx-db-prev-k)]
        (cond
          (neg? cmp) (do
                       (reset! !db-iterated-last true)
                       db-k)
          (zero? cmp) (do
                        (reset! !db-iterated-last false)
                        (reset! !last-seek-other-k db-k)
                        tx-db-prev-k)
          :else (do
                  (reset! !db-iterated-last false)
                  (reset! !last-seek-other-k db-k)
                  tx-db-prev-k)))))

  (value [_]
    (if @!db-iterated-last
      (value db-iterator)
      (value tx-db-iterator)))

  Closeable
  (close [_]))
