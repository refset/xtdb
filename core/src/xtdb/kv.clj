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
  (seek-to-last [this])
  (next [this])
  (prev [this])
  (value [this]))

(defprotocol KvSnapshot
  (new-iterator ^java.io.Closeable [this])
  (get-value [this k]))

(defprotocol KvStoreTx
  (abort-kv-tx [this])
  (commit-kv-tx [this]))

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

(defn compare-k [k1 k2]
  (if (and (some? k1) (some? k2))
    (.compare mem/buffer-comparator k1 k2)
    (if (some? k1)
      -1
      1)))

(deftype MergedKvIterator [db-iterator tx-db-iterator !db-iterated-last !last-seek-k !last-seek-other-k !prev-init-db !prev-init-db-tx]
  KvIterator
  (seek [_ k]
    (reset! !prev-init-db true)
    (reset! !prev-init-db-tx true)
    (let [db-k (seek db-iterator k)
          tx-db-k (seek tx-db-iterator k)
          cmp (compare-k db-k tx-db-k)]
      (cond
        (neg? cmp)
        (do
          (reset! !db-iterated-last true)
          (reset! !last-seek-k db-k)
          (reset! !last-seek-other-k tx-db-k)
          db-k)
        (zero? cmp)
        (do
          (reset! !db-iterated-last false)
          (reset! !last-seek-k tx-db-k)
          (reset! !last-seek-other-k (next db-iterator))
          tx-db-k)
        :else
        (do
          (reset! !db-iterated-last false)
          (reset! !last-seek-k tx-db-k)
          (reset! !last-seek-other-k db-k)
          tx-db-k))))

  (seek-to-last [_]
    (reset! !prev-init-db true)
    (reset! !prev-init-db-tx true)
    (let [db-k (seek-to-last db-iterator)
          tx-db-k (seek-to-last tx-db-iterator)
          cmp (compare-k tx-db-k db-k )]
      (cond
        (neg? cmp)
        (do
          (reset! !db-iterated-last true)
          (reset! !last-seek-k db-k)
          (reset! !last-seek-other-k tx-db-k)
          db-k)
        (zero? cmp)
        (do
          (reset! !db-iterated-last false)
          (reset! !last-seek-k tx-db-k)
          (reset! !last-seek-other-k (next db-iterator))
          tx-db-k)
        :else
        (do
          (reset! !db-iterated-last false)
          (reset! !last-seek-k tx-db-k)
          (reset! !last-seek-other-k db-k)
          tx-db-k))))

  (next [_]
    (if @!db-iterated-last
      (let [db-next-k (next db-iterator)
            tx-db-k @!last-seek-other-k
            cmp (compare-k db-next-k tx-db-k)]
        (cond
          (neg? cmp)
          db-next-k

          (zero? cmp)
          (do
            (reset! !db-iterated-last false)
            (reset! !last-seek-other-k (next db-iterator))
            tx-db-k)

          :else
          (do
            (reset! !db-iterated-last false)
            (reset! !last-seek-other-k db-next-k)
            tx-db-k)))
      (let [db-k @!last-seek-other-k
            tx-db-next-k (next tx-db-iterator)
            cmp (compare-k db-k tx-db-next-k)]
        (cond
          (neg? cmp)
          (do
            (reset! !db-iterated-last true)
            (reset! !last-seek-other-k tx-db-next-k)
            db-k)

          (zero? cmp)
          (do
            (reset! !last-seek-other-k (next db-iterator))
            tx-db-next-k)

          :else
          (do
            (reset! !last-seek-other-k db-k)
            tx-db-next-k)))))

  (prev [_]
    (if @!db-iterated-last
      (let [db-prev-k (prev db-iterator)
            tx-db-k
            (or @!last-seek-other-k
                (seek-to-last tx-db-iterator))
            #_(or (when @!prev-init-db-tx
                          (or
                           (seek db-iterator db-prev-k)
                           (seek-to-last tx-db-iterator)))
                        @!last-seek-other-k)
            #_(or @!last-seek-other-k
                        (when @!prev-init-db-tx
                          (seek-to-last tx-db-iterator)))]
        (reset! !prev-init-db-tx false)
        (when (or (some? db-prev-k) (some? tx-db-k))
          (let [cmp (compare-k db-prev-k tx-db-k)]
            (cond
              (pos? cmp)
              (do
                (prn true :neg)
                db-prev-k)

              (zero? cmp)
              (do
                (prn true :zero)
                (reset! !db-iterated-last false)
                (reset! !last-seek-other-k (prev db-iterator))
                tx-db-k)

              :else
              (do
                (prn true :pos)
                (reset! !db-iterated-last false)
                (reset! !last-seek-other-k db-prev-k)
                tx-db-k)))))
      (let [tx-db-prev-k (prev tx-db-iterator)
            db-k
            (or
             @!last-seek-other-k
             (seek-to-last db-iterator))
            #_(or (if @!prev-init-db
                       (seek db-iterator tx-db-prev-k)
                       (seek-to-last db-iterator))
                       @!last-seek-other-k)
            #_(or @!last-seek-other-k
                (when @!prev-init-db-tx
                  (seek-to-last tx-db-iterator)))]
        (reset! !prev-init-db false)
        (cond (and (some? tx-db-prev-k) (some? db-k))
          (let [cmp (compare-k db-k tx-db-prev-k)]
            (cond
              (pos? cmp)
              (do
                (prn false :neg)
                (reset! !db-iterated-last true)
                (reset! !last-seek-other-k tx-db-prev-k)
                db-k)

              (zero? cmp)
              (do
                (prn false :zero)
                (reset! !last-seek-other-k (prev db-iterator))
                tx-db-prev-k)

              :else
              (do
                (prn false :pos db-k tx-db-prev-k)
                (reset! !last-seek-other-k db-k)
                tx-db-prev-k)))))))

  (value [_]
    (if @!db-iterated-last
      (value db-iterator)
      (value tx-db-iterator)))

  Closeable
  (close [_]))
