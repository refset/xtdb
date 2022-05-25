(ns ^:no-doc xtdb.rocksdb
  "RocksDB KV backend for XTDB."
  (:require [clojure.tools.logging :as log]
            [xtdb.kv :as kv]
            [xtdb.rocksdb.loader]
            [xtdb.memory :as mem]
            [xtdb.system :as sys]
            [xtdb.io :as xio]
            [xtdb.checkpoint :as cp]
            [xtdb.kv.index-store :as kvi]
            [xtdb.codec :as c])
  (:import (java.io Closeable File)
           (java.nio.file Files Path)
           java.util.Map
           java.util.List
           java.nio.file.attribute.FileAttribute
           (org.rocksdb BlockBasedTableConfig Checkpoint CompressionType FlushOptions LRUCache BloomFilter
                        Options ReadOptions RocksDB RocksIterator
                        DBOptions
                        ColumnFamilyOptions ColumnFamilyDescriptor ColumnFamilyHandle
                        WriteBatch WriteOptions Statistics StatsLevel)))

(set! *unchecked-math* :warn-on-boxed)

(def ^:const column-family-defs [c/entity+vt+tt+tx-id->content-hash-index-id
                                   c/entity+z+tx-id->content-hash-index-id])

(defn ^ColumnFamilyHandle ->column-family-handle [{:keys [^Map column-family-handles
                                                          ^ColumnFamilyHandle default-column-family]} k]
  (get column-family-handles k default-column-family))

(defn- iterator->key [^RocksIterator i ]
  (when (.isValid i)
    (mem/as-buffer (.key i))))

(defrecord RocksKvIterator [i ^RocksDB db ^List column-family-handles, ^ColumnFamilyHandle default-column-family, ^ReadOptions read-options]
  kv/KvIterator
  (seek [this k]
    (when-not @i
      (reset! i (.newIterator db (->column-family-handle this (.getByte ^org.agrona.DirectBuffer k 0)) read-options)))
    (.seek ^RocksIterator @i (mem/direct-byte-buffer k))
    (iterator->key ^RocksIterator @i))

  (next [this]
    (.next ^RocksIterator @i)
    (iterator->key ^RocksIterator @i))

  (prev [this]
    (.prev ^RocksIterator @i)
    (iterator->key ^RocksIterator @i))

  (value [this]
    (mem/as-buffer (.value ^RocksIterator @i)))

  Closeable
  (close [this]
    (when @i
      (.close ^RocksIterator @i))))

(defrecord RocksKvSnapshot [^RocksDB db, ^ReadOptions read-options, ^List column-family-handles, ^ColumnFamilyHandle default-column-family, snapshot]
  kv/KvSnapshot
  (new-iterator [this]
    (->RocksKvIterator (atom nil) db column-family-handles default-column-family read-options))

  (get-value [this k]
    (some-> (.get db
                  (->column-family-handle this (.getByte ^org.agrona.DirectBuffer k 0))
                  read-options (mem/->on-heap k))
            (mem/as-buffer)))

  Closeable
  (close [_]
    (.close read-options)
    (.releaseSnapshot db snapshot)))

(defrecord RocksKv [^RocksDB db, ^WriteOptions write-options, ^Options options, ^Closeable metrics, ^Closeable cp-job, db-dir, ^Map column-family-handles, ^ColumnFamilyHandle default-column-family]
  kv/KvStore
  (new-snapshot [_]
    (let [snapshot (.getSnapshot db)]
      (->RocksKvSnapshot db
                         (doto (ReadOptions.)
                           (.setSnapshot snapshot))
                         column-family-handles
                         default-column-family
                         snapshot)))

  (store [this kvs]
    (with-open [wb (WriteBatch.)]
      (doseq [[k v] kvs]
        (if v
          (do
            (let [kbb (mem/direct-byte-buffer k)
                  cfh (->column-family-handle this (.get kbb 0))]
              (.put wb
                    cfh
                    kbb (mem/direct-byte-buffer v))))
          (.remove wb (mem/direct-byte-buffer k))))
      (.write db write-options wb)))

  (compact [_]
    (.compactRange db))

  (fsync [_]
    (when (and (not (.sync write-options))
               (.disableWAL write-options))
      (with-open [flush-options (doto (FlushOptions.)
                                  (.setWaitForFlush true))]
        (.flush db flush-options))))

  (count-keys [_]
    (-> (.getProperty db "rocksdb.estimate-num-keys")
        (Long/parseLong)))

  (db-dir [_]
    (str db-dir))

  (kv-name [this]
    (.getName (class this)))

  cp/CheckpointSource
  (save-checkpoint [this dir]
    (xio/delete-dir dir)
    (let [tx (kvi/latest-completed-tx this)]
      (with-open [checkpoint (Checkpoint/create db)]
        (.createCheckpoint checkpoint (.getAbsolutePath ^File dir)))
      {:tx tx}))

  Closeable
  (close [_]
    (xio/try-close db)
    (xio/try-close options)
    (xio/try-close write-options)
    (xio/try-close metrics)
    (xio/try-close cp-job)))

(def ^:private cp-format {:index-version c/index-version, ::version "6"})

(defn ->lru-block-cache {::sys/args {:cache-size {:doc "Cache size"
                                                  :default (* 8 1024 1024)
                                                  :spec ::sys/nat-int}}}
  [{:keys [cache-size]}]
  (LRUCache. cache-size))

(defn ->kv-store {::sys/deps {:metrics (fn [_])
                              :checkpointer (fn [_])
                              :block-cache `->lru-block-cache}
                  ::sys/args {:db-dir {:doc "Directory to store K/V files"
                                       :required? true
                                       :spec ::sys/path}
                              :sync? {:doc "Sync the KV store to disk after every write."
                                      :default false
                                      :spec ::sys/boolean}
                              :db-options {:doc "RocksDB Options"
                                           :spec #(instance? Options %)}
                              :disable-wal? {:doc "Disable Write Ahead Log"
                                             :default false
                                             :spec ::sys/boolean}}}
  [{:keys [^Path db-dir sync? disable-wal? metrics checkpointer ^Options db-options block-cache] :as options}]

  (RocksDB/loadLibrary)

  (when (and (nil? @xio/malloc-arena-max)
             (xio/glibc?))
    #_{:clj-kondo/ignore [:inline-def]}
    (defonce warn-once-on-malloc-arena-max
      (log/warn "MALLOC_ARENA_MAX not set, memory usage might be high, recommended setting for XTDB is 2")))

  (when checkpointer
    (cp/try-restore checkpointer (.toFile db-dir) cp-format))

  (let [ ;; https://github.com/facebook/rocksdb/blob/master/java/src/main/java/org/rocksdb/RocksDB.java
        ;; https://stackoverflow.com/questions/52504792/how-to-refer-to-a-particular-column-family-in-put-and-get-in-rocksdb-with-java-c

        ;; final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        ;;                 new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
        ;;                 new ColumnFamilyDescriptor("my-first-columnfamily".getBytes(), cfOpts)
        ;;         );

        default-cfo (doto (ColumnFamilyOptions.)
                      (.optimizeUniversalStyleCompaction )
                      (.setCompressionType CompressionType/LZ4_COMPRESSION)
                      (.setBottommostCompressionType CompressionType/ZSTD_COMPRESSION))

        _ (when block-cache
            (.setTableFormatConfig default-cfo (doto (BlockBasedTableConfig.)
                                                 (.setBlockCache block-cache))))

        cfo (doto (ColumnFamilyOptions.)
              (.optimizeUniversalStyleCompaction )
              (.setCompressionType CompressionType/LZ4_COMPRESSION)
              (.setBottommostCompressionType CompressionType/ZSTD_COMPRESSION))

        bloom-filter (BloomFilter. 10 false)
        _ (when block-cache
            (.setTableFormatConfig cfo (doto (BlockBasedTableConfig.)
                                         (.setBlockCache block-cache)
                                         ;; (.setFilterPolicy bloom-filter)
                                         ;; (.setWholeKeyFiltering false)
                                         ;; (.setCacheIndexAndFilterBlocks true)
                                         ;; (.setPinL0FilterAndIndexBlocksInCache true)
                                         ;; (.setCacheIndexAndFilterBlocksWithHighPriority true)
                                         )))

        ;; _ (.useFixedLengthPrefixExtractor cfo 21)
        ;; _ (.setMemtablePrefixBloomSizeRatio cfo 0.1)

        column-family-descriptors
        (into [(ColumnFamilyDescriptor. RocksDB/DEFAULT_COLUMN_FAMILY default-cfo)]
              (for [c column-family-defs]
                (ColumnFamilyDescriptor. (byte-array [(byte c)]))))

        column-family-handles-vector (java.util.Vector.)

        stats (when metrics (doto (Statistics.) (.setStatsLevel (StatsLevel/EXCEPT_DETAILED_TIMERS))))
        opts (doto (or ^DBOptions db-options (DBOptions.))
               (cond-> metrics (.setStatistics stats))
               (.setCreateIfMissing true)
               (.setCreateMissingColumnFamilies true))

        db (try
             (RocksDB/open opts (-> (Files/createDirectories db-dir (make-array FileAttribute 0))
                                    (.toAbsolutePath)
                                    (str))
                           column-family-descriptors
                           column-family-handles-vector)
             (catch Throwable t
               (.close opts)
               (throw t)))
        metrics (when metrics (metrics db stats))

        column-family-handles (into {}
                                    (for [[^int i cfd] (map-indexed vector column-family-defs)]
                                      [cfd (.get column-family-handles-vector (inc i))]))

        kv-store (map->RocksKv {:db-dir db-dir
                                :options opts
                                :db db
                                :metrics metrics
                                :write-options (doto (WriteOptions.)
                                                 (.setSync (boolean sync?))
                                                 (.setDisableWAL (boolean disable-wal?)))
                                :column-family-handles column-family-handles
                                :default-column-family (first column-family-handles-vector)})]
    (cond-> kv-store
      checkpointer (assoc :cp-job (cp/start checkpointer kv-store {::cp/cp-format cp-format})))))
