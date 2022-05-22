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
           java.nio.file.attribute.FileAttribute
           (org.rocksdb BloomFilter BlockBasedTableConfig Checkpoint CompressionType FlushOptions LRUCache
                        Options ReadOptions RocksDB RocksIterator
                        DBOptions
                        ColumnFamilyOptions ColumnFamilyDescriptor ColumnFamilyHandle
                        WriteBatch WriteOptions Statistics StatsLevel)))

(set! *unchecked-math* :warn-on-boxed)

(comment (remove-ns 'xtdb.rocksdb))

(defn column-family-id ^Integer [^java.nio.ByteBuffer k]
  (.get k 0))

(defn wash-cfi ^Integer [k]
  (if (<= ^byte k 15) k 0))

(defn ^ColumnFamilyHandle ->column-family-handle [{:keys [^java.util.List column-family-handles]} ^Integer i]
  (let [cfh (.get column-family-handles i)]
    (when-not cfh
      (throw (IllegalStateException. "Should have a column family defined")))
    cfh))

(defn ^ColumnFamilyHandle ->column-family-handle2 [{:keys [^java.util.List column-family-handles]} k]
  (let [cfh (.get column-family-handles (wash-cfi k))]
    (when-not cfh
      (throw (IllegalStateException. "Should have a column family defined")))
    cfh))

(defn- iterator->key [^RocksIterator i ]
  (when (.isValid i)
    (mem/as-buffer (.key i))))

(defrecord RocksKvIterator [!i ^RocksDB db ^java.util.List column-family-handles ^ReadOptions read-options]
  kv/KvIterator
  (seek [this k]
    (when-not @!i
      (vreset! !i (.newIterator db (->column-family-handle2 this (first (mem/->on-heap k))) read-options)))
    (.seek ^RocksIterator @!i (mem/direct-byte-buffer k))
    (iterator->key ^RocksIterator @!i))

  (next [this]
    (.next ^RocksIterator @!i)
    (iterator->key ^RocksIterator @!i))

  (prev [this]
    (.prev ^RocksIterator @!i)
    (iterator->key ^RocksIterator @!i))

  (value [this]
    (mem/as-buffer (.value ^RocksIterator @!i)))

  Closeable
  (close [this]
    (when @!i
      (.close ^RocksIterator @!i))))

(defrecord RocksKvSnapshot [^RocksDB db ^ReadOptions read-options ^java.util.List column-family-handles snapshot]
  kv/KvSnapshot
  (new-iterator [this]
    (->RocksKvIterator (volatile! nil) db column-family-handles read-options))

  (get-value [this k]
    (some-> (.get db
                  (->column-family-handle2 this (first (mem/->on-heap k)))
                  read-options (mem/->on-heap k))
            (mem/as-buffer)))

  Closeable
  (close [_]
    (.close read-options)
    (.releaseSnapshot db snapshot)))

(defrecord RocksKv [^RocksDB db, ^WriteOptions write-options, ^Options options, ^Closeable metrics, ^Closeable cp-job, db-dir, column-family-handles]
  kv/KvStore
  (new-snapshot [_]
    (let [snapshot (.getSnapshot db)]
      (->RocksKvSnapshot db
                         (doto (ReadOptions.)
                           (.setSnapshot snapshot)
                           (.setPrefixSameAsStart true))
                         column-family-handles
                         snapshot)))

  (store [this kvs]
    (with-open [wb (WriteBatch.)]
      (doseq [[k v] kvs] ; avoid laziness?
        (if v
          (do
            (let [kbb (mem/direct-byte-buffer k) #_(.byteBuffer (mem/as-buffer k))
                  i (wash-cfi (column-family-id kbb))
                  cfh (->column-family-handle this i)]
              (.put wb
                    cfh
                    kbb (mem/direct-byte-buffer v) #_(.byteBuffer (mem/as-buffer v)))))
          (.remove wb (mem/direct-byte-buffer k) #_(.byteBuffer (mem/as-buffer k)))))
      (.write db write-options wb)))

  (compact [_]
    (.compactRange db))

  (fsync [_]
    #_(when (and (not (.sync write-options))
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

        cfo (.optimizeUniversalStyleCompaction (ColumnFamilyOptions.))
        _ (.setCompressionType cfo CompressionType/LZ4_COMPRESSION)
        _ (.setBottommostCompressionType cfo CompressionType/ZSTD_COMPRESSION)

        _ (when block-cache
            (.setTableFormatConfig cfo (doto (BlockBasedTableConfig.)
                                         (.setBlockCache block-cache))))

        bloom-filter (BloomFilter. 10 false)
        _ (.setTableFormatConfig cfo (doto (BlockBasedTableConfig.)
                                       (.setFilterPolicy bloom-filter)
                                      #_ (.setWholeKeyFiltering false)))


        ;;TODO use these only for entityasof index, and implement new entity-exists? index-store fn, rely on rocks' bloom filter but what about lmdb
                                        ;setCacheIndexAndFilterBlocksWithHighPriority
                                        ;setCacheIndexAndFilterBlocks

     ;   .setCacheIndexAndFilterBlocks(true)
     ;   .setPinL0FilterAndIndexBlocksInCache(true)
     ;   .setCacheIndexAndFilterBlocksWithHighPriority(true)
     ;   // default is binary search, but all of our scans are prefix based which is a good use
     ;;   // case for efficient hashing
     ;   .setIndexType(IndexType.kHashSearch)
     ;   .setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash)
     ;   .setDataBlockHashTableUtilRatio(0.5);

        _ (.useFixedLengthPrefixExtractor cfo 1)
        ;; also explore
        ;;  https://github.com/camunda/zeebe/issues/4002
        ;; kHashSearch and kDataBlockBinaryAndHash
        ;; .setMaxManifestFileSize(256 * 1024 * 1024L)
        ;; .setBytesPerSync(4 * 1024 * 1024L)

        ;; .memtablePrefixBloomSizeRatio 0.1

        ;; fixedlength = 41 for ave, aev !!
        ;; and likewise 21 for av ae
        ;; as-of probably would be 21 too

        ;; .setEnv(Env.getDefault().setBackgroundThreads(configuration.getBackgroundThreadCount()))

        ;; setMinWriteBufferNumberToMerge(int)

        ;; setMaxOpenFiles -1

        ;; setBloomLocality <6 (default number of probes)

        cfc 20
        column-family-descriptors;; [(ColumnFamilyDescriptor. RocksDB/DEFAULT_COLUMN_FAMILY cfo)]
        (into [(ColumnFamilyDescriptor. RocksDB/DEFAULT_COLUMN_FAMILY cfo)]
                                        (for [n (range cfc)]
                                          (ColumnFamilyDescriptor. (byte-array [(byte n)]))))

        column-family-handles (java.util.Vector. cfc)

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
                           column-family-handles)
             (catch Throwable t
               (.close opts)
               (throw t)))
        metrics (when metrics (metrics db stats))

        kv-store (map->RocksKv {:db-dir db-dir
                                :options opts
                                :db db
                                :metrics metrics
                                :write-options (doto (WriteOptions.)
                                                 (.setSync false #_(boolean sync?))
                                                 (.setDisableWAL true #_ (boolean disable-wal?)))
                                :column-family-handles column-family-handles})]
    (cond-> kv-store
      checkpointer (assoc :cp-job (cp/start checkpointer kv-store {::cp/cp-format cp-format})))))
