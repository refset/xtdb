(ns xtdb.metrics
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.node :as xtn])
  (:import (io.micrometer.core.instrument Counter Gauge MeterRegistry Tag Timer Timer$Sample)
           (io.micrometer.core.instrument.binder MeterBinder)
           (io.micrometer.core.instrument.binder.jvm ClassLoaderMetrics JvmGcMetrics JvmHeapPressureMetrics JvmMemoryMetrics JvmThreadMetrics)
           (io.micrometer.core.instrument.binder.system ProcessorMetrics)
           io.micrometer.core.instrument.composite.CompositeMeterRegistry
           java.util.List
           (java.util.stream Stream)
           (org.apache.arrow.memory BufferAllocator)
           (xtdb.cache Stats MemoryCacheStats)))

(defn add-counter
  ([reg name] (add-counter reg name {}))
  ([reg name {:keys [description]}]
   (cond-> (Counter/builder name)
     description (.description description)
     :always (.register reg))))

(def percentiles [0.75 0.85 0.95 0.98 0.99 0.999])

(defn add-timer [reg name {:keys [^String description]}]
  (cond-> (.. (Timer/builder name)
              (publishPercentiles (double-array percentiles)))
    description (.description description)
    :always (.register reg)))

(defmacro wrap-query [q timer]
  `(let [^Timer$Sample sample# (Timer/start)
         ^Stream stream# ~q]
     (.onClose stream# (fn [] (.stop sample# ~timer)))))

(defn add-gauge
  ([reg meter-name f] (add-gauge reg meter-name f {}))
  ([^MeterRegistry reg meter-name f {:keys [unit tag]}]
   (let [[tag-key tag-value] tag]
     (-> (Gauge/builder meter-name f)
         (cond->
             unit (.baseUnit (str unit))
             tag (.tag tag-key tag-value))
         (.register reg)))))

(defn add-allocator-gauge [reg meter-name ^BufferAllocator allocator]
  (add-gauge reg meter-name (fn [] (.getAllocatedMemory allocator)) {:unit "bytes"}))

(defn add-cache-gauges [reg meter-name get-stats]
  (doto reg
    (add-gauge (str meter-name ".pinnedBytes")
               #(.getPinnedBytes ^Stats (get-stats))
               {:unit "bytes"})
    (add-gauge (str meter-name ".evictableBytes")
               #(.getEvictableBytes ^Stats (get-stats))
               {:unit "bytes"})
    (add-gauge (str meter-name ".freeBytes")
               #(.getFreeBytes ^Stats (get-stats))
               {:unit "bytes"})))

(defn add-mem-cache-gauges [reg meter-name get-stats]
  (add-cache-gauges reg meter-name get-stats)
  (doto reg
    (add-gauge (str meter-name ".metaSliceCount")
               #(.getSliceCount (.getMetaStats ^MemoryCacheStats (get-stats)))
               {:tag ["type" "meta"]})
    (add-gauge (str meter-name ".dataSliceCount")
               #(.getSliceCount (.getDataStats ^MemoryCacheStats (get-stats)))
               {:tag ["type" "data"]})
    (add-gauge (str meter-name ".metaWeightBytes")
               #(.getWeightBytes (.getMetaStats ^MemoryCacheStats (get-stats)))
               {:unit "bytes" :tag ["type" "meta"]})
    (add-gauge (str meter-name ".dataWeightBytes")
               #(.getWeightBytes (.getDataStats ^MemoryCacheStats (get-stats)))
               {:unit "bytes" :tag ["type" "data"]})
    (add-gauge (str meter-name ".metaPinned")
               #(.getPinned (.getMetaStats ^MemoryCacheStats (get-stats)))
               {:tag ["type" "meta"]})
    (add-gauge (str meter-name ".dataPinned")
               #(.getPinned (.getDataStats ^MemoryCacheStats (get-stats)))
               {:tag ["type" "data"]})
    (add-gauge (str meter-name ".metaUnpinned")
               #(.getUnpinned (.getMetaStats ^MemoryCacheStats (get-stats)))
               {:tag ["type" "meta"]})
    (add-gauge (str meter-name ".dataUnpinned")
               #(.getUnpinned (.getDataStats ^MemoryCacheStats (get-stats)))
               {:tag ["type" "data"]})))

(defn random-node-id []
  (format "xtdb-node-%1s" (subs (str (random-uuid)) 0 6)))

(defmethod ig/init-key :xtdb.metrics/registry [_ _]
  (let [reg (CompositeMeterRegistry.)]

    ;; Add common tag for the node
    (let [node-id (or (System/getenv "XTDB_NODE_ID") (random-node-id))
          ^List tags [(Tag/of "node-id" node-id)]]
      (log/infof "tagging all metrics with node-id: %s" node-id)
      (-> (.config reg)
          (.commonTags tags)))

    (doseq [^MeterBinder metric [(ClassLoaderMetrics.) (JvmMemoryMetrics.) (JvmHeapPressureMetrics.)
                                 (JvmGcMetrics.) (ProcessorMetrics.) (JvmThreadMetrics.)]]
      (.bindTo metric reg))

    reg))

(defmethod xtn/apply-config! ::cloudwatch [config _k v]
  (xtn/apply-config! config :xtdb.aws.cloudwatch/metrics v))

(defmethod xtn/apply-config! ::azure-monitor [config _k v]
  (xtn/apply-config! config :xtdb.azure.monitor/metrics v))
