(ns xtdb.bench.fusion-server
  (:require [clojure.tools.logging :as log]
            [xtdb.bench :as b]
            [xtdb.bench.fusion-bespoke :as bespoke]
            [xtdb.compactor :as compactor]
            [xtdb.db-catalog :as db]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.trie-catalog :as cat]
            [xtdb.util :as util])
  (:import (xtdb.table TableRef)))

(defn start-fusion-server
  [{:keys [node-dir pg-port devices readings]
    :or {node-dir "/tmp/fusion-server"
         pg-port 5433
         devices 1000
         readings 1000}}]
  (let [node-path (util/->path node-dir)]
    (util/delete-dir node-path)
    (log/infof "Starting XTDB node at %s with pgwire on port %d" node-dir pg-port)
    (let [node (xtn/start-node
                {:server {:port pg-port}
                 :healthz {:port 8081 :host "*"}
                 :log [:local {:path (.resolve node-path "log")}]
                 :storage [:local {:path (.resolve node-path "objects")}]})]
      (try
        (binding [tu/*allocator* (util/component node :xtdb/allocator)]
          (let [benchmark (b/->benchmark :fusion {:devices devices
                                                  :readings readings
                                                  :batch-size 500
                                                  :update-batch-size 100
                                                  :updates-per-system 5
                                                  :staged-only? true})
                benchmark-fn (b/compile-benchmark benchmark)]
            (log/info "Running fusion benchmark ingestion...")
            (benchmark-fn node)

            (log/info "Running compaction...")
            (compactor/compact-all! node nil)
            (log/info "Compaction complete")

            (doseq [tn bespoke/table-names]
              (let [table (TableRef. "xtdb" "public" tn)
                    tc (.getTrieCatalog (db/primary-db node))
                    live (cat/current-tries (cat/trie-state tc table))]
                (log/infof "  %s: %d files" tn (count live))))

            (log/infof "XTDB ready. pgwire on port %d, storage at %s/objects" pg-port node-dir)
            (log/info "Run: ./snapshot_duckdb --storage-root /tmp/fusion-server/objects")
            (log/info "Press Ctrl+C to stop.")

            (let [latch (java.util.concurrent.CountDownLatch. 1)]
              (.addShutdownHook (Runtime/getRuntime)
                                (Thread. (fn [] (.countDown latch))))
              (.await latch))))
        (finally
          (.close node))))))

(defn -main [& _args]
  (start-fusion-server {}))
