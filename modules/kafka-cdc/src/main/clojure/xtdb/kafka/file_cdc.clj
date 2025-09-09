(ns xtdb.kafka.file-cdc
  (:require [xtdb.api :as xt]
            [xtdb.db-catalog :as db]
            [xtdb.node :as xtn]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import [xtdb.api Xtdb$Config]
           [xtdb.kafka.cdc FileCdcFactory FileCdcModule]))

(defmethod xtn/apply-config! :xtdb.kafka/file-cdc
  [^Xtdb$Config config _ {:keys [output-directory
                                 pretty-print?
                                 lag-threshold
                                 enabled?]
                          :or {output-directory "live-cdc-output"
                               pretty-print? true
                               lag-threshold "PT10S"
                               enabled? true}}]
  (when enabled?
    (let [factory (doto (FileCdcFactory.)
                    (.setOutputDirectory output-directory)
                    (.setPrettyPrint pretty-print?)
                    (.setLagThreshold (time/->duration lag-threshold))
                    (.setEnabled enabled?))]
      ;; Register as module
      (.module config "file-cdc" factory)
      ;; Also store factory for database startup hooks
      (.put (.getExtensions config) ::file-cdc-factory factory))))

(defn start-file-cdc-processor
  "Starts the file-based CDC processor for a database if file CDC is configured"
  [config db allocator meter-registry]
  (when-let [factory (get (.getExtensions config) ::file-cdc-factory)]
    (let [module (.openModule factory nil)]
      (when (instance? FileCdcModule module)
        (.startCdcProcessor module db allocator meter-registry))
      module)))

;; Configuration example:
(comment
  ;; In your XTDB configuration:
  {:xtdb/node
   {...}
   
   :xtdb.kafka/file-cdc
   {:output-directory "live-cdc-output"    ;; Directory to write CDC files
    :pretty-print? true                    ;; Format JSON nicely
    :lag-threshold "PT30S"                 ;; 30 seconds lag threshold
    :enabled? true}})