(ns xtdb.kafka.cdc
  (:require [xtdb.api :as xt]
            [xtdb.db-catalog :as db]
            [xtdb.node :as xtn]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import [xtdb.api Xtdb$Config]
           [xtdb.kafka.cdc CdcFactory CdcModule]))

(defmethod xtn/apply-config! ::cdc
  [^Xtdb$Config config _ [cdc-alias {:keys [bootstrap-servers 
                                             schema-registry-url
                                             topic-prefix
                                             node-name
                                             lag-threshold
                                             use-json-string-approach?
                                             enabled?
                                             producer-properties]
                                      :or {topic-prefix "xtdb.cdc."
                                           lag-threshold "PT10S"
                                           use-json-string-approach? true
                                           enabled? true}}]]
  (when enabled?
    (let [factory (doto (CdcFactory.)
                    (.setBootstrapServers bootstrap-servers)
                    (.setSchemaRegistryUrl schema-registry-url)
                    (.setTopicPrefix topic-prefix)
                    (.setNodeName (or node-name (str (symbol cdc-alias))))
                    (.setLagThreshold (time/->duration lag-threshold))
                    (.setUseJsonStringApproach use-json-string-approach?)
                    (.setEnabled enabled?))]
      (when producer-properties
        (.setProducerProperties factory (util/->properties producer-properties)))
      ;; Store the factory in the config extensions
      (.put (.getExtensions config) ::cdc-factory factory))))

(defn start-cdc-processor
  "Starts the CDC processor for a database if CDC is configured"
  [config db allocator meter-registry]
  (when-let [factory (get (.getExtensions config) ::cdc-factory)]
    (let [module (.openModule factory nil)]
      (when (instance? CdcModule module)
        (.startCdcProcessor module db allocator meter-registry))
      module)))

;; Configuration example:
(comment
  ;; In your XTDB configuration:
  {:xtdb/node
   {...}
   
   :xtdb.kafka/cdc
   {:bootstrap-servers "localhost:9092"
    :schema-registry-url "http://localhost:8081"
    :topic-prefix "xtdb.cdc."
    :node-name "xtdb-node-1"
    :lag-threshold "PT30S" ;; 30 seconds
    :use-json-string-approach? true  ;; Use JSON strings (recommended)
    :enabled? true
    :producer-properties {"compression.type" "snappy"
                          "batch.size" "16384"}}})