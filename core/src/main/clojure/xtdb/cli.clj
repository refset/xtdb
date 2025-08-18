(ns ^:no-doc xtdb.cli
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [xtdb.compactor.reset :as cr]
            [xtdb.error :as err]
            [xtdb.cli-help :as cli-help]
            [xtdb.logging :as logging]
            [xtdb.node :as xtn]
            [xtdb.pgwire :as pgw]
            [xtdb.util :as util])
  (:import java.io.File))

(defn- if-it-exists [^File f]
  (when (.exists f)
    f))

(defn read-env-var [env-var]
  (System/getenv (str env-var)))

(defn edn-read-string [edn-string]
  (edn/read-string {:readers {'env read-env-var}} edn-string))

(defn edn-file->config-opts
  [^File f]
  (if (.exists f)
    (edn-read-string (slurp f))
    (throw (err/incorrect :opts-file-not-found (format "File not found: '%s'" (.getName f))))))

(defn parse-args [args cli-spec]
  (let [{:keys [errors options summary] :as res} (cli/parse-opts args cli-spec)]
    (cond
      errors [:error errors]
      (:help options) [:help summary]
      :else [:success res])))

(defn- handling-arg-errors-or-help [[tag arg]]
  (case tag
    :error (binding [*out* *err*]
             (doseq [error arg]
               (println error))
             (System/exit 2))

    :help (do
            (println arg)
            (System/exit 0))

    :success arg))

(defn file->node-opts [file]
  (if-let [^File file (or file
                          (some-> (io/file "xtdb.yaml") if-it-exists)
                          (some-> (io/resource "xtdb.yaml") (io/file))
                          (some-> (io/file "xtdb.yml") if-it-exists)
                          (some-> (io/resource "xtdb.yml") (io/file))
                          (some-> (io/file "xtdb.edn") if-it-exists)
                          (some-> (io/resource "xtdb.edn") (io/file)))]
    (case (some-> file util/file-extension)
      ("yaml" "yml") file
      "edn" (edn-file->config-opts file)
      (throw (err/incorrect :config-file-not-found (format "File not found: '%s'" (.getName file)))))

    {}))

(defn- shutdown-hook-promise
  "NOTE: Don't register this until the node manages to start up cleanly, so that ctrl-c keeps working as expected in case the node fails to start. "
  []
  (let [main-thread (Thread/currentThread)
        !shutdown? (promise)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn []
                                 (let [shutdown-ms 10000]
                                   (deliver !shutdown? true)
                                   (.join main-thread shutdown-ms)
                                   (if (.isAlive main-thread)
                                     (do
                                       (log/warn "could not stop node cleanly after" shutdown-ms "ms, forcing exit")
                                       (.halt (Runtime/getRuntime) 1))

                                     (log/info "Node stopped."))))
                               "xtdb.shutdown-hook-thread"))
    !shutdown?))

(defn- start-profiler! []
  "Start clj-async-profiler if available"
  (try
    (let [prof (requiring-resolve 'clj-async-profiler.core/start)]
      (prof {})
      (println "🔥 Profiler started - will run for 60 seconds then serve results at http://localhost:8080")
      true)
    (catch Exception e
      (println "❌ Profiler not available:" (.getMessage e))
      false)))

(defn- stop-and-serve-profiler! []
  "Stop profiler and serve results"
  (try
    (let [stop (requiring-resolve 'clj-async-profiler.core/stop)
          serve (requiring-resolve 'clj-async-profiler.core/serve-ui)]
      (stop)
      (println "🔥 Profiler stopped, generating flame graph...")
      (serve 8080)
      (println "🌐 Flame graph available at: http://localhost:8080")
      (println "Press Ctrl+C to exit"))
    (catch Exception e
      (println "❌ Error stopping profiler:" (.getMessage e)))))

(defn- start-with-profiling [start-fn args]
  "Start node with profiling for 60 seconds"
  (if (start-profiler!)
    (do
      ;; Start profiler timer
      (future
        (Thread/sleep 60000) ; 60 seconds
        (stop-and-serve-profiler!))
      
      ;; Start the node normally
      (start-fn args))
    
    ;; Fall back to normal startup if profiler unavailable
    (start-fn args)))

(defn- check-for-profiling! [args]
  "Check args for --profile flag and start profiler immediately if present"
  (when (some #{"--profile"} args)
    (start-profiler!)))

(def config-file-opt
  ["-f" "--file CONFIG_FILE" "Config file to load XTDB options from - EDN, YAML"
   :id :file
   :parse-fn io/file
   :validate [if-it-exists "Config file doesn't exist"
              #(contains? #{"edn" "yaml"} (util/file-extension %)) "Config file must be .edn or .yaml"]])

(def compactor-cli-spec
  [config-file-opt
   ["-h" "--help"]])

(defn- start-compactor [args]
  (let [{{:keys [file]} :options} (-> (parse-args args compactor-cli-spec)
                                      (handling-arg-errors-or-help))]
    (log/info "Starting in compact-only mode...")

    (util/with-open [_node (xtn/start-compactor (file->node-opts file))]
      (log/info "Compactor started")
      @(shutdown-hook-promise))))

(def playground-cli-spec
  [["-p" "--port PORT"
    :id :port
    :parse-fn parse-long
    :default 5432]

   ["--profile" "Profile startup for 60s then serve flame graph at http://localhost:8080"]
   ["-h" "--help"]])

(defn- start-playground [args]
  (let [{{:keys [port profile]} :options} (-> (parse-args args playground-cli-spec)
                                              (handling-arg-errors-or-help))]
    (log/info "Starting in playground mode...")
    
    ;; Set up profiler shutdown timer if profiling (check raw args since profiler started early)
    (when (some #{"--profile"} args)
      (future
        (Thread/sleep 60000) ; 60 seconds
        (stop-and-serve-profiler!)))
    
    (util/with-open [_node (pgw/open-playground {:port port})]
      @(shutdown-hook-promise))))

(def node-cli-spec
  [config-file-opt
   ["-p" "--profile" "Profile startup for 60s then serve flame graph at http://localhost:8080"]
   ["-h" "--help"]])

(defn- start-node [args]
  (let [{{:keys [file profile]} :options} (-> (parse-args args node-cli-spec)
                                              (handling-arg-errors-or-help))]
    ;; Set up profiler shutdown timer if profiling (check raw args since profiler started early)
    (when (some #{"--profile"} args)
      (future
        (Thread/sleep 60000) ; 60 seconds
        (stop-and-serve-profiler!)))
    
    (util/with-open [_node (xtn/start-node (file->node-opts file))]
      @(shutdown-hook-promise))))

(def reset-compactor-cli-spec
  [config-file-opt
   [nil "--dry-run"
    "Lists files that would be deleted, without actually deleting them"
    :id :dry-run?]
   ["-h" "--help"]])

(defn- reset-compactor! [args]
  (let [{{:keys [dry-run? file]} :options, [db-name] :arguments} (-> (parse-args args reset-compactor-cli-spec)
                                                                     (handling-arg-errors-or-help))]
    (when (nil? db-name)
      (binding [*out* *err*]
        (println "Missing db-name: `reset-compactor <db-name> [opts]`")
        (System/exit 2)))
    (cr/reset-compactor! (file->node-opts file) db-name {:dry-run? dry-run?})))


#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn start-node-from-command-line [[cmd & more-args :as args]]
  ;; Profiler is already started by xtdb.main if needed
  
  (util/install-uncaught-exception-handler!)

  (logging/set-from-env! (System/getenv))

  (try
    (case cmd
      "compactor" (start-compactor more-args)
      "playground" (start-playground more-args)
      "node" (start-node more-args)

      "reset-compactor" (do
                          (reset-compactor! more-args)
                          (System/exit 0))

      ("help" "-h" "--help") (do
                               (cli-help/print-help)
                               (System/exit 0))

      (if (or (empty? args) (str/starts-with? (first args) "-"))
        (start-node args)

        (do
          (cli-help/print-help)
          (System/exit 2))))

    (catch Throwable t
      (shutdown-agents)
      (log/error t "Uncaught exception running XTDB")
      (System/exit 1))))
