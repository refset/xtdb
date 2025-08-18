(ns xtdb.main
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.cli-help :as cli-help]))

(defn- help-requested? [args]
  (contains? #{"-h" "--help" "help"} (first args)))

(defn- profile-requested? [args]
  (some #{"--profile"} (flatten args)))

(defn- start-profiler! []
  "Start clj-async-profiler immediately"
  (try
    (let [prof (requiring-resolve 'clj-async-profiler.core/start)]
      (prof {})
      (println "🔥 Profiler started - capturing everything from the very beginning!")
      true)
    (catch Exception e
      (println "❌ Profiler not available:" (.getMessage e))
      false)))

(defn -main [& args]
  ;; Start profiler IMMEDIATELY if --profile anywhere in args
  (when (profile-requested? args)
    (start-profiler!))
  
  (if (help-requested? args)
    (do
      (cli-help/print-help)
      (System/exit 0))
    (do
      (log/info (str "Starting " (cli-help/version-string) " ..."))
      ((requiring-resolve 'xtdb.cli/start-node-from-command-line) args))))
