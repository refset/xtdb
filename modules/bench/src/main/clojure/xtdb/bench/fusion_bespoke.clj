(ns xtdb.bench.fusion-bespoke
  (:require [clojure.data.json :as json]
            [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.bench.fusion :as fusion]
            [xtdb.compactor :as compactor]
            [xtdb.db-catalog :as db]
            [xtdb.test-util :as tu]
            [xtdb.trie-catalog :as cat]
            [xtdb.util :as util])
  (:import (java.nio.file Path)
           (java.time Instant)
           (xtdb.database Database)
           (xtdb.storage LocalStorage)
           (xtdb.table TableRef)
           (xtdb.trie Trie)))

(def table-names
  ["organisation" "device_series" "device_model" "site" "system"
   "device" "readings" "test_suite" "test_case" "test_suite_run" "test_case_run"])

(defn generate-catalog
  [node]
  (let [^Database db (db/primary-db node)
        ^LocalStorage bp (.getBufferPool db)
        tc (.getTrieCatalog db)
        ^Path root-path (.getRootPath bp)]
    (into {}
          (for [table-name table-names]
            (let [table (TableRef. "xtdb" "public" table-name)
                  table-tries (cat/trie-state tc table)
                  live-tries (cat/current-tries table-tries)
                  data-files (vec (for [{:keys [^String trie-key]} live-tries]
                                    (let [rel-path (Trie/dataFilePath table trie-key)]
                                      {:trie-key trie-key
                                       :path (str (.resolve root-path rel-path))})))]
              [table-name {:data-files data-files}])))))

(defn- temporal->micros ^long [t]
  (let [^Instant inst (if (instance? Instant t)
                        t
                        (.toInstant ^java.time.ZonedDateTime t))]
    (+ (* (.getEpochSecond inst) 1000000)
       (quot (.getNano inst) 1000))))

(defn write-catalog
  "Write catalog JSON for the C++ engine."
  [catalog-path node {:keys [system-ids min-valid-time max-valid-time]}]
  (let [catalog (generate-catalog node)
        metadata {:sample-system-id (first system-ids)
                  :min-valid-time (str (temporal->micros min-valid-time))
                  :max-valid-time (str (temporal->micros max-valid-time))
                  :catalog-time (str (Instant/now))}
        output {:tables catalog :params metadata}]
    (spit (str catalog-path) (json/write-str output))
    (log/infof "Wrote catalog to %s (%d tables)" catalog-path (count catalog))
    output))

(def ^:private q-system-count-over-time
  "WITH dates AS (
     SELECT d::timestamptz AS d
     FROM generate_series(DATE_BIN(INTERVAL 'PT1H', ?::timestamptz), ?::timestamptz, INTERVAL 'PT1H') AS x(d)
   )
   SELECT dates.d, COUNT(DISTINCT system._id) AS c
   FROM dates
   LEFT OUTER JOIN system FOR ALL VALID_TIME ON system._valid_time CONTAINS dates.d
   LEFT OUTER JOIN device FOR ALL VALID_TIME ON device.system_id = system._id AND device._valid_time CONTAINS dates.d
   LEFT OUTER JOIN device_model FOR ALL VALID_TIME ON device_model._id = device.device_model_id AND device_model._valid_time CONTAINS dates.d
   LEFT OUTER JOIN device_series FOR ALL VALID_TIME ON device_series._id = device_model.device_series_id AND device_series._valid_time CONTAINS dates.d
   LEFT OUTER JOIN organisation FOR ALL VALID_TIME ON organisation._id = device_series.organisation_id AND organisation._valid_time CONTAINS dates.d
   LEFT OUTER JOIN site FOR ALL VALID_TIME ON site._id = system.site_id AND site._valid_time CONTAINS dates.d
   GROUP BY dates.d
   ORDER BY dates.d")

(def ^:private q-readings-range-bins
  "WITH corrected_readings AS (
     SELECT r._valid_from, r._valid_to, r.value,
            (bin)._from AS corrected_from,
            (bin)._weight AS corrected_weight,
            r.value * (bin)._weight AS corrected_portion
     FROM readings FOR ALL VALID_TIME AS r, UNNEST(range_bins(INTERVAL 'PT1H', r._valid_time)) AS b(bin)
     WHERE r._valid_from >= ? AND r._valid_from < ?
   )
   SELECT corrected_from AS t, SUM(corrected_portion) / SUM(corrected_weight) AS value
   FROM corrected_readings
   GROUP BY corrected_from
   ORDER BY t")

(def ^:private q-cumulative-registration
  "WITH gen AS (
     SELECT d::timestamptz AS t
     FROM generate_series(DATE_BIN(INTERVAL 'PT1H', ?::timestamptz), ?::timestamptz, INTERVAL 'PT1H') AS x(d)
   ),
   latest_test_suite_run AS (
     SELECT ranked.* FROM (
       SELECT gen.t,
              test_suite_run.*,
              ROW_NUMBER() OVER (
                PARTITION BY gen.t, test_suite_run.system_id
                ORDER BY test_suite_run._system_from DESC
              ) AS rn
       FROM gen
       JOIN test_suite_run FOR ALL VALID_TIME ON test_suite_run._valid_time CONTAINS gen.t
       JOIN test_suite FOR ALL VALID_TIME ON test_suite._id = test_suite_run.test_suite_id
                       AND test_suite._valid_time CONTAINS gen.t
     ) ranked WHERE ranked.rn = 1
   ),
   expected_test_cases AS (
     SELECT latest_test_suite_run.t AS t,
            latest_test_suite_run._id AS test_suite_run_id,
            COUNT(*) AS count
     FROM latest_test_suite_run
     JOIN test_suite FOR ALL VALID_TIME ON test_suite._id = latest_test_suite_run.test_suite_id
                     AND test_suite._valid_time CONTAINS latest_test_suite_run.t
     JOIN test_case FOR ALL VALID_TIME ON test_case.test_suite_id = test_suite._id
                    AND test_case._valid_time CONTAINS latest_test_suite_run.t
     GROUP BY latest_test_suite_run.t, latest_test_suite_run._id
   ),
   passing_test_cases AS (
     SELECT latest_test_suite_run.t AS t,
            latest_test_suite_run._id AS test_suite_run_id,
            COUNT(*) AS count
     FROM latest_test_suite_run
     JOIN test_case_run FOR ALL VALID_TIME ON test_case_run.test_suite_run_id = latest_test_suite_run._id
                        AND test_case_run._valid_time CONTAINS latest_test_suite_run.t
     WHERE test_case_run.status = 'OK'
     GROUP BY latest_test_suite_run.t, latest_test_suite_run._id
   ),
   data AS (
     SELECT gen.t,
            system._id AS system_id,
            system.created_at AS created_at,
            site._id IS NOT NULL AS site_linked,
            COUNT(device._id) >= 1 AS devices_linked,
            COALESCE(latest_test_suite_run.status = 'DONE', FALSE) AS test_suite_run_ok,
            COALESCE(expected_test_cases.count, 0) AS expected_test_cases,
            COALESCE(passing_test_cases.count, 0) AS passing_test_cases
     FROM gen
     JOIN system FOR ALL VALID_TIME ON system._valid_time CONTAINS gen.t
     LEFT OUTER JOIN site FOR ALL VALID_TIME ON site._id = system.site_id AND site._valid_time CONTAINS gen.t
     LEFT OUTER JOIN device FOR ALL VALID_TIME ON device.system_id = system._id AND device._valid_time CONTAINS gen.t
     LEFT OUTER JOIN device_model FOR ALL VALID_TIME ON device_model._id = device.device_model_id AND device_model._valid_time CONTAINS gen.t
     LEFT OUTER JOIN latest_test_suite_run ON latest_test_suite_run.system_id = system._id
                                           AND latest_test_suite_run.t = gen.t
     LEFT OUTER JOIN expected_test_cases ON expected_test_cases.test_suite_run_id = latest_test_suite_run._id
                                         AND expected_test_cases.t = gen.t
     LEFT OUTER JOIN passing_test_cases ON passing_test_cases.test_suite_run_id = latest_test_suite_run._id
                                        AND passing_test_cases.t = gen.t
     GROUP BY gen.t, system._id, system.created_at, site._id, latest_test_suite_run.status,
              expected_test_cases.count, passing_test_cases.count
   ),
   data_with_status AS (
     SELECT t,
            system_id,
            CASE
              WHEN (site_linked AND devices_linked AND test_suite_run_ok
                    AND expected_test_cases = passing_test_cases) THEN 'Success'
              WHEN (created_at + INTERVAL 'PT48H' < t) THEN 'Failed'
              ELSE 'Pending'
            END AS registration_status
     FROM data
   )
   SELECT gen.t, registration_status, COUNT(system_id) AS c
   FROM gen
   LEFT OUTER JOIN data_with_status ON data_with_status.t = gen.t
   GROUP BY gen.t, registration_status
   ORDER BY gen.t, registration_status")

(defn run-xtdb-queries
  [node state n]
  (let [{:keys [system-ids min-valid-time max-valid-time latest-completed-tx]} state
        opts {:current-time (:system-time latest-completed-tx)}
        sample-id (first system-ids)]
    (into {}
          (for [[qname qfn] [[:system-settings #(fusion/exec-system-settings node sample-id opts)]
                             [:readings-for-system #(fusion/exec-readings-for-system node sample-id min-valid-time max-valid-time opts)]
                             [:system-count-over-time #(xt/q node [q-system-count-over-time min-valid-time max-valid-time] opts)]
                             [:readings-range-bins #(xt/q node [q-readings-range-bins min-valid-time max-valid-time] opts)]
                             [:cumulative-registration #(xt/q node [q-cumulative-registration min-valid-time max-valid-time] opts)]]]
            (let [latencies (mapv (fn [_]
                                    (let [start (System/nanoTime)
                                          result (qfn)
                                          elapsed (/ (- (System/nanoTime) start) 1e6)]
                                      {:result result :elapsed-ms elapsed}))
                                  (range n))]
              [qname {:results (-> latencies first :result)
                      :latencies-ms (mapv :elapsed-ms latencies)}])))))

(defn run-cpp-engine [cpp-binary catalog-path]
  (let [pb (ProcessBuilder. ^java.util.List [cpp-binary (str catalog-path)])
        proc (.start pb)
        stdout (slurp (.getInputStream proc))
        stderr (slurp (.getErrorStream proc))
        exit-code (.waitFor proc)]
    (when (seq stderr)
      (log/info stderr))
    (when (not= 0 exit-code)
      (throw (ex-info "C++ engine failed" {:exit-code exit-code :stderr stderr :stdout stdout})))
    (json/read-str stdout :key-fn keyword)))

(defn median [xs]
  (let [sorted (sort xs)
        n (count sorted)]
    (if (odd? n)
      (nth sorted (/ n 2))
      (/ (+ (nth sorted (dec (/ n 2)))
            (nth sorted (/ n 2)))
         2.0))))

(defn print-comparison [xtdb-results cpp-results]
  (println)
  (println (format "%-30s %12s %12s %8s %10s %10s" "Query" "XTDB (ms)" "Bespoke (ms)" "Speedup" "XTDB rows" "C++ rows"))
  (println (apply str (repeat 88 "-")))
  (doseq [[qname xtdb-data] xtdb-results]
    (let [xtdb-med (median (:latencies-ms xtdb-data))
          xtdb-row-count (count (:results xtdb-data))
          cpp-entry (first (filter #(= (name qname) (:query %)) cpp-results))
          cpp-ms (when cpp-entry (/ (:latency_us cpp-entry) 1000.0))
          cpp-row-count (when cpp-entry (:row_count cpp-entry))
          speedup (when (and cpp-ms (pos? cpp-ms)) (/ xtdb-med cpp-ms))]
      (println (format "%-30s %12.1f %12.1f %8.1fx %10d %10d"
                       (name qname)
                       (double xtdb-med)
                       (double (or cpp-ms 0))
                       (double (or speedup 0))
                       xtdb-row-count
                       (or cpp-row-count 0)))))
  (println)
  (doseq [[qname xtdb-data] xtdb-results]
    (let [cpp-entry (first (filter #(= (name qname) (:query %)) cpp-results))
          xtdb-rows (:results xtdb-data)
          cpp-rows (when cpp-entry (:results cpp-entry))]
      (cond
        (not cpp-rows) nil

        (not= (count xtdb-rows) (count cpp-rows))
        (do (println (format "MISMATCH %s: XTDB=%d rows, C++=%d rows"
                             (name qname) (count xtdb-rows) (count cpp-rows)))
            (println "  XTDB first 3:" (take 3 xtdb-rows))
            (println "  C++  first 3:" (take 3 cpp-rows)))

        :else
        (let [match-detail
              (case qname
                :system-settings
                (let [xt-id (get (first xtdb-rows) :xt/id)
                      cpp-id (get (first cpp-rows) :_id)]
                  (if (= (str xt-id) (str cpp-id)) "id OK" (str "id DIFF: " xt-id " vs " cpp-id)))

                :system-count-over-time
                (let [xt-counts (mapv :c xtdb-rows)
                      cpp-counts (mapv :c cpp-rows)]
                  (if (= xt-counts cpp-counts) "counts OK"
                      (str "counts DIFF: xt=" xt-counts " cpp=" cpp-counts)))

                :readings-for-system
                (let [xt-count (count xtdb-rows)
                      cpp-count (count cpp-rows)]
                  (str xt-count " rows"))

                :readings-range-bins
                (let [xt-vals (mapv (comp double :value) xtdb-rows)
                      cpp-vals (mapv (comp double :value) cpp-rows)
                      diffs (map #(Math/abs (- %1 %2)) xt-vals cpp-vals)
                      max-diff (when (seq diffs) (apply max diffs))]
                  (if (and max-diff (< max-diff 0.01)) "values OK" (str "max diff: " max-diff)))

                :cumulative-registration
                (let [normalize (fn [row]
                                  (let [status (or (:registration-status row) (:registration_status row))
                                        t-val (let [t (:t row)]
                                                (if (number? t) t (temporal->micros t)))]
                                    [t-val status (:c row)]))
                      xt-norm (sort (mapv normalize xtdb-rows))
                      cpp-norm (sort (mapv normalize cpp-rows))]
                  (if (= xt-norm cpp-norm) "status+counts OK"
                      (str "DIFF\n  xt=" (vec (take 4 xt-norm)) "\n  c++=" (vec (take 4 cpp-norm)))))

                "OK")]
          (println (format "MATCH %s: %d rows — %s" (name qname) (count xtdb-rows) match-detail)))))))

(t/deftest ^:benchmark run-fusion-bespoke
  (let [node-path (util/->path "/tmp/fusion-bespoke")
        catalog-path "/tmp/fusion-bespoke-catalog.json"
        cpp-binary "/home/jdt/ghq/github.com/xtdb/xtdb2/modules/bench/bespoke-olap/build/bespoke_olap"]
    (util/delete-dir node-path)
    (with-open [node (tu/->local-node {:node-dir node-path})]
      (binding [tu/*allocator* (util/component node :xtdb/allocator)]
        (let [benchmark (b/->benchmark :fusion {:devices 1000 :readings 1000 :batch-size 500
                                                :update-batch-size 100 :updates-per-system 5
                                                :staged-only? true})
              benchmark-fn (b/compile-benchmark benchmark)]
          (benchmark-fn node)

          (log/info "Running compaction...")
          (compactor/compact-all! node nil)
          (log/info "Compaction complete")

          (doseq [tn table-names]
            (let [table (TableRef. "xtdb" "public" tn)
                  tc (.getTrieCatalog (db/primary-db node))
                  live (cat/current-tries (cat/trie-state tc table))]
              (log/infof "  %s: %d files" tn (count live))))

          (let [latest-completed-tx (-> (xt/status node) (get-in [:latest-completed-txs "xtdb" 0]))
                max-vt (-> (xt/q node "SELECT max(_valid_from) AS m FROM readings FOR ALL VALID_TIME") first :m)
                min-vt (-> (xt/q node "SELECT min(_valid_from) AS m FROM readings FOR ALL VALID_TIME") first :m)
                system-ids (mapv :xt/id (xt/q node "SELECT _id FROM system FOR ALL VALID_TIME"))
                state {:system-ids system-ids
                       :min-valid-time min-vt
                       :max-valid-time max-vt
                       :latest-completed-tx latest-completed-tx}]

            (write-catalog catalog-path node state)

            (log/info "Running XTDB queries (3 iterations)...")
            (let [xtdb-results (run-xtdb-queries node state 3)]

              (log/info "Running C++ bespoke engine...")
              (let [cpp-results (try
                                  (run-cpp-engine cpp-binary catalog-path)
                                  (catch Exception e
                                    (log/warn e "C++ engine not available, skipping")
                                    nil))]
                (print-comparison xtdb-results (or (:queries cpp-results) []))))))))))
