(ns ^{:clojure.tools.namespace.repl/load false}
    xtdb.serde
  (:require [clojure.string :as str]
            [cognitect.transit :as transit]
            [xtdb.error :as err]
            [xtdb.mirrors.time-literals :as tl]
            [xtdb.time :as time]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.xtql.edn :as xtql.edn])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream Writer]
           (java.time Duration Period)
           java.util.List
           [org.apache.arrow.vector PeriodDuration]
           (xtdb.api TransactionAborted TransactionCommitted TransactionKey)
           (xtdb.api.query Binding IKeyFn IKeyFn$KeyFn XtqlQuery)
           (xtdb.api.tx TxOp$Sql TxOps)
           (xtdb.tx_ops AssertExists AssertNotExists Call Delete DeleteDocs Erase EraseDocs Insert PutDocs Update XtqlAndArgs)
           (xtdb.types ClojureForm IntervalDayTime IntervalMonthDayNano IntervalYearMonth ZonedDateTimeRange)))

(defrecord TxKey [tx-id system-time]
  TransactionKey
  (getTxId [_] tx-id)
  (getSystemTime [_] system-time))

(defmethod print-dup TxKey [^TxKey tx-key, ^Writer w]
  (.write w (str "#xt/tx-key " (pr-str (into {} tx-key)))))

(defmethod print-method TxKey [tx-key w]
  (print-dup tx-key w))

(defrecord TxCommitted [tx-id system-time committed?]
  TransactionCommitted
  (getTxId [_] tx-id)
  (getSystemTime [_] system-time))

(defn ->tx-committed
  ([^TransactionKey tx-key] (->tx-committed (.getTxId tx-key) (.getSystemTime tx-key)))
  ([tx-id system-time] (->TxCommitted tx-id system-time true)))

(defrecord TxAborted [tx-id system-time committed? error]
  TransactionAborted
  (getTxId [_] tx-id)
  (getSystemTime [_] system-time)
  (getError [_] error))

(defn ->tx-aborted
  ([^TransactionKey tx-key error] (->tx-aborted (.getTxId tx-key) (.getSystemTime tx-key) error))
  ([tx-id system-time error] (->TxAborted tx-id system-time false error)))

(defn period-duration-reader [[p d]]
  (PeriodDuration. (Period/parse p) (Duration/parse d)))

(defmethod print-dup PeriodDuration [^PeriodDuration pd ^Writer w]
  (.write w (format "#xt/period-duration %s" (pr-str [(str (.getPeriod pd)) (str (.getDuration pd))]))))

(defmethod print-method PeriodDuration [c ^Writer w]
  (print-dup c w))

(defn interval-ym-reader [p]
  (IntervalYearMonth. (Period/parse p)))

(defmethod print-dup IntervalYearMonth [^IntervalYearMonth i, ^Writer w]
  (.write w (format "#xt/interval-ym %s" (pr-str (str (.period i))))))

(defmethod print-method IntervalYearMonth [i ^Writer w]
  (print-dup i w))

(defn interval-dt-reader [[p d]]
  (IntervalDayTime. (Period/parse p) (Duration/parse d)))

(defmethod print-dup IntervalDayTime [^IntervalDayTime i, ^Writer w]
  (.write w (format "#xt/interval-dt %s" (pr-str [(str (.period i)) (str (.duration i))]))))

(defmethod print-method IntervalDayTime [i ^Writer w]
  (print-dup i w))

(defn interval-mdn-reader [[p d]]
  (IntervalMonthDayNano. (Period/parse p) (Duration/parse d)))

(defmethod print-dup IntervalMonthDayNano [^IntervalMonthDayNano i, ^Writer w]
  (.write w (format "#xt/interval-mdn %s" (pr-str [(str (.period i)) (str (.duration i))]))))

(defmethod print-method IntervalMonthDayNano [i ^Writer w]
  (print-dup i w))

(defn- render-tstz-range [^ZonedDateTimeRange range]
  [(.getFrom range) (.getTo range)])

(defmethod print-dup ZonedDateTimeRange [^ZonedDateTimeRange tstz-range, ^Writer w]
  (.write w (str "#xt/tstz-range " (pr-str (render-tstz-range tstz-range)))))

(defmethod print-method ZonedDateTimeRange [tstz-range w]
  (print-dup tstz-range w))

(defn tstz-range-reader [[from to]]
  (ZonedDateTimeRange. (time/->zdt from) (time/->zdt to)))

(defn- render-binding [binding]
  (xtql.edn/unparse-binding identity identity binding))

(defn- render-query [^XtqlQuery query]
  (xtql.edn/unparse-query query))

(defn- xtql-query-reader [q-edn]
  (xtql.edn/parse-query q-edn))

(defn- render-sql-op [^TxOp$Sql op]
  {:sql (.sql op), :arg-rows (.argRows op)})

(defn sql-op-reader [{:keys [sql ^List arg-rows]}]
  (-> (TxOps/sql sql)
      (.argRows arg-rows)))

(defmethod print-dup TxOp$Sql [op ^Writer w]
  (.write w (format "#xt.tx/sql %s" (pr-str (render-sql-op op)))))

(defmethod print-method TxOp$Sql [op ^Writer w]
  (print-dup op w))

(defn write-key-fn [^IKeyFn$KeyFn key-fn]
  (-> (.name key-fn)
      (str/lower-case)
      (str/replace #"_" "-")
      keyword))

(def ^:private key-fns
  (->> (IKeyFn$KeyFn/values)
       (into {} (map (juxt write-key-fn identity)))))

(defn read-key-fn [k]
  (if (instance? IKeyFn k)
    k
    (or (get key-fns k)
        (throw (err/illegal-arg :xtdb/invalid-key-fn {:key k})))))

(defmethod print-dup IKeyFn$KeyFn [key-fn ^Writer w]
  (.write w (str "#xt/key-fn " (write-key-fn key-fn))))

(defmethod print-method IKeyFn$KeyFn [e, ^Writer w]
  (print-dup e w))

(defmethod print-dup TxKey [tx-key ^Writer w]
  (.write w "#xt/tx-key ")
  (print-method (into {} tx-key) w))

(defmethod print-method TxKey [tx-key w]
  (print-dup tx-key w))

(defn tx-result-read-fn [{:keys [committed?] :as tx-res}]
  (if committed?
    (map->TxCommitted tx-res)
    (map->TxAborted tx-res)))

(defmethod print-dup TxCommitted [tx-result ^Writer w]
  (.write w "#xt/tx-result ")
  (print-method (into {} tx-result) w))

(defmethod print-method TxCommitted [tx-result ^Writer w]
  (print-dup tx-result w))

(defmethod print-dup TxAborted [tx-result ^Writer w]
  (.write w "#xt/tx-result ")
  (print-method (into {} tx-result) w))

(defmethod print-method TxAborted [tx-result ^Writer w]
  (print-dup tx-result w))

(defn- render-iae [^xtdb.IllegalArgumentException e]
  [(.getKey e) (ex-message e) (-> (ex-data e) (dissoc ::err/error-key))])

(defmethod print-dup xtdb.IllegalArgumentException [e, ^Writer w]
  (.write w (str "#xt/illegal-arg " (render-iae e))))

(defmethod print-method xtdb.IllegalArgumentException [e, ^Writer w]
  (print-dup e w))

(defn iae-reader [[k message data]]
  (xtdb.IllegalArgumentException. k message data nil))

(defn- render-runex [^xtdb.RuntimeException e]
  [(.getKey e) (ex-message e) (-> (ex-data e) (dissoc ::err/error-key))])

(defmethod print-dup xtdb.RuntimeException [e, ^Writer w]
  (.write w (str "#xt/runtime-err " (render-runex e))))

(defmethod print-method xtdb.RuntimeException [e, ^Writer w]
  (print-dup e w))

(defn runex-reader [[k message data]]
  (xtdb.RuntimeException. k message data nil))

(def transit-read-handlers
  (merge transit/default-read-handlers
         tl/transit-read-handlers
         {"xtdb/clj-form" (transit/read-handler ClojureForm/new)
          "xtdb/tx-key" (transit/read-handler map->TxKey)
          "xtdb/tx-result" (transit/read-handler tx-result-read-fn)
          "xtdb/key-fn" (transit/read-handler read-key-fn)
          "xtdb/illegal-arg" (transit/read-handler iae-reader)
          "xtdb/runtime-err" (transit/read-handler runex-reader)
          "xtdb/exception-info" (transit/read-handler #(ex-info (first %) (second %)))
          "xtdb/period-duration" period-duration-reader
          "xtdb.interval/year-month" interval-ym-reader
          "xtdb.interval/day-time" interval-dt-reader
          "xtdb.interval/month-day-nano" interval-mdn-reader
          "xtdb/tstz-range" (transit/read-handler tstz-range-reader)
          "xtdb.query/xtql" (transit/read-handler xtql-query-reader)
          "xtdb.tx/sql" (transit/read-handler sql-op-reader)
          "xtdb.tx/xtql" (transit/read-handler tx-ops/parse-tx-op)
          "xtdb.tx/put-docs" (transit/read-handler tx-ops/map->PutDocs)
          "xtdb.tx/delete-docs" (transit/read-handler tx-ops/map->DeleteDocs)
          "xtdb.tx/erase-docs" (transit/read-handler tx-ops/map->EraseDocs)
          "xtdb.tx/call" (transit/read-handler tx-ops/map->Call)
          "f64" (transit/read-handler double)
          "f32" (transit/read-handler float)
          "i64" (transit/read-handler long)
          "i32" (transit/read-handler int)
          "i16" (transit/read-handler short)
          "i8" (transit/read-handler byte)}))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]} ; TransitVector
(defn transit-msgpack-reader [in]
  (.r ^cognitect.transit.Reader (transit/reader in :msgpack {:handlers transit-read-handlers})))

(def transit-write-handlers
  (merge transit/default-write-handlers
         tl/transit-write-handlers
         {TxKey (transit/write-handler "xtdb/tx-key" #(into {} %))
          TxCommitted (transit/write-handler "xtdb/tx-result" #(into {} %))
          TxAborted (transit/write-handler "xtdb/tx-result" #(into {} %))
          xtdb.IllegalArgumentException (transit/write-handler "xtdb/illegal-arg" render-iae)
          xtdb.RuntimeException (transit/write-handler "xtdb/runtime-err" render-runex)
          clojure.lang.ExceptionInfo (transit/write-handler "xtdb/exception-info" (juxt ex-message ex-data))
          IKeyFn$KeyFn (transit/write-handler "xtdb/key-fn" write-key-fn)

          ClojureForm (transit/write-handler "xtdb/clj-form" #(.form ^ClojureForm %))

          IntervalYearMonth (transit/write-handler "xtdb.interval/year-month" #(str (.period ^IntervalYearMonth %)))

          IntervalDayTime (transit/write-handler "xtdb.interval/day-time"
                                                 #(vector (str (.period ^IntervalDayTime %))
                                                          (str (.duration ^IntervalDayTime %))))

          IntervalMonthDayNano (transit/write-handler "xtdb.interval/month-day-nano"
                                                      #(vector (str (.period ^IntervalMonthDayNano %))
                                                               (str (.duration ^IntervalMonthDayNano %))))

          ZonedDateTimeRange (transit/write-handler "xtdb/tstz-range" render-tstz-range)

          Binding (transit/write-handler "xtdb.query/binding" render-binding)
          XtqlQuery (transit/write-handler "xtdb.query/xtql" render-query)

          TxOp$Sql (transit/write-handler "xtdb.tx/sql" render-sql-op)

          PutDocs (transit/write-handler "xtdb.tx/put-docs" (partial into {}))
          DeleteDocs (transit/write-handler "xtdb.tx/delete-docs" (partial into {}))
          EraseDocs (transit/write-handler "xtdb.tx/erase-docs" (partial into {}))
          Call (transit/write-handler "xtdb.tx/call" (partial into {}))

          XtqlAndArgs (transit/write-handler "xtdb.tx/xtql" tx-ops/unparse-tx-op)
          Insert (transit/write-handler "xtdb.tx/xtql" tx-ops/unparse-tx-op)
          Update (transit/write-handler "xtdb.tx/xtql" tx-ops/unparse-tx-op)
          Delete (transit/write-handler "xtdb.tx/xtql" tx-ops/unparse-tx-op)
          Erase (transit/write-handler "xtdb.tx/xtql" tx-ops/unparse-tx-op)
          AssertExists (transit/write-handler "xtdb.tx/xtql" tx-ops/unparse-tx-op)
          AssertNotExists (transit/write-handler "xtdb.tx/xtql" tx-ops/unparse-tx-op)}))

(defn read-transit
  ([bytes] (read-transit bytes nil))
  ([bytes fmt]
   (with-open [bais (ByteArrayInputStream. bytes)]
     (transit/read (transit/reader bais (or fmt :msgpack) {:handlers transit-read-handlers})))))

(defn write-transit
  (^bytes [v] (write-transit v nil))
  (^bytes [v fmt]
   (with-open [baos (ByteArrayOutputStream.)]
     (-> (transit/writer baos (or fmt :msgpack) {:handlers transit-write-handlers})
         (transit/write v))
     (.toByteArray baos))))
