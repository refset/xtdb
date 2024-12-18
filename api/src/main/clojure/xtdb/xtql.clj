(ns xtdb.xtql
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [xtdb.error :as err])
  (:import (clojure.lang MapEntry)
           (java.util List)
           (xtdb.api.query Binding Expr$Bool Expr$Call Expr$Double Expr$Exists Expr$Get Expr$ListExpr Expr$LogicVar Expr$Long Expr$MapExpr Expr$Null Expr$Obj Expr$Param Expr$Pull Expr$PullMany Expr$SetExpr Expr$Subquery Exprs TemporalFilter$AllTime TemporalFilter$At TemporalFilter$In TemporalFilters XtqlQuery XtqlQuery$QueryTail XtqlQuery$UnifyClause)))

(defn check-opt-keys [valid-keys opts]
  (when-let [invalid-opt-keys (not-empty (set/difference (set (keys opts)) valid-keys))]
    (throw (err/illegal-arg :invalid-opt-keys
                            {:opts opts, :valid-keys valid-keys
                             :invalid-keys invalid-opt-keys
                             ::err/message "Invalid keys provided to option map"}))))

(defn- query-type? [q] (seq? q))

(defmulti ^xtdb.api.query.XtqlQuery parse-query
  (fn [query]
    (when-not (query-type? query)
      (throw (err/illegal-arg :xtql/malformed-query {:query query})))

    (let [[op] query]
      (when-not (symbol? op)
        (throw (err/illegal-arg :xtql/malformed-query {:query query})))

      op)))

(defmethod parse-query :default [[op]]
  (throw (err/illegal-arg :xtql/unknown-query-op {:op op})))

(defmulti parse-query-tail
  (fn [query-tail]
    (when-not (query-type? query-tail)
      (throw (err/illegal-arg :xtql/malformed-query-tail {:query-tail query-tail})))

    (let [[op] query-tail]
      (when-not (symbol? op)
        (throw (err/illegal-arg :xtql/malformed-query-tail {:query-tail query-tail})))

      op)))

(defmethod parse-query-tail :default [[op]]
  (throw (err/illegal-arg :xtql/unknown-query-tail {:op op})))

(defmulti parse-unify-clause
  (fn [clause]
    (when-not (query-type? clause)
      (throw (err/illegal-arg :xtql/malformed-unify-clause {:clause clause})))

    (let [[op] clause]
      (when-not (symbol? op)
        (throw (err/illegal-arg :xtql/malformed-unify-clause {:clause clause})))

      op)))

(defmethod parse-unify-clause :default [[op]]
  (throw (err/illegal-arg :xtql/unknown-unify-clause {:op op})))

(defprotocol Unparse (unparse [this]))
(defprotocol UnparseQuery (unparse-query [this]))
(defprotocol UnparseQueryTail (unparse-query-tail [this]))
(defprotocol UnparseUnifyClause (unparse-unify-clause [this]))

(declare parse-arg-specs)

(defn parse-expr ^xtdb.api.query.Expr [expr]
  (cond
    (nil? expr) Expr$Null/INSTANCE
    (true? expr) Expr$Bool/TRUE
    (false? expr) Expr$Bool/FALSE
    (int? expr) (Exprs/val (long expr))
    (double? expr) (Exprs/val (double expr))
    (symbol? expr) (if (= 'xtdb/end-of-time expr)
                     (Exprs/val 'xtdb/end-of-time)
                     (let [str-expr (str expr)]
                       (if (str/starts-with? str-expr "$")
                         (Exprs/param str-expr)
                         (Exprs/lVar str-expr))))
    (keyword? expr) (Exprs/val expr)
    (vector? expr) (Exprs/list ^List (mapv parse-expr expr))
    (set? expr) (Exprs/set ^List (mapv parse-expr expr))
    (map? expr) (Exprs/map (into {} (map (juxt (comp #(subs % 1) str key) (comp parse-expr val))) expr))

    (seq? expr) (do
                  (when (empty? expr)
                    (throw (err/illegal-arg :xtql/malformed-call {:call expr})))

                  (let [[f & args] expr]
                    (when-not (symbol? f)
                      (throw (err/illegal-arg :xtql/malformed-call {:call expr})))

                    (case f
                      .
                      (do
                        (when-not (and (= (count args) 2)
                                       (first args)
                                       (symbol? (second args)))
                          (throw (err/illegal-arg :xtql/malformed-get {:expr expr})))

                        (Exprs/get (parse-expr (first args)) (str (second args))))

                      (exists? q pull pull*)
                      (do
                        (when-not (and (<= 1 (count args) 2)
                                       (or (nil? (second args))
                                           (map? (second args))))
                          (throw (err/illegal-arg :xtql/malformed-subquery {:expr expr})))

                        (let [[query {:keys [args]}] args
                              parsed-query (parse-query query)
                              parsed-args (some-> args (parse-arg-specs expr))]
                          (case f
                            exists? (Exprs/exists parsed-query parsed-args)
                            q (Exprs/q parsed-query parsed-args)
                            pull (Exprs/pull parsed-query parsed-args)
                            pull* (Exprs/pullMany parsed-query parsed-args))))

                      (Exprs/call (str f) ^List (mapv parse-expr args)))))

    :else (Exprs/val expr)))

;;NOTE out-specs and arg-specs are currently indentical structurally, but one is an input binding,
;;the other an output binding.
;;I could see remerging these into a single binding-spec,
;;that being said, its possible we don't want arbitrary exprs in the from of arg specs, only plain vars
;;
;;TODO binding-spec-errs
(defn parse-out-specs
  "[{:from to-var} from-col-to-var {:col (pred)}]"
  ^List [specs _query]
  (letfn [(parse-out-spec [[attr expr]]
            (when-not (keyword? attr)
              (throw (err/illegal-arg :xtql/malformed-bind-spec
                                      {:attr attr :expr expr ::err/message "Attribute in bind spec must be keyword"})))

            (Binding. (str (symbol attr)) (parse-expr expr)))]

    (if (vector? specs)
      (->> specs
           (into [] (mapcat (fn [spec]
                              (cond
                                (symbol? spec) [(Binding. (str spec))]
                                (map? spec) (map parse-out-spec spec))))))

      (throw (UnsupportedOperationException.)))))

(defn- parse-arg-specs
  "[{:to-var (from-expr)} to-var-from-var]"
  [specs _query]
  (->> specs
       (into [] (mapcat (fn [spec]
                          (cond
                            (symbol? spec) [(Binding. (str spec))]
                            (map? spec) (for [[attr expr] spec]
                                          (do
                                            #_{:clj-kondo/ignore [:missing-body-in-when]}
                                            (when-not (keyword? attr)
                                              ;; TODO error
                                              )
                                            (Binding. (str (symbol attr)) (parse-expr expr))))))))))
(defn- parse-var-specs
  "[{to-var (from-expr)}]"
  [specs _query]
  (letfn [(parse-var-spec [[attr expr]]
            (when-not (symbol? attr)
              (throw (err/illegal-arg
                      :xtql/malformed-var-spec {:attr attr :expr expr
                       ::err/message "Attribute in var spec must be symbol"})))


            (Binding. (str attr) (parse-expr expr)))]

    (cond
      (map? specs) (mapv parse-var-spec specs)
      (sequential? specs) (->> specs
                               (into [] (mapcat (fn [spec]
                                                  (if (map? spec) (map parse-var-spec spec)
                                                      (throw (err/illegal-arg
                                                              :xtql/malformed-var-spec
                                                              {:spec spec
                                                               ::err/message "Var specs must be pairs of bindings"})))))))
      :else (throw (UnsupportedOperationException.)))))

(defn parse-col-specs
  "[{:to-col (from-expr)} :col ...]"
  [specs _query]
  (letfn [(parse-col-spec [[attr expr]]
            (when-not (keyword? attr)
              (throw (err/illegal-arg
                      :xtql/malformed-col-spec
                      {:attr attr :expr expr ::err/message "Attribute in col spec must be keyword"})))

            (Binding. (str (symbol attr)) (parse-expr expr)))]

    (cond
      (map? specs) (mapv parse-col-spec specs)
      (sequential? specs) (->> specs
                               (into [] (mapcat (fn [spec]
                                                  (cond
                                                    (symbol? spec) [(Binding. (str spec))]
                                                    (map? spec) (map parse-col-spec spec)

                                                    :else
                                                    (throw (err/illegal-arg
                                                            :xtql/malformed-col-spec
                                                            {:spec spec ::err/message "Short form of col spec must be a symbol"})))))))
      :else (throw (UnsupportedOperationException.)))))

(defn parse-temporal-filter [v k query]
  (let [ctx {:v v, :filter k, :query query}]
    (if (= :all-time v)
      TemporalFilters/allTime

      (do
        (when-not (and (seq? v) (not-empty v))
          (throw (err/illegal-arg :xtql/malformed-temporal-filter ctx)))

        (let [[tag & args] v]
          (when-not (symbol? tag)
            (throw (err/illegal-arg :xtql/malformed-temporal-filter ctx)))

          (letfn [(assert-arg-count [expected args]
                    (when-not (= expected (count args))
                      (throw (err/illegal-arg :xtql/malformed-temporal-filter (into ctx {:tag tag, :at args}))))

                    args)]
            (case tag
              at (let [[at] (assert-arg-count 1 args)]
                   (TemporalFilters/at (parse-expr at)))

              in (let [[from to] (assert-arg-count 2 args)]
                   (TemporalFilters/in (parse-expr from) (parse-expr to)))

              from (let [[from] (assert-arg-count 1 args)]
                     (TemporalFilters/from (parse-expr from)))

              to (let [[to] (assert-arg-count 1 args)]
                   (TemporalFilters/to (parse-expr to)))

              (throw (err/illegal-arg :xtql/malformed-temporal-filter (into ctx {:tag tag}))))))))))

(extend-protocol Unparse
  TemporalFilter$AllTime (unparse [_] :all-time)
  TemporalFilter$At (unparse [at] (list 'at (unparse (.getAt at))))
  TemporalFilter$In (unparse [in] (list 'in (some-> (.getFrom in) unparse) (some-> (.getTo in) unparse))))

(defn unparse-binding [base-type nested-type, ^Binding binding]
  (let [attr (.getBinding binding)
        expr (.getExpr binding)]
    (if base-type
      (if (and (instance? Expr$LogicVar expr)
               (= (.lv ^Expr$LogicVar expr) attr))
        (base-type attr)
        {(nested-type attr) (unparse expr)})
      {(nested-type attr) (unparse expr)})))

(def unparse-out-spec (partial unparse-binding symbol keyword))
(def unparse-col-spec (partial unparse-binding symbol keyword))
(def unparse-arg-spec (partial unparse-binding symbol keyword))
(def unparse-var-spec (partial unparse-binding nil symbol))

(defrecord Pipeline [head tails]
  XtqlQuery
  UnparseQuery (unparse-query [_] (list* '-> (unparse-query head) (mapv unparse-query-tail tails))))

(defmethod parse-query '-> [[_ head & tails :as this]]
  (when-not head
    (throw (err/illegal-arg :xtql/malformed-pipeline {:pipeline this, :message "Pipeline most contain at least one operator"})))

  (->Pipeline (parse-query head)
              (mapv parse-query-tail tails)))

(defrecord Unify [unify-clauses]
  XtqlQuery
  UnparseQuery (unparse-query [_] (list* 'unify (mapv unparse-unify-clause unify-clauses))))

(defmethod parse-query 'unify [[_ & clauses :as this]]
  (when (> 1 (count clauses))
    (throw (err/illegal-arg :xtql/malformed-unify {:unify this, :message "Unify most contain at least one sub clause"})))
  (->Unify (mapv parse-unify-clause clauses)))

(defrecord From [table for-valid-time for-system-time bindings project-all-cols?]
  XtqlQuery
  XtqlQuery$UnifyClause

  UnparseQuery
  (unparse-query [from]
    (let [bind (mapv unparse-out-spec bindings)
          bind (if project-all-cols? (vec (cons '* bind)) bind)]
      (list 'from (keyword (.table from))
            (if (or for-valid-time for-system-time)
              (cond-> {:bind bind}
                for-valid-time (assoc :for-valid-time (unparse for-valid-time))
                for-system-time (assoc :for-system-time (unparse for-system-time)))
              bind))))

  UnparseUnifyClause (unparse-unify-clause [from] (unparse-query from)))

(def from-opt-keys #{:bind :for-valid-time :for-system-time})

(defn find-star-projection [star-ident bindings]
  ;;TODO worth checking here or in parse-out-specs for * in col/attr position? {* var}??
  ;;Arguably not checking for this could allow * to be a valid col name?
  (reduce
   (fn [acc binding]
     (if (= binding star-ident)
       (assoc acc :project-all-cols? true)
       (update acc :bind conj binding)))
   {:project-all-cols? false
    :bind []}
   bindings))

(defn parse-from [[_ table opts :as this]]
  (if-not (keyword? table)
    (throw (err/illegal-arg :xtql/malformed-table {:table table, :from this}))

    (cond
      (or (nil? opts) (map? opts))

      (do
        (check-opt-keys from-opt-keys opts)

        (let [{:keys [for-valid-time for-system-time bind]} opts]
          (cond
            (nil? bind)
            (throw (err/illegal-arg :xtql/missing-bind {:opts opts, :from this}))

            (not (vector? bind))
            (throw (err/illegal-arg :xtql/malformed-bind {:opts opts, :from this}))

            :else
            (let [{:keys [bind project-all-cols?]} (find-star-projection '* bind)]
              (->From table
                      (some-> for-valid-time (parse-temporal-filter :for-valid-time this))
                      (some-> for-system-time (parse-temporal-filter :for-system-time this))
                      (parse-out-specs bind this)
                      project-all-cols?)))))

      (vector? opts)
      (let [{:keys [bind project-all-cols?]} (find-star-projection '* opts)]
        (map->From {:table table
                    :bindings (parse-out-specs bind this)
                    :project-all-cols? project-all-cols?}))

      :else (throw (err/illegal-arg :xtql/malformed-table-opts {:opts opts, :from this})))))

(defmethod parse-query 'from [this] (parse-from this))
(defmethod parse-unify-clause 'from [this] (parse-from this))

(defrecord Where [preds]
  XtqlQuery$QueryTail
  XtqlQuery$UnifyClause
  UnparseQueryTail (unparse-query-tail [_] (list* 'where (mapv unparse preds)))
  UnparseUnifyClause (unparse-unify-clause [this] (unparse-query-tail this)))

(defn parse-where [[_ & preds :as this]]
  (when (> 1 (count preds))
    (throw (err/illegal-arg :xtql/malformed-where {:where this, :message "Where most contain at least one predicate"})))

  (->Where (mapv parse-expr preds)))

(defmethod parse-query-tail 'where [this] (parse-where this))
(defmethod parse-unify-clause 'where [this] (parse-where this))

(defrecord With [with-bindings]
  XtqlQuery$QueryTail
  XtqlQuery$UnifyClause
  UnparseQueryTail (unparse-query-tail [_] (list* 'with (mapv unparse-col-spec with-bindings)))
  UnparseUnifyClause (unparse-unify-clause [_] (list* 'with (mapv unparse-var-spec with-bindings))))

(defmethod parse-query-tail 'with [[_ & cols :as this]]
  ;;TODO with uses col-specs but doesn't support short form, this needs handling
  (->With (parse-col-specs cols this)))

(defmethod parse-unify-clause 'with [[_ & vars :as this]]
  (->With (parse-var-specs vars this)))

(defrecord Without [without-cols]
  XtqlQuery$QueryTail
  XtqlQuery$UnifyClause
  UnparseQueryTail (unparse-query-tail [_] (list* 'without without-cols)))

(defmethod parse-query-tail 'without [[_ & cols :as this]]
  (when-not (every? keyword? cols)
    (throw (err/illegal-arg :xtql/malformed-without {:without this
                                                     ::err/message "Columns must be keywords in without"})))
  (->Without cols))

(defrecord Return [return-bindings]
  XtqlQuery$QueryTail
  UnparseQueryTail (unparse-query-tail [_] (list* 'return (mapv unparse-col-spec return-bindings))))

(defmethod parse-query-tail 'return [[_ & cols :as this]]
  (->Return (parse-col-specs cols this)))

(defrecord CallRule [rule-name args bindings]
  XtqlQuery$UnifyClause)

(defrecord Join [join-query args bindings]
  XtqlQuery$UnifyClause
  UnparseUnifyClause
  (unparse-unify-clause [_]
    (let [args args
          bind (mapv unparse-col-spec bindings)]
      (list 'join (unparse-query join-query)
            (if args
              (cond-> {:bind bind}
                (seq args) (assoc :args (mapv unparse-arg-spec args)))
              bind)))))

(def join-clause-opt-keys #{:args :bind})

(defmethod parse-unify-clause 'join [[_ query opts :as join]]
  (cond
    (nil? opts) (throw (err/illegal-arg :missing-join-opts {:opts opts, :join join}))

    (map? opts) (do
                  (check-opt-keys join-clause-opt-keys opts)
                  (let [{:keys [args bind]} opts]
                    (->Join (parse-query query) (parse-arg-specs args join)
                            (some-> bind (parse-out-specs join)))))

    :else (->Join (parse-query query) nil (parse-out-specs opts join))))

(defrecord LeftJoin [left-join-query args bindings]
  XtqlQuery$UnifyClause
  UnparseUnifyClause
  (unparse-unify-clause [_]
    (let [bind (mapv unparse-col-spec bindings)]
      (list 'left-join (unparse-query left-join-query)
            (if args
              (cond-> {:bind bind}
                (seq args) (assoc :args (mapv unparse-arg-spec args)))
              bind)))))

(defmethod parse-unify-clause 'left-join [[_ query opts :as left-join]]
  (cond
    (nil? opts) (throw (err/illegal-arg :missing-join-opts {:opts opts, :left-join left-join}))

    (map? opts) (do
                  (check-opt-keys join-clause-opt-keys opts)
                  (let [{:keys [args bind]} opts]
                    (->LeftJoin (parse-query query) (parse-arg-specs args left-join)
                                (some-> bind (parse-out-specs left-join)))))

    :else (->LeftJoin (parse-query query) nil (parse-out-specs opts left-join))))

(defrecord Aggregate [agg-bindings]
  XtqlQuery$QueryTail
  UnparseQueryTail (unparse-query-tail [_] (list* 'aggregate (mapv unparse-col-spec agg-bindings))))

(defmethod parse-query-tail 'aggregate [[_ & cols :as this]]
  (->Aggregate (parse-col-specs cols this)))

(defrecord OrderSpec [expr direction nulls]
  Unparse
  (unparse [_]
    (let [expr (unparse expr)]
      (if (and (not direction) (not nulls))
        expr
        (cond-> {:val expr}
          direction (assoc :dir direction)
          nulls (assoc :nulls nulls))))))

(def order-spec-opt-keys #{:val :dir :nulls})

(defn- parse-order-spec [order-spec this]
  (if (map? order-spec)
    (let [{:keys [val dir nulls]} order-spec]
      (check-opt-keys order-spec-opt-keys order-spec)

      (when-not (contains? order-spec :val)
        (throw (err/illegal-arg :xtql/order-by-val-missing
                                {:order-spec order-spec, :query this})))

      (->OrderSpec (parse-expr val)
                   (case dir
                     (nil :asc :desc) dir

                     (throw (err/illegal-arg :xtql/malformed-order-by-direction
                                             {:direction dir, :order-spec order-spec, :query this})))
                   (case nulls
                     (nil :first :last) nulls

                     (throw (err/illegal-arg :xtql/malformed-order-by-nulls
                                             {:nulls nulls, :order-spec order-spec, :query this})))))

    (->OrderSpec (parse-expr order-spec) nil nil)))

(defrecord OrderBy [order-specs]
  XtqlQuery$QueryTail
  UnparseQueryTail (unparse-query-tail [_] (list* 'order-by (mapv unparse order-specs))))

(defmethod parse-query-tail 'order-by [[_ & order-specs :as this]]
  (->OrderBy (mapv #(parse-order-spec % this) order-specs)))

(defrecord Limit [limit]
  XtqlQuery$QueryTail
  UnparseQueryTail (unparse-query-tail [_] (list 'limit limit)))

(defmethod parse-query-tail 'limit [[_ length :as this]]
  (when-not (= 2 (count this))
    (throw (err/illegal-arg :xtql/limit {:limit this :message "Limit can only take a single value"})))
  (->Limit length))

(defrecord Offset [offset]
  XtqlQuery$QueryTail
  UnparseQueryTail (unparse-query-tail [_] (list 'offset offset)))

(defmethod parse-query-tail 'offset [[_ length :as this]]
  (when-not (= 2 (count this))
    (throw (err/illegal-arg :xtql/offset {:offset this :message "Offset can only take a single value"})))
  (->Offset length))

(defrecord DocsRelation [documents bindings]
  XtqlQuery
  XtqlQuery$UnifyClause

  UnparseQuery
  (unparse-query [_]
    (list 'rel
          (mapv #(into {} (map (fn [[k v]] (MapEntry/create (keyword k) (unparse v)))) %) documents)
          (mapv unparse-out-spec bindings)))

  UnparseUnifyClause (unparse-unify-clause [this] (unparse-query this)))

(defrecord ParamRelation [^Expr$Param param, bindings]
  XtqlQuery
  XtqlQuery$UnifyClause

  UnparseQuery
  (unparse-query [_]
    (list 'rel (symbol (.v param)) (mapv unparse-out-spec bindings)))

  UnparseUnifyClause (unparse-unify-clause [this] (unparse-query this)))

(defn- keyword-map? [m]
  (every? keyword? (keys m)))

(defn parse-rel [[_ param-or-docs bind :as this]]
  (when-not (= 3 (count this))
    (throw (err/illegal-arg :xtql/rel {:rel this, :message "`rel` takes exactly 3 arguments"})))

  (when-not (or (symbol? param-or-docs)
                (and (vector? param-or-docs) (every? keyword-map? param-or-docs)))
    (throw (err/illegal-arg :xtql/rel {:rel this, :message "`rel` takes a param or an explicit relation"})))

  (let [parsed-bind (parse-out-specs bind this)]
    (if (symbol? param-or-docs)
      (let [parsed-expr (parse-expr param-or-docs)]
        (if (instance? Expr$Param parsed-expr)
          (->ParamRelation parsed-expr parsed-bind)
          (throw (err/illegal-arg :xtql/rel {::err/message "Illegal second argument to `rel`"
                                             :arg param-or-docs}))))
      (->DocsRelation (vec
                       (for [doc param-or-docs]
                         (->> doc
                              (into {} (map (fn [[k v]]
                                              (MapEntry/create k (parse-expr v))))))))
                      parsed-bind))))

(defmethod parse-query 'rel [this] (parse-rel this))
(defmethod parse-unify-clause 'rel [this] (parse-rel this))

(defrecord Unnest [unnest-binding]
  XtqlQuery$QueryTail
  XtqlQuery$UnifyClause
  UnparseQueryTail (unparse-query-tail [_] (list 'unnest (unparse-col-spec unnest-binding)))
  UnparseUnifyClause (unparse-unify-clause [_] (list 'unnest (unparse-var-spec unnest-binding))))

(defn check-unnest [binding unnest]
  (when-not (and (= 2 (count unnest))
                 (map? binding)
                 (= 1 (count binding)))
    (throw (err/illegal-arg :xtql/unnest {:unnest unnest ::err/message "Unnest takes only a single binding"}))))

(defmethod parse-query-tail 'unnest [[_ binding :as this]]
  (check-unnest binding this)
  (->Unnest (first (parse-col-specs binding this))))

(defmethod parse-unify-clause 'unnest [[_ binding :as this]]
  (check-unnest binding this)
  (->Unnest (first (parse-var-specs binding this))))

(defrecord UnionAll [union-all-queries]
  XtqlQuery
  UnparseQuery (unparse-query [_] (list* 'union-all (mapv unparse-query union-all-queries))))

(defmethod parse-query 'union-all [[_ & queries :as this]]
  (when (> 1 (count queries))
    (throw (err/illegal-arg :xtql/malformed-union {:union this
                                                   :message "Union must contain a least one sub query"})))
  (->UnionAll (mapv parse-query queries)))

(extend-protocol Unparse
  Expr$Null (unparse [_] nil)
  Expr$LogicVar (unparse [e] (symbol (.lv e)))
  Expr$Param (unparse [e] (symbol (.v e)))
  Expr$Call (unparse [e] (list* (symbol (.f e)) (mapv unparse (.args e))))
  Expr$Bool (unparse [e] (.bool e))
  Expr$Double (unparse [e] (.dbl e))
  Expr$Long (unparse [e] (.lng e))
  Expr$ListExpr (unparse [v] (mapv unparse (.elements v)))
  Expr$SetExpr (unparse [s] (into #{} (map unparse (.elements s))))
  Expr$MapExpr (unparse [m] (-> (.elements m)
                                (update-keys keyword)
                                (update-vals unparse)))

  Expr$Obj
  (unparse [e] (.obj e))

  Expr$Get
  (unparse [e]
    (list '. (unparse (.expr e)) (symbol (.field e))))

  Expr$Exists
  (unparse [e]
    (list* 'exists? (unparse-query (.query e))
           (when-let [args (.args e)]
             [{:args (mapv unparse-arg-spec args)}])))


  Expr$Subquery
  (unparse [e]
    (list* 'q (unparse-query (.query e))
           (when-let [args (.args e)]
             [{:args (mapv unparse-arg-spec args)}])))

  Expr$Pull
  (unparse [e]
    (list* 'pull (unparse-query (.query e))
           (when-let [args (.args e)]
             [{:args (mapv unparse-arg-spec args)}])))

  Expr$PullMany
  (unparse [e]
    (list* 'pull* (unparse-query (.query e))
           (when-let [args (.args e)]
             [{:args (mapv unparse-arg-spec args)}]))))