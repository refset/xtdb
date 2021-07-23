(remove-ns 'crux.lucene.extension-test)
(ns crux.lucene.extension-test
  (:require [clojure.test :as t]
            [clojure.spec.alpha :as s]
            [crux.api :as c]
            [crux.codec :as cc]
            [crux.query :as q]
            [crux.system :as sys]
            [crux.fixtures :as fix :refer [*api* submit+await-tx]]
            [crux.lucene :as l]
            [crux.api :as crux])
  (:import java.io.Closeable
           crux.lucene.DocumentId
           crux.query.VarBinding
           [org.apache.lucene.analysis.core KeywordAnalyzer KeywordTokenizerFactory LowerCaseFilterFactory]
           [org.apache.lucene.analysis.custom CustomAnalyzer]
           org.apache.lucene.analysis.Analyzer
           org.apache.lucene.analysis.standard.StandardAnalyzer
           [org.apache.lucene.document Document Field$Store StringField TextField]
           [org.apache.lucene.index IndexWriter IndexWriterConfig KeepOnlyLastCommitDeletionPolicy SnapshotDeletionPolicy Term]
           org.apache.lucene.queries.function.FunctionScoreQuery
           org.apache.lucene.queryparser.classic.QueryParser
           [org.apache.lucene.search BooleanClause$Occur BooleanQuery$Builder DoubleValuesSource IndexSearcher Query ScoreDoc SearcherManager TermQuery TopDocs]
           [org.apache.lucene.store ByteBuffersDirectory FSDirectory IOContext]))

(require 'crux.lucene.multi-field :reload) ; for dev, due to changing protocols

(declare ->custom-index-writer-analyzer)
(declare ->custom-ave-indexer)

(t/use-fixtures :each (fix/with-opts {:ave {:crux/module 'crux.lucene/->lucene-store
                                            :analyzer 'crux.lucene/->analyzer
                                            :indexer 'crux.lucene/->indexer}
                                      :multi {:crux/module 'crux.lucene/->lucene-store
                                              :analyzer 'crux.lucene/->analyzer
                                              :indexer 'crux.lucene.multi-field/->indexer}
                                      :custom {:crux/module 'crux.lucene/->lucene-store
                                               :analyzer 'crux.lucene.extension-test/->custom-index-writer-analyzer
                                               :indexer 'crux.lucene.extension-test/->custom-ave-indexer}})
  fix/with-node)

(t/deftest test-multiple-lucene-stores
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan
                                   :firstname "Fred"
                                   :surname "Smith"}]])

  (with-open [db (c/open-db *api*)]
    (t/is (seq (c/q db '{:find [?e]
                         :in [$l1 ?fred]
                         :where [[(text-search :firstname "Fre*" $l1) [[?e]]]
                                 [(lucene-text-search "firstname:%s AND surname:%s" ?fred "Smith" {:lucene-store-k :multi}) [[?e]]]]}
                    {:lucene-store-k :ave}
                    "Fred")))))

(defn custom-pred-constraint [query-builder results-resolver {:keys [arg-bindings idx-id return-type tuple-idxs-in-join-order ::l/lucene-store] :as pred-ctx}]
  (fn custom-pred-lucene-constraint [index-snapshot db idx-id->idx join-keys]
    (let [arg-bindings (map (fn [a]
                              (if (instance? VarBinding a)
                                (q/bound-result-for-var index-snapshot a join-keys)
                                a))
                            (rest arg-bindings))
          {:keys [result-limit lucene-store-k] :as opts} (let [m (last arg-bindings)]
                                                                     (when (map? m)
                                                                       m))
          arg-bindings (if opts
                         (butlast arg-bindings)
                         arg-bindings)
          lucene-store (or (get pred-ctx lucene-store-k)
                           lucene-store)
          query (query-builder arg-bindings)
          tuples (with-open [search-results ^crux.api.ICursor (l/search {:!system (atom {(or lucene-store-k ::l/lucene-store) lucene-store})} query {:lucene-store-k lucene-store-k})]
                   (-> search-results
                       iterator-seq
                       (->> (results-resolver index-snapshot db))
                       (cond->> result-limit (take result-limit))
                       (->> (into []))))]
      (q/bind-binding return-type tuple-idxs-in-join-order (get idx-id->idx idx-id) tuples))))

(defmethod q/pred-args-spec 'custom-text-search-limit [_]
  (s/cat :pred-fn  #{'custom-text-search-limit}
         :args (s/spec
                (s/cat :attr keyword?
                       :v (some-fn string? q/logic-var?)
                       :opts (s/? (some-fn map? q/logic-var?))))
         :return (s/? :crux.query/binding)))

(defmethod q/pred-constraint 'custom-text-search-limit [_ pred-ctx]
  (let [resolver (partial l/resolve-search-results-a-v (second (:arg-bindings pred-ctx)))]
    (custom-pred-constraint #'l/build-query resolver pred-ctx)))

(t/deftest test-custom-limit-predicate
  (submit+await-tx (for [n (range 1000)] [:crux.tx/put {:crux.db/id n, :description (str "Entity " n)}]))
  (submit+await-tx (for [n (range 400)] [:crux.tx/put {:crux.db/id n, :description (str "Entity v2 " n)}]))
  (with-open [db (c/open-db *api*)]
    (t/is (= 0 (count
                (c/q db {:find '[?e]
                         :where '[[(custom-text-search-limit :description "Entity*" {:lucene-store-k :ave
                                                                                     :result-limit 0}) [[?e]]]]}))))
    (t/is (= 500 (count (c/q db {:find '[?e]
                                 :where '[[(custom-text-search-limit :description "Entity*" {:lucene-store-k :ave
                                                                                             :result-limit 500}) [[?e]]]]}))))))

(defn userspace-text-search-iterator [db lucene-store-k a v raw-limit result-limit]
  (c/open-q db {:find '[e v s]
                :limit result-limit
                :in '[[r ...]]
                :where ['[(identity r) [v s]]
                        ['e a 'v]]}
            (with-open [s (l/search *api* v {:default-field (subs (str a) 1)
                                             :lucene-store-k lucene-store-k})]
              (->> (iterator-seq s)
                   (take raw-limit)
                   (map (fn doc->rel [[^Document doc score]]
                          [(.get ^Document doc l/field-crux-val) score]))
                   (into [])))))

(defn userspace-text-search [& args]
  (with-open [s ^Closeable (apply userspace-text-search-iterator args)]
    (into [] (iterator-seq s))))

(t/deftest test-userspace-custom-limit-predicate
  (submit+await-tx (for [n (range 1000)] [:crux.tx/put {:crux.db/id n, :description (str "Entity " n)}]))
  (submit+await-tx (for [n (range 400)] [:crux.tx/put {:crux.db/id n, :description (str "Entity v2 " n)}]))
  (with-open [db (c/open-db *api*)]
    (t/is (> 500 (count
                  (c/q db {:find '[?e]
                           :where '[[(crux.lucene.extension-test/userspace-text-search $ :ave :description "Entity*" 500 500) [[?e]]]]}))))
    (t/is (= 500 (count
                  (c/q db {:find '[?e]
                           :where '[[(crux.lucene.extension-test/userspace-text-search $ :ave :description "Entity*" 1400 500) [[?e]]]]}))))))

(def ^:const field-crux-val-exact "_crux_val_exact")

(defn ^String keyword->kcs [k]
  (subs (str k "-exact") 1))

(defrecord CustomLuceneAveIndexer [index-store]
  l/LuceneIndexer

  (index! [_ index-writer docs]
    (doseq [{e :crux.db/id, :as crux-doc} (vals docs)
            [a ^String v] (->> (dissoc crux-doc :crux.db/id)
                       (mapcat (fn [[a v]]
                                 (for [v (cc/vectorize-value v)
                                       :when (string? v)]
                                   [a v]))))
            :let [id-str (l/->hash-str (l/->DocumentId e a v))
                  doc (doto (Document.)
                        ;; To search for triples by e-a-v for deduping
                        (.add (StringField. l/field-crux-id, id-str, Field$Store/NO))
                        ;; The actual term, which will be tokenized
                        (.add (TextField. (l/keyword->k a), v, Field$Store/YES))
                        ;; Custom - the actual term, to be used for exact matches
                        (.add (StringField. (keyword->kcs a), v, Field$Store/YES))
                        ;; Custom - used for wildcard searches (case-insensitive)
                        (.add (TextField. l/field-crux-val, v, Field$Store/YES))
                        ;; Custom - used for wildcard searches (case-sensitive)
                        (.add (StringField. field-crux-val-exact, v, Field$Store/YES))
                        ;; Used for eviction
                        (.add (TextField. l/field-crux-eid, (l/->hash-str e), Field$Store/YES))
                        ;; Used for wildcard searches
                        (.add (StringField. l/field-crux-attr, (l/keyword->k a), Field$Store/YES)))]]
      (.updateDocument ^IndexWriter index-writer (Term. l/field-crux-id id-str) doc)))

  (evict! [_ index-writer eids]
    (let [qs (for [eid eids]
               (TermQuery. (Term. l/field-crux-eid (l/->hash-str eid))))]
      (.deleteDocuments ^IndexWriter index-writer ^"[Lorg.apache.lucene.search.Query;" (into-array Query qs)))))

(defn ->custom-ave-indexer
  {::sys/deps {:index-store :crux/index-store}}
  [{:keys [index-store]}]
  (CustomLuceneAveIndexer. index-store))

;; Index and query in a case-insensitive way
(defn ^Analyzer ->custom-index-writer-analyzer
  [_]
  (.build (doto (CustomAnalyzer/builder)
            (.withTokenizer ^String KeywordTokenizerFactory/NAME ^"[Ljava.lang.String;" (into-array String []))
            (.addTokenFilter ^String LowerCaseFilterFactory/NAME ^"[Ljava.lang.String;" (into-array String [])))))

(defn ^Query build-query-text-search-prefix-wildcard
  "Standard build query fn (case-insensitive), taking a single field/val lucene term string."
  [[k v]]
  (when-not (string? v)
    (throw (IllegalArgumentException. "Lucene text search values must be String")))
  (let [qp (doto (QueryParser. (l/keyword->k k) (->custom-index-writer-analyzer nil)) (.setAllowLeadingWildcard true))
        b (doto (BooleanQuery$Builder.)
            (.add (.parse qp v) BooleanClause$Occur/MUST))]
    (.build b)))

(defmethod q/pred-args-spec 'text-search-prefix-wildcard [_]
  (s/cat :pred-fn  #{'text-search-prefix-wildcard} :args (s/spec (s/cat :attr keyword? :v (some-fn string? symbol?) :opts (s/? (some-fn map? q/logic-var?)))) :return (s/? :crux.query/binding)))

(defmethod q/pred-constraint 'text-search-prefix-wildcard [_ pred-ctx]
  (let [resolver (partial l/resolve-search-results-a-v (second (:arg-bindings pred-ctx)))]
    (l/pred-constraint #'build-query-text-search-prefix-wildcard resolver pred-ctx)))

(t/deftest test-custom-index-writer-analyzer
  (submit+await-tx [[:crux.tx/put {:crux.db/id 0, :description "Some Entity"}]])
  (with-open [db (c/open-db *api*)]
    (t/is (seq (c/q db {:find '[?e]
                         :where '[[(text-search-prefix-wildcard :description "*en*" {:lucene-store-k :custom}) [[?e]]]]})))))

;; For case-sensitive queries
(defn ^Analyzer ->custom-query-analyzer
  [_]
  (KeywordAnalyzer.))

(defn ^Query build-query-text-search-case-sensitive
  "Standard build query fn (case-sensitive), taking a single field/val lucene term string."
  [[k v]]
  (when-not (string? v)
    (throw (IllegalArgumentException. "Lucene text search values must be String")))
  (let [qp (doto (QueryParser. (keyword->kcs k) (->custom-query-analyzer nil)) (.setAllowLeadingWildcard true))
        b (doto (BooleanQuery$Builder.)
            (.add (.parse qp v) BooleanClause$Occur/MUST))]
    (.build b)))

(defmethod q/pred-args-spec 'text-search-case-sensitive [_]
  (s/cat :pred-fn  #{'text-search-case-sensitive} :args (s/spec (s/cat :attr keyword? :v (some-fn string? symbol?) :opts (s/? (some-fn map? q/logic-var?)))) :return (s/? :crux.query/binding)))

(defmethod q/pred-constraint 'text-search-case-sensitive [_ pred-ctx]
  (let [resolver (partial l/resolve-search-results-a-v (second (:arg-bindings pred-ctx)))]
    (l/pred-constraint #'build-query-text-search-case-sensitive resolver pred-ctx)))

(t/deftest test-custom-query-analyzer
  (submit+await-tx [[:crux.tx/put {:crux.db/id 0, :description "Some Entity"}]])
  (with-open [db (c/open-db *api*)]
    (t/is (seq (c/q db {:find '[?e]
                        :where '[[(text-search-case-sensitive :description "*En*" {:lucene-store-k :custom}) [[?e]]]]})))
    (t/is (empty? (c/q db {:find '[?e]
                           :where '[[(text-search-case-sensitive :description "*en*" {:lucene-store-k :custom}) [[?e]]]]})))))

(defn ^Query build-query-wildcard-case-sensitive
  "Wildcard query builder (case-sensitive)"
  [[v]]
  (when-not (string? v)
    (throw (IllegalArgumentException. "Lucene text search values must be String")))
  (let [qp (doto (QueryParser. field-crux-val-exact (->custom-query-analyzer nil)) (.setAllowLeadingWildcard true))
        b (doto (BooleanQuery$Builder.)
                (.add (.parse qp v) BooleanClause$Occur/MUST))]
    (.build b)))

(defmethod q/pred-args-spec 'wildcard-case-sensitive [_]
  (s/cat :pred-fn #{'wildcard-case-sensitive} :args (s/spec (s/cat :v string? :opts (s/? (some-fn map? q/logic-var?)))) :return (s/? :crux.query/binding)))

(defmethod q/pred-constraint 'wildcard-case-sensitive [_ pred-ctx]
  (l/pred-constraint #'build-query-wildcard-case-sensitive #'l/resolve-search-results-a-v-wildcard pred-ctx))

(t/deftest test-custom-query-analyzer-wildcard
  (submit+await-tx [[:crux.tx/put {:crux.db/id 0, :description "Some Entity"}]])
  (with-open [db (c/open-db *api*)]
    (t/is (seq (c/q db {:find '[?e ?a]
                        :where '[[(wildcard-case-sensitive "*En*" {:lucene-store-k :custom}) [[?e ?a]]]]})))
    (t/is (empty? (c/q db {:find '[?e ?a]
                           :where '[[(wildcard-case-sensitive "*en*" {:lucene-store-k :custom}) [[?e ?a]]]]})))))
