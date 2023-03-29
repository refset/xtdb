(ns xtdb.tx-fn-test
  (:require [clojure.test :as t]
            [xtdb.datalog :as xt]
            [xtdb.node :as node]
            [xtdb.indexer :as idx]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest test-call-tx-fn
  (t/testing "simple call"
    (let [_tx (xt/submit-tx tu/*node* [[:put 'xt_docs {:id :my-fn,
                                                       :fn #xt/clj-form (fn [id n]
                                                                          [[:put 'xt_docs {:id id, :n n}]])}]
                                       [:call :my-fn :foo 0]
                                       [:call :my-fn :bar 1]])]
      (t/is (= [{:id :foo, :n 0}
                {:id :bar, :n 1}]
               (xt/q tu/*node*
                     '{:find [id n]
                       :where [(match xt_docs [id])
                               [id :n n]]})))))

  (t/testing "nested tx fn"
    (let [_tx (xt/submit-tx tu/*node* [[:put 'xt_docs {:id :inner-fn,
                                                       :fn #xt/clj-form (fn [id]
                                                                          [[:put 'xt_docs {:id (keyword (str (name id) "-inner")), :from :inner}]])}]
                                       [:put 'xt_docs {:id :outer-fn,
                                                       :fn #xt/clj-form (fn [id]
                                                                          [[:call :inner-fn id]
                                                                           [:put 'xt_docs {:id (keyword (str (name id) "-outer")), :from :outer}]])}]
                                       [:call :inner-fn :foo]
                                       [:call :outer-fn :bar]])]
      (t/is (= [{:id :foo-inner, :from :inner}
                {:id :bar-inner, :from :inner}
                {:id :bar-outer, :from :outer}]
               (xt/q tu/*node*
                     '{:find [id from]
                       :where [(match xt_docs [id])
                               [id :from from]]}))))))

(t/deftest test-tx-fn-return-values
  (xt/submit-tx tu/*node* [[:put 'xt_docs {:id :identity,
                                           :fn #xt/clj-form identity}]])

  (letfn [(run-test [ret-val put-id]
            (let [tx (xt/submit-tx tu/*node* [[:call :identity ret-val]
                                              [:put 'xt_docs {:id put-id}]])]
              (->> (xt/q tu/*node*
                         '{:find [id]
                           :in [id]
                           :where [(match xt_docs [id])
                                   [id :id]]}
                         put-id)
                   (into #{} (map :id)))))]

    (t/is (= #{:empty-list} (run-test [] :empty-list)))
    (t/is (= #{} (run-test false :false)))
    (t/is (= #{:nil} (run-test nil :nil)))
    (t/is (= #{:true} (run-test true :true)))))

(t/deftest test-tx-fn-q
  (let [_tx (xt/submit-tx tu/*node* [[:put 'xt_docs {:id :doc-counter,
                                                     :fn #xt/clj-form (fn [id]
                                                                        (let [doc-count (count (q '{:find [id]
                                                                                                    :where [(match xt_docs [id])
                                                                                                            [id :id]]}))]
                                                                          [[:put 'xt_docs {:id id, :doc-count doc-count}]]))}]
                                     [:call :doc-counter :foo]
                                     [:call :doc-counter :bar]])]
    (t/is (= [{:id :foo, :doc-count 1}
              {:id :bar, :doc-count 2}]
             (xt/q tu/*node*
                   '{:find [id doc-count]
                     :where [(match xt_docs [id])
                             [id :doc-count doc-count]]}))))

  (let [_tx2 (xt/submit-tx tu/*node* [[:put 'xt_docs {:id :petr :balance 100}]
                                      [:put 'xt_docs {:id :ivan :balance 200}]
                                      [:put 'xt_docs {:id :update-balance,
                                                      :fn #xt/clj-form (fn [id]
                                                                         (let [[account] (q '{:find [id balance]
                                                                                              :in [id]
                                                                                              :where [(match xt_docs [id])
                                                                                                      [id :balance balance]]}
                                                                                            id)]
                                                                           (if account
                                                                             [[:put 'xt_docs (update account :balance inc)]]
                                                                             [])))}]
                                      [:call :update-balance :petr]
                                      [:call :update-balance :undefined]])]
    (t/is (= #{{:id :petr, :balance 101}
               {:id :ivan, :balance 200}}
             (set (xt/q tu/*node*
                        '{:find [id balance]
                          :where [(match xt_docs [id])
                                  [id :balance balance]]})))
          "query in tx-fn with in-args")))

(t/deftest test-tx-fn-sql-q
  (let [_tx (xt/submit-tx tu/*node* [[:put 'xt_docs {:id :doc-counter,
                                                     :fn #xt/clj-form (fn [id]
                                                                        (let [[{doc-count :doc_count}] (sql-q "SELECT COUNT(*) doc_count FROM xt_docs")]
                                                                          [[:put 'xt_docs {:id id, :doc-count doc-count}]]))}]
                                     [:call :doc-counter :foo]
                                     [:call :doc-counter :bar]])]
    (t/is (= [{:id :foo, :doc-count 1}
              {:id :bar, :doc-count 2}]
             (xt/q tu/*node*
                   '{:find [id doc-count]
                     :where [(match xt_docs [id])
                             [id :doc-count doc-count]]})))))

(t/deftest test-tx-fn-current-tx
  (let [{tt0 :sys-time} (xt/submit-tx tu/*node* [[:put 'xt_docs {:id :with-tx
                                                                 :fn #xt/clj-form (fn [id]
                                                                                    [[:put 'xt_docs (into {:id id} *current-tx*)]])}]
                                                 [:call :with-tx :foo]
                                                 [:call :with-tx :bar]])

        {tt1 :sys-time, :as _tx1} (xt/submit-tx tu/*node* [[:call :with-tx :baz]])]

    (t/is (= [{:id :foo, :tx-id 0, :sys-time (util/->zdt tt0)}
              {:id :bar, :tx-id 0, :sys-time (util/->zdt tt0)}
              {:id :baz, :tx-id 1, :sys-time (util/->zdt tt1)}]
             (xt/q tu/*node*
                   '{:find [id tx-id sys-time]
                     :where [(match xt_docs [id])
                             [id :tx-id tx-id]
                             [id :sys-time sys-time]]})))))

(t/deftest test-tx-fn-exceptions
  (letfn [(foo-version [_tx]
            (-> (xt/q tu/*node*
                      '{:find [id version],
                        :where [(match xt_docs [id])
                                [id :version version]]})
                first :version))]

    (let [tx (xt/submit-tx tu/*node* [[:put 'xt_docs {:id :assoc-version
                                                      :fn #xt/clj-form (fn [version]
                                                                         [[:put 'xt_docs {:id :foo, :version version}]])}]
                                      [:call :assoc-version 0]])]
      (t/is (= 0 (foo-version tx))))

    (t/testing "non existing tx fn"
      (let [tx (xt/submit-tx tu/*node* '[[:call :non-existing-fn]
                                         [:call :assoc-version :fail]])]
        (t/is (= 0 (foo-version tx)))
        (t/is (thrown-with-msg? RuntimeException
                                #":xtdb.call/no-such-tx-fn"
                                (some-> (idx/reset-tx-fn-error!) throw)))))

    (t/testing "invalid results"
      (xt/submit-tx tu/*node* [[:put 'xt_docs {:id :invalid-fn
                                               :fn #xt/clj-form (fn []
                                                                  [[:foo]])}]])
      (let [tx (xt/submit-tx tu/*node* '[[:call :invalid-fn]
                                         [:call :assoc-version :fail]])]
        (t/is (= 0 (foo-version tx))))

      (t/is (thrown-with-msg? IllegalArgumentException #"Invalid tx op"
                              (some-> (idx/reset-tx-fn-error!) throw))))

    (t/testing "no :fn"
      (xt/submit-tx tu/*node* [[:put 'xt_docs {:id :no-fn}]])

      (let [tx (xt/submit-tx tu/*node* '[[:call :no-fn]
                                         [:call :assoc-version :fail]])]
        (t/is (= 0 (foo-version tx)))
        (t/is (thrown-with-msg? RuntimeException
                                #":xtdb.call/no-such-tx-fn"
                                (some-> (idx/reset-tx-fn-error!) throw)))))

    (t/testing "not a fn"
      (xt/submit-tx tu/*node* [[:put 'xt_docs {:id :not-a-fn, :fn 0}]])
      (let [tx (xt/submit-tx tu/*node* '[[:call :not-a-fn]
                                         [:call :assoc-version :fail]])]
        (t/is (= 0 (foo-version tx)))

        (t/is (thrown-with-msg? RuntimeException
                                #":xtdb.call/invalid-tx-fn"
                                (some-> (idx/reset-tx-fn-error!) throw)))))

    (t/testing "compilation errors"
      (xt/submit-tx tu/*node* [[:put 'xt_docs {:id :compilation-error-fn
                                               :fn #xt/clj-form (fn [] unknown-symbol)}]])
      (let [tx (xt/submit-tx tu/*node* '[[:call :compilation-error-fn]
                                         [:call :assoc-version :fail]])]
        (t/is (= 0 (foo-version tx)))
        (t/is (thrown-with-msg? RuntimeException #":xtdb.call/error-compiling-tx-fn"
                                (some-> (idx/reset-tx-fn-error!) throw)))))

    (t/testing "exception thrown"
      #_{:clj-kondo/ignore [:unused-value]}
      (xt/submit-tx tu/*node* [[:put 'xt_docs {:id :exception-fn
                                               :fn #xt/clj-form (fn []
                                                                  (/ 1 0)
                                                                  [])}]])

      (let [tx (xt/submit-tx tu/*node* '[[:call :exception-fn]
                                         [:call :assoc-version :fail]])]
        (t/is (= 0 (foo-version tx)))
        (t/is (thrown-with-msg? RuntimeException #":xtdb.call/error-evaluating-tx-fn"
                                (some-> (idx/reset-tx-fn-error!) throw)))))

    (t/testing "still working after all these errors"
      (let [tx (xt/submit-tx tu/*node* [[:call :update-version :done]])]
        (= :done (foo-version tx))))))

(t/deftest handle-interrupted-exception-614
  (t/is (thrown-with-msg?
         Exception #"sleep interrupted"
         @(with-open [node (node/start-node {})]
            (xt/submit-tx node [[:put 'xt_docs {:id :hello-world
                                                :fn #xt/clj-form (fn hello-world [id]
                                                                   (sleep 200)
                                                                   [[:put 'xt_docs {:id id :foo (str id)}]])}]])
            (xt/submit-tx node [[:call :hello-world 1]])

            (Thread/sleep 100)

            (xt/q& node '{:find [id]
                          :where [(match xt_docs [id])
                                  [id :foo]]})))))


(t/deftest test-call-tx-fn-with-ns-attr
  (t/testing "simple call"
    (let [_tx (xt/submit-tx tu/*node* [[:put 'xt_docs {:id :my-fn,
                                                       :fn #xt/clj-form (fn [id n]
                                                                          [[:put 'xt_docs {:id id, :a/b n}]])}]
                                       [:call :my-fn :foo 0]
                                       [:call :my-fn :bar 1]])]
      (t/is (= [{:id :foo, :n 0}
                {:id :bar, :n 1}]
               (xt/q tu/*node*
                     '{:find [id n]
                       :where [(match xt_docs [id])
                               [id :a/b n]]}))))))