(ns crux.lucene.extension-test
  (:require [clojure.test :as t]
            [crux.api :as c]
            [crux.fixtures :as fix :refer [*api* submit+await-tx]]
            [crux.lucene :as l]))

;; during development, when re-evaluating crux.lucene, make sure to also re-evaluate crux.lucene.multi-field to avoid protocol errors

(t/use-fixtures :each (fix/with-opts {:av {:crux/module 'crux.lucene/->lucene-store
                                           :analyzer 'crux.lucene/->analyzer
                                           :indexer 'crux.lucene/->indexer}
                                      :multi {:crux/module 'crux.lucene/->lucene-store
                                              :analyzer 'crux.lucene/->analyzer
                                              :indexer 'crux.lucene.multi-field/->indexer}})
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
                    {:lucene-store-k :av}
                    "Fred")))))

(t/deftest test-limits
  (submit+await-tx (for [n (range 1000)] [:crux.tx/put {:crux.db/id n, :description (str "Entity " n)}]))
  (submit+await-tx (for [n (range 400)] [:crux.tx/put {:crux.db/id n, :description (str "Entity v2 " n)}]))
  (with-open [db (c/open-db *api*)]
    (t/is (= 0 (count (c/q db {:find '[?e]
                               :where '[[(text-search :description "Entity*" {:lucene-store-k :av
                                                                              :raw-limit 0
                                                                              :result-limit 0}) [[?e]]]]}))))
    (t/is (> 500 (count (c/q db {:find '[?e]
                                 :where '[[(text-search :description "Entity*" {:lucene-store-k :av
                                                                                :raw-limit 500
                                                                                :result-limit 500}) [[?e]]]]}))))
    (t/is (= 500 (count (c/q db {:find '[?e]
                                 :where '[[(text-search :description "Entity*" {:lucene-store-k :av
                                                                                :raw-limit 1400
                                                                                :result-limit 500}) [[?e]]]]}))))))
