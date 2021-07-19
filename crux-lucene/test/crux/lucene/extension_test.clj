(ns crux.lucene.extension-test
  (:require [clojure.test :as t]
            [crux.api :as c]
            [crux.fixtures :as fix :refer [*api* submit+await-tx]]
            [crux.lucene :as l]))

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
