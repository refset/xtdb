(ns xtdb.nippy-test
  (:require  [clojure.test :as t]
             [xtdb.memory :as mem]
             [xtdb.nippy-fork :as nippyf]
             [xtdb.nippy-utils :as nu]))

(t/deftest test-sanity-check
  (t/is (= {:xt/id "ivan" :name "Ivan"}
           (->> {:xt/id "ivan" :name "Ivan"}
                mem/->nippy-buffer
                nu/doc->kvs
                (map (fn [[^"[B" kb ^"[B" vb]]
                       ;; NOTE: hardcoded for string values
                       [(keyword (String. kb)) (String. vb)]))
                (into {})))))
