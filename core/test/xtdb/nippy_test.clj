(ns xtdb.nippy-test
  (:require  [clojure.test :as t]
             [xtdb.memory :as mem]
             [xtdb.nippy-fork :as nippyf]
             [xtdb.nippy-utils :as nu])
  (:import [org.agrona DirectBuffer]))

(t/deftest test-sanity-check
  (t/is (= {:xt/id "ivan" :name "Ivan"}
           (->> {:xt/id "ivan" :name "Ivan"}
                mem/->nippy-buffer
                nu/doc->kvs
                (map (fn [[kb vb]]
                       [(mem/<-nippy-buffer kb)
                        (mem/<-nippy-buffer vb)]))
                (into {})))))

(t/deftest test-nested-map
  (t/is (= {:xt/id "ivan" :name "Ivan" :nested {:a :b}}
           (->> {:xt/id "ivan" :name "Ivan" :nested {:a :b}}
                mem/->nippy-buffer
                nu/doc->kvs
                (map (fn [[kb vb]]
                       [(mem/<-nippy-buffer kb)
                        (mem/<-nippy-buffer vb)]))
                (into {})))))

#_(t/deftest test-find-eid
  (t/is (= :ivan
           (->> {:name "Ivan" :crux.db/id :ivan}
                mem/->nippy-buffer
                nu/doc->eid
                mem/<-nippy-buffer))))

#_(t/deftest test-doc-to-map-of-slices
  (t/is (= {:xt/id "ivan" :name "Ivan" :nested {:a :b}}
           (->> {:xt/id "ivan" :name "Ivan" :nested {:a :b}}
                mem/->nippy-buffer
                nu/doc->kv-slices
                (map (fn [[kb vb]]
                       [(mem/<-nippy-buffer kb)
                        (mem/<-nippy-buffer vb)]))
                (into {})))))
