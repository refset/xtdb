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
                (map (fn [[^DirectBuffer kb ^DirectBuffer vb]]
                       ;; NOTE: hardcoded for string values
                       [(mem/<-nippy-buffer kb)
                        (mem/<-nippy-buffer vb)]))
                (into {})))))
