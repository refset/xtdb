(ns xtdb.nippy-test
  (:require  [clojure.test :as t]
             [xtdb.memory :as mem]
             [xtdb.nippy-utils :as nu]))

(t/deftest test-sanity-check
  (t/is (= #{[(.getBytes "xt/id") (.getBytes "ivan")]
             [(.getBytes "name") (.getBytes "Ivan")]}
           (set (nu/doc->kvs (mem/->nippy-buffer {:xt/id :ivan :name "Ivan"}))))))
