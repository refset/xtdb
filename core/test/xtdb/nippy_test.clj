(ns xtdb.nippy-test
  (:require  [clojure.test :as t]
             [xtdb.memory :as mem]
             [xtdb.nippy-fork :as nippyf]
             [xtdb.nippy-utils :as nu])
  (:import [org.agrona DirectBuffer]))

(t/deftest test-find-eid
  (t/is (= :ivan
           (->> {:name "Ivan" :att1 #{:foo #{:bar {:baz :qux}}} :att2 {:foo {:bar :baz}} :crux.db/id :ivan :att3 :val}
                mem/->nippy-buffer
                (#(let [[offset len] (nu/doc->eid-offset-and-len %)]
                    (mem/slice-buffer % offset len)))
                mem/<-nippy-buffer))))
