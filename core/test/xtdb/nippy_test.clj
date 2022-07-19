(ns xtdb.nippy-test
  (:require  [clojure.test :as t]
             [xtdb.codec :as c]
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

(t/deftest test-kv-visit
  (t/is (= [[:ivan (mem/buffer->hex (c/->id-buffer :name)) "Ivan"]
            [:ivan (mem/buffer->hex (c/->id-buffer :att1)) :foo]
            [:ivan (mem/buffer->hex (c/->id-buffer :att1)) #{{:baz :qux} :bar}]
            [:ivan (mem/buffer->hex (c/->id-buffer :att2)) {:foo {:bar :baz}}]
            [:ivan (mem/buffer->hex (c/->id-buffer :crux.db/id)) :ivan]
            [:ivan (mem/buffer->hex (c/->id-buffer :att3)) :val]]
           (let [bufs (atom [])]
             (-> {:name "Ivan"
                  :att1 #{:foo #{:bar {:baz :qux}}}
                  :att2 {:foo {:bar :baz}}
                  :crux.db/id :ivan
                  :att3 :val}
                 mem/->nippy-buffer
                 (nu/doc-kv-visit (fn [e a v]
                                    (swap! bufs conj [(mem/<-nippy-buffer e)
                                                      (mem/buffer->hex a)
                                                      (mem/<-nippy-buffer v)]))))
             @bufs))))
