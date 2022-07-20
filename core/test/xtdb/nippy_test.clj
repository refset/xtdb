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
  (t/is (= (map (fn [[e a v]]
                  [(mem/buffer->hex (c/->value-buffer e))
                   (mem/buffer->hex (c/->id-buffer a))
                   (mem/buffer->hex (c/->value-buffer v))])
                [[:ivan :name "Ivan"]
                 [:ivan :att1 :foo]
                 [:ivan :att1 #{{:baz :qux} :bar}]
                 [:ivan :att2 {:foo {:bar :baz}}]
                 [:ivan :crux.db/id :ivan]
                 [:ivan :att3 :val]])
           (let [bufs (atom [])]
             (-> {:name "Ivan"
                  :att1 #{:foo #{:bar {:baz :qux}}}
                  :att2 {:foo {:bar :baz}}
                  :crux.db/id :ivan
                  :att3 :val}
                 mem/->nippy-buffer
                 (nu/doc-kv-visit (fn [e a v]
                                    (swap! bufs conj (doall (map mem/buffer->hex [e a v]))))))
             @bufs))))
