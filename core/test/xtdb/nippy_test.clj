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
  (let [map->eavs (fn map->eavs [m]
                    (let [e (:crux.db/id m)]
                      (reduce (fn [acc [a v]]
                                (apply conj acc (for [v (c/vectorize-value v)]
                                                  (into [e] [a v]))))
                              [] m)))
        m {:name "Ivan"
           :att1 #{:foo #{:bar {:baz :qux}}}
           :att2 {:foo {:bar :baz}}
           :crux.db/id :ivan
           :att3 nil}]
    (t/is (= (sort (map (fn [[e a v]]
                          [(mem/buffer->hex (c/->value-buffer e))
                           (mem/buffer->hex (c/->id-buffer a))
                           (mem/buffer->hex (c/->value-buffer v))])
                        (map->eavs m)))
             (let [bufs (atom [])]
               (-> m
                   mem/->nippy-buffer
                   (nu/doc-kv-visit (fn [e a v]
                                      (swap! bufs conj (doall (mapv mem/buffer->hex [e a v]))))))
               (sort @bufs))))))
