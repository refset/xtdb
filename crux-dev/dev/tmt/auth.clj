(ns tmt.auth
  (:require
    [crux.api :as c]
    [crux.auth :as a]
    [crux.auth.admin :as aa]
    [crux.auth.setup :as as]))

(def node (c/start-node {:crux.node/topology :crux.standalone/topology
                         :crux.node/kv-store "crux.kv.memdb/kv"
                         :crux.kv/db-dir "data/db-dir-1"
                         :crux.standalone/event-log-dir "data/eventlog-1"
                         :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"}))


#_(c/submit-tx node [[:crux.tx/put {:crux.db/id :person/tmt}]])
(aa/add-user node "tmt")

(c/submit-tx node [[:crux.tx/put {:crux.db/id :test2
                                      :val :yo}]
                       [:crux.tx/put {:crux.db/id :test3
                                      :val :other}]])

(a/q {:crux.auth/user :crux.auth.user/tmt}
     (c/db node)
     {:find ['p] :where [['p :crux.db/id :test2]
                         ['z :crux.db/id '_]]})

(def que {:find ['e] :where [['e :crux.db/id :test2]
                             ['z :crux.db/id '_]
                             ['z :val :yo]]})

(defn- tag-values
  [query]
  {:find (vec (reduce #(conj %1 (first %2)) #{} (:where query)))
   :where (:where query)})

((fn [query] (assoc query :where (concat (:where query)
                                         (mapv (fn [el] [(gensym) :crux.auth/doc el]) (reduce #(conj %1 (first %2)) #{} (:where query))))))

 que)

(a/get-auth-doc nil que)
(take 10 (repeat `test#))


(mapv first (:where que))

(into #{} 3)

(char 2)

(c/q (c/db node) {:find ['user] :where [['user :crux.db/id '_]
                                        ['user :val (println)]] :full-results? true})

