(ns tmt.auth
  (:require
    [crux.api :as c]
    [crux.auth :as a]
    [crux.auth.admin :as aa]
    [crux.auth.setup :as as]
    [dev]))

#_(c/submit-tx node [[:crux.tx/put {:crux.db/id :person/tmt}]])
(aa/add-user dev/node "tmt")

(c/submit-tx dev/node [[:crux.tx/put {:crux.db/id :test2
                                      :val :yo}]
                       [:crux.tx/put {:crux.db/id :test3
                                      :val :other}]])

(a/q {} (c/db dev/node) {:find ['p] :where [['p :crux.db/id :test2]
                                            ['z :crux.db/id '_]]})

(def que {:find ['e] :where [['e :crux.db/id :test2]
                             ['z :crux.db/id '_]
                             ['z :val :yo]]})

((fn [query] {:find (vec (reduce #(conj %1 (first %2)) #{} (:where query))) :where (:where query)})
 que)


(mapv first (:where que))

(into #{} 3)
