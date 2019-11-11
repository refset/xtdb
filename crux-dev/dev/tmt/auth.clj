(ns tmt.auth
  (:require
    [crux.api :as c]
    [crux.auth :as a]
    [crux.auth.admin :as aa]
    [crux.auth.setup :as as]
    [dev]))

#_(c/submit-tx node [[:crux.tx/put {:crux.db/id :person/tmt}]])
(aa/add-user dev/node "tmt")

#_(c/q (c/db dev/node) {:find ['p] :where [['p :crux.db/id :test2]] :full-results? true})
(c/submit-tx dev/node [[:crux.tx/put {:crux.db/id :test2
                                      :val :yo}]
                       [:crux.tx/put {:nothing :test1}]])

