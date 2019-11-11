(ns tmt.auth
  (:require
    [crux.api :as c]
    [crux.auth :as a]
    [crux.auth.admin :as aa]
    [crux.auth.setup :as as]
    [dev]))

#_(c/submit-tx node [[:crux.tx/put {:crux.db/id :person/tmt}]])
(aa/add-user dev/node :person/tmt)
