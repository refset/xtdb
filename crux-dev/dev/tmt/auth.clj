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

(c/submit-tx node [[:crux.tx/put {:crux.db/id :this
                   :val [:one :two]}]])


(c/q (c/db node) {:find ['e 'v]
                  :where [['e :crux.db/id :this]
                          ['e :val 'v]]
                  :full-results? true})


(aa/add-user node "root" {:crux.auth/permissions [:r :w]})

(aa/add-user node "tmt")

#_(aa/get-auth-doc :crux.auth.user/tmt
                   {:find ['k 'n]
                    :where [['p :person/ident :person/tmt]
                            ['p :person/nok 'k]
                            ['k :nok/name 'n]]}
                   :ice)



(.close node)
