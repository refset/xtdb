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

(aa/add-admin node "root" {:crux.auth/permissions [:r :w]})

(def tmtdoc {:crux.db/id :person/tmt
             :person/dob #inst "1996-06-21"
             :person/nok :person.nok/tmt})
(def tmtnok {:crux.db/id :person.nok/tmt
             :nok/name "Eve"
             :nok/phone "+4479138382672"})

(a/submit-tx {:crux.auth/user :crux.auth.user/root}
             node
             [[:crux.tx/put
               :crux.auth.user/tmt
               tmtdoc]])

(a/alter-put {:crux.auth.user/root} (c/db node) )

#_(aa/get-auth-doc :crux.auth.user/tmt
                   {:find ['k 'n]
                    :where [['p :person/ident :person/tmt]
                            ['p :person/nok 'k]
                            ['k :nok/name 'n]]}
                   :ice)

(merge {:me "tom"
        :you "test"}
       {:me "you"})

(.close node)

(concat [:test] nil nil)
