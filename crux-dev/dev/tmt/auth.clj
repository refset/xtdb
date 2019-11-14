(ns tmt.auth
    (:require
        [crux.api :as c]
        [crux.auth :as a]
        [crux.auth.admin :as aa]))

(def node (c/start-node {:crux.node/topology :crux.standalone/topology
                         :crux.node/kv-store "crux.kv.memdb/kv"
                         :crux.kv/db-dir "data/db-dir-1"
                         :crux.standalone/event-log-dir "data/eventlog-1"
                         :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"}))

(aa/add-admin node "root" {:crux.auth.user/permissions [:r :w]})

(c/q (c/db node) {:find ['e] :where [['e :crux.db/id '_]] :full-results? true})

(def tmtdoc {:crux.db/id :person/tmt
             :person/dob #inst "1996-06-21"
             :person/nok :person.nok/tmt})
(def tmtnok {:crux.db/id :person.nok/tmt
             :nok/name "Eve"
             :nok/phone "+4479138382672"})

;; where ∄ doc and user has auth
;; alter-put => [[tmtdoc][tmtdoc-auth][tmtdoc-meta]]
(a/submit-tx {:crux.auth/user :crux.auth.user/root}
             node
             [[:crux.tx/put
               :crux.auth.user/tmt
               tmtdoc]])

;; where ∃ doc and user has auth
;; => [[tmtdoc]]
(a/alter-put {:crux.auth/user :crux.auth.user/root}
             (c/db node)
             [:crux.auth.user/tmt tmtdoc])

;; where ∃ doc and user doesn't have auth
;; => []
(a/alter-put {:crux.auth/user :crux.auth.user/nobody}
             (c/db node)
             [:crux.auth.user/tmt tmtdoc])

;; where ∄ doc and user doesn't have auth
;; => []
(a/alter-put {:crux.auth/user :crux.auth.user/nobody}
             (c/db node)
             [:crux.auth.user/tmt tmtnok])

;; Shouldn't submit anything as user doesn't have write auth
;; returns a submit map but if you look at the tap you can see that an empty
;; map is entered to submit-tx
(a/submit-tx {:crux.auth/user :crux.auth.user/nobody}
             node
             [[:crux.tx/put :crux.auth.user/tmt tmtnok]
              [:crux.tx/put :crux.auth.user/tmt tmtdoc]])

;; Succeeds
(a/submit-tx {:crux.auth/user :crux.auth.user/root}
             node
             [[:crux.tx/put :crux.auth.user/tmt tmtnok]])

;; Currently everyone can read
(a/q {:crux.auth/user :crux.auth.user/mal}
     (c/db node)
     {:find ['me-nok]
      :where [['me-nok :crux.db/id :person.nok/tmt]]
      :full-results? true})

;; Restrict nok to just :crux.auth.user/tmt
(a/set-privilage {:crux.auth/user :crux.auth.user/tmt}
                 node
                 :person.nok/tmt
                 {:crux.auth/read [:crux.auth.user/tmt]})

;; Now mal gets no results
(a/q {:crux.auth/user :crux.auth.user/mal}
     (c/db node)
     {:find ['me-nok]
      :where [['me-nok :crux.db/id :person.nok/tmt]]
      :full-results? true})

(contains? nil :type)

(.close node)
