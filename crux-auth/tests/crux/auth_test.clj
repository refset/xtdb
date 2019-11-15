(ns crux.auth-test
  (:require [crux.api :as c]
            [crux.auth :as a]
            [crux.io :as cio]
            [crux.auth.admin :as aa]
            [clojure.test :as t]))

(def ^:private tmtdoc {:crux.db/id :person/tmt
                       :person/name "Tom"
                       :person/dob #inst "1996-06-21"
                       :person/nok :person.nok/tmt})

(def ^:private tmtnok {:crux.db/id :person.nok/tmt
                       :nok/name "Eve"
                       :nok/phone "+4479138382672"})

#_(def node (c/start-node {:crux.node/topology :crux.standalone/topology
                           :crux.node/kv-store "crux.kv.memdb/kv"
                           :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"
                           :crux.kv/db-dir "data/db-dir-auth-t"
                           :crux.standalone/event-log-dir "data/eventlog-auth-t"}))
#_(.close node)

(defn- clear-test-dirs [f]
  (try
    (println "test")
    (f)
    (finally
      (cio/delete-dir "data"))))

(t/use-fixtures :each clear-test-dirs)

#_(t/test-var #'crux.auth-test/submit-tx-test)
(t/deftest submit-tx-test
  (with-open [^crux.api.ICruxAPI node (c/start-node {:crux.node/topology :crux.standalone/topology
                                                     :crux.node/kv-store "crux.kv.memdb/kv"
                                                     :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"
                                                     :crux.kv/db-dir "data/db-dir-auth-t"
                                                     :crux.standalone/event-log-dir "data/eventlog-auth-t"})]
    ;; Add admin
    (c/sync node (:crux.tx/tx-time (aa/add-admin node "root" {:crux.auth.user/permissions [:r :w]})) nil)

    ;; Valid creds and ∄ doc ∴ produces tmtdoc with auth and meta doc
    (c/sync node (:crux.tx/tx-time (a/submit-tx {:crux.auth/user :crux.auth.user/root}
                                                node
                                                [[:crux.tx/put :crux.auth.user/tmt tmtdoc]])) nil)
    ;; ? doc ∃
    (t/is (= (ffirst (c/q (c/db node) {:find ['d]
                                       :where [['d :crux.db/id :person/tmt]]
                                       :full-results? true}))
             tmtdoc))
    ;; ? doc-auth ∃
    (t/is (= (into #{} (c/q (c/db node)
                            {:find ['t 'd 'w 'r]
                             :where [['da :crux.auth/type 't]
                                     ['da :crux.auth/doc 'd]
                                     ['da :crux.auth/write 'w]
                                     ['da :crux.auth/read 'r]]}))
             #{[:crux.auth/doc :person/tmt :crux.auth.user/tmt :all]
               [:crux.auth/doc :person/tmt :crux.auth.user/root :all]}))

    ;; ? doc-meta ∃
    (t/is (= (into #{} (c/q (c/db node)
                            {:find ['t 'w 'r]
                             :where [['ma :crux.auth/type 't]
                                     ['ma :crux.auth/write 'w]
                                     ['ma :crux.auth/read 'r]]}))
             #{[:crux.auth/meta :crux.auth.user/tmt :all]
               [:crux.auth/doc :crux.auth.user/root :all]
               [:crux.auth/meta :crux.auth.user/root :all]
               [:crux.auth/doc :crux.auth.user/tmt :all]}))

    ;; NOTE a/alter-put is the function a/submit-tx uses to transform
    ;; :crux.tx/put operations

    ;; where ∃ doc and user has auth
    ;; => [[tmtdoc]]
    (t/is (= (a/alter-put {:crux.auth/user :crux.auth.user/root}
                          (c/db node)
                          [:crux.auth.user/tmt tmtdoc])
             [[:crux.tx/put tmtdoc]]))
    ;; where ∃ doc and user doesn't have auth
    ;; => []
    (t/is (= (a/alter-put {:crux.auth/user :crux.auth.user/nobody}
                          (c/db node)
                          [:crux.auth.user/tmt tmtdoc]) []))
    ;; where ∄ doc and user doesn't have auth
    ;; => []
    (t/is (= (a/alter-put {:crux.auth/user :crux.auth.user/nobody}
                          (c/db node)
                          [:crux.auth.user/tmt tmtnok]) []))

    ;; Shouldn't submit anything as user doesn't have write auth
    ;; returns a submit map but if you look at the tap you can see that an empty
    ;; map is entered to submit-tx
    (t/is (= (a/alter-put {:crux.auth/user :crux.auth.user/tmt}
                          (c/db node)
                          [:crux.auth.user/tmt tmtnok]) []))))

(t/deftest set-auth-doc-test
  (with-open [^crux.api.ICruxAPI node (c/start-node {:crux.node/topology :crux.standalone/topology
                                                     :crux.node/kv-store "crux.kv.memdb/kv"
                                                     :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"
                                                     :crux.kv/db-dir "data/db-dir-auth-t"
                                                     :crux.standalone/event-log-dir "data/eventlog-auth-t"})]

    ))
