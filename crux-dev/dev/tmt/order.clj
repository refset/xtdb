(ns tmt.order
  (:require [crux.api :as api]))

(def opts
  {:crux.node/topology :crux.standalone/topology
   :crux.node/kv-store "crux.kv.memdb/kv"
   :crux.kv/db-dir "data/db-dir-1"
   :crux.standalone/event-log-dir "data/eventlog-1"
   :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"})

(def node
  (api/start-node opts))

(api/submit-tx
  node
  [[:crux.tx/put
    {:crux.db/id :one
     :val 1}]
   [:crux.tx/put
    {:crux.db/id :two
     :val 2}]
   [:crux.tx/put
    {:crux.db/id :three
     :val 3}]])

(api/q (api/db node) '{:find [e v]
                       :where [[x :crux.db/id e]
                               [x :val v]]
                       :order-by [[v :desc]]})
;=> [[:three 3] [:two 2] [:one 1]]

(api/q (api/db node) '{:find [e]
                       :where [[x :crux.db/id e]
                               [x :val v]]
                       :order-by [[v :desc]]})

;=> [[:two] [:three] [:one]]
