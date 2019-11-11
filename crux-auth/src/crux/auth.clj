(ns crux.auth (:require [crux.api :as c]))

;; Types of auth docs
;; auth doc -> contains id of document in reference, a list of users who can
;;             :crux.auth.read/users, a list of users who can
;;             :crux.auth.write/users. Also :crux.auth.condition, this contains
;;             a map of reasons for accessing the document e.g. :emergency
;;             :death. This can be merged into the document if the query is
;;             supplied with this value see crux.auth/q for more details
;; meta auth doc -> contains a meta document for the auth doc detailing read
;;                  and write permissions. e.g.
;;                  crux.auth.auth-doc: (:crux.db/id of auth)
;;                  crux.auth.read/users: [:person/tmt]
;;                  crux.auth.write/users: [:person/tmt]
;;                  TODO who can change this doc? for the moment just su and
;;                  owner. maybe we remove this and just use the write users
;;                  on the auth doc.
;; user auth doc -> a document which stores general user privileges e.g.
;;                  superuser has :rw. everyone else by default has nil.
;;                  someone who can see all data but not alter it will have :r.
;;                  :w allows someone to add new documents (but not change).

(defn q
  "Wraps crux.api/q in an authentication layer"
  [cred db query]
  (c/q db query))
