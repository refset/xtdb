(ns crux.auth (:require [crux.api :as c]))

;; Types of auth docs
;; :crux.auth/doc <crux.db.id> TODO better naming scheme
;; auth doc -> contains id of document in reference, a list of users who can
;;             :crux.auth.read/users, a list of users who can
;;             :crux.auth.write/users. Also :crux.auth.condition, this contains
;;             a map of reasons for accessing the document e.g. :emergency
;;             :death. This can be merged into the document if the query is
;;             supplied with this value see crux.auth/q for more details
;; meta auth doc -> contains a meta document for the auth doc detailing read
;;                  and write permissions. e.g.
;;                  crux.auth/doc: (:crux.db/id of auth)
;;                  crux.auth/read: [:person/tmt]
;;                  crux.auth/write: [:person/tmt]
;;                  TODO who can change this doc? for the moment just su and
;;                  owner. maybe we remove this and just use the write users
;;                  on the auth doc.
;; user auth doc -> a document which stores general user privileges e.g.
;;                  superuser has :r :w. everyone else by default has nil.
;;                  someone who can see all data but not alter it will have :r.
;;                  :w allows someone to add new documents (but not change).
;;                  {:crux.db/id :crux.auth.user/tmt
;;                   :crux.auth.user/permissions [:r :w]}
;;                  NOTE maybe shouldn't have global permissions, up to GDPR.
;;
;; NOTE it's not up to crux to validate someones credentials, just verify the
;;      supplied identity is allowed to execute the required action

;; this function may have an edge case where it gives back a crux.id that isn't
;; actually returned in the original find, however this is really a fault of
;; the query.
;; Maybe move to utils.
(defn get-auth-doc
  "Joins the authentication doc into the query. Requires that for any doc "
  [user query condition]
  (assoc query
         :where
         (vec (concat (:where query)
                      (apply concat
                             (mapv (fn [el] (let [el-auth (gensym (str el))]
                                              [[el-auth :crux.auth/doc el]
                                               [el-auth :crux.auth/read user]
                                               [el-auth :crux.auth/condition condition]]))
                                   (reduce #(conj %1 (first %2)) #{} (:where query))))))))

(defn q
  "Wraps crux.api/q in an authentication layer.
  Filters out all elements that come from unavailable documents."
  [cred db query]
  (assert (:crux.auth/user cred))
  (cond
    ;; if user ∋ :r don't filter query
    (first (c/q db {:find ['user] :where [['user :crux.db/id (:crux.auth/user cred)]
                                          ['user :crux.auth.user/permissions :r]]}))
    (c/q db query)
    ;; if user ∌ :r filter query
    :else
    (let [condition (:crux.auth/condition cred)
          user (:crux.auth/user cred)]
      (c/q db (get-auth-doc user query condition)))))

;; TODO things submit-tx could do
;;            -> only allow crux.db/id within certain namespaces
;;            -> only allow certain elements
;;            -> give inital permissions
(defn submit-tx
  "Wraps crux.api/submit-tx in an authentication layer"
  [cred node txs]
  (let [condition (:crux.auth/condition cred)
        user (:crux.auth/user cred)
        tx-types (into #{} (map txs first))]))

