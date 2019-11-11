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
;; NOTE it's not up to crux to validate someones credentials, just verify the
;;      supplied identity is allowed to execute the required action

;; Is a 10^25 chance there is a collision per inspected document. This won't
;; throw an error, instead no results will be returned
(defn rand-var
  []
  (.toLowerCase
    (apply str
           (take 10
                 (repeatedly #(char (+ (rand 26) 65)))))))

;; this function may have an edge case where it gives back a crux.id that isn't
;; actually returned in the original find, however this is really a fault of
;; the query.
(defn get-auth-doc
  [user query]
  (assoc query
         :where
         (vec (concat (:where query)
                      (apply concat
                             (mapv (fn [el] (let [placeholer (symbol (rand-var))]
                                              [[placeholer :crux.auth/doc el]
                                             [placeholer :crux.auth/read user]]))
                                   (reduce #(conj %1 (first %2)) #{} (:where query))))))))

(defn q
  "Wraps crux.api/q in an authentication layer.
  Filters out all elements that come from unavailable documents."
  [cred db query]
  (let [condition (:crux.auth/condition cred)
        user (:crux.auth/user cred)
        docs (c/q db (get-auth-doc user query))]
    ; look for the auth doc
    #_(map docs #(c/q db {}))
    #_(c/q db query)
    docs))

(defn submit-tx
  "Wraps crux.api/submit-tx in an authentication layer"
  [cred node txs]
  (let [condition (:crux.auth/condition cred)
        user (:crux.auth/user cred)
        tx-types (into #{} (map txs first))]

    ))
