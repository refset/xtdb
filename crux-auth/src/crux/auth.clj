(ns crux.auth (:require [crux.api :as c]))


;; this function may have an edge case where it gives back a crux.id that isn't
;; actually returned in the original find, however this is really a fault of
;; the query.
;; Maybe move to utils.
(defn alter-query
  "Joins the authentication doc into the query."
  [user query & condition]
  (let [uc-sym (gensym "user-cond")]
    (assoc query
           :where
           (vec (concat (:where query)
                        (apply concat
                               (mapv (fn [el] (let [el-auth (gensym (str el))]
                                                [[el-auth :crux.auth/doc el]
                                                 [el-auth :crux.auth.doc/read uc-sym]]))
                                     (reduce
                                       #(conj %1 (first %2))
                                       #{}
                                       (:where query))))))
           :args
           (vec (concat (:args query)
                        ;; TODO definitely a better way of writing this
                        (concat [{uc-sym user}]
                                (when (first condition)
                                  [{uc-sym [(first condition) user]}
                                   {uc-sym :all}])))))))


(defn q
  "Wraps crux.api/q in an authentication layer.
  Filters out all elements that come from unavailable documents."
  [cred db query]
  (assert (:crux.auth/user cred))
  (cond
    ;; if user ∋ :r don't filter query
    (first (c/q db {:find ['user]
                    :where [['user :crux.db/id (:crux.auth/user cred)]
                            ['user :crux.auth.user/permissions :r]]}))
    (c/q db query)
    ;; if user ∌ :r filter query
    :else
    (let [condition (:crux.auth/condition cred)
          user (:crux.auth/user cred)]
      (c/q db (alter-query user query condition)))))

(defn alter-put
  [creds db tx]
  (let [user (:crux.auth/user creds)
        owner (get tx 0)
        doc (get tx 1)
        ;validtime (get tx 2)
        ;endvaltime (get tx 3)
        target-doc (c/q db {:find ['d]
                            :where [['d :crux.db/id (:crux.db/id doc)]]})]
    (if-not (empty? target-doc)
      ;; does the user have write permission for the doc?
      (if-not (empty? (c/q db {:find ['p]
                               :where [['d :crux.db/id (:crux.db/id doc)]
                                       ['d :crux.auth/doc 'p]
                                       ['p :crux.auth.doc/write user]]}))
        ;; write it
        [[:crux.tx/put doc]]
        ;; else TODO throw exception?
        [])
      ;; else user ∌ :w TODO specify
      (if-not (empty? (c/q db {:find ['p]
                               :where [['p :crux.db/id user]
                                       ['p :crux.auth.user/permissions :w]]}))
        ;; write it & add auth doc
        (let [auth-doc-id (java.util.UUID/randomUUID)]
          [[:crux.tx/put doc]
         [:crux.tx/put {:crux.db/id auth-doc-id
                        :crux.auth/doc (:crux.db/id doc)
                        :crux.auth.doc/read [:all]
                        :crux.auth.doc/write (vec (distinct [owner user]))}]
         [:crux.tx/put {:crux.db/id (java.util.UUID/randomUUID)
                        :crux.auth/meta auth-doc-id
                        :crux.auth.meta/write (vec (distinct [owner user]))}]])
        ;; else TODO throw exception?
        []))))

;; TODO things submit-tx could do
;;            -> only allow crux.db/id within certain namespaces
;;            -> only allow certain elements
;; TODO things submit-tx should do
;;            -> put-tx -> create auth-doc
;;                      -> create meta-auth-doc
;;                      -> give inital permissions
;; TODO should we include conditions for write?
;;
;; put-tx input: [:crux.tx/put doc validtime endvaltime]
;; TODO sort out validtime & endvaltime
(defn submit-tx
  "Wraps crux.api/submit-tx in an authentication layer.
  If put atempts to put existing document if an optional auth-doc is specified"
  [cred node txs]
  (apply concat (map
                    (fn [tx]
                      (case (first tx)
                        :crux.tx/put (alter-put cred (c/db node) (rest tx))
                        []))
                    txs)))

