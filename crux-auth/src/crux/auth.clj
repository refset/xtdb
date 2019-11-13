(ns crux.auth (:require [crux.api :as c]))


;; this function may have an edge case where it gives back a crux.id that isn't
;; actually returned in the original find, however this is really a fault of
;; the query.
;; Maybe move to utils.
(defn get-auth-doc
  "Joins the authentication doc into the query. Requires that for any doc "
  [user query & condition]
  (let [uc-sym (gensym "user-cond")]
    (assoc query
           :where
           (vec (concat (:where query)
                        (apply concat
                               (mapv (fn [el] (let [el-auth (gensym (str el))]
                                                [[el-auth ::doc el]
                                                 [el-auth ::read uc-sym]]))
                                     (reduce
                                       #(conj %1 (first %2))
                                       #{}
                                       (:where query))))))
           :args
           (vec (concat (:args query)
                        ;; TODO definitely a better way of writing this
                        (concat [{uc-sym user}]
                                (when (first condition)
                                  [{uc-sym [(first condition) user]}])))))))

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
      (c/q db (get-auth-doc user query condition)))))

;; TODO things submit-tx could do
;;            -> only allow crux.db/id within certain namespaces
;;            -> only allow certain elements
;; TODO things submit-tx should do
;;            -> put-tx -> create auth-doc
;;                      -> create meta-auth-doc
;;                      -> give inital permissions
;; TODO should we include conditions for write?
(defn submit-tx
  "Wraps crux.api/submit-tx in an authentication layer"
  [cred node txs]
  (let [condition (:crux.auth/condition cred)
        user (:crux.auth/user cred)
        tx-types (into #{} (map txs first))]))

