(ns crux.auth.admin
  "Administrator functions"
  (:require [crux.api :as c]))

(defn add-user
  "Add a new user to the authentication model. Take a crux node, a user id and
  optionally extra attributes in a map which will be appened to the document.
  For example if you want to add a users github with their account or a
  password hash. Gives the user no privilages by default
  Noteable extra-attribs: crux.auth.permissions [:r :w]"
  [node user & extra-attribs]
  (c/submit-tx node [[:crux.tx/put (merge {:crux.db/id (keyword "crux.auth.user" user)
                                           :crux.auth.user/username user}
                                          (first extra-attribs))]]))

; TODO search for instances of user in document privilages
(defn delete-user
  [node user]
  (c/submit-tx node [[:crux.tx/delete user]]))

; TODO search for instances of user in document privilages
(defn evict-user
  [node user]
  (c/submit-tx node [[:crux.tx/evict user]]))

(defn privilage
  "example (ca/privilage node :crux.auth.user/mal :tmt/nok [:r :w])"
  [node user doc-id priv & condition]
  ;; not a fan of UUID's. Could maybe malnipulate the doc-id
  (c/submit-tx node [[:crux.tx/put {:crux.db/id (java.util.UUID/randomUUID)
                                    :crux.auth/doc doc-id
                                    :crux.auth/user user
                                    :crux.auth/permissions priv}]]))

