(ns crux.auth.admin
  "Administrator functions"
  (:require [crux.api :as c]))

(defn add-user
  "Add a new user to the authentication model. Take a crux node, a user id and
  optionally extra attributes in a map which will be appened to the document.
  For example if you want to add a users github with their account or a
  password hash."
  [node user & extra-attribs]
  (c/submit-tx node [[:crux.tx/put (merge {:crux.db/id (java.util.UUID/randomUUID)
                                           :crux.auth/user user}
                                          extra-attribs)]]))

(defn delete-user
  [node user]
  ; TODO search for instances of user in document privilages
  (c/submit-tx node [[:crux.tx/delete user]]))

(defn evict-user
  [node user]
  (c/submit-tx node [[:crux.tx/evict user]]))

(defn privilage
  "example (ca/privilage node :tmt/nok [:r :w])"
  [node user doc-id priv]
  (c/submit-tx node [[:crux.tx/put {:crux.db/id (java.util.UUID/randomUUID)
                                    :crux.auth/doc doc-id
                                    :crux.auth/user user
                                    :crux.auth/permissions priv}]]))

#_(do
    (c/submit-tx dev/node [[:crux.tx/put {:crux.db/id :this}]])
    (c/submit-tx dev/node [[:crux.tx/evict :this]])
    (c/q (c/db dev/node) {:find ['e] :where [['e :crux.db/id '_]] :full-results? true})
    )
