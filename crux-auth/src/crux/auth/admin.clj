(ns crux.auth.admin
  "Administrator functions"
  (:require [crux.api :as c]))

(defn add-admin
  "giving an admin {:crux.auth/permissions [:r]} gives global read access
  :w gives global put access"
  [node username & extra-attribs]
  (c/submit-tx node [[:crux.tx/put (merge {:crux.db/id (keyword "crux.auth.user" username)
                                           :crux.auth.user/username username}
                                          (first extra-attribs))]]))
