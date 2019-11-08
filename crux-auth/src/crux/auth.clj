(ns crux.auth
  (:require [crux.api :as c]))

(defn q
  "Wraps crux.api/q in an authentication layer"
  [cred db query]
  (c/q db query))

