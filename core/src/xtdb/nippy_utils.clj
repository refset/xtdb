(ns xtdb.nippy-utils
  (:require [juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy :as nippy])
  (:import [org.agrona.io DirectBufferInputStream ExpandableDirectBufferOutputStream]
           [java.io DataInputStream DataOutputStream]))

;; Nippify a doc, then convert the doc into a WriteBatch
;; Takes our transducer friend

;; Baby step
;; 1) get the ks and vs as byte-arrays,
;; 2) but extract the id

;; Assumptions, As are always Keywords

(def ^:const ^:private id-map-sm 112)

(defn doc->kvs
  "Convert a nippified XT document to raw K/V byte arrays"
  [buf]
  (let [in (-> (DirectBufferInputStream. buf)
               (DataInputStream.))]
    (assert (= id-map-sm (.readByte in)))
    []
    ;(nippy/thaw-from-in! in)
    ))
