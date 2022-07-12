(ns xtdb.nippy-utils
  (:require [xtdb.nippy-fork :as nippyf])
  (:import [org.agrona.io DirectBufferInputStream ExpandableDirectBufferOutputStream]
           [java.io DataInputStream DataOutputStream]))

;; Nippify a doc, then convert the doc into a WriteBatch
;; Takes our transducer friend

;; Baby step
;; 1) get the ks and vs as byte-arrays,
;; 2) but extract the id

;; Assumptions, As are always Keywords

(def ^:const ^:private id-map-sm 112)


;; finding the end of a v is hard, we want to preserve the orignal byte-array, keep track

(defn doc->kvs
  "Convert a nippified XT document to raw K/V byte arrays"
  [buf]
  (let [in (-> (DirectBufferInputStream. buf)
               (DataInputStream.))]
    (assert (= id-map-sm (.readByte in)))
    (let [kv-count (.readByte  in)]
      (println kv-count))

    ;; Can we just read the bytes?

    ;; Like, we want
    []
    ;(nippy/thaw-from-in! in)
    ))
