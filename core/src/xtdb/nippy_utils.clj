(ns xtdb.nippy-utils
  (:require [xtdb.nippy-fork :as nippyf]
            [juxt.clojars-mirrors.encore.v3v21v0.taoensso.encore :as enc])
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

    (enc/reduce-n (fn [acc _] (assoc acc (nippyf/get-bytes-from-in! in) (nippyf/get-bytes-from-in! in)))
                  {} (.readByte in))))
