(ns xtdb.nippy-utils
  (:require [xtdb.nippy-fork :as nippyf]
            [juxt.clojars-mirrors.encore.v3v21v0.taoensso.encore :as enc])
  (:import [org.agrona.io DirectBufferInputStream]
           [org.agrona ExpandableDirectByteBuffer]))

(def ^:const ^:private id-map-sm 112)

(defn edbb []
  (ExpandableDirectByteBuffer. 32))

(defn doc->kvs
  "Convert a nippified XT document to raw K/V byte arrays"
  [buf]
  (let [in (-> (DirectBufferInputStream. buf)
               (DataInputStream.))]
    (assert (= id-map-sm (.readByte in)))

    (enc/reduce-n (fn [acc _] (assoc acc
                                     (doto (edbb) (nippyf/get-bytes-from-in! 0 in))
                                     (doto (edbb) (nippyf/get-bytes-from-in! 0 in))))
                  {} (.readByte in))))
