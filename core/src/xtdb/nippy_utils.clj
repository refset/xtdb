(ns xtdb.nippy-utils
  (:require [xtdb.nippy-fork :as nippyf]
            [xtdb.memory :as mem]
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

(def idkb (mem/->nippy-buffer :crux.db/id))

(defn doc->eid
  "Convert a nippified XT document to raw K/V byte arrays"
  [buf]
  (let [in (-> (DirectBufferInputStream. buf)
               (DataInputStream.))]
    (assert (= id-map-sm (.readByte in)))

    (first (enc/reduce-n (fn [acc _]
                           (let [b (edbb)
                                 limited-b (nippyf/get-bytes-from-in! b 0 in)]
                             (if (mem/buffers=? idkb limited-b)
                               (do
                                 (conj acc (nippyf/get-bytes-from-in! b 0 in))) ;; TODO should break after match...don't really want a `reduce` here at all
                               (do
                                 (nippyf/get-bytes-from-in! b 0 in)
                                 acc))))
                         [] (.readByte in)))))
