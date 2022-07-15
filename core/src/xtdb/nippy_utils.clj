(ns xtdb.nippy-utils
  (:require [xtdb.nippy-fork :as nippyf]
            [xtdb.memory :as mem]
            [juxt.clojars-mirrors.encore.v3v21v0.taoensso.encore :as enc])
  (:import [org.agrona ExpandableDirectByteBuffer MutableDirectBuffer]))

(def ^:const ^:private id-map-sm 112)

(defn edbb []
  (ExpandableDirectByteBuffer. 32))

(defn doc->kvs
  "Convert a nippified XT document to raw K/V byte arrays"
  [^MutableDirectBuffer buf]
  (assert (= id-map-sm (.getByte buf 0)))

;;  (prn (.capacity buf))
;;  (prn (mem/buffer->hex buf))

  (second (enc/reduce-n (fn [[k-offset m] _]
                          (let [k-len (nippyf/get-len buf k-offset)
;;                                _ (prn k-offset k-len)
                                v-offset (+ k-offset k-len)
                                v-len (nippyf/get-len buf v-offset)
                                offset (+ v-offset v-len)]
;;                            (prn k-offset k-len v-offset v-len offset)
                            [offset
                             (assoc m
                                    (doto (mem/slice-buffer buf k-offset k-len) #_(#(prn (mem/buffer->hex %))))
                                    (doto (mem/slice-buffer buf v-offset v-len) #_(#(prn (mem/buffer->hex %)))))]))
                        [2 {}] (.getByte buf 1))))

(def idkb (mem/->nippy-buffer :crux.db/id))

#_(defn doc->eid
  "Extract encoded eid from nippified document"
  [^MutableDirectBuffer buf]
  (let [in (-> (DirectBufferInputStream. buf)
               (DataInputStream.))
        b (edbb)]
    (assert (= id-map-sm (.readByte in)))

    (first (enc/reduce-n (fn [acc _]
                           (let [limited-b (nippyf/get-bytes-from-in! b 0 in)]
                             (if (mem/buffers=? idkb limited-b)
                               (do
                                 (conj acc (nippyf/get-bytes-from-in! b 0 in))) ;; TODO should break after match...don't really want a `reduce` here at all
                               (do
                                 (nippyf/get-bytes-from-in! b 0 in)
                                 acc))))
                         [] (.readByte in)))))

#_(defn doc->kv-slices
  "Convert a nippified XT document to raw K/V byte arrays"
  [buf]
  (let [m (mem/slice-buffer buf 1)
        kv1 (mem/slice-buffer m 0 10)]

    (prn (mem/buffer->hex kv1))
    []
    #_(enc/reduce-n (fn [acc _] (assoc acc
                                     (doto (edbb) (nippyf/get-bytes-from-in! 0 in))
                                     (doto (edbb) (nippyf/get-bytes-from-in! 0 in))))
                  {} (.readByte in))))
