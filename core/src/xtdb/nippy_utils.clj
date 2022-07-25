(ns xtdb.nippy-utils
  (:require [xtdb.nippy-fork :as nippyf]
            [xtdb.codec :as c]
            [xtdb.memory :as mem]
            [juxt.clojars-mirrors.encore.v3v21v0.taoensso.encore :as enc])
  (:import [java.util.function Supplier]
           [org.agrona MutableDirectBuffer ExpandableDirectByteBuffer]
           [org.agrona.concurrent UnsafeBuffer]))

;; nippy.clj VERBATIM START

(def ^:private type-ids
  "{<byte-id> <type-name-kw>}, ~random ordinal ids for historical reasons.
  -ive ids reserved for custom (user-defined) types.

  Size-optimized suffixes:
    -0  (empty       => 0-sized)
    -sm (small       => byte-sized)
    -md (medium      => short-sized)
    -lg (large       => int-sized)   ; Default when no suffix
    -xl (extra large => long-sized)"

  {82  :prefixed-custom

   47  :reader-sm
   51  :reader-md
   52  :reader-lg

   75  :serializable-q-sm ; Quarantined
   76  :serializable-q-md ; ''

   48  :record-sm
   49  :record-md
   80  :record-lg ; Unrealistic, future removal candidate

   81  :type

   3   :nil
   8   :true
   9   :false
   10  :char

   34  :str-0
   105 :str-sm
   16  :str-md
   13  :str-lg

   106 :kw-sm
   85  :kw-md
   14  :kw-lg ; Unrealistic, future removal candidate

   56  :sym-sm
   86  :sym-md
   57  :sym-lg ; Unrealistic, future removal candidate

   58  :regex
   71  :uri

   53  :bytes-0
   7   :bytes-sm
   15  :bytes-md
   2   :bytes-lg

   17  :vec-0
   113 :vec-2
   114 :vec-3
   110 :vec-sm
   69  :vec-md
   21  :vec-lg

   115 :objects-lg ; TODO Could include md, sm, 0 later if there's demand

   18  :set-0
   111 :set-sm
   32  :set-md
   23  :set-lg

   19  :map-0
   112 :map-sm
   33  :map-md
   30  :map-lg

   35  :list-0
   36  :list-sm
   54  :list-md
   20  :list-lg

   37  :seq-0
   38  :seq-sm
   39  :seq-md
   24  :seq-lg

   28  :sorted-set
   31  :sorted-map
   26  :queue
   25  :meta

   40  :byte
   41  :short
   42  :integer

   0   :long-zero
   100 :long-sm
   101 :long-md
   102 :long-lg
   43  :long-xl

   44  :bigint
   45  :biginteger

   60  :float
   55  :double-zero
   61  :double
   62  :bigdec
   70  :ratio

   90  :date
   91  :uuid

   59  :cached-0
   63  :cached-1
   64  :cached-2
   65  :cached-3
   66  :cached-4
   72  :cached-5
   73  :cached-6
   74  :cached-7
   67  :cached-sm
   68  :cached-md

   79  :time-instant  ; JVM 8+
   83  :time-duration ; ''
   84  :time-period   ; ''

   ;;; DEPRECATED (only support thawing)
   5   :reader-lg2 ; == :reader-lg, used only for back-compatible thawing
   1   :reader-depr1          ; v0.9.2 for +64k support
   11  :str-depr1             ; ''
   22  :map-depr1             ; v0.9.0 for more efficient thaw
   12  :kw-depr1              ; v2.0.0-alpha5 for str consistecy
   27  :map-depr2             ; v2.11 for count/2
   29  :sorted-map-depr1      ; ''
   4   :boolean-depr1         ; v2.12 for switch to true/false ids
   77  :kw-md-depr1           ; Buggy size field, Ref. #138 2020-11-18
   78  :sym-md-depr1          ; Buggy size field, Ref. #138 2020-11-18

   46  :serializable-uq-sm ; Unquarantined
   50  :serializable-uq-md ; ''
   6   :serializable-uq-lg ; ''; unrealistic, future removal candidate
   })

(defmacro ^:private defids []
  `(do
     ~@(map
         (fn [[id# name#]]
           (let [name# (str "id-" (name name#))
                 sym#  (with-meta (symbol name#)
                         {:const true :private true})]
             `(def ~sym# (byte ~id#))))
         type-ids)))

(defids)

(do
  (defmacro ^:private get-sm-count [buf offset] `(.getByte  ~buf ~offset))
  (defmacro ^:private get-md-count [buf offset] `(.getShort ~buf ~offset))
  (defmacro ^:private get-lg-count [buf offset] `(.getInt ~buf ~offset)))

;; nippy.clj VERBATIM END

(def sm-count 1)
(def md-count 2)
(def lg-count 4)

(defn get-len
  "Calculate total byte length of fast-frozen Nippy value at offset."
  [^MutableDirectBuffer buf offset]
  (let [type-id (.getByte buf offset)
        offset (inc offset)]
    ;;(println (str "get-len: " type-id))
    (try
      (let [clen (enc/case-eval type-id

                   id-reader-sm          sm-count
                   id-reader-md          md-count
                   id-reader-lg          lg-count
                   id-reader-lg2         lg-count
                   id-record-sm          sm-count
                   id-record-md          md-count
                   id-record-lg          lg-count

                   id-serializable-q-sm  sm-count
                   id-serializable-q-md  md-count

                   id-serializable-uq-sm sm-count
                   id-serializable-uq-md md-count
                   id-serializable-uq-lg lg-count

                   id-nil          0
                   id-true         0
                   id-false        0
                   id-char         1

                   id-cached-0     0
                   id-cached-1     0
                   id-cached-2     0
                   id-cached-3     0
                   id-cached-4     0
                   id-cached-5     0
                   id-cached-6     0
                   id-cached-7     0
                   id-cached-sm    sm-count
                   id-cached-md    md-count

                   id-bytes-0      0
                   id-bytes-sm     sm-count
                   id-bytes-md     md-count
                   id-bytes-lg     lg-count

                   id-objects-lg   lg-count

                   id-str-0        0
                   id-str-sm       sm-count
                   id-str-md       md-count
                   id-str-lg       lg-count

                   id-kw-sm        sm-count
                   id-kw-md        md-count
                   id-kw-md-depr1  lg-count
                   id-kw-lg        lg-count

                   id-sym-sm       sm-count
                   id-sym-md       md-count
                   id-sym-md-depr1 lg-count
                   id-sym-lg       lg-count

                   id-vec-0        0

                   id-set-0        0

                   id-map-0        0

                   id-list-0       0

                   id-seq-0        0

                   id-byte         1
                   id-short        2
                   id-integer      4
                   id-long-zero    0
                   id-long-sm      1
                   id-long-md      2
                   id-long-lg      4
                   id-long-xl      8

                   id-bigint       4
                   id-biginteger   4

                   id-float        4
                   id-double-zero  0
                   id-double       8
                   id-bigdec       4

                   id-ratio        8

                   id-date         8
                   id-uuid         16

                   id-time-instant 12

                   id-time-duration 12

                   id-time-period 12

                   ;; Deprecated ------------------------------------------------------
                   ;; NOTE: all predate the first XT release, so there should be nothing in the wild to thaw
                   ;; id-boolean-depr1 _
                   ;; id-sorted-map-depr1 _
                   ;; id-map-depr2 _
                   ;; id-reader-depr1 _
                   ;; id-str-depr1 _
                   ;; id-kw-depr1 _
                   ;; id-map-depr1 _

                   ;; -----------------------------------------------------------------

                   ;; NOTE: read-custom! not supported by XT
                   ;; id-prefixed-custom (read-custom! in :prefixed (.readShort in))

                   id-vec-sm      0
                   id-vec-md      0
                   id-vec-lg      0
                   id-set-sm      0
                   id-set-md      0
                   id-set-lg      0
                   id-map-sm      0
                   id-map-md      0
                   id-map-lg      0
                   id-queue       0
                   id-sorted-set  0
                   id-sorted-map  0
                   id-list-sm     0
                   id-list-md     0
                   id-list-lg     0
                   id-seq-sm      0
                   id-seq-md      0
                   id-seq-lg      0

                   id-type        0
                   id-regex       0
                   id-uri         0

                   id-vec-2       0
                   id-vec-3       0
                   id-meta        2

                   (throw
                    (ex-info
                     (str "Unrecognized Nippy type id (" type-id "). Data frozen with newer Nippy version? `read-custom!` is not supported by XT")
                     {:type-id type-id})))
            deep-len (or (enc/case-eval type-id
                           id-type (get-len buf offset)
                           id-regex (get-len buf offset)
                           id-uri (get-len buf offset)

                           id-meta  (let [l (get-len buf offset)]
                                      (+ l (get-len buf (+ offset l))))
                           id-vec-2 (let [l (get-len buf offset)]
                                      (+ l (get-len buf (+ offset l))))
                           id-vec-3 (let [l (get-len buf offset)
                                          l2 (+ l (get-len buf (+ offset l)))]
                                      (+ l l2 (get-len buf (+ offset l l2)))))
                         (let [clen (enc/case-eval type-id
                                      id-vec-sm      sm-count
                                      id-vec-md      md-count
                                      id-vec-lg      lg-count
                                      id-set-sm      sm-count
                                      id-set-md      md-count
                                      id-set-lg      lg-count
                                      id-map-sm      sm-count
                                      id-map-md      md-count
                                      id-map-lg      lg-count
                                      id-queue       lg-count
                                      id-sorted-set  lg-count
                                      id-sorted-map  lg-count
                                      id-list-sm     sm-count
                                      id-list-md     md-count
                                      id-list-lg     lg-count
                                      id-seq-sm      sm-count
                                      id-seq-md      md-count
                                      id-seq-lg      lg-count
                                      (do false))]
                           (and clen
                                (loop [acc clen
                                       i (* (enc/case-eval (int clen)
                                              sm-count (get-sm-count buf offset)
                                              md-count (get-md-count buf offset)
                                              lg-count (get-lg-count buf offset))
                                            (enc/case-eval type-id
                                              id-map-sm 2
                                              id-map-md 2
                                              id-map-lg 2
                                              id-sorted-map 2
                                              (do 1)))]
                                  (if (< 0 i)
                                    (recur (+ acc (get-len buf (+ offset acc))) (dec i))
                                    acc)))))
            vlen
            (or deep-len
                (enc/case-eval (int clen)
                  0 0
                  sm-count (get-sm-count buf offset)
                  md-count (get-md-count buf offset)
                  lg-count (get-lg-count buf offset)))]
        ;;(prn :lens clen vlen)
        (+ 1 clen vlen))
      (catch Exception e
        (throw (ex-info (str "Thaw failed against type-id: " type-id)
                        {:type-id type-id} e))))))

(def idkb (mem/->nippy-buffer :crux.db/id))

(def ^:private ^ThreadLocal eid-ebb-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ExpandableDirectByteBuffer.)))))

(def ^:private ^ThreadLocal att-b-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (mem/allocate-buffer c/id-size)))))

(def ^:private ^ThreadLocal val-ebb-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ExpandableDirectByteBuffer.)))))

(def ^:private ^ThreadLocal eid-buffer-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (UnsafeBuffer.)))))

(def ^:private ^ThreadLocal att-buffer-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (UnsafeBuffer.)))))

(def ^:private ^ThreadLocal val-buffer-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (UnsafeBuffer.)))))

(defn doc->eid-offset-and-len
  "Extract encoded eid"
  [^MutableDirectBuffer buf]
  (let [clen
        (enc/case-eval (.getByte buf 0)
          id-map-sm sm-count
          id-map-md md-count
          id-map-lg lg-count
          (throw (AssertionError. "Invalid Nippy document")))
        eid-buf ^UnsafeBuffer (.get eid-buffer-tl)]
    (loop [k-offset ^Integer (inc clen)]
      (let [k-len ^Integer (get-len buf k-offset)
            v-offset (+ k-offset k-len)
            v-len ^Integer (get-len buf v-offset)]
        (.wrap eid-buf buf k-offset k-len)
        (if (mem/buffers=? idkb eid-buf)
          [v-offset
           v-len]
          (recur (+ v-offset v-len)))))))

(defn get-coll-count-and-first-offset-len [^MutableDirectBuffer buf offset]
  (let [coll-clen (enc/case-eval (.getByte buf offset)
                    id-set-sm      sm-count
                    id-set-md      md-count
                    id-set-lg      lg-count
                    id-vec-0 0
                    id-vec-2 0
                    id-vec-3 0
                    id-vec-sm sm-count
                    id-vec-md md-count
                    id-vec-lg lg-count
                    (do false))]
    (if (false? coll-clen)
      [1 offset (get-len buf offset)]
      (let [offset* (inc offset)
            coll-count (or (and coll-clen
                                (enc/case-eval (int coll-clen)
                                  sm-count (get-sm-count buf offset*)
                                  md-count (get-md-count buf offset*)
                                  lg-count (get-lg-count buf offset*)))
                           0)
            v-offset (+ offset* coll-clen)]
        (if (= coll-count 0)
          [0 v-offset 0] ;; id-vec-0
          [coll-count v-offset (get-len buf v-offset)])))))

(defn nippy-buffer->xt-id-buffer [^UnsafeBuffer to ^MutableDirectBuffer buf ^Integer offset ^Integer len]
  (let [clen
        (enc/case-eval (.getByte buf offset)
          id-kw-sm        sm-count
          id-kw-md        md-count
          id-kw-lg        lg-count
          (do false))]
    (if (false? clen)
      (let [_ (.wrap to buf offset len)
            b (c/->id-buffer (mem/<-nippy-buffer to))]
        (.wrap to b 0 (.capacity b)))
      (let [_ (.wrap to buf ^Integer (+ offset 1 clen) ^Integer (- len clen 1))
            b (c/id-function ^ExpandableDirectByteBuffer (.get att-b-tl) to)]
        (.wrap to b 0 (.capacity b))))))

(defn nippy-buffer->xt-value-buffer [^UnsafeBuffer to ^MutableDirectBuffer buf ^Integer offset ^Integer len]
  (let [type-id (.getByte buf offset)
        clen
        (enc/case-eval type-id
      ;    id-kw-sm        sm-count
      ;    id-kw-md        md-count
      ;    id-kw-lg        lg-count
          (do false))]
    (if (false? clen)
      (enc/case-eval type-id
        id-nil (doto to
                 (.putByte 0 c/nil-value-type-id)
                 (.wrap to 0 c/value-type-id-size))
        id-true (doto to
                  (.putByte 0 c/boolean-value-type-id)
                  (.putByte c/value-type-id-size 1)
                  (.wrap to 0 (+ c/value-type-id-size Byte/BYTES)))
        (let [_ (.wrap to buf offset len)
              b (c/->value-buffer (mem/<-nippy-buffer to))]
          (.wrap to b 0 (.capacity b))))
      (let [_ (.wrap to buf ^Integer (+ offset 1 clen) ^Integer (- len clen 1))
            b (c/id-function (mem/allocate-buffer c/id-size) to)]
        (.wrap to b 0 (.capacity b))))))

(defn doc-kv-visit
  "Extract encoded eid"
  [^MutableDirectBuffer buf f]
  (let [clen
        (enc/case-eval (.getByte buf 0)
          id-map-sm sm-count
          id-map-md md-count
          id-map-lg lg-count
          (throw (AssertionError. "Invalid Nippy document")))
        eid-buf ^UnsafeBuffer (.get eid-buffer-tl)
        att-buf ^UnsafeBuffer (.get att-buffer-tl)
        val-buf ^UnsafeBuffer (.get val-buffer-tl)]
    (let [[offset len] (doc->eid-offset-and-len buf)]
      (nippy-buffer->xt-value-buffer eid-buf buf offset len))
    (loop [k-offset ^Integer (inc clen)
           k-len ^Integer (get-len buf k-offset)
           [v-coll-count ^Integer v-offset ^Integer v-len] (get-coll-count-and-first-offset-len buf (+ k-offset k-len))]
      (nippy-buffer->xt-id-buffer att-buf buf k-offset k-len)
      (when (> v-coll-count 0)
        (nippy-buffer->xt-value-buffer val-buf buf v-offset v-len)
        (f eid-buf att-buf val-buf))
      (when (< (+ v-offset v-len) (.capacity buf))
        (let [next-offset (+ v-offset v-len)
              [k-offset* k-len* [v-coll-count* ^Integer v-offset* ^Integer v-len*]]
              (if (= v-coll-count 1)
                (let [k-offset* next-offset
                      k-len* ^Integer (get-len buf k-offset*)]
                  [k-offset*
                   k-len*
                   (get-coll-count-and-first-offset-len buf (+ k-offset* k-len*))])
                [k-offset k-len [(dec v-coll-count) next-offset (get-len buf next-offset)]])]
          (recur k-offset*
                 k-len*
                 [v-coll-count* v-offset* v-len*]))))))



;; index-store helper buffer-or-value-buffer uses c/->value-buffer, for v and e
;; thaw-from-in! and value->buffer on all types, except priorities, e.g. strings/keywords >224 bytes
;; v and e should be symmetrical, and luckily e only happens once
