(ns xtdb.json-serde-test
  (:require [clojure.test :as t]
            [xtdb.error :as err])
  (:import (java.util UUID)
           (xtdb JsonSerde)))

(defn- roundtrip-json-ld [v]
  (-> v JsonSerde/encode JsonSerde/decode))

(t/deftest test-json-ld-roundtripping
  (let [v {"keyword" :foo/bar
           "set-key" #{:foo :baz}
           "instant" #time/instant "2023-12-06T09:31:27.570827956Z"
           "date" #time/date "2020-01-01"
           "date-time" #time/date-time "2020-01-01T12:34:56.789"
           "zoned-date-time" #time/zoned-date-time "2020-01-01T12:34:56.789Z"
           "time-zone" #time/zone "America/Los_Angeles"
           "duration" #time/duration "PT3H1M35.23S"
           "period" #time/period "P18Y"
           "uuid" (UUID/randomUUID)}]
    (t/is (= v
             (roundtrip-json-ld v))
          "testing json ld values"))

  (let [ex (err/illegal-arg :divison-by-zero {:foo "bar"})
        roundtripped-ex (roundtrip-json-ld ex)]

    (t/testing "testing exception encoding/decoding"
      (t/is (= (ex-message ex)
               (ex-message roundtripped-ex)))

      (t/is (= (ex-data ex)
               (ex-data roundtripped-ex))))))
