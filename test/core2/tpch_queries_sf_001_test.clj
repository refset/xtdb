(ns core2.tpch-queries-sf-001-test
  (:require [clojure.test :as t]
            [core2.util :as util]
            [core2.tpch-queries :as tpch-queries])
  (:import org.apache.arrow.vector.util.Text))

(t/use-fixtures :once (tpch-queries/with-tpch-data 0.01 "tpch-queries-sf-001"))

(t/deftest ^:integration test-q1-pricing-summary-report
  (t/is (= [{:l_returnflag (Text. "A")
             :l_linestatus (Text. "F")
             :sum_qty 380456.0
             :sum_base_price 5.3234821165E8
             :sum_disc_price 5.058224414861E8
             :sum_charge 5.26165934000839E8
             :avg_qty 25.575154611454693
             :avg_price 35785.709306937344
             :avg_disc 0.05008133906964238
             :count_order 14876}
            {:l_returnflag (Text. "N")
             :l_linestatus (Text. "F")
             :sum_qty 8971.0
             :sum_base_price 1.238480137E7
             :sum_disc_price 1.1798257208E7
             :sum_charge 1.2282485056933E7
             :avg_qty 25.778735632183906
             :avg_price 35588.50968390804
             :avg_disc 0.047758620689655175
             :count_order 348}
            {:l_returnflag (Text. "N")
             :l_linestatus (Text. "O")
             :sum_qty 742802.0
             :sum_base_price 1.04150284145E9
             :sum_disc_price 9.897375186346E8
             :sum_charge 1.02941853152335E9
             :avg_qty 25.45498783454988
             :avg_price 35691.1292090744
             :avg_disc 0.04993111956409993
             :count_order 29181}
            {:l_returnflag (Text. "R")
             :l_linestatus (Text. "F")
             :sum_qty 381449.0
             :sum_base_price 5.3459444535E8
             :sum_disc_price 5.079964544067E8
             :sum_charge 5.28524219358903E8
             :avg_qty 25.597168165346933
             :avg_price 35874.00653268018
             :avg_disc 0.049827539927526504
             :count_order 14902}]
           (tpch-queries/tpch-q1-pricing-summary-report))))

(t/deftest ^:integration test-q2-minimum-cost-supplier
  (t/is (= [{:s_acctbal 4186.95
             :s_name (Text. "Supplier#000000077")
             :n_name (Text. "GERMANY")
             :p_partkey (Text. "partkey_249")
             :p_mfgr (Text. "Manufacturer#4")
             :s_address (Text. "wVtcr0uH3CyrSiWMLsqnB09Syo,UuZxPMeBghlY")
             :s_phone (Text. "17-281-345-4863")
             :s_comment (Text. "the slyly final asymptotes. blithely pending theodoli")}
            {:s_acctbal 1883.37
             :s_name (Text. "Supplier#000000086")
             :n_name (Text. "ROMANIA")
             :p_partkey (Text. "partkey_1015")
             :p_mfgr (Text. "Manufacturer#4")
             :s_address (Text. "J1fgg5QaqnN")
             :s_phone (Text. "29-903-665-7065")
             :s_comment
             (Text. "cajole furiously special, final requests: furiously spec")}
            {:s_acctbal 1687.81
             :s_name (Text. "Supplier#000000017")
             :n_name (Text. "ROMANIA")
             :p_partkey (Text. "partkey_1634")
             :p_mfgr (Text. "Manufacturer#2")
             :s_address (Text. "c2d,ESHRSkK3WYnxpgw6aOqN0q")
             :s_phone (Text. "29-601-884-9219")
             :s_comment (Text. "eep against the furiously bold ideas. fluffily bold packa")}
            {:s_acctbal 287.16
             :s_name (Text. "Supplier#000000052")
             :n_name (Text. "ROMANIA")
             :p_partkey (Text. "partkey_323")
             :p_mfgr (Text. "Manufacturer#4")
             :s_address (Text. "WCk XCHYzBA1dvJDSol4ZJQQcQN,")
             :s_phone (Text. "29-974-934-4713")
             :s_comment (Text. "dolites are slyly against the furiously regular packages. ironic, final deposits cajole quickly")}]
           (tpch-queries/tpch-q2-minimum-cost-supplier))))

(t/deftest ^:integration test-q3-shipping-priority
  (t/is (= [{:l_orderkey (Text. "orderkey_47714")
             :revenue 267010.5894
             :o_orderdate (util/date->local-date-time #inst "1995-03-11")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_22276")
             :revenue 266351.5562
             :o_orderdate (util/date->local-date-time #inst "1995-01-29")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_32965")
             :revenue 263768.3414
             :o_orderdate (util/date->local-date-time #inst "1995-02-25")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_21956")
             :revenue 254541.1285
             :o_orderdate (util/date->local-date-time #inst "1995-02-02")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_1637")
             :revenue 243512.79809999999
             :o_orderdate (util/date->local-date-time #inst "1995-02-08")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_10916")
             :revenue 241320.08140000002
             :o_orderdate (util/date->local-date-time #inst "1995-03-11")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_30497")
             :revenue 208566.69689999998
             :o_orderdate (util/date->local-date-time #inst "1995-02-07")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_450")
             :revenue 205447.42320000002
             :o_orderdate (util/date->local-date-time #inst "1995-03-05")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_47204")
             :revenue 204478.52130000002
             :o_orderdate (util/date->local-date-time #inst "1995-03-13")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_9696")
             :revenue 201502.21879999997
             :o_orderdate (util/date->local-date-time #inst "1995-02-20")
             :o_shippriority 0}]
           (tpch-queries/tpch-q3-shipping-priority))))

(t/deftest ^:integration test-q4-order-priority-checking
  (t/is (= [{:o_orderpriority (Text. "1-URGENT"), :order_count 93}
            {:o_orderpriority (Text. "2-HIGH"), :order_count 103}
            {:o_orderpriority (Text. "3-MEDIUM"), :order_count 109}
            {:o_orderpriority (Text. "4-NOT SPECIFIED"), :order_count 102}
            {:o_orderpriority (Text. "5-LOW"), :order_count 128}]
           (tpch-queries/tpch-q4-order-priority-checking))))

(t/deftest ^:integration test-q5-local-supplier-volume
  (t/is (= [{:n_name (Text. "VIETNAM")
             :revenue 1000926.6999}
            {:n_name (Text. "CHINA")
             :revenue 740210.757}
            {:n_name (Text. "JAPAN")
             :revenue 660651.2424999999}
            {:n_name (Text. "INDONESIA")
             :revenue 566379.5276}
            {:n_name (Text. "INDIA")
             :revenue 422874.68439999997}]
           (tpch-queries/tpch-q5-local-supplier-volume))))

(t/deftest ^:integration test-q6-forecasting-revenue-change
  (t/is (= [{:revenue 1193053.2253}]
           (tpch-queries/tpch-q6-forecasting-revenue-change))))

(t/deftest ^:integration test-q7-volume-shipping
  (t/is (= [{:supp_nation (Text. "FRANCE")
             :cust_nation (Text. "GERMANY")
             :l_year 1995
             :revenue 268068.5774}
            {:supp_nation (Text. "FRANCE")
            :cust_nation (Text. "GERMANY")
            :l_year 1996,
            :revenue 303862.298}
            {:supp_nation (Text. "GERMANY")
             :cust_nation (Text. "FRANCE")
             :l_year 1995
             :revenue 621159.4882}
            {:supp_nation (Text. "GERMANY")
             :cust_nation(Text. "FRANCE")
             :l_year 1996,
             :revenue 379095.88539999997}]
           (tpch-queries/tpch-q7-volume-shipping))))

(t/deftest ^:integration test-q8-national-market-share
  (t/is (= [{:o_year 1995, :mkt_share 0.0}
            {:o_year 1996, :mkt_share 0.0}]
           (tpch-queries/tpch-q8-national-market-share))))

(t/deftest ^:integration test-q9-product-type-profit-measure
  (t/is (= [{:nation (Text. "ALGERIA") :o_year 1998 :sum_profit 97864.56820000001}
            {:nation (Text. "ALGERIA") :o_year 1997 :sum_profit 368231.6695}
            {:nation (Text. "ALGERIA") :o_year 1996 :sum_profit 196525.80459999997}
            {:nation (Text. "ALGERIA") :o_year 1995 :sum_profit 341438.6885}
            {:nation (Text. "ALGERIA") :o_year 1994 :sum_profit 677444.016}
            {:nation (Text. "ALGERIA") :o_year 1993 :sum_profit 458756.91569999995}
            {:nation (Text. "ALGERIA") :o_year 1992 :sum_profit 549243.9511}
            {:nation (Text. "ARGENTINA") :o_year 1998 :sum_profit 80448.76800000001}
            {:nation (Text. "ARGENTINA") :o_year 1997 :sum_profit 186279.16179999997}
            {:nation (Text. "ARGENTINA") :o_year 1996 :sum_profit 154041.88220000002}
            {:nation (Text. "ARGENTINA") :o_year 1995 :sum_profit 113143.3119}
            {:nation (Text. "ARGENTINA") :o_year 1994 :sum_profit 169680.4239}
            {:nation (Text. "ARGENTINA") :o_year 1993 :sum_profit 116513.81409999999}
            {:nation (Text. "ARGENTINA") :o_year 1992 :sum_profit 202404.7608}
            {:nation (Text. "BRAZIL") :o_year 1998 :sum_profit 75952.5946}
            {:nation (Text. "BRAZIL") :o_year 1997 :sum_profit 190548.11039999998}
            {:nation (Text. "BRAZIL") :o_year 1996 :sum_profit 219059.06919999997}
            {:nation (Text. "BRAZIL") :o_year 1995 :sum_profit 186435.2023}
            {:nation (Text. "BRAZIL") :o_year 1994 :sum_profit 96835.187}
            {:nation (Text. "BRAZIL") :o_year 1993 :sum_profit 186365.4109}
            {:nation (Text. "BRAZIL") :o_year 1992 :sum_profit 152546.44389999998}
            {:nation (Text. "CANADA") :o_year 1998 :sum_profit 101030.3336}
            {:nation (Text. "CANADA") :o_year 1997 :sum_profit 101197.34409999999}
            {:nation (Text. "CANADA") :o_year 1996 :sum_profit 257697.1355}
            {:nation (Text. "CANADA") :o_year 1995 :sum_profit 91474.88200000001}
            {:nation (Text. "CANADA") :o_year 1994 :sum_profit 249182.7548}
            {:nation (Text. "CANADA") :o_year 1993 :sum_profit 185737.83789999998}
            {:nation (Text. "CANADA") :o_year 1992 :sum_profit 143371.7465}
            {:nation (Text. "CHINA") :o_year 1998 :sum_profit 508364.5444}
            {:nation (Text. "CHINA") :o_year 1997 :sum_profit 650235.1646}
            {:nation (Text. "CHINA") :o_year 1996 :sum_profit 911366.0697999999}
            {:nation (Text. "CHINA") :o_year 1995 :sum_profit 797268.4075999999}
            {:nation (Text. "CHINA") :o_year 1994 :sum_profit 529989.3095}
            {:nation (Text. "CHINA") :o_year 1993 :sum_profit 573864.3972}
            {:nation (Text. "CHINA") :o_year 1992 :sum_profit 751688.7613}
            {:nation (Text. "EGYPT") :o_year 1998 :sum_profit 306325.2842}
            {:nation (Text. "EGYPT") :o_year 1997 :sum_profit 568461.6699}
            {:nation (Text. "EGYPT") :o_year 1996 :sum_profit 465081.9232}
            {:nation (Text. "EGYPT") :o_year 1995 :sum_profit 542886.5087}
            {:nation (Text. "EGYPT") :o_year 1994 :sum_profit 745807.8123}
            {:nation (Text. "EGYPT") :o_year 1993 :sum_profit 381503.2008}
            {:nation (Text. "EGYPT") :o_year 1992 :sum_profit 641866.4367}
            {:nation (Text. "ETHIOPIA") :o_year 1998 :sum_profit 226054.5716}
            {:nation (Text. "ETHIOPIA") :o_year 1997 :sum_profit 585193.2802}
            {:nation (Text. "ETHIOPIA") :o_year 1996 :sum_profit 405412.7741}
            {:nation (Text. "ETHIOPIA") :o_year 1995 :sum_profit 270455.7637}
            {:nation (Text. "ETHIOPIA") :o_year 1994 :sum_profit 567875.4279}
            {:nation (Text. "ETHIOPIA") :o_year 1993 :sum_profit 412302.28709999996}
            {:nation (Text. "ETHIOPIA") :o_year 1992 :sum_profit 551284.5821}
            {:nation (Text. "FRANCE") :o_year 1998 :sum_profit 135723.405}
            {:nation (Text. "FRANCE") :o_year 1997 :sum_profit 249664.7578}
            {:nation (Text. "FRANCE") :o_year 1996 :sum_profit 175882.8934}
            {:nation (Text. "FRANCE") :o_year 1995 :sum_profit 116394.78659999999}
            {:nation (Text. "FRANCE") :o_year 1994 :sum_profit 197695.24379999997}
            {:nation (Text. "FRANCE") :o_year 1993 :sum_profit 231878.6201}
            {:nation (Text. "FRANCE") :o_year 1992 :sum_profit 199131.20369999998}
            {:nation (Text. "GERMANY") :o_year 1998 :sum_profit 172741.1024}
            {:nation (Text. "GERMANY") :o_year 1997 :sum_profit 393833.46599999996}
            {:nation (Text. "GERMANY") :o_year 1996 :sum_profit 335634.59359999996}
            {:nation (Text. "GERMANY") :o_year 1995 :sum_profit 378106.0763}
            {:nation (Text. "GERMANY") :o_year 1994 :sum_profit 250107.6653}
            {:nation (Text. "GERMANY") :o_year 1993 :sum_profit 327154.9365}
            {:nation (Text. "GERMANY") :o_year 1992 :sum_profit 387240.08849999995}
            {:nation (Text. "INDIA") :o_year 1998 :sum_profit 347548.76039999997}
            {:nation (Text. "INDIA") :o_year 1997 :sum_profit 656797.967}
            {:nation (Text. "INDIA") :o_year 1996 :sum_profit 522759.3529}
            {:nation (Text. "INDIA") :o_year 1995 :sum_profit 574428.6693}
            {:nation (Text. "INDIA") :o_year 1994 :sum_profit 741983.7846}
            {:nation (Text. "INDIA") :o_year 1993 :sum_profit 729948.5340999999}
            {:nation (Text. "INDIA") :o_year 1992 :sum_profit 661061.1415}
            {:nation (Text. "INDONESIA") :o_year 1998 :sum_profit 91791.50959999999}
            {:nation (Text. "INDONESIA") :o_year 1997 :sum_profit 183956.46130000002}
            {:nation (Text. "INDONESIA") :o_year 1996 :sum_profit 415234.7848}
            {:nation (Text. "INDONESIA") :o_year 1995 :sum_profit 427155.38039999997}
            {:nation (Text. "INDONESIA") :o_year 1994 :sum_profit 286271.2875}
            {:nation (Text. "INDONESIA") :o_year 1993 :sum_profit 551178.8822999999}
            {:nation (Text. "INDONESIA") :o_year 1992 :sum_profit 274513.2685}
            {:nation (Text. "IRAN") :o_year 1998 :sum_profit 47959.821899999995}
            {:nation (Text. "IRAN") :o_year 1997 :sum_profit 184335.0615}
            {:nation (Text. "IRAN") :o_year 1996 :sum_profit 223115.2464}
            {:nation (Text. "IRAN") :o_year 1995 :sum_profit 125339.09270000001}
            {:nation (Text. "IRAN") :o_year 1994 :sum_profit 117228.31219999999}
            {:nation (Text. "IRAN") :o_year 1993 :sum_profit 208030.3229}
            {:nation (Text. "IRAN") :o_year 1992 :sum_profit 161835.5475}
            {:nation (Text. "IRAQ") :o_year 1998 :sum_profit 161797.4924}
            {:nation (Text. "IRAQ") :o_year 1997 :sum_profit 224876.5436}
            {:nation (Text. "IRAQ") :o_year 1996 :sum_profit 145277.89800000002}
            {:nation (Text. "IRAQ") :o_year 1995 :sum_profit 467955.25049999997}
            {:nation (Text. "IRAQ") :o_year 1994 :sum_profit 97455.299}
            {:nation (Text. "IRAQ") :o_year 1993 :sum_profit 114821.644}
            {:nation (Text. "IRAQ") :o_year 1992 :sum_profit 213307.1574}
            {:nation (Text. "JAPAN") :o_year 1998 :sum_profit 307594.598}
            {:nation (Text. "JAPAN") :o_year 1997 :sum_profit 339018.14879999997}
            {:nation (Text. "JAPAN") :o_year 1996 :sum_profit 649578.3367999999}
            {:nation (Text. "JAPAN") :o_year 1995 :sum_profit 671644.0911}
            {:nation (Text. "JAPAN") :o_year 1994 :sum_profit 576266.2386}
            {:nation (Text. "JAPAN") :o_year 1993 :sum_profit 514190.84369999997}
            {:nation (Text. "JAPAN") :o_year 1992 :sum_profit 534914.9339}
            {:nation (Text. "JORDAN") :o_year 1996 :sum_profit 33460.2447}
            {:nation (Text. "JORDAN") :o_year 1995 :sum_profit 20364.162300000004}
            {:nation (Text. "JORDAN") :o_year 1994 :sum_profit 15528.608800000002}
            {:nation (Text. "JORDAN") :o_year 1993 :sum_profit 14640.988899999998}
            {:nation (Text. "JORDAN") :o_year 1992 :sum_profit 10904.293099999999}
            {:nation (Text. "KENYA") :o_year 1998 :sum_profit 521926.5198}
            {:nation (Text. "KENYA") :o_year 1997 :sum_profit 559632.3408}
            {:nation (Text. "KENYA") :o_year 1996 :sum_profit 772855.7939}
            {:nation (Text. "KENYA") :o_year 1995 :sum_profit 516452.50669999997}
            {:nation (Text. "KENYA") :o_year 1994 :sum_profit 543665.8154}
            {:nation (Text. "KENYA") :o_year 1993 :sum_profit 866924.8754}
            {:nation (Text. "KENYA") :o_year 1992 :sum_profit 567410.5501999999}
            {:nation (Text. "MOROCCO") :o_year 1998 :sum_profit 217794.49730000002}
            {:nation (Text. "MOROCCO") :o_year 1997 :sum_profit 439240.9287}
            {:nation (Text. "MOROCCO") :o_year 1996 :sum_profit 399969.46799999994}
            {:nation (Text. "MOROCCO") :o_year 1995 :sum_profit 258131.9398}
            {:nation (Text. "MOROCCO") :o_year 1994 :sum_profit 386972.14239999995}
            {:nation (Text. "MOROCCO") :o_year 1993 :sum_profit 145468.0381}
            {:nation (Text. "MOROCCO") :o_year 1992 :sum_profit 284314.2813}
            {:nation (Text. "MOZAMBIQUE") :o_year 1998 :sum_profit 518693.2238}
            {:nation (Text. "MOZAMBIQUE") :o_year 1997 :sum_profit 613873.2960999999}
            {:nation (Text. "MOZAMBIQUE") :o_year 1996 :sum_profit 936793.5612}
            {:nation (Text. "MOZAMBIQUE") :o_year 1995 :sum_profit 727204.7718}
            {:nation (Text. "MOZAMBIQUE") :o_year 1994 :sum_profit 1104618.1807}
            {:nation (Text. "MOZAMBIQUE") :o_year 1993 :sum_profit 893266.053}
            {:nation (Text. "MOZAMBIQUE") :o_year 1992 :sum_profit 1062432.0884}
            {:nation (Text. "PERU") :o_year 1998 :sum_profit 287242.97969999997}
            {:nation (Text. "PERU") :o_year 1997 :sum_profit 532358.366}
            {:nation (Text. "PERU") :o_year 1996 :sum_profit 398435.7507}
            {:nation (Text. "PERU") :o_year 1995 :sum_profit 462031.6251}
            {:nation (Text. "PERU") :o_year 1994 :sum_profit 304235.4118}
            {:nation (Text. "PERU") :o_year 1993 :sum_profit 505885.48899999994}
            {:nation (Text. "PERU") :o_year 1992 :sum_profit 382290.09469999996}
            {:nation (Text. "ROMANIA") :o_year 1998 :sum_profit 357824.55280000006}
            {:nation (Text. "ROMANIA") :o_year 1997 :sum_profit 569806.5564}
            {:nation (Text. "ROMANIA") :o_year 1996 :sum_profit 732001.5568}
            {:nation (Text. "ROMANIA") :o_year 1995 :sum_profit 408657.1154}
            {:nation (Text. "ROMANIA") :o_year 1994 :sum_profit 540702.5463}
            {:nation (Text. "ROMANIA") :o_year 1993 :sum_profit 883158.5056}
            {:nation (Text. "ROMANIA") :o_year 1992 :sum_profit 505488.9501}
            {:nation (Text. "RUSSIA") :o_year 1998 :sum_profit 34448.63569999999}
            {:nation (Text. "RUSSIA") :o_year 1997 :sum_profit 314972.04459999996}
            {:nation (Text. "RUSSIA") :o_year 1996 :sum_profit 430049.5821}
            {:nation (Text. "RUSSIA") :o_year 1995 :sum_profit 360538.0586}
            {:nation (Text. "RUSSIA") :o_year 1994 :sum_profit 301791.0114}
            {:nation (Text. "RUSSIA") :o_year 1993 :sum_profit 308993.9622}
            {:nation (Text. "RUSSIA") :o_year 1992 :sum_profit 289868.6564}
            {:nation (Text. "SAUDI ARABIA") :o_year 1998 :sum_profit 16502.41}
            {:nation (Text. "SAUDI ARABIA") :o_year 1997 :sum_profit 61830.9556}
            {:nation (Text. "SAUDI ARABIA") :o_year 1996 :sum_profit 213650.28089999998}
            {:nation (Text. "SAUDI ARABIA") :o_year 1995 :sum_profit 62668.72499999999}
            {:nation (Text. "SAUDI ARABIA") :o_year 1994 :sum_profit 94629.15379999999}
            {:nation (Text. "SAUDI ARABIA") :o_year 1993 :sum_profit 57768.307100000005}
            {:nation (Text. "SAUDI ARABIA") :o_year 1992 :sum_profit 66520.10930000001}
            {:nation (Text. "UNITED KINGDOM") :o_year 1998 :sum_profit 80437.6523}
            {:nation (Text. "UNITED KINGDOM") :o_year 1997 :sum_profit 252509.7351}
            {:nation (Text. "UNITED KINGDOM") :o_year 1996 :sum_profit 231152.85820000002}
            {:nation (Text. "UNITED KINGDOM") :o_year 1995 :sum_profit 181310.88079999998}
            {:nation (Text. "UNITED KINGDOM") :o_year 1994 :sum_profit 239161.20609999998}
            {:nation (Text. "UNITED KINGDOM") :o_year 1993 :sum_profit 122103.11420000001}
            {:nation (Text. "UNITED KINGDOM") :o_year 1992 :sum_profit 60882.308000000005}
            {:nation (Text. "UNITED STATES") :o_year 1998 :sum_profit 440347.66579999996}
            {:nation (Text. "UNITED STATES") :o_year 1997 :sum_profit 652958.9371}
            {:nation (Text. "UNITED STATES") :o_year 1996 :sum_profit 1004593.8282}
            {:nation (Text. "UNITED STATES") :o_year 1995 :sum_profit 860144.1029}
            {:nation (Text. "UNITED STATES") :o_year 1994 :sum_profit 807797.4876999999}
            {:nation (Text. "UNITED STATES") :o_year 1993 :sum_profit 736669.4711}
            {:nation (Text. "UNITED STATES") :o_year 1992 :sum_profit 877851.4103}
            {:nation (Text. "VIETNAM") :o_year 1998 :sum_profit 358248.0159}
            {:nation (Text. "VIETNAM") :o_year 1997 :sum_profit 394817.2842}
            {:nation (Text. "VIETNAM") :o_year 1996 :sum_profit 439390.0836}
            {:nation (Text. "VIETNAM") :o_year 1995 :sum_profit 418626.6325}
            {:nation (Text. "VIETNAM") :o_year 1994 :sum_profit 422644.81680000003}
            {:nation (Text. "VIETNAM") :o_year 1993 :sum_profit 309063.402}
            {:nation (Text. "VIETNAM") :o_year 1992 :sum_profit 716126.5378}]
           (tpch-queries/tpch-q9-product-type-profit-measure))))

(t/deftest ^:integration test-q10-returned-item-reporting
  (t/is (= [{:c_custkey (Text. "custkey_679")
             :c_name (Text. "Customer#000000679")
             :revenue 378211.32519999996
             :c_acctbal 1394.44
             :c_phone (Text. "20-146-696-9508")
             :n_name (Text. "IRAN")
             :c_address (Text. "IJf1FlZL9I9m,rvofcoKy5pRUOjUQV")
             :c_comment (Text. "ely pending frays boost carefully")}
            {:c_custkey (Text. "custkey_1201")
             :c_name (Text. "Customer#000001201")
             :revenue 374331.534
             :c_acctbal 5165.39
             :c_phone (Text. "20-825-400-1187")
             :n_name (Text. "IRAN")
             :c_address (Text. "LfCSVKWozyWOGDW02g9UX,XgH5YU2o5ql1zBrN")
             :c_comment (Text. "lyly pending packages. special requests sleep-- platelets use blithely after the instructions. sometimes even id")}
            {:c_custkey (Text. "custkey_422")
             :c_name (Text. "Customer#000000422")
             :revenue 366451.0126
             :c_acctbal -272.14
             :c_phone (Text. "19-299-247-2444")
             :n_name (Text. "INDONESIA")
             :c_address (Text. "AyNzZBvmIDo42JtjP9xzaK3pnvkh Qc0o08ssnvq")
             :c_comment (Text. "eposits; furiously ironic packages accordi")}
            {:c_custkey (Text. "custkey_334")
             :c_name (Text. "Customer#000000334")
             :revenue 360370.755
             :c_acctbal -405.91
             :c_phone (Text. "14-947-291-5002")
             :n_name (Text. "EGYPT")
             :c_address (Text. "OPN1N7t4aQ23TnCpc")
             :c_comment (Text. "fully busily special ideas. carefully final excuses lose slyly carefully express accounts. even, ironic platelets ar")}
            {:c_custkey (Text. "custkey_805")
             :c_name (Text. "Customer#000000805")
             :revenue 359448.9036
             :c_acctbal 511.69
             :c_phone (Text. "20-732-989-5653")
             :n_name (Text. "IRAN")
             :c_address (Text. "wCKx5zcHvwpSffyc9qfi9dvqcm9LT,cLAG")
             :c_comment (Text. "busy sentiments. pending packages haggle among the express requests-- slyly regular excuses above the slyl")}
            {:c_custkey (Text. "custkey_932")
             :c_name (Text. "Customer#000000932")
             :revenue 341608.2753
             :c_acctbal 6553.37
             :c_phone (Text. "23-300-708-7927")
             :n_name (Text. "JORDAN")
             :c_address (Text. "HN9Ap0NsJG7Mb8O")
             :c_comment (Text. "packages boost slyly along the furiously express foxes. ev")}
            {:c_custkey (Text. "custkey_853")
             :c_name (Text. "Customer#000000853")
             :revenue 341236.6246
             :c_acctbal -444.73
             :c_phone (Text. "12-869-161-3468")
             :n_name (Text. "BRAZIL")
             :c_address (Text. "U0 9PrwAgWK8AE0GHmnCGtH9BTexWWv87k")
             :c_comment (Text. "yly special deposits wake alongside of")}
            {:c_custkey (Text. "custkey_872")
             :c_name (Text. "Customer#000000872")
             :revenue 338328.7808
             :c_acctbal -858.61
             :c_phone (Text. "27-357-139-7164")
             :n_name (Text. "PERU")
             :c_address (Text. "vLP7iNZBK4B,HANFTKabVI3AO Y9O8H")
             :c_comment (Text. " detect. packages wake slyly express foxes. even deposits ru")}
            {:c_custkey (Text. "custkey_737")
             :c_name (Text. "Customer#000000737")
             :revenue 338185.3365
             :c_acctbal 2501.74
             :c_phone (Text. "28-658-938-1102")
             :n_name (Text. "CHINA")
             :c_address (Text. "NdjG1k243iCLSoy1lYqMIrpvuH1Uf75")
             :c_comment (Text. "ding to the final platelets. regular packages against the carefully final ideas hag")}
            {:c_custkey (Text. "custkey_1118")
             :c_name (Text. "Customer#000001118")
             :revenue 319875.728
             :c_acctbal 4130.18
             :c_phone (Text. "21-583-715-8627")
             :n_name (Text. "IRAQ")
             :c_address (Text. "QHg,DNvEVXaYoCdrywazjAJ")
             :c_comment (Text. "y regular requests above the blithely ironic accounts use slyly bold packages: regular pinto beans eat carefully spe")}
            {:c_custkey (Text. "custkey_223")
             :c_name (Text. "Customer#000000223")
             :revenue 319564.27499999997
             :c_acctbal 7476.2
             :c_phone (Text. "30-193-643-1517")
             :n_name (Text. "SAUDI ARABIA")
             :c_address (Text. "ftau6Pk,brboMyEl,,kFm")
             :c_comment (Text. "al, regular requests run furiously blithely silent packages. blithely ironic accounts across the furious")}
            {:c_custkey (Text. "custkey_808")
             :c_name (Text. "Customer#000000808")
             :revenue 314774.6167
             :c_acctbal 5561.93
             :c_phone (Text. "29-531-319-7726")
             :n_name (Text. "ROMANIA")
             :c_address (Text. "S2WkSKCGtnbhcFOp6MWcuB3rzFlFemVNrg ")
             :c_comment (Text. " unusual deposits. furiously even packages against the furiously even ac")}
            {:c_custkey (Text. "custkey_478")
             :c_name (Text. "Customer#000000478")
             :revenue 299651.8026
             :c_acctbal -210.4
             :c_phone (Text. "11-655-291-2694")
             :n_name (Text. "ARGENTINA")
             :c_address (Text. "clyq458DIkXXt4qLyHlbe,n JueoniF")
             :c_comment (Text. "o the foxes. ironic requests sleep. c")}
            {:c_custkey (Text. "custkey_1441")
             :c_name (Text. "Customer#000001441")
             :revenue 294705.3935
             :c_acctbal 9465.15
             :c_phone (Text. "33-681-334-4499")
             :n_name (Text. "UNITED KINGDOM")
             :c_address (Text. "u0YYZb46w,pwKo5H9vz d6B9zK4BOHhG jx")
             :c_comment (Text. "nts haggle quietly quickly final accounts. slyly regular accounts among the sl")}
            {:c_custkey (Text. "custkey_1478")
             :c_name (Text. "Customer#000001478")
             :revenue 294431.9178
             :c_acctbal 9701.54
             :c_phone (Text. "17-420-484-5959")
             :n_name (Text. "GERMANY")
             :c_address (Text. "x7HDvJDDpR3MqZ5vg2CanfQ1hF0j4")
             :c_comment (Text. "ng the furiously bold foxes. even notornis above the unusual ")}
            {:c_custkey (Text. "custkey_211")
             :c_name (Text. "Customer#000000211")
             :revenue 287905.6368
             :c_acctbal 4198.72
             :c_phone (Text. "23-965-335-9471")
             :n_name (Text. "JORDAN")
             :c_address (Text. "URhlVPzz4FqXem")
             :c_comment (Text. "furiously regular foxes boost fluffily special ideas. carefully regular dependencies are. slyly ironic ")}
            {:c_custkey (Text. "custkey_197")
             :c_name (Text. "Customer#000000197")
             :revenue 283190.48069999996
             :c_acctbal 9860.22
             :c_phone (Text. "11-107-312-6585")
             :n_name (Text. "ARGENTINA")
             :c_address (Text. "UeVqssepNuXmtZ38D")
             :c_comment (Text. "ickly final accounts cajole. furiously re")}
            {:c_custkey (Text. "custkey_1030")
             :c_name (Text. "Customer#000001030")
             :revenue 282557.3566
             :c_acctbal 6359.27
             :c_phone (Text. "18-759-877-1870")
             :n_name (Text. "INDIA")
             :c_address (Text. "Xpt1BiB5h9o")
             :c_comment (Text. "ding to the slyly unusual accounts. even requests among the evenly")}
            {:c_custkey (Text. "custkey_1049")
             :c_name (Text. "Customer#000001049")
             :revenue 281134.1117
             :c_acctbal 8747.99
             :c_phone (Text. "19-499-258-2851")
             :n_name (Text. "INDONESIA")
             :c_address (Text. "bZ1OcFhHaIZ5gMiH")
             :c_comment (Text. "uriously according to the furiously silent packages")}
            {:c_custkey (Text. "custkey_1094")
             :c_name (Text. "Customer#000001094")
             :revenue 274877.444
             :c_acctbal 2544.49
             :c_phone (Text. "12-234-721-9871")
             :n_name (Text. "BRAZIL")
             :c_address (Text. "OFz0eedTmPmXk2 3XM9v9Mcp13NVC0PK")
             :c_comment (Text. "tes serve blithely quickly pending foxes. express, quick accounts")}]
           (tpch-queries/tpch-q10-returned-item-reporting))))

(t/deftest ^:integration test-q11-important-stock-identification
  (t/is (= [{:ps_partkey (Text. "partkey_1376") :value 1.327124989E7}
            {:ps_partkey (Text. "partkey_788") :value 9498648.06}
            {:ps_partkey (Text. "partkey_1071") :value 9388264.4}
            {:ps_partkey (Text. "partkey_1768") :value 9207199.75}
            {:ps_partkey (Text. "partkey_1168") :value 8881908.959999999}
            {:ps_partkey (Text. "partkey_1084") :value 8709494.16}
            {:ps_partkey (Text. "partkey_1415") :value 8471489.56}
            {:ps_partkey (Text. "partkey_1338") :value 8293841.12}
            {:ps_partkey (Text. "partkey_124") :value 8203209.3}
            {:ps_partkey (Text. "partkey_1232") :value 8111663.34}
            {:ps_partkey (Text. "partkey_1643") :value 7975862.75}
            {:ps_partkey (Text. "partkey_1952") :value 7936947.61}
            {:ps_partkey (Text. "partkey_1944") :value 7880018.6}
            {:ps_partkey (Text. "partkey_1884") :value 7513422.84}
            {:ps_partkey (Text. "partkey_942") :value 7511018.76}
            {:ps_partkey (Text. "partkey_670") :value 7299956.800000001}
            {:ps_partkey (Text. "partkey_1532") :value 7222347.199999999}
            {:ps_partkey (Text. "partkey_1052") :value 7158586.0}
            {:ps_partkey (Text. "partkey_455") :value 7064285.84}
            {:ps_partkey (Text. "partkey_1176") :value 7060670.890000001}
            {:ps_partkey (Text. "partkey_143") :value 7037648.64}
            {:ps_partkey (Text. "partkey_1653") :value 6949533.7}
            {:ps_partkey (Text. "partkey_1140") :value 6929464.08}
            {:ps_partkey (Text. "partkey_1076") :value 6877472.960000001}
            {:ps_partkey (Text. "partkey_2000") :value 6720009.38}
            {:ps_partkey (Text. "partkey_348") :value 6681307.34}
            {:ps_partkey (Text. "partkey_810") :value 6576640.95}
            {:ps_partkey (Text. "partkey_943") :value 6458641.7}
            {:ps_partkey (Text. "partkey_720") :value 6391330.27}
            {:ps_partkey (Text. "partkey_1748") :value 6341530.4}
            {:ps_partkey (Text. "partkey_1241") :value 6304944.66}
            {:ps_partkey (Text. "partkey_1384") :value 6279261.12}
            {:ps_partkey (Text. "partkey_1784") :value 6247863.25}
            {:ps_partkey (Text. "partkey_984") :value 6136927.0}
            {:ps_partkey (Text. "partkey_445") :value 6127784.28}
            {:ps_partkey (Text. "partkey_1976") :value 6079237.08}
            {:ps_partkey (Text. "partkey_1609") :value 6022720.8}
            {:ps_partkey (Text. "partkey_1563") :value 5978195.08}
            {:ps_partkey (Text. "partkey_452") :value 5838052.0}
            {:ps_partkey (Text. "partkey_222") :value 5737162.24}
            {:ps_partkey (Text. "partkey_1629") :value 5703117.12}
            {:ps_partkey (Text. "partkey_1454") :value 5694804.18}
            {:ps_partkey (Text. "partkey_1082") :value 5681981.25}
            {:ps_partkey (Text. "partkey_691") :value 5633589.720000001}
            {:ps_partkey (Text. "partkey_1474") :value 5614673.64}
            {:ps_partkey (Text. "partkey_1900") :value 5591905.359999999}
            {:ps_partkey (Text. "partkey_262") :value 5553285.32}
            {:ps_partkey (Text. "partkey_1876") :value 5517997.59}
            {:ps_partkey (Text. "partkey_1027") :value 5490916.0}
            {:ps_partkey (Text. "partkey_1833") :value 5451495.0}
            {:ps_partkey (Text. "partkey_513") :value 5374426.22}
            {:ps_partkey (Text. "partkey_752") :value 5358919.7}
            {:ps_partkey (Text. "partkey_1367") :value 5352773.25}
            {:ps_partkey (Text. "partkey_543") :value 5189101.68}
            {:ps_partkey (Text. "partkey_1144") :value 5174388.5600000005}
            {:ps_partkey (Text. "partkey_403") :value 5126118.15}
            {:ps_partkey (Text. "partkey_1406") :value 5121886.44}
            {:ps_partkey (Text. "partkey_320") :value 5072099.76}
            {:ps_partkey (Text. "partkey_1940") :value 5069178.399999999}
            {:ps_partkey (Text. "partkey_1503") :value 5050895.5}
            {:ps_partkey (Text. "partkey_1437") :value 5039590.600000001}
            {:ps_partkey (Text. "partkey_743") :value 5039271.42}
            {:ps_partkey (Text. "partkey_82") :value 4995939.0}
            {:ps_partkey (Text. "partkey_916") :value 4994730.1}
            {:ps_partkey (Text. "partkey_732") :value 4932809.82}
            {:ps_partkey (Text. "partkey_356") :value 4879860.09}
            {:ps_partkey (Text. "partkey_1592") :value 4831242.6}
            {:ps_partkey (Text. "partkey_1043") :value 4825921.3100000005}
            {:ps_partkey (Text. "partkey_132") :value 4781984.14}
            {:ps_partkey (Text. "partkey_1006") :value 4733954.64}
            {:ps_partkey (Text. "partkey_497") :value 4711173.600000001}
            {:ps_partkey (Text. "partkey_1008") :value 4565588.85}
            {:ps_partkey (Text. "partkey_1370") :value 4563830.100000001}
            {:ps_partkey (Text. "partkey_216") :value 4561143.8}
            {:ps_partkey (Text. "partkey_34") :value 4501982.71}
            {:ps_partkey (Text. "partkey_1908") :value 4417931.8}
            {:ps_partkey (Text. "partkey_982") :value 4391495.46}
            {:ps_partkey (Text. "partkey_1652") :value 4358793.14}
            {:ps_partkey (Text. "partkey_614") :value 4356657.45}
            {:ps_partkey (Text. "partkey_1552") :value 4355541.7}
            {:ps_partkey (Text. "partkey_359") :value 4353566.87}
            {:ps_partkey (Text. "partkey_1104") :value 4347515.9}
            {:ps_partkey (Text. "partkey_198") :value 4315049.0}
            {:ps_partkey (Text. "partkey_998") :value 4167784.8800000004}
            {:ps_partkey (Text. "partkey_1543") :value 4159568.16}
            {:ps_partkey (Text. "partkey_1308") :value 4153124.95}
            {:ps_partkey (Text. "partkey_474") :value 4123819.2}
            {:ps_partkey (Text. "partkey_1394") :value 4122729.3299999996}
            {:ps_partkey (Text. "partkey_271") :value 4095180.96}
            {:ps_partkey (Text. "partkey_908") :value 4088856.2}
            {:ps_partkey (Text. "partkey_1135") :value 4045014.13}
            {:ps_partkey (Text. "partkey_1632") :value 4010794.9}
            {:ps_partkey (Text. "partkey_1362") :value 3982060.16}
            {:ps_partkey (Text. "partkey_158") :value 3941881.65}
            {:ps_partkey (Text. "partkey_1852") :value 3923035.02}
            {:ps_partkey (Text. "partkey_1556") :value 3896709.54}
            {:ps_partkey (Text. "partkey_584") :value 3843848.3000000003}
            {:ps_partkey (Text. "partkey_885") :value 3826021.1599999997}
            {:ps_partkey (Text. "partkey_376") :value 3781201.96}
            {:ps_partkey (Text. "partkey_712") :value 3749696.8000000003}
            {:ps_partkey (Text. "partkey_2") :value 3743241.43}
            {:ps_partkey (Text. "partkey_676") :value 3735715.1999999997}
            {:ps_partkey (Text. "partkey_1832") :value 3709008.5999999996}
            {:ps_partkey (Text. "partkey_1955") :value 3702794.6999999997}
            {:ps_partkey (Text. "partkey_68") :value 3690702.41}
            {:ps_partkey (Text. "partkey_1435") :value 3659114.1}
            {:ps_partkey (Text. "partkey_1443") :value 3656762.84}
            {:ps_partkey (Text. "partkey_1278") :value 3653100.6599999997}
            {:ps_partkey (Text. "partkey_1920") :value 3647892.54}
            {:ps_partkey (Text. "partkey_423") :value 3602031.8000000003}
            {:ps_partkey (Text. "partkey_818") :value 3589047.6}
            {:ps_partkey (Text. "partkey_779") :value 3559597.5300000003}
            {:ps_partkey (Text. "partkey_485") :value 3558511.4400000004}
            {:ps_partkey (Text. "partkey_552") :value 3555470.1}
            {:ps_partkey (Text. "partkey_1269") :value 3510427.6500000004}
            {:ps_partkey (Text. "partkey_1602") :value 3492117.6999999997}
            {:ps_partkey (Text. "partkey_426") :value 3486888.02}
            {:ps_partkey (Text. "partkey_1452") :value 3480825.5999999996}
            {:ps_partkey (Text. "partkey_756") :value 3469373.7}
            {:ps_partkey (Text. "partkey_832") :value 3447746.46}
            {:ps_partkey (Text. "partkey_1493") :value 3446867.4}
            {:ps_partkey (Text. "partkey_1650") :value 3417752.58}
            {:ps_partkey (Text. "partkey_205") :value 3403046.25}
            {:ps_partkey (Text. "partkey_93") :value 3361425.8899999997}
            {:ps_partkey (Text. "partkey_76") :value 3342081.82}
            {:ps_partkey (Text. "partkey_1759") :value 3303050.4}
            {:ps_partkey (Text. "partkey_886") :value 3302180.6999999997}
            {:ps_partkey (Text. "partkey_1544") :value 3288573.16}
            {:ps_partkey (Text. "partkey_1932") :value 3270900.4000000004}
            {:ps_partkey (Text. "partkey_489") :value 3253368.3}
            {:ps_partkey (Text. "partkey_594") :value 3177408.5700000003}
            {:ps_partkey (Text. "partkey_184") :value 3177162.05}
            {:ps_partkey (Text. "partkey_950") :value 3165213.01}
            {:ps_partkey (Text. "partkey_1124") :value 3143279.36}
            {:ps_partkey (Text. "partkey_106") :value 3099021.98}
            {:ps_partkey (Text. "partkey_1964") :value 3016553.1}
            {:ps_partkey (Text. "partkey_384") :value 2964262.77}
            {:ps_partkey (Text. "partkey_974") :value 2959497.0999999996}
            {:ps_partkey (Text. "partkey_964") :value 2951329.4499999997}
            {:ps_partkey (Text. "partkey_1984") :value 2907345.36}
            {:ps_partkey (Text. "partkey_200") :value 2895688.3200000003}
            {:ps_partkey (Text. "partkey_683") :value 2829476.95}
            {:ps_partkey (Text. "partkey_1564") :value 2816506.56}
            {:ps_partkey (Text. "partkey_546") :value 2788059.64}
            {:ps_partkey (Text. "partkey_502") :value 2780828.64}
            {:ps_partkey (Text. "partkey_396") :value 2778421.39}
            {:ps_partkey (Text. "partkey_203") :value 2761439.88}
            {:ps_partkey (Text. "partkey_866") :value 2753031.1999999997}
            {:ps_partkey (Text. "partkey_1743") :value 2743889.4899999998}
            {:ps_partkey (Text. "partkey_1041") :value 2738083.92}
            {:ps_partkey (Text. "partkey_1432") :value 2713412.16}
            {:ps_partkey (Text. "partkey_43") :value 2587359.58}
            {:ps_partkey (Text. "partkey_941") :value 2587091.52}
            {:ps_partkey (Text. "partkey_1890") :value 2558739.69}
            {:ps_partkey (Text. "partkey_1866") :value 2545838.4}
            {:ps_partkey (Text. "partkey_747") :value 2511745.32}
            {:ps_partkey (Text. "partkey_776") :value 2506489.89}
            {:ps_partkey (Text. "partkey_554") :value 2505417.25}
            {:ps_partkey (Text. "partkey_1210") :value 2490820.92}
            {:ps_partkey (Text. "partkey_1239") :value 2405206.3000000003}
            {:ps_partkey (Text. "partkey_443") :value 2382150.05}
            {:ps_partkey (Text. "partkey_1661") :value 2370574.16}
            {:ps_partkey (Text. "partkey_1079") :value 2363505.11}
            {:ps_partkey (Text. "partkey_1329") :value 2305870.42}
            {:ps_partkey (Text. "partkey_1691") :value 2261159.92}
            {:ps_partkey (Text. "partkey_1247") :value 2239553.2800000003}
            {:ps_partkey (Text. "partkey_1752") :value 2230055.7600000002}
            {:ps_partkey (Text. "partkey_150") :value 2217043.59}
            {:ps_partkey (Text. "partkey_1814") :value 2213635.2}
            {:ps_partkey (Text. "partkey_289") :value 2187160.4499999997}
            {:ps_partkey (Text. "partkey_1400") :value 2139845.1}
            {:ps_partkey (Text. "partkey_1898") :value 2130114.96}
            {:ps_partkey (Text. "partkey_1809") :value 2122758.7199999997}
            {:ps_partkey (Text. "partkey_884") :value 2107479.56}
            {:ps_partkey (Text. "partkey_1038") :value 2096868.97}
            {:ps_partkey (Text. "partkey_1318") :value 2051302.4400000002}
            {:ps_partkey (Text. "partkey_524") :value 2035262.22}
            {:ps_partkey (Text. "partkey_414") :value 2029692.4499999997}
            {:ps_partkey (Text. "partkey_298") :value 2026981.74}
            {:ps_partkey (Text. "partkey_1996") :value 2020953.5399999998}
            {:ps_partkey (Text. "partkey_1742") :value 2019190.7999999998}
            {:ps_partkey (Text. "partkey_1620") :value 2010112.0}
            {:ps_partkey (Text. "partkey_877") :value 1956429.1800000002}
            {:ps_partkey (Text. "partkey_1332") :value 1919029.56}
            {:ps_partkey (Text. "partkey_1536") :value 1859318.1500000001}
            {:ps_partkey (Text. "partkey_1116") :value 1852588.28}
            {:ps_partkey (Text. "partkey_447") :value 1817951.32}
            {:ps_partkey (Text. "partkey_1676") :value 1802306.08}
            {:ps_partkey (Text. "partkey_1911") :value 1779646.44}
            {:ps_partkey (Text. "partkey_1459") :value 1767602.3}
            {:ps_partkey (Text. "partkey_576") :value 1761838.75}
            {:ps_partkey (Text. "partkey_1273") :value 1754235.01}
            {:ps_partkey (Text. "partkey_583") :value 1725649.9200000002}
            {:ps_partkey (Text. "partkey_532") :value 1682311.48}
            {:ps_partkey (Text. "partkey_1732") :value 1652831.2000000002}
            {:ps_partkey (Text. "partkey_1572") :value 1650953.52}
            {:ps_partkey (Text. "partkey_1889") :value 1638443.72}
            {:ps_partkey (Text. "partkey_476") :value 1631154.06}
            {:ps_partkey (Text. "partkey_1221") :value 1629883.46}
            {:ps_partkey (Text. "partkey_1792") :value 1606346.1}
            {:ps_partkey (Text. "partkey_243") :value 1603235.16}
            {:ps_partkey (Text. "partkey_328") :value 1569826.72}
            {:ps_partkey (Text. "partkey_1999") :value 1553706.0}
            {:ps_partkey (Text. "partkey_1611") :value 1529857.01}
            {:ps_partkey (Text. "partkey_643") :value 1512838.8}
            {:ps_partkey (Text. "partkey_1276") :value 1467567.2799999998}
            {:ps_partkey (Text. "partkey_1823") :value 1462292.9999999998}
            {:ps_partkey (Text. "partkey_1") :value 1456050.96}
            {:ps_partkey (Text. "partkey_27") :value 1425832.4}
            {:ps_partkey (Text. "partkey_632") :value 1408087.26}
            {:ps_partkey (Text. "partkey_1184") :value 1406101.78}
            {:ps_partkey (Text. "partkey_252") :value 1379186.35}
            {:ps_partkey (Text. "partkey_392") :value 1354813.18}
            {:ps_partkey (Text. "partkey_1215") :value 1344383.2000000002}
            {:ps_partkey (Text. "partkey_26") :value 1337002.8900000001}
            {:ps_partkey (Text. "partkey_84") :value 1334146.71}
            {:ps_partkey (Text. "partkey_784") :value 1327297.01}
            {:ps_partkey (Text. "partkey_1803") :value 1327045.06}
            {:ps_partkey (Text. "partkey_352") :value 1326102.34}
            {:ps_partkey (Text. "partkey_165") :value 1289075.76}
            {:ps_partkey (Text. "partkey_176") :value 1285866.2}
            {:ps_partkey (Text. "partkey_1314") :value 1244173.26}
            {:ps_partkey (Text. "partkey_1701") :value 1239095.4400000002}
            {:ps_partkey (Text. "partkey_844") :value 1225696.05}
            {:ps_partkey (Text. "partkey_1988") :value 1216798.33}
            {:ps_partkey (Text. "partkey_1847") :value 1202012.13}
            {:ps_partkey (Text. "partkey_1706") :value 1184125.1}
            {:ps_partkey (Text. "partkey_744") :value 1182820.8}
            {:ps_partkey (Text. "partkey_230") :value 1165932.3}
            {:ps_partkey (Text. "partkey_418") :value 1078321.4400000002}
            {:ps_partkey (Text. "partkey_174") :value 1060584.8}
            {:ps_partkey (Text. "partkey_1073") :value 1028449.89}
            {:ps_partkey (Text. "partkey_1726") :value 1018673.04}
            {:ps_partkey (Text. "partkey_1206") :value 1002319.49}
            {:ps_partkey (Text. "partkey_1343") :value 998105.76}
            {:ps_partkey (Text. "partkey_952") :value 997684.24}
            {:ps_partkey (Text. "partkey_484") :value 991530.93}
            {:ps_partkey (Text. "partkey_932") :value 980620.6799999999}
            {:ps_partkey (Text. "partkey_843") :value 978862.9199999999}
            {:ps_partkey (Text. "partkey_1841") :value 962131.8600000001}
            {:ps_partkey (Text. "partkey_494") :value 957575.34}
            {:ps_partkey (Text. "partkey_659") :value 954291.0499999999}
            {:ps_partkey (Text. "partkey_251") :value 939764.7}
            {:ps_partkey (Text. "partkey_1413") :value 936951.94}
            {:ps_partkey (Text. "partkey_572") :value 906111.99}
            {:ps_partkey (Text. "partkey_32") :value 894484.09}
            {:ps_partkey (Text. "partkey_9") :value 893905.9199999999}
            {:ps_partkey (Text. "partkey_1498") :value 890887.85}
            {:ps_partkey (Text. "partkey_1790") :value 878923.64}
            {:ps_partkey (Text. "partkey_1670") :value 854046.43}
            {:ps_partkey (Text. "partkey_876") :value 842245.6699999999}
            {:ps_partkey (Text. "partkey_1758") :value 841275.42}
            {:ps_partkey (Text. "partkey_930") :value 832963.6799999999}
            {:ps_partkey (Text. "partkey_284") :value 826642.6000000001}
            {:ps_partkey (Text. "partkey_1710") :value 811504.38}
            {:ps_partkey (Text. "partkey_1047") :value 791214.45}
            {:ps_partkey (Text. "partkey_653") :value 788974.21}
            {:ps_partkey (Text. "partkey_315") :value 770526.0499999999}
            {:ps_partkey (Text. "partkey_1734") :value 763569.4}
            {:ps_partkey (Text. "partkey_1017") :value 715302.72}
            {:ps_partkey (Text. "partkey_1305") :value 713351.43}
            {:ps_partkey (Text. "partkey_77") :value 688865.82}
            {:ps_partkey (Text. "partkey_1512") :value 682434.15}
            {:ps_partkey (Text. "partkey_276") :value 680239.04}
            {:ps_partkey (Text. "partkey_1284") :value 671225.9400000001}
            {:ps_partkey (Text. "partkey_1356") :value 665716.83}
            {:ps_partkey (Text. "partkey_800") :value 663414.65}
            {:ps_partkey (Text. "partkey_117") :value 639650.88}
            {:ps_partkey (Text. "partkey_652") :value 635629.28}
            {:ps_partkey (Text. "partkey_57") :value 630987.4400000001}
            {:ps_partkey (Text. "partkey_1426") :value 628241.25}
            {:ps_partkey (Text. "partkey_1196") :value 622427.16}
            {:ps_partkey (Text. "partkey_51") :value 622249.54}
            {:ps_partkey (Text. "partkey_1846") :value 621068.7999999999}
            {:ps_partkey (Text. "partkey_601") :value 615942.6}
            {:ps_partkey (Text. "partkey_645") :value 607985.8400000001}
            {:ps_partkey (Text. "partkey_684") :value 571490.7000000001}
            {:ps_partkey (Text. "partkey_465") :value 570337.4}
            {:ps_partkey (Text. "partkey_562") :value 567651.24}
            {:ps_partkey (Text. "partkey_387") :value 556634.76}
            {:ps_partkey (Text. "partkey_1152") :value 555989.28}
            {:ps_partkey (Text. "partkey_1202") :value 553818.1799999999}
            {:ps_partkey (Text. "partkey_1112") :value 552658.6799999999}
            {:ps_partkey (Text. "partkey_304") :value 535868.1599999999}
            {:ps_partkey (Text. "partkey_368") :value 526995.84}
            {:ps_partkey (Text. "partkey_1800") :value 526711.11}
            {:ps_partkey (Text. "partkey_1148") :value 515702.16000000003}
            {:ps_partkey (Text. "partkey_225") :value 513587.57}
            {:ps_partkey (Text. "partkey_324") :value 500954.58}
            {:ps_partkey (Text. "partkey_586") :value 499475.58}
            {:ps_partkey (Text. "partkey_1576") :value 494401.05}
            {:ps_partkey (Text. "partkey_1484") :value 462396.27}
            {:ps_partkey (Text. "partkey_126") :value 461263.74}
            {:ps_partkey (Text. "partkey_1132") :value 455492.24}
            {:ps_partkey (Text. "partkey_622") :value 449685.6}
            {:ps_partkey (Text. "partkey_1160") :value 448183.06}
            {:ps_partkey (Text. "partkey_1352") :value 439967.04000000004}
            {:ps_partkey (Text. "partkey_18") :value 426442.07999999996}
            {:ps_partkey (Text. "partkey_7") :value 414558.2}
            {:ps_partkey (Text. "partkey_833") :value 398540.87}
            {:ps_partkey (Text. "partkey_1694") :value 376443.98}
            {:ps_partkey (Text. "partkey_650") :value 370900.99}
            {:ps_partkey (Text. "partkey_1504") :value 370815.9}
            {:ps_partkey (Text. "partkey_432") :value 370528.51999999996}
            {:ps_partkey (Text. "partkey_612") :value 367894.5}
            {:ps_partkey (Text. "partkey_542") :value 367653.66}
            {:ps_partkey (Text. "partkey_456") :value 360911.32}
            {:ps_partkey (Text. "partkey_52") :value 358792.36}
            {:ps_partkey (Text. "partkey_1346") :value 350637.43}
            {:ps_partkey (Text. "partkey_59") :value 342221.48000000004}
            {:ps_partkey (Text. "partkey_1107") :value 341805.2}
            {:ps_partkey (Text. "partkey_1171") :value 334938.04}
            {:ps_partkey (Text. "partkey_1062") :value 326445.89999999997}
            {:ps_partkey (Text. "partkey_592") :value 313081.75}
            {:ps_partkey (Text. "partkey_1750") :value 312229.33}
            {:ps_partkey (Text. "partkey_1843") :value 309456.95}
            {:ps_partkey (Text. "partkey_180") :value 308539.84}
            {:ps_partkey (Text. "partkey_899") :value 301989.5}
            {:ps_partkey (Text. "partkey_1180") :value 293452.5}
            {:ps_partkey (Text. "partkey_522") :value 291601.75}
            {:ps_partkey (Text. "partkey_249") :value 282520.32}
            {:ps_partkey (Text. "partkey_1584") :value 278559.38}
            {:ps_partkey (Text. "partkey_1404") :value 276057.89999999997}
            {:ps_partkey (Text. "partkey_1265") :value 271079.76}
            {:ps_partkey (Text. "partkey_154") :value 269641.42}
            {:ps_partkey (Text. "partkey_1295") :value 265566.56}
            {:ps_partkey (Text. "partkey_1523") :value 263158.9}
            {:ps_partkey (Text. "partkey_1635") :value 254834.56000000003}
            {:ps_partkey (Text. "partkey_1776") :value 234181.19999999998}
            {:ps_partkey (Text. "partkey_1097") :value 234113.55000000002}
            {:ps_partkey (Text. "partkey_1258") :value 233500.61000000002}
            {:ps_partkey (Text. "partkey_621") :value 233431.30000000002}
            {:ps_partkey (Text. "partkey_152") :value 229781.6}
            {:ps_partkey (Text. "partkey_278") :value 216372.84}
            {:ps_partkey (Text. "partkey_232") :value 211879.92}
            {:ps_partkey (Text. "partkey_1684") :value 201386.22}
            {:ps_partkey (Text. "partkey_1243") :value 199587.53999999998}
            {:ps_partkey (Text. "partkey_976") :value 197432.1}
            {:ps_partkey (Text. "partkey_819") :value 191475.90000000002}
            {:ps_partkey (Text. "partkey_1943") :value 191247.75999999998}
            {:ps_partkey (Text. "partkey_853") :value 189232.64}
            {:ps_partkey (Text. "partkey_400") :value 188941.19999999998}
            {:ps_partkey (Text. "partkey_639") :value 186533.28}
            {:ps_partkey (Text. "partkey_851") :value 184103.16}
            {:ps_partkey (Text. "partkey_909") :value 175099.0}
            {:ps_partkey (Text. "partkey_257") :value 169033.44}
            {:ps_partkey (Text. "partkey_1445") :value 164888.68}
            {:ps_partkey (Text. "partkey_1855") :value 164614.81}
            {:ps_partkey (Text. "partkey_1252") :value 158680.9}
            {:ps_partkey (Text. "partkey_1014") :value 156465.82}
            {:ps_partkey (Text. "partkey_1717") :value 148325.75}
            {:ps_partkey (Text. "partkey_1032") :value 146408.4}
            {:ps_partkey (Text. "partkey_780") :value 136296.26}
            {:ps_partkey (Text. "partkey_918") :value 135268.32}
            {:ps_partkey (Text. "partkey_690") :value 133826.88}
            {:ps_partkey (Text. "partkey_711") :value 113268.84}
            {:ps_partkey (Text. "partkey_332") :value 112181.3}
            {:ps_partkey (Text. "partkey_1596") :value 110565.0}
            {:ps_partkey (Text. "partkey_295") :value 97604.25}]
           (tpch-queries/tpch-q11-important-stock-identification))))

(t/deftest ^:integration test-q12-shipping-modes-and-order-priority
  (t/is (= [{:l_shipmode (Text. "MAIL")
             :high_line_count 64
             :low_line_count 86}
            {:l_shipmode (Text. "SHIP")
             :high_line_count 61
             :low_line_count 96}]
           (tpch-queries/tpch-q12-shipping-modes-and-order-priority))))

(t/deftest ^:integration test-q13-customer-distribution
  (t/is (= [{:c_count 0 :custdist 500}
            {:c_count 11 :custdist 68}
            {:c_count 10 :custdist 64}
            {:c_count 12 :custdist 62}
            {:c_count 9 :custdist 62}
            {:c_count 8 :custdist 61}
            {:c_count 14 :custdist 54}
            {:c_count 13 :custdist 52}
            {:c_count 7 :custdist 49}
            {:c_count 20 :custdist 48}
            {:c_count 21 :custdist 47}
            {:c_count 16 :custdist 46}
            {:c_count 15 :custdist 45}
            {:c_count 19 :custdist 44}
            {:c_count 17 :custdist 41}
            {:c_count 18 :custdist 38}
            {:c_count 22 :custdist 33}
            {:c_count 6 :custdist 33}
            {:c_count 24 :custdist 30}
            {:c_count 23 :custdist 27}
            {:c_count 25 :custdist 21}
            {:c_count 27 :custdist 17}
            {:c_count 26 :custdist 15}
            {:c_count 5 :custdist 14}
            {:c_count 28 :custdist 6}
            {:c_count 4 :custdist 6}
            {:c_count 32 :custdist 5}
            {:c_count 29 :custdist 5}
            {:c_count 30 :custdist 2}
            {:c_count 3 :custdist 2}
            {:c_count 31 :custdist 1}
            {:c_count 2 :custdist 1}
            {:c_count 1 :custdist 1}]
           (tpch-queries/tpch-q13-customer-distribution))))

(t/deftest ^:integration test-q14-promotion-effect
  (t/is (= [{:promo_revenue 15.486545812284072}]
           (tpch-queries/tpch-q14-promotion-effect))))

(t/deftest ^:integration test-q15-top-supplier
  (t/is (= [{:total_revenue 1161099.4636
             :s_suppkey (Text. "suppkey_21")
             :s_name (Text. "Supplier#000000021")
             :s_address (Text. "81CavellcrJ0PQ3CPBID0Z0JwyJm0ka5igEs")
             :s_phone (Text. "12-253-590-5816")}]
           (tpch-queries/tpch-q15-top-supplier))))

(def ^:private q16-result [{:p_brand (Text. "Brand#14") :p_type (Text. "PROMO BRUSHED STEEL") :p_size 9 :supplier_cnt 8}
                           {:p_brand (Text. "Brand#35") :p_type (Text. "SMALL POLISHED COPPER") :p_size 14 :supplier_cnt 8}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "LARGE BURNISHED TIN") :p_size 36 :supplier_cnt 6}
                           {:p_brand (Text. "Brand#11") :p_type (Text. "ECONOMY BURNISHED NICKEL") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#11") :p_type (Text. "LARGE PLATED TIN") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#11") :p_type (Text. "MEDIUM ANODIZED BRASS") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#11") :p_type (Text. "MEDIUM BRUSHED BRASS") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#11") :p_type (Text. "PROMO ANODIZED BRASS") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#11") :p_type (Text. "PROMO ANODIZED BRASS") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#11") :p_type (Text. "PROMO ANODIZED TIN") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#11") :p_type (Text. "PROMO BURNISHED BRASS") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#11") :p_type (Text. "SMALL ANODIZED TIN") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#11") :p_type (Text. "SMALL PLATED COPPER") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#11") :p_type (Text. "STANDARD POLISHED NICKEL") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#11") :p_type (Text. "STANDARD POLISHED TIN") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#12") :p_type (Text. "ECONOMY BURNISHED COPPER") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#12") :p_type (Text. "LARGE ANODIZED TIN") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#12") :p_type (Text. "LARGE BURNISHED BRASS") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#12") :p_type (Text. "LARGE PLATED STEEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#12") :p_type (Text. "MEDIUM PLATED BRASS") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#12") :p_type (Text. "PROMO BRUSHED COPPER") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#12") :p_type (Text. "PROMO BURNISHED BRASS") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#12") :p_type (Text. "SMALL ANODIZED COPPER") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#12") :p_type (Text. "STANDARD ANODIZED BRASS") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#12") :p_type (Text. "STANDARD BURNISHED TIN") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#12") :p_type (Text. "STANDARD PLATED STEEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#13") :p_type (Text. "ECONOMY PLATED STEEL") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#13") :p_type (Text. "ECONOMY POLISHED BRASS") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#13") :p_type (Text. "ECONOMY POLISHED COPPER") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#13") :p_type (Text. "LARGE ANODIZED TIN") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#13") :p_type (Text. "LARGE BURNISHED TIN") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#13") :p_type (Text. "LARGE POLISHED BRASS") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#13") :p_type (Text. "MEDIUM ANODIZED STEEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#13") :p_type (Text. "MEDIUM PLATED COPPER") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#13") :p_type (Text. "PROMO BRUSHED COPPER") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#13") :p_type (Text. "PROMO PLATED TIN") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#13") :p_type (Text. "SMALL BRUSHED NICKEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#13") :p_type (Text. "SMALL BURNISHED BRASS") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#14") :p_type (Text. "ECONOMY ANODIZED STEEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#14") :p_type (Text. "ECONOMY BURNISHED TIN") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#14") :p_type (Text. "ECONOMY PLATED STEEL") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#14") :p_type (Text. "ECONOMY PLATED TIN") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#14") :p_type (Text. "LARGE ANODIZED NICKEL") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#14") :p_type (Text. "LARGE BRUSHED NICKEL") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#14") :p_type (Text. "SMALL ANODIZED NICKEL") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#14") :p_type (Text. "SMALL BURNISHED COPPER") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#14") :p_type (Text. "SMALL BURNISHED TIN") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#15") :p_type (Text. "ECONOMY ANODIZED STEEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#15") :p_type (Text. "ECONOMY BRUSHED BRASS") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#15") :p_type (Text. "ECONOMY BURNISHED BRASS") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#15") :p_type (Text. "ECONOMY PLATED STEEL") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#15") :p_type (Text. "LARGE ANODIZED BRASS") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#15") :p_type (Text. "LARGE ANODIZED COPPER") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#15") :p_type (Text. "MEDIUM ANODIZED COPPER") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#15") :p_type (Text. "MEDIUM PLATED TIN") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#15") :p_type (Text. "PROMO POLISHED TIN") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#15") :p_type (Text. "SMALL POLISHED STEEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#15") :p_type (Text. "STANDARD BURNISHED STEEL") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#15") :p_type (Text. "STANDARD PLATED NICKEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#15") :p_type (Text. "STANDARD PLATED TIN") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#21") :p_type (Text. "ECONOMY ANODIZED STEEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#21") :p_type (Text. "ECONOMY BRUSHED TIN") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#21") :p_type (Text. "LARGE BURNISHED COPPER") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#21") :p_type (Text. "MEDIUM ANODIZED TIN") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#21") :p_type (Text. "MEDIUM BURNISHED STEEL") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#21") :p_type (Text. "PROMO BRUSHED STEEL") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#21") :p_type (Text. "PROMO BURNISHED COPPER") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#21") :p_type (Text. "STANDARD PLATED BRASS") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#21") :p_type (Text. "STANDARD POLISHED TIN") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "ECONOMY BURNISHED NICKEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "LARGE ANODIZED STEEL") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "LARGE BURNISHED STEEL") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "LARGE BURNISHED STEEL") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "LARGE BURNISHED TIN") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "LARGE POLISHED NICKEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "MEDIUM ANODIZED TIN") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "MEDIUM BRUSHED BRASS") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "MEDIUM BRUSHED COPPER") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "MEDIUM BRUSHED COPPER") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "MEDIUM BURNISHED TIN") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "MEDIUM BURNISHED TIN") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "MEDIUM PLATED BRASS") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "PROMO BRUSHED BRASS") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "PROMO BRUSHED STEEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "SMALL BRUSHED NICKEL") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "SMALL BURNISHED STEEL") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "STANDARD PLATED NICKEL") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#22") :p_type (Text. "STANDARD PLATED TIN") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#23") :p_type (Text. "ECONOMY BRUSHED COPPER") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#23") :p_type (Text. "LARGE ANODIZED COPPER") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#23") :p_type (Text. "LARGE PLATED BRASS") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#23") :p_type (Text. "MEDIUM BRUSHED NICKEL") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#23") :p_type (Text. "PROMO ANODIZED COPPER") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#23") :p_type (Text. "PROMO BURNISHED COPPER") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#23") :p_type (Text. "PROMO POLISHED BRASS") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#23") :p_type (Text. "SMALL BRUSHED BRASS") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#23") :p_type (Text. "SMALL BRUSHED COPPER") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#23") :p_type (Text. "SMALL BURNISHED COPPER") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#23") :p_type (Text. "SMALL PLATED BRASS") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#23") :p_type (Text. "SMALL POLISHED BRASS") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#23") :p_type (Text. "STANDARD BRUSHED TIN") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#23") :p_type (Text. "STANDARD PLATED BRASS") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#23") :p_type (Text. "STANDARD PLATED STEEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#23") :p_type (Text. "STANDARD PLATED TIN") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#24") :p_type (Text. "ECONOMY BRUSHED BRASS") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#24") :p_type (Text. "ECONOMY PLATED COPPER") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#24") :p_type (Text. "LARGE PLATED NICKEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#24") :p_type (Text. "MEDIUM PLATED STEEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#24") :p_type (Text. "PROMO POLISHED BRASS") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#24") :p_type (Text. "SMALL ANODIZED COPPER") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#24") :p_type (Text. "STANDARD BRUSHED BRASS") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#24") :p_type (Text. "STANDARD BRUSHED STEEL") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#24") :p_type (Text. "STANDARD POLISHED NICKEL") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#25") :p_type (Text. "ECONOMY BURNISHED TIN") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#25") :p_type (Text. "ECONOMY PLATED NICKEL") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#25") :p_type (Text. "LARGE ANODIZED NICKEL") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#25") :p_type (Text. "LARGE BRUSHED NICKEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#25") :p_type (Text. "LARGE BURNISHED TIN") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#25") :p_type (Text. "MEDIUM BURNISHED NICKEL") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#25") :p_type (Text. "MEDIUM PLATED BRASS") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#25") :p_type (Text. "PROMO ANODIZED TIN") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#25") :p_type (Text. "PROMO BURNISHED COPPER") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#25") :p_type (Text. "PROMO PLATED NICKEL") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#25") :p_type (Text. "SMALL BURNISHED COPPER") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#25") :p_type (Text. "SMALL PLATED TIN") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#25") :p_type (Text. "STANDARD ANODIZED TIN") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#25") :p_type (Text. "STANDARD PLATED NICKEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#31") :p_type (Text. "ECONOMY BURNISHED COPPER") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#31") :p_type (Text. "ECONOMY PLATED STEEL") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#31") :p_type (Text. "LARGE PLATED NICKEL") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#31") :p_type (Text. "MEDIUM BURNISHED COPPER") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#31") :p_type (Text. "MEDIUM PLATED TIN") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#31") :p_type (Text. "PROMO ANODIZED NICKEL") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#31") :p_type (Text. "PROMO POLISHED TIN") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#31") :p_type (Text. "SMALL ANODIZED COPPER") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#31") :p_type (Text. "SMALL ANODIZED COPPER") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#31") :p_type (Text. "SMALL BRUSHED NICKEL") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#31") :p_type (Text. "SMALL PLATED COPPER") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#32") :p_type (Text. "ECONOMY ANODIZED COPPER") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#32") :p_type (Text. "ECONOMY PLATED COPPER") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#32") :p_type (Text. "LARGE ANODIZED STEEL") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#32") :p_type (Text. "MEDIUM ANODIZED STEEL") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#32") :p_type (Text. "MEDIUM BURNISHED BRASS") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#32") :p_type (Text. "MEDIUM BURNISHED BRASS") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#32") :p_type (Text. "PROMO BRUSHED STEEL") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#32") :p_type (Text. "PROMO BURNISHED TIN") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#32") :p_type (Text. "SMALL ANODIZED TIN") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#32") :p_type (Text. "SMALL BRUSHED COPPER") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#32") :p_type (Text. "SMALL PLATED COPPER") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#32") :p_type (Text. "SMALL POLISHED STEEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#32") :p_type (Text. "SMALL POLISHED TIN") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#32") :p_type (Text. "STANDARD PLATED STEEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#33") :p_type (Text. "ECONOMY BURNISHED COPPER") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#33") :p_type (Text. "ECONOMY POLISHED BRASS") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#33") :p_type (Text. "LARGE BRUSHED TIN") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#33") :p_type (Text. "MEDIUM ANODIZED BRASS") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#33") :p_type (Text. "MEDIUM BURNISHED COPPER") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#33") :p_type (Text. "MEDIUM PLATED STEEL") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#33") :p_type (Text. "PROMO PLATED STEEL") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#33") :p_type (Text. "PROMO PLATED TIN") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#33") :p_type (Text. "PROMO POLISHED STEEL") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#33") :p_type (Text. "SMALL ANODIZED COPPER") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#33") :p_type (Text. "SMALL BRUSHED STEEL") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#33") :p_type (Text. "SMALL BURNISHED NICKEL") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#33") :p_type (Text. "STANDARD PLATED NICKEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#34") :p_type (Text. "ECONOMY ANODIZED TIN") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#34") :p_type (Text. "LARGE ANODIZED BRASS") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#34") :p_type (Text. "LARGE BRUSHED COPPER") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#34") :p_type (Text. "LARGE BURNISHED TIN") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#34") :p_type (Text. "LARGE PLATED BRASS") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#34") :p_type (Text. "MEDIUM BRUSHED COPPER") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#34") :p_type (Text. "MEDIUM BRUSHED TIN") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#34") :p_type (Text. "MEDIUM BURNISHED NICKEL") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#34") :p_type (Text. "SMALL ANODIZED STEEL") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#34") :p_type (Text. "SMALL BRUSHED TIN") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#34") :p_type (Text. "SMALL PLATED BRASS") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#34") :p_type (Text. "STANDARD ANODIZED NICKEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#34") :p_type (Text. "STANDARD BRUSHED TIN") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#34") :p_type (Text. "STANDARD BURNISHED TIN") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#34") :p_type (Text. "STANDARD PLATED NICKEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#35") :p_type (Text. "PROMO BURNISHED BRASS") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#35") :p_type (Text. "PROMO BURNISHED STEEL") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#35") :p_type (Text. "PROMO PLATED BRASS") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#35") :p_type (Text. "STANDARD ANODIZED NICKEL") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#35") :p_type (Text. "STANDARD ANODIZED STEEL") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#35") :p_type (Text. "STANDARD BRUSHED BRASS") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#35") :p_type (Text. "STANDARD BRUSHED NICKEL") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#35") :p_type (Text. "STANDARD PLATED STEEL") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#41") :p_type (Text. "MEDIUM ANODIZED NICKEL") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#41") :p_type (Text. "MEDIUM BRUSHED TIN") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#41") :p_type (Text. "MEDIUM PLATED STEEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#41") :p_type (Text. "PROMO ANODIZED NICKEL") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#41") :p_type (Text. "SMALL ANODIZED STEEL") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#41") :p_type (Text. "SMALL POLISHED COPPER") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#41") :p_type (Text. "STANDARD ANODIZED NICKEL") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#41") :p_type (Text. "STANDARD ANODIZED TIN") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#41") :p_type (Text. "STANDARD ANODIZED TIN") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#41") :p_type (Text. "STANDARD BRUSHED TIN") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#41") :p_type (Text. "STANDARD PLATED TIN") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#42") :p_type (Text. "ECONOMY BRUSHED COPPER") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#42") :p_type (Text. "LARGE ANODIZED NICKEL") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#42") :p_type (Text. "MEDIUM PLATED TIN") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#42") :p_type (Text. "PROMO BRUSHED STEEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#42") :p_type (Text. "PROMO BURNISHED TIN") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#42") :p_type (Text. "PROMO PLATED STEEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#42") :p_type (Text. "PROMO PLATED STEEL") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#42") :p_type (Text. "STANDARD BURNISHED NICKEL") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#42") :p_type (Text. "STANDARD PLATED COPPER") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#43") :p_type (Text. "ECONOMY ANODIZED COPPER") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#43") :p_type (Text. "ECONOMY ANODIZED NICKEL") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#43") :p_type (Text. "ECONOMY PLATED TIN") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#43") :p_type (Text. "ECONOMY POLISHED TIN") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#43") :p_type (Text. "LARGE BURNISHED COPPER") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#43") :p_type (Text. "LARGE POLISHED TIN") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#43") :p_type (Text. "MEDIUM ANODIZED BRASS") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#43") :p_type (Text. "MEDIUM ANODIZED COPPER") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#43") :p_type (Text. "MEDIUM ANODIZED COPPER") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#43") :p_type (Text. "MEDIUM BURNISHED TIN") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#43") :p_type (Text. "PROMO BRUSHED BRASS") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#43") :p_type (Text. "PROMO BURNISHED STEEL") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#43") :p_type (Text. "PROMO POLISHED BRASS") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#43") :p_type (Text. "SMALL BRUSHED NICKEL") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#43") :p_type (Text. "SMALL POLISHED STEEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#43") :p_type (Text. "STANDARD ANODIZED BRASS") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#43") :p_type (Text. "STANDARD PLATED TIN") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#44") :p_type (Text. "ECONOMY ANODIZED NICKEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#44") :p_type (Text. "ECONOMY POLISHED NICKEL") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#44") :p_type (Text. "LARGE ANODIZED BRASS") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#44") :p_type (Text. "LARGE BRUSHED TIN") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#44") :p_type (Text. "MEDIUM BRUSHED STEEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#44") :p_type (Text. "MEDIUM BURNISHED COPPER") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#44") :p_type (Text. "MEDIUM BURNISHED NICKEL") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#44") :p_type (Text. "MEDIUM PLATED COPPER") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#44") :p_type (Text. "SMALL ANODIZED COPPER") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#44") :p_type (Text. "SMALL ANODIZED TIN") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#44") :p_type (Text. "SMALL PLATED COPPER") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#44") :p_type (Text. "STANDARD ANODIZED COPPER") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#44") :p_type (Text. "STANDARD ANODIZED NICKEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#51") :p_type (Text. "ECONOMY ANODIZED STEEL") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#51") :p_type (Text. "ECONOMY PLATED NICKEL") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#51") :p_type (Text. "ECONOMY POLISHED COPPER") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#51") :p_type (Text. "ECONOMY POLISHED STEEL") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#51") :p_type (Text. "LARGE BURNISHED BRASS") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#51") :p_type (Text. "LARGE POLISHED STEEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#51") :p_type (Text. "MEDIUM ANODIZED TIN") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#51") :p_type (Text. "PROMO BRUSHED BRASS") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#51") :p_type (Text. "PROMO POLISHED STEEL") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#51") :p_type (Text. "SMALL BRUSHED TIN") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#51") :p_type (Text. "SMALL POLISHED STEEL") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#51") :p_type (Text. "STANDARD BRUSHED COPPER") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#51") :p_type (Text. "STANDARD BRUSHED NICKEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#51") :p_type (Text. "STANDARD BURNISHED COPPER") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#52") :p_type (Text. "ECONOMY ANODIZED BRASS") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#52") :p_type (Text. "ECONOMY ANODIZED COPPER") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#52") :p_type (Text. "ECONOMY BURNISHED NICKEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#52") :p_type (Text. "ECONOMY BURNISHED STEEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#52") :p_type (Text. "ECONOMY PLATED TIN") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#52") :p_type (Text. "LARGE BRUSHED NICKEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#52") :p_type (Text. "LARGE BURNISHED TIN") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#52") :p_type (Text. "LARGE PLATED STEEL") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#52") :p_type (Text. "LARGE PLATED TIN") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#52") :p_type (Text. "LARGE POLISHED NICKEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#52") :p_type (Text. "MEDIUM BURNISHED TIN") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#52") :p_type (Text. "SMALL ANODIZED NICKEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#52") :p_type (Text. "SMALL ANODIZED STEEL") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#52") :p_type (Text. "SMALL BRUSHED STEEL") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#52") :p_type (Text. "SMALL BURNISHED NICKEL") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#52") :p_type (Text. "STANDARD POLISHED STEEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#53") :p_type (Text. "LARGE BURNISHED NICKEL") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#53") :p_type (Text. "LARGE PLATED BRASS") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#53") :p_type (Text. "LARGE PLATED STEEL") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#53") :p_type (Text. "MEDIUM BRUSHED COPPER") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#53") :p_type (Text. "MEDIUM BRUSHED STEEL") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#53") :p_type (Text. "SMALL BRUSHED BRASS") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#53") :p_type (Text. "STANDARD PLATED STEEL") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#54") :p_type (Text. "ECONOMY ANODIZED BRASS") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#54") :p_type (Text. "ECONOMY BRUSHED TIN") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#54") :p_type (Text. "ECONOMY POLISHED BRASS") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#54") :p_type (Text. "LARGE ANODIZED BRASS") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#54") :p_type (Text. "LARGE BURNISHED BRASS") :p_size 49 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#54") :p_type (Text. "LARGE BURNISHED TIN") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#54") :p_type (Text. "LARGE POLISHED BRASS") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#54") :p_type (Text. "MEDIUM BURNISHED STEEL") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#54") :p_type (Text. "SMALL BURNISHED STEEL") :p_size 19 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#54") :p_type (Text. "SMALL PLATED BRASS") :p_size 23 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#54") :p_type (Text. "SMALL PLATED TIN") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#55") :p_type (Text. "LARGE BRUSHED NICKEL") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#55") :p_type (Text. "LARGE PLATED TIN") :p_size 9 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#55") :p_type (Text. "LARGE POLISHED STEEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#55") :p_type (Text. "MEDIUM BRUSHED TIN") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#55") :p_type (Text. "PROMO BRUSHED STEEL") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#55") :p_type (Text. "PROMO BURNISHED STEEL") :p_size 14 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#55") :p_type (Text. "SMALL PLATED COPPER") :p_size 45 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#55") :p_type (Text. "STANDARD ANODIZED BRASS") :p_size 36 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#55") :p_type (Text. "STANDARD BRUSHED COPPER") :p_size 3 :supplier_cnt 4}
                           {:p_brand (Text. "Brand#55") :p_type (Text. "STANDARD BRUSHED STEEL") :p_size 19 :supplier_cnt 4}])

(t/deftest ^:integration test-q16-part-supplier-relationship
  (t/is (= q16-result (tpch-queries/tpch-q16-part-supplier-relationship))))

(t/deftest ^:integration test-q17-small-quantity-order-revenue
  (t/is (= []
           (tpch-queries/tpch-q17-small-quantity-order-revenue))))

(t/deftest ^:integration test-q18-large-volume-customer
  (t/is (= [{:c_name (Text. "Customer#000000667"),
             :c_custkey (Text. "custkey_667"),
             :o_orderkey (Text. "orderkey_29158"),
             :o_orderdate (util/date->local-date-time #inst "1995-10-21"),
             :o_totalprice 439687.23,
             :sum_qty 305.0}
            {:c_name (Text. "Customer#000000178"),
             :c_custkey (Text. "custkey_178"),
             :o_orderkey (Text. "orderkey_6882"),
             :o_orderdate (util/date->local-date-time #inst "1997-04-09")
             :o_totalprice 422359.65,
             :sum_qty 303.0}]
           (tpch-queries/tpch-q18-large-volume-customer))))

(t/deftest ^:integration test-q19-discounted-revenue
  (t/is (= [{:revenue 22923.028}]
           (tpch-queries/tpch-q19-discounted-revenue))))
