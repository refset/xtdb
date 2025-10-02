(ns xtdb.logical-plan-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.logical-plan :as lp]
            [xtdb.sql :as sql]
            xtdb.sql-test
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest test-count-star-rule-9
  (t/testing "count-star should be rewritten to count(dep-col) where dep col is a projected inner col of value 1")
  (xt/execute-tx tu/*node* [[:put-docs :t1 {:xt/id 1 :x 1}]
                            [:put-docs :t2 {:xt/id 2 :y 2}]])

  (t/is (= [{:t1-count 10, :y 2}]
           (xt/q tu/*node* "SELECT (SELECT (10 + count(*) + count(*)) FROM t1 WHERE t1.x = t2.y ) AS t1_count, t2.y FROM t2"))))

(t/deftest test-not-equal
  (xt/execute-tx tu/*node* [[:put-docs :t {:xt/id 1 :x 1}]
                            [:put-docs :t {:xt/id 2 :x 2}]])
  (t/is (= [{:cnt 1}]
           (xt/q tu/*node* "SELECT count(*) as cnt FROM t WHERE x != 1"))))

(t/deftest test-push-predicate-down-past-period-constructor
  (t/is
   (=plan-file
    "test-push-predicate-down-past-period-constructor-valid"
    (lp/push-predicate-down-past-period-constructor
     true
     (xt/template
      [:select
       (and
        (<=
         (lower ~(sql/->col-sym '_valid_time))
         (lower
          (period
           #xt/zoned-date-time "2000-01-01T00:00Z"
           #xt/zoned-date-time "2001-01-01T00:00Z")))
        (>=
         (coalesce (upper ~(sql/->col-sym '_valid_time)) xtdb/end-of-time)
         (coalesce
          (upper
           (period
            #xt/zoned-date-time "2000-01-01T00:00Z"
            #xt/zoned-date-time "2001-01-01T00:00Z"))
          xtdb/end-of-time)))
       [:project
        [{~(sql/->col-sym '_valid_time)
          (period ~(sql/->col-sym '_valid_from)
                  ~(sql/->col-sym '_valid_to))}]
        [:scan {:table public/docs}
         [~(sql/->col-sym '_valid_from) ~(sql/->col-sym '_valid_to)]]]]))))

  (t/testing "only pushes past period constructors"
    (t/is
     (= nil
        (lp/push-predicate-down-past-period-constructor
         true
         (xt/template
          [:select
           (and
            (<=
             (lower ~(sql/->col-sym '_valid_time))
             (lower
              (period
               #xt/zoned-date-time "2000-01-01T00:00Z"
               #xt/zoned-date-time "2001-01-01T00:00Z")))
            (>=
             (coalesce (upper ~(sql/->col-sym '_valid_time)) xtdb/end-of-time)
             (coalesce
              (upper
               (period
                #xt/zoned-date-time "2000-01-01T00:00Z"
                #xt/zoned-date-time "2001-01-01T00:00Z"))
              xtdb/end-of-time)))
           [:project
            [{~(sql/->col-sym '_valid_time) (+ 1 ~(sql/->col-sym '_valid_from))}]
            [:scan {:table public/docs}
             [~(sql/->col-sym '_valid_from) ~(sql/->col-sym '_valid_to)]]]])))))

  (t/testing "scalar extends expressions are handled"
    (t/is
     (=plan-file
      "test-push-predicate-down-past-period-constructor-scalar-extends"
      (lp/push-predicate-down-past-period-constructor
       true
       (xt/template
        [:select
         '(= ~(sql/->col-sym '_valid_time) 1)
         [:project
          [{~(sql/->col-sym '_foo) 4}
           {~(sql/->col-sym '_valid_time)
            (period ~(sql/->col-sym '_valid_from)
                    ~(sql/->col-sym '_valid_to))}]
          [:scan {:table public/docs} [~(sql/->col-sym '_bar)]]]])))))

  (t/testing "only push predicate if all columns referenced (aside from the new period) present in inner rel"
    (t/is
     (nil?
      (lp/push-predicate-down-past-period-constructor
       true
       (xt/template
        [:select
         '(= ~(sql/->col-sym '_valid_time) ~(sql/->col-sym '_foo))
         [:project
          [{~(sql/->col-sym '_foo) 4}
           {~(sql/->col-sym '_valid_time)
            (period ~(sql/->col-sym '_valid_from)
                    ~(sql/->col-sym '_valid_to))}]
          [:scan {:table public/docs} [~(sql/->col-sym '_bar)]]]]))))))

(t/deftest test-remove-redundant-period-constructors
  (t/is
   (= #xt/zoned-date-time "2001-01-01T00:00Z"
      (lp/remove-redudant-period-constructors
       (xt/template
        (upper
         (period
          #xt/zoned-date-time "2000-01-01T00:00Z"
          #xt/zoned-date-time "2001-01-01T00:00Z"))))))

  (t/is
   (= #xt/zoned-date-time "2000-01-01T00:00Z"
      (lp/remove-redudant-period-constructors
       (xt/template
        (lower
         (period
          #xt/zoned-date-time "2000-01-01T00:00Z"
          #xt/zoned-date-time "2001-01-01T00:00Z"))))))

  (t/is
   (= nil
      (lp/remove-redudant-period-constructors
       (xt/template
        (lower
         (+ 1 1))))))

  (t/is
   (= nil
      (lp/remove-redudant-period-constructors
       (xt/template
        (period
         #xt/zoned-date-time "2000-01-01T00:00Z"
         #xt/zoned-date-time "2001-01-01T00:00Z"))))))

(t/deftest test-optimise-contains-period-predicate
  (t/testing "only able to optimise the case where both arguments are period constructors"
    (let [f1 #xt/zoned-date-time "2000-01-01T00:00Z"
          t1 #xt/zoned-date-time "2001-01-01T00:00Z"
          f2 #xt/zoned-date-time "2002-01-01T00:00Z"
          t2 #xt/zoned-date-time "2003-01-01T00:00Z"]
      (t/is
       (=
        (xt/template
         (and (<= ~f1 ~f2)
              (>=
               (coalesce ~t1 xtdb/end-of-time)
               (coalesce ~t2 xtdb/end-of-time))))
        (lp/optimise-contains-period-predicate
         (xt/template
          (contains?
           (period ~f1 ~t1)
           (period ~f2 ~t2))))))

      (t/is
       (nil? (lp/optimise-contains-period-predicate
              (xt/template
               (contains?
                (period ~f1 ~t1)
                ~(sql/->col-sym 'col1)))))
       "not possible to tell if col1 is a period or a scalar temporal value (timestamp etc.)"))))

(t/deftest test-eliminate-transitive-join-relation
  (t/testing "eliminates a relation that only serves as a bridge between two other relations"
    (let [s-id (sql/->col-sym 's.sub_id)
          s-feed (sql/->col-sym 's.sub$feed)
          f-id (sql/->col-sym 'f._id)
          i-id (sql/->col-sym 'i._id)
          i-feed (sql/->col-sym 'i.item$feed)

          subs-rel [:scan {:table 'public/subs} [s-id s-feed]]
          feeds-rel [:scan {:table 'public/feeds} [f-id]]
          items-rel [:scan {:table 'public/items} [i-id i-feed]]

          input [:mega-join
                 [(list '= s-feed f-id)
                  (list '= f-id i-feed)]
                 [subs-rel feeds-rel items-rel]]

          expected [:mega-join
                    [(list '= s-feed i-feed)]
                    [subs-rel items-rel]]]

      (t/is (= expected (#'lp/eliminate-transitive-join-relation input))
            "should eliminate feeds table and create direct join between subs and items")))

  (t/testing "does not eliminate when relation has more than 2 join predicates"
    (let [s-id (sql/->col-sym 's.sub_id)
          s-feed (sql/->col-sym 's.sub$feed)
          f-id (sql/->col-sym 'f._id)
          f-name (sql/->col-sym 'f.name)
          i-feed (sql/->col-sym 'i.item$feed)

          subs-rel [:scan {:table 'public/subs} [s-id s-feed]]
          feeds-rel [:scan {:table 'public/feeds} [f-id f-name]]
          items-rel [:scan {:table 'public/items} [i-feed]]

          input [:mega-join
                 [(list '= s-feed f-id)
                  (list '= f-id i-feed)
                  (list '= f-name "foo")]
                 [subs-rel feeds-rel items-rel]]]

      (t/is (nil? (#'lp/eliminate-transitive-join-relation input))
            "should not eliminate when relation is used in additional predicates")))

  (t/testing "does not eliminate when only 2 relations in join"
    (let [s-feed (sql/->col-sym 's.sub$feed)
          f-id (sql/->col-sym 'f._id)

          subs-rel [:scan {:table 'public/subs} [s-feed]]
          feeds-rel [:scan {:table 'public/feeds} [f-id]]

          input [:mega-join
                 [(list '= s-feed f-id)]
                 [subs-rel feeds-rel]]]

      (t/is (nil? (#'lp/eliminate-transitive-join-relation input))
            "should require at least 3 relations to eliminate one")))

  (t/testing "handles 4-way join eliminating middle relation"
    (let [u-id (sql/->col-sym 'u._id)
          s-user (sql/->col-sym 's.sub$user)
          s-feed (sql/->col-sym 's.sub$feed)
          f-id (sql/->col-sym 'f._id)
          i-feed (sql/->col-sym 'i.item$feed)

          users-rel [:scan {:table 'public/users} [u-id]]
          subs-rel [:scan {:table 'public/subs} [s-user s-feed]]
          feeds-rel [:scan {:table 'public/feeds} [f-id]]
          items-rel [:scan {:table 'public/items} [i-feed]]

          input [:mega-join
                 [(list '= s-user u-id)
                  (list '= s-feed f-id)
                  (list '= f-id i-feed)]
                 [users-rel subs-rel feeds-rel items-rel]]

          result (#'lp/eliminate-transitive-join-relation input)
          [_ result-preds result-rels] result

          expected-preds #{(list '= s-user u-id) (list '= s-feed i-feed)}
          expected-rels [users-rel subs-rel items-rel]]

      (t/is (= :mega-join (first result))
            "should return a mega-join")
      (t/is (= expected-preds (set result-preds))
            "should have correct predicates (order independent)")
      (t/is (= expected-rels result-rels)
            "should eliminate feeds and preserve other relations"))))
