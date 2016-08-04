(ns witan.datasets-test
  (:require [witan.datasets :as wds]
            [clojure.core.matrix.dataset :as ds]
            [clojure.test :refer :all]
            [incanter.core :as i]
            [incanter.stats :as st]
            [incanter.datasets :as data]))

(def data
  (ds/dataset [:label :label2 :value]
              (mapv conj
                    (cycle (for [f [:a :b :c]
                                 s [:x :y :z]]
                             [f s]))
                    (range 100001))))

(def results
  {:max {[] (ds/dataset [:value] [[100000]])
         [:label] (ds/dataset [:label :value]
                              [[:a 100000] [:b 99995] [:c 99998]])
         [:label :label2] (ds/dataset [:label :label2 :value]
                                      [[:a :x 99999] [:a :y 100000] [:a :z 99992]
                                       [:b :x 99993] [:b :y 99994] [:b :z 99995]
                                       [:c :x 99996] [:c :y 99997] [:c :z 99998]])}
   :min {[] (ds/dataset [:value] [[0]])
         [:label] (ds/dataset [:label :value]
                              [[:a 0] [:b 3] [:c 6]])
         [:label :label2] (ds/dataset [:label :label2 :value]
                                      [[:a :x 0] [:a :y 1] [:a :z 2]
                                       [:b :x 3] [:b :y 4] [:b :z 5]
                                       [:c :x 6] [:c :y 7] [:c :z 8]])}
   :sum {[] (ds/dataset [:value] [[5000050000]])
         [:label] (ds/dataset [:label :value]
                              [[:a 1666716667] [:b 1666616667] [:c 1666716666]])
         [:label :label2] (ds/dataset [:label :label2 :value]
                                      [[:a :x 555594444] [:a :y 555605556] [:a :z 555516667]
                                       [:b :x 555527778] [:b :y 555538889] [:b :z 555550000]
                                       [:c :x 555561111] [:c :y 555572222] [:c :z 555583333]])}
   :count {[] (ds/dataset [:value] [[100001]])
           [:label] (ds/dataset [:label :value]
                                [[:a 33335] [:b 33333] [:c 33333]])
           [:label :label2] (ds/dataset [:label :label2 :value]
                                        [[:a :x 11112] [:a :y 11112] [:a :z 11111]
                                         [:b :x 11111] [:b :y 11111] [:b :z 11111]
                                         [:c :x 11111] [:c :y 11111] [:c :z 11111]])}
   :mean {[] (ds/dataset [:value] [[50000]])
          [:label] (ds/dataset [:label :value]
                               [[:a (/ 1666716667 33335)] [:b 49999] [:c 50002]])
          [:label :label2] (ds/dataset [:label :label2 :value]
                                       [[:a :x (/ 555594444 11112)] [:a :y (/ 555605556 11112)] [:a :z (/ 555516667 11111)]
                                        [:b :x (/ 555527778 11111)] [:b :y (/ 555538889 11111)] [:b :z (/ 555550000 11111)]
                                        [:c :x (/ 555561111 11111)] [:c :y (/ 555572222 11111)] [:c :z (/ 555583333 11111)]])}})

(deftest rollup
  (doseq [aggregate [:max :min :sum :count :mean]
          groupby [[] [:label] [:label :label2]]]
    (is
     (=
      (get-in results [aggregate groupby])
      (wds/rollup data aggregate :value groupby))
     (str "Checking " aggregate " grouped by " groupby))))

(def data-size 10000)

(def test-data
  (map zipmap
       (repeat [:a :b :c])
       (map #(vector % % %) (range data-size))))

(defn d
  [ks values]
  (ds/dataset
   (map zipmap
        (repeat ks)
        values)))

(deftest add-derived-column-test
  (testing "New column added"
    (is
     (= (ds/dataset (map #(assoc % :d (* (:a %) (:b %))) test-data))
        (wds/add-derived-column (ds/dataset test-data) :d [:a :b] *))))
  (testing "Column replaced when derived-col-name alread exists"
    (is
     (= (ds/dataset (map #(update % :a (fn [_] (* (:a %) (:b %)))) test-data))
        (wds/add-derived-column (ds/dataset test-data) :a [:a :b] *)))))

(deftest build-index-test
  (is
   (= (zipmap (map (comp first vals) test-data) (map (comp rest vals) test-data))
      (wds/build-index first rest (ds/dataset test-data)))))

(deftest join-test
  (let [ds1 (d [:a :b] [[1 2] [2 4] [3 2]])
        ds2 (d [:a :c] [[1 5] [2 1] [3 3]])
        ds3 (d [:a :e] [[1 8] [2 0] [4 1]])
        ds4 (d [:a :x :y] [[1 8 9]])]
    (testing "right joins"
      (is
       (= (d [:a :c :b] [[1 5 2] [2 1 4] [3 3 2]])
          (wds/right-join ds1 ds2 [:a])))
      (is
       (= (d [:a :e :b] [[1 8 2] [2 0 4] [4 1 nil]])
          (wds/right-join ds1 ds3 [:a])))
      (is
       (= (d [:a :b :x :y] [[1 2 8 9] [2 4 nil nil] [3 2 nil nil]])
          (wds/right-join ds4 ds1 [:a]))))
    (testing "empty cell provision"
      (is
       (= (d [:a :b :x :y] [[1 2 8 9] [2 4 :foo :foo] [3 2 :foo :foo]])
          (wds/right-join ds4 ds1 [:a] :empty-cell :foo))))
    (testing "unnatural join"
      (is
       (= (ds/dataset [:a :b :a] [[1 2 2] [2 4 nil] [3 2 3]])
          (wds/right-join ds2 ds1 [[:a] [:c]]))))
    (testing "left joins"
      (is
       (= (d [:a :b :c] [[1 2 5] [2 4 1] [3 2 3]])
          (wds/left-join ds1 ds2 [:a])))
      (is
       (= (d [:a :b :e] [[1 2 8] [2 4 0] [3 2 nil]])
          (wds/left-join ds1 ds3 [:a])))
      (is
       (= (d [:a :x :y :b] [[1 8 9 2]])
          (wds/left-join ds4 ds1 [:a]))))
    (testing "joins"
      (is
       (= (d [:a :c :b] [[1 5 2] [2 1 4] [3 3 2]])
          (wds/join ds1 ds2 [:a])))
      (is
       (= (d [:a :e :b] [[1 8 2] [2 0 4] [4 1 nil]])
          (wds/join ds1 ds3 [:a])))
      (is
       (= (d [:a :b :x :y] [[1 2 8 9] [2 4 nil nil] [3 2 nil nil]])
          (wds/join ds4 ds1 [:a])))))
  (testing "Large join"
    (let [right (ds/dataset (map #(-> %
                                      (assoc :d (:a %))
                                      (dissoc :a))
                                 test-data))
          joined (wds/left-join (ds/dataset test-data) right [:b :c])]
      (is
       (= (wds/row-count joined)
          (wds/row-count right)
          (count test-data)))
      (is
       (every? #(= (:d %) (:a %)) (ds/row-maps joined)))
      (is
       (= '(:a :b :c :d)
          (ds/column-names joined))))))

(deftest filter-dataset-test
  (let [filter-dataset-fn (fn [a]
                            (<= (/ data-size 2)
                                a))
        filter-maps-fn (fn [m]
                         (<= (/ data-size 2)
                             (:a m)))
        filtered (wds/filter-dataset (ds/dataset test-data) [:a] filter-dataset-fn)]
    (is
     (= (/ data-size 2)
        (wds/row-count filtered)))
    (is
     (= (ds/dataset
         (filter filter-maps-fn test-data))
        filtered))))

(def test-ds (ds/dataset [{:a 1 :b 2 :c 3}
                          {:a 4 :b 5 :c 6}
                          {:a 7 :b 8 :c 9}]))

(def iris-ds (data/get-dataset :iris))

(def iris (i/to-matrix iris-ds))

(deftest select-from-ds-test
  (testing "The function works as the Incanter equivalent."
    (is (= (ds/dataset [{:a 7 :b 8 :c 9}])
           (wds/select-from-ds test-ds
                               {:a {:gte 4} :b {:gt 5}})))
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;; Reproduce Incanter examples in query-dataset fn docstring:
    (let [cars (data/get-dataset :cars)]
      (is (= (i/query-dataset cars {:speed 10.0})
             (wds/select-from-ds cars {:speed 10.0})))
      (is (= (i/query-dataset cars {:speed {:$in #{17.0 14.0 19.0}}})
             (wds/select-from-ds cars {:speed {:$in #{17.0 14.0 19.0}}})))
      (is (= (i/query-dataset cars {:speed {:$lt 20.0 :$gt 10.0}})
             (wds/select-from-ds cars {:speed {:$lt 20.0 :$gt 10.0}})))
      (is (= (i/query-dataset cars {:speed {:$fn #(> (i/log %) 3.0)}})
             (wds/select-from-ds cars {:speed {:$fn #(> (i/log %) 3.0)}})))
      (is (= (i/query-dataset cars (fn [row] (> (/ (:speed row) (:dist row)) 1/2)))
             (wds/select-from-ds cars (fn [row] (> (/ (:speed row) (:dist row)) 1/2))))))))

(deftest subset-ds-test
  (testing "The function works as the Incanter equivalent."
    (is (= 4
           (wds/subset-ds test-ds :rows 1 :cols :a)))
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;; Reproduce Incanter examples in the sel multimethod docstring:
    (let [us-arrests (data/get-dataset :us-arrests)]
      (is (= (i/sel iris 0 0) (wds/subset-ds iris 0 0)))
      (is (= (i/sel iris :rows 0 :cols 0) (wds/subset-ds iris :rows 0 :cols 0)))
      (is (= (i/sel iris :cols 0) (wds/subset-ds iris :cols 0)))
      (is (= (i/sel iris :cols [0 2]) (wds/subset-ds iris :cols [0 2])))
      (is (= (i/sel iris :rows (range 10) :cols (range 2))
             (wds/subset-ds iris :rows (range 10) :cols (range 2))))
      (is (= (i/sel iris :rows (range 10)) (wds/subset-ds iris :rows (range 10))))
      (is (= (i/sel iris :except-rows (range 10)) (wds/subset-ds iris :except-rows (range 10))))
      (is (= (i/sel iris :except-cols 1) (wds/subset-ds iris :except-cols 1)))
      (is (= (i/sel us-arrests :cols :State) (wds/subset-ds us-arrests :cols :State)))
      (is (= (i/sel us-arrests :cols [:State :Murder])
             (wds/subset-ds us-arrests :cols [:State :Murder]))))))

(deftest group-ds-test
  (testing "The function works as the Incanter equivalent."
    (is (= {{:a 1 :b 2}
            (ds/dataset [{:a 1 :b 2 :c 3}])
            {:a 4 :b 5}
            (ds/dataset [{:a 4 :b 5 :c 6}])
            {:a 7 :b 8}
            (ds/dataset [{:a 7 :b 8 :c 9}])}
           (wds/group-ds test-ds [:a :b])))
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;; Reproduce Incanter examples in the $group-by function docstring:
    (let [h-e-color (data/get-dataset :hair-eye-color)]
      (is (= (i/$group-by :Species iris-ds)
             (wds/group-ds iris-ds :Species)))
      (is (= (i/$group-by [:hair :eye] h-e-color)
             (wds/group-ds h-e-color [:hair :eye]))))))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

(deftest linear-model-test
  (testing "The function works as the Incanter equivalent."
    (let [test-y [1 2 3 4 5 6 7 8 9]
          test-x [2 4 6 8 10 12 14 16 18]]
      (is (fp-equals? 0.9999999999999999
                      (:r-square (wds/linear-model test-y test-x)) 0.00000000001))
      (is (every? true?
                  (map #(fp-equals? %1 %2 0.00000000001)
                       '(-3.552713678800501E-15 0.4999999999999998)
                       (:coefs (wds/linear-model test-y test-x :intercept 1))))))
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;; Reproduce Incanter examples in linear-model fn docstring:
    (let [y (i/sel iris :cols 0)
          x (i/sel iris :cols (range 1 5))
          iris-lm-i (st/linear-model y x)
          iris-lm (wds/linear-model y x)]
      (is (= (keys iris-lm-i) (keys iris-lm)))
      (is (= (:coefs iris-lm-i) (:coefs iris-lm)))
      (is (= (:sse iris-lm-i) (:sse iris-lm)))
      (is (= (st/quantile (:residuals iris-lm-i)) (st/quantile (:residuals iris-lm))))
      (is (= (:r-square iris-lm-i) (:r-square iris-lm)))
      (is (= (:adj-r-square iris-lm-i) (:adj-r-square iris-lm)))
      (is (= (:f-stat iris-lm-i) (:f-stat iris-lm)))
      (is (= (:f-prob iris-lm-i) (:f-prob iris-lm)))
      (is (= (:df iris-lm-i) (:df iris-lm))))))
