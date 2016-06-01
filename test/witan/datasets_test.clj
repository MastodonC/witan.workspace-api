(ns witan.datasets-test
  (:require [witan.datasets :as wds]
            [clojure.core.matrix.dataset :as ds]
            [clojure.test :refer :all]))

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

(def test-data
  (map zipmap
       (repeat [:a :b :c]) 
       (map #(vector % % %) (range 100000))))

(deftest add-derived-column-test
  (is
   (= (ds/dataset (map #(assoc % :d (* (:a %) (:b %))) test-data))
    (wds/add-derived-column (ds/dataset test-data) :d [:a :b] *))))

(deftest build-index-test
  (is
   (= (zipmap (map (comp first vals) test-data) (map (comp rest vals) test-data))
      (wds/build-index first rest (ds/dataset test-data)))))

(deftest join-test
  (let [right (ds/dataset (map #(-> %
                                    (assoc :d (:a %))
                                    (dissoc :a))
                               test-data))
        joined (wds/join [:b :c] (ds/dataset test-data) right)]
    (is
     (= (wds/row-count joined)
        (wds/row-count right)
        (count test-data)))
    (is
     (every? #(= (:d %) (:a %)) (ds/row-maps joined)))
    (is
     (= '(:a :b :c :d)
        (ds/column-names joined)))))
