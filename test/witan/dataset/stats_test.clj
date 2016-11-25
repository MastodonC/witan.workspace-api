(ns witan.dataset.stats-test
  (:require [clojure.test :refer :all]
            [incanter.stats :as st]
            [witan.datasets.stats :refer :all]))

(deftest standard-deviation-test
  (testing "This wrapper function calculate the standard deviation
            as Incanter sd function"
    (is (= (st/sd (range 100))
           (standard-deviation (range 100))))
    (is (.isNaN (standard-deviation [9])))))
