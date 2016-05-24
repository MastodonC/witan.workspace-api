(ns witan.workspace-api.functions-test
  (:require [witan.workspace-api.functions :refer :all]
            [clojure.test :refer :all]))

(deftest rename-keys-test
  (testing "happy path for rename-keys"
    (is (= {:foo "bar" :baz "bar"}
           (rename-keys {:baz "bar"} {:baz :foo})))))
