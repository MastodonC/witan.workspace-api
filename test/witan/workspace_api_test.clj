(ns witan.workspace-api-test
  (:require [clojure.test :refer :all]
            [schema.core :as s]
            [witan.workspace-api :refer :all]))

(defworkflowfn inc*
  {:witan/name          :witan.test-fns.inc
   :witan/version       "1.0"
   :witan/exported?     true
   :witan/input-schema  {:input s/Num}
   :witan/output-schema {:numberA s/Num}}
  [{:keys [input]} _]
  {:numberA (+ input 1)})

(defworkflowfn mul2
  {:witan/name          :witan.test-fns.mul2
   :witan/version       "1.0"
   :witan/exported?     true
   :witan/input-schema  {:numberB s/Num}
   :witan/output-schema {:numberB s/Num}}
  [{:keys [numberB]} _]
  {:numberB (* 2 numberB)})

(defworkflowfn mulX
  {:witan/name          :witan.test-fns.mulX
   :witan/version       "1.0"
   :witan/exported?     true
   :witan/input-schema  {:numberC s/Num}
   :witan/output-schema {:number s/Num}
   :witan/param-schema  {:multiple s/Num}}
  [{:keys [numberC]} {:keys [multiple]}]
  {:number (* multiple numberC)})

(defworkflowfn broken
  {:witan/name          :witan.test-fns.broken
   :witan/version       "1.0"
   :witan/exported?     true
   :witan/input-schema  {:foo s/Num}
   :witan/output-schema {:number s/Num}
   :witan/param-schema  {:baz s/Num}}
  [{:keys [foo]} {:keys [baz]}]
  {:bar (+ foo baz)})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest happy-thread-test
  (testing "Can the workflow functions be thread-first'ed?"
    (is (=
         (-> {:number 1 :foo "bar"}
             (rename-keys {:number :input})
             (inc*)
             (rename-keys {:numberA :numberB})
             (mul2)
             (rename-keys {:numberB :numberC})
             (mulX {:multiple 3}))
         {:input 1 :number 12, :foo "bar", :numberA 2, :numberB 4, :numberC 4}))))

(deftest merge-macro-test
  (testing "Does the merge-> macro operate as expected?"
    (is (= (merge-> {:input 2 :numberC 4}
                    inc*
                    (mulX {:multiple 3}))
           {:input 2 :numberA 3 :numberC 4 :number 12}))))

(deftest find-workflowfn-test
  (testing "Can we find all the exported workflow functions in this namespace?"
    (is (= (ns-workflowfns 'witan.workspace-api-test)
           [#'witan.workspace-api-test/inc*
            #'witan.workspace-api-test/mul2
            #'witan.workspace-api-test/broken
            #'witan.workspace-api-test/mulX]))))

(deftest schema-errors-test
  (testing "Does the macro catch errors in input schema?"
    (is (thrown-with-msg?
         Exception
         #"Value does not match schema: \{:input missing-required-key\}"
         (inc* {:number 12}))))
  (testing "Does the macro catch errors in output schema?"
    (is (thrown-with-msg?
         Exception
         #"Value does not match schema: \{:number missing-required-key\}"
         (broken {:foo 12} {:baz 6}))))
  (testing "Does the macro catch errors in params schema?"
    (is (thrown-with-msg?
         Exception
         #"Value does not match schema: \{:baz \(not \(instance\? java.lang.Number \"6\"\)\)\}"
         (broken {:foo 12} {:baz "6"})))))

(deftest metadata-is-applied
  (testing "Does the correct metadata get applied?"
    (let [m (meta #'inc*)]
      (is m)
      (is (contains? m :witan/workflowfn))
      (is (= (:witan/workflowfn m)
             {:witan/name          :witan.test-fns.inc
              :witan/version       "1.0"
              :witan/exported?     true
              :witan/input-schema  {:input s/Num}
              :witan/output-schema {:numberA s/Num}})))))
