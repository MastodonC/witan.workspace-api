(ns witan.workspace-api-test
  (:require [clojure.test :refer :all]
            [schema.core :as s]
            [witan.workspace-api :refer :all]
            [witan.workspace-api.functions :refer :all]))

(defworkflowfn inc*
  "inc* has a doc-string"
  {:witan/name          :witan.test-fns.inc
   :witan/version       "1.0"
   :witan/exported?     true
   :witan/input-schema  {:input s/Num}
   :witan/output-schema {:numberA s/Num}}
  [{:keys [input]} _]
  {:numberA (+ input 1)})

(defworkflowfn inc-loop
  {:witan/name          :witan.test-fns.inc2
   :witan/version       "1.0"
   :witan/exported?     true
   :witan/input-schema  {:number s/Num}
   :witan/output-schema {:number s/Num}}
  [{:keys [number]} _]
  {:number (+ number 1)})

(defworkflowfn mul2
  {:witan/name          :witan.test-fns.mul2
   :witan/version       "1.0"
   :witan/exported?     true
   :witan/input-schema  {:input s/Num}
   :witan/output-schema {:numberB s/Num}}
  [{:keys [input]} _]
  {:numberB (* 2 input)})

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

(defn not-enough?
  [{:keys [number]}]
  (< number 5))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest happy-thread-test
  (testing "Can the workflow functions be thread-first'ed?"
    (is (=
         (-> {:number 1 :foo "bar"}
             (rename-keys {:number :input})
             (inc*)
             (rename-keys {:numberA :input})
             (mul2)
             (rename-keys {:numberB :numberC})
             (mulX {:multiple 3}))
         {:input 2 :number 12, :foo "bar", :numberA 2, :numberB 4, :numberC 4}))))

(deftest merge-macro-test
  (testing "Does the merge-> macro operate as expected?"
    (is (= (merge-> {:input 2 :numberC 4}
                    inc*
                    (mulX {:multiple 3}))
           {:input 2 :numberA 3 :numberC 4 :number 12})))
  (testing "Does the merge-> macro allow inline fns and embedded macros?"
    (is (= (merge-> {:input 2 :numberC 4}
                    (-> inc*)
                    ((fn [x] (mulX x {:multiple 3}))))
           {:input 2 :numberA 3 :numberC 4 :number 12}))))

(deftest do-while-macro-test
  (testing "Does the do-while-> loop macro operate as expected?"
    (is (= (do-while-> (not-enough?)
             {:number 1}
             (inc-loop))
           {:number 5}))))

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
              :witan/output-schema {:numberA s/Num}
              :witan/doc "inc* has a doc-string"})))))

(deftest select-schema-keys-test
  (testing "Does the select-schema-keys macro work as intended?"
    (is (= (select-schema-keys {:foo s/Num} {:foo 123 :bar "xyz"})
           {:foo 123})))
  (testing "Are non-maps rejected?"
    (is (thrown-with-msg?
         Exception
         #"Schema must be a map"
         (select-schema-keys s/Num 123))))
  (testing "Do bad inputs cause a validation fail?"
    (is (thrown-with-msg?
         Exception
         #"Value does not match schema: \{:foo \(not \(instance\? java.lang.String 123\)\)\}"
         (select-schema-keys {:foo s/Str} {:foo 123})))))

(deftest doc-string-test
  (testing "Is the doc-string of a function persisted in the :witan/doc meta key?"
    (= "inc* has a doc-string"
       (-> (meta #'inc*) :witan/workflowfn :witan/doc))))
