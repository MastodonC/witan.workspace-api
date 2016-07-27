(ns witan.workspace-api-test
  (:require [clojure.test :refer :all]
            [schema.core :as s]
            [witan.workspace-api :refer :all]
            [witan.workspace-api.functions :refer :all]))

;; functions
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

;; model
(defmodel default-model
  "doc"
  {:witan/name :default
   :witan/version "1.0"}
  {:workflow [[:in :a]
              [:b  :c]
              [:c  :out]]
   :catalog [{:witan/name :in
              :witan/fn :test.fn/inc
              :witan/type :input
              :witan/version "1.0"
              :witan/params {:foo "bar"}}
             {:witan/name :a
              :witan/type :function
              :witan/fn :test.fn/inc
              :witan/version "1.0"}
             {:witan/name :b
              :witan/type :function
              :witan/fn :test.fn/inc
              :witan/version "1.0"}
             {:witan/name :c
              :witan/type :function
              :witan/fn :test.fn/inc
              :witan/version "1.0"}
             {:witan/name :out
              :witan/type :output
              :witan/fn :test.fn/inc
              :witan/version "1.0"}]})

;; predicate
(defworkflowpred less-than
  "pred doc"
  {:witan/name :witan.test-preds.less-than
   :witan/version "1.0"
   :witan/input-schema {:number s/Num}
   :witan/param-schema {:value s/Num}}
  [{:keys [number]} {:keys [value]}]
  (< number value))

(defworkflowpred greater-than-10
  "pred doc"
  {:witan/name :witan.test-preds.greater-than-10
   :witan/version "1.0"
   :witan/input-schema {:number s/Num}}
  [{:keys [number]} _]
  (> number 10))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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
    (is (= (do-while-> (less-than {:value 5})
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
      (is (contains? m :witan/metadata))
      (is (= (:witan/metadata m)
             {:witan/name          :witan.test-fns.inc
              :witan/version       "1.0"
              :witan/exported?     true
              :witan/input-schema  {:input s/Num}
              :witan/output-schema {:numberA s/Num}
              :witan/type :function
              :witan/impl :witan.workspace-api-test/inc*
              :witan/doc "inc* has a doc-string"})))))

(deftest select-schema-keys-test
  (testing "Does the select-schema-keys macro work as intended?"
    (is (= {:foo 123}
           (select-schema-keys {:foo s/Num} {:foo 123 :bar "xyz"}))))
  (testing "Are non-maps rejected?"
    (is (thrown-with-msg?
         Exception
         #"Schema must be a map"
         (select-schema-keys s/Num 123))))
  (testing "Do bad inputs cause a validation fail?"
    (is (thrown-with-msg?
         Exception
         #"Value does not match schema: \{:foo \(not \(instance\? java.lang.String 123\)\)\}"
         (select-schema-keys {:foo s/Str} {:foo 123}))))
  (testing "Does wildcard work?"
    (let [data {:foo 123 :bar "134"}]
      (is (= data
             (select-schema-keys {:* s/Num :bar s/Str} data))))))

(deftest doc-string-test
  (testing "Is the doc-string of a function persisted in the :witan/doc meta key?"
    (is (= "inc* has a doc-string"
           (-> (meta #'inc*) :witan/metadata :witan/doc)))))

(deftest model
  (testing "models work"
    (is (= {:witan/name :default
            :witan/version "1.0"
            :witan/type :model
            :witan/doc "doc"}
           (:witan/metadata
            (meta #'default-model))))
    (is (= {:workflow [[:in :a]
                       [:b  :c]
                       [:c  :out]]
            :catalog [{:witan/name :in
                       :witan/fn :test.fn/inc
                       :witan/type :input
                       :witan/version "1.0"
                       :witan/params {:foo "bar"}}
                      {:witan/name :a
                       :witan/type :function
                       :witan/fn :test.fn/inc
                       :witan/version "1.0"}
                      {:witan/name :b
                       :witan/type :function
                       :witan/fn :test.fn/inc
                       :witan/version "1.0"}
                      {:witan/name :c
                       :witan/type :function
                       :witan/fn :test.fn/inc
                       :witan/version "1.0"}
                      {:witan/name :out
                       :witan/type :output
                       :witan/fn :test.fn/inc
                       :witan/version "1.0"}]}
           default-model))))

(deftest workflowpred-meta
  (testing "Is predicate metadata applied?"
    (is (= {:witan/name :witan.test-preds.less-than
            :witan/version "1.0"
            :witan/input-schema {:number s/Num}
            :witan/param-schema {:value s/Num}
            :witan/doc "pred doc"
            :witan/impl :witan.workspace-api-test/less-than
            :witan/type :predicate}
           (:witan/metadata
            (meta #'less-than)))))
  (testing "Do predicates behave as expected?"
    (is (= (less-than {:number 2} {:value 3}) true))
    (is (= (less-than {:number 2} {:value 1}) false))
    (is (thrown-with-msg?
         Exception
         #"Value does not match schema\: \{\:value missing-required-key\}"
         (less-than {:number 2})))
    (is (= (greater-than-10 {:number 2}) false))
    (is (= (greater-than-10 {:number 12}) true))
    (is (= (greater-than-10 {:number 12} {:does-nothing "foo-bar"}) true))))

(defworkflowinput an-input
  {:witan/name :an-input
   :witan/version "1.0"
   :witan/doc "doc"
   :witan/param-schema {:param s/Num}
   :witan/output-schema {:number s/Num}}
  [_ {:keys [param]}]
  {:number param})

(deftest workflowinput
  (testing "input meta works"
    (is (= {:witan/name :an-input
            :witan/version "1.0"
            :witan/doc "doc"
            :witan/impl :witan.workspace-api-test/an-input
            :witan/param-schema {:param s/Num}
            :witan/output-schema {:number s/Num}
            :witan/type :input}
           (:witan/metadata
            (meta #'an-input)))))
  (testing "inputs can propagate data"
    (let [result (an-input {:foo "bar"} {:param 1})]
      (is (= {:number 1} result)))))

(defworkflowoutput an-output
  {:witan/name :an-output
   :witan/version "1.0"
   :witan/doc "doc"
   :witan/input-schema {:foo s/Str}}
  [_ _])

(deftest workflowoutput
  (testing "output meta works"
    (is (= {:witan/name :an-output
            :witan/version "1.0"
            :witan/doc "doc"
            :witan/impl :witan.workspace-api-test/an-output
            :witan/input-schema {:foo s/Str}
            :witan/type :output}
           (:witan/metadata
            (meta #'an-output)))))
  (testing "outputs can propagate data"
    (let [result (an-output {:foo "bar" :bar "baz" :baz "quaz"} nil)]
      (is (= nil result)))))

(deftest logging-test
  (testing "log can be switched on"
    (let [result (with-out-str
                   (set-api-logging! println)
                   (inc* {:input 1} {})
                   (Thread/sleep 100))] ;; logging is async so need it to flush
      (is (= (clojure.string/replace
              "1 witan.workspace-api -> calling fn::witan.test-fns.inc
               2 witan.workspace-api <- finished fn::witan.test-fns.inc\n" #"\n +" "\n")
             result))))
  (testing "log can be switched off"
    (let [result (with-out-str
                   (set-api-logging! identity)
                   (inc* {:input 1} {}))]
      (is (= ""
             result)))))
