(ns witan.workspace-api.schema
  (:require [clojure.set]
            [schema.core :as s]))

(defn semver?
  [x]
  (re-find #"(?<=^[Vv]|^)\d+\.\d+\.\d+$" x))

(defn node-type?
  [type]
  (fn [m]
    (= (:witan/type m) type)))

(defn model-valid?
  [{:keys [workflow catalog]}]
  (try
    (let [node-names    (-> workflow flatten set)
          catalog-names (set (map :witan/name catalog))
          groups        (group-by :witan/name catalog)]
      (cond
        (not-empty (clojure.set/difference node-names catalog-names))
        (println "An error occurred: There are missing :witan/name entries in the catalog."
                 (clojure.set/difference node-names catalog-names))
        (some (comp (partial < 1) count second) groups)
        (println "An error occurred: There are duplicate :witan/name entries in the catalog.")
        :else true))))

(def Semver
  (s/pred semver?))

(def FnTypeEnum
  (s/enum :function
          :predicate
          :input
          :output))

(def ContractBase
  {:witan/name          s/Keyword
   :witan/impl          s/Keyword
   :witan/version       Semver
   :witan/type          FnTypeEnum
   :witan/doc           s/Str
   (s/optional-key :witan/exported?) s/Bool})

(def WorkflowFnMetaData
  "Schema for the Witan workflow function metadata"
  (merge ContractBase
         {:witan/input-schema  {s/Keyword s/Any}
          :witan/output-schema {s/Keyword s/Any}
          (s/optional-key :witan/param-schema) {s/Any s/Any}}))

(def WorkflowPredicateMetaData
  "Schema for the Witan workflow predicate metadata"
  (merge ContractBase
         {:witan/input-schema  {s/Keyword s/Any}
          (s/optional-key :witan/param-schema) {s/Any s/Any}}))

(def WorkflowInputMetaData
  "Schema for the Witan workflow input metadata"
  (merge ContractBase
         {:witan/output-schema  {s/Keyword s/Any}
          (s/optional-key :witan/param-schema) {s/Any s/Any}}))

(def WorkflowOutputMetaData
  "Schema for the Witan workflow output metadata"
  (merge ContractBase
         {:witan/input-schema  {s/Keyword s/Any}
          (s/optional-key :witan/param-schema) {s/Any s/Any}}))

(def WorkflowStatement
  [(s/one s/Keyword "ingress")
   (s/one (s/if keyword?
            s/Keyword
            [(s/one s/Keyword "predicate")
             (s/one s/Keyword "predicate-true")
             (s/one s/Keyword "predicate-false")]) "egress")])

(def CatalogEntry
  {:witan/name    s/Keyword
   :witan/fn      s/Keyword
   :witan/version Semver
   :witan/type    FnTypeEnum
   (s/optional-key :witan/params) {s/Keyword s/Any}})

(def ModelMetaData
  "Schema for the Witan workflow model metadata"
  {:witan/name          s/Keyword
   :witan/version       Semver
   :witan/doc           s/Str
   :witan/type          (s/eq :model)})

(def Model
  "Schema for the Witan workflow model metadata"
  (s/constrained {:workflow [WorkflowStatement]
                  :catalog  [CatalogEntry]} model-valid?))

(def WorkflowBranch
  [(s/one s/Keyword "pred")
   (s/one s/Keyword "exit")
   (s/one s/Keyword "loop")])

(def WorkflowNode
  [(s/one s/Keyword "from")
   (s/one
    (s/conditional
     keyword? s/Keyword
     :else WorkflowBranch)
    "to")])

(def Contract
  (s/conditional
   (node-type? :predicate) WorkflowPredicateMetaData
   (node-type? :function)  WorkflowFnMetaData
   (node-type? :input)     WorkflowInputMetaData
   (node-type? :output)    WorkflowOutputMetaData))

(def Workspace
  {:workflow  [WorkflowNode]
   :contracts [Contract]
   :catalog   [CatalogEntry]})
