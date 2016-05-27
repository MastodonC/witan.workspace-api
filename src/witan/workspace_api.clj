(ns witan.workspace-api
  (:require [schema.core :as s]))

(def wildcard-keyword
  :*)

(defn select-schema-keys
  "Like select-keys but deduces keys from a schema and performs validation"
  [schema m]
  (when-not (map? schema) (throw (Exception. "Schema must be a map")))
  (let [has-any? (fn [x] (some #(= % wildcard-keyword) x))
        schema' (clojure.set/rename-keys schema {wildcard-keyword s/Keyword})
        result (if-not (-> schema keys has-any?) (select-keys m (keys schema)) m)]
    (s/validate schema' result)))



(def WorkflowFnMetaData
  "Schema for the Witan workflow function metadata"
  {:witan/name          s/Keyword
   :witan/version       s/Str
   :witan/input-schema  {s/Keyword s/Any}
   :witan/output-schema {s/Keyword s/Any}
   :witan/doc           s/Str
   (s/optional-key :witan/param-schema) {s/Any s/Any}
   (s/optional-key :witan/exported?) s/Bool})

(def WorkflowPredicateMetaData
  "Schema for the Witan workflow predicate metadata"
  {:witan/name          s/Keyword
   :witan/version       s/Str
   :witan/input-schema  {s/Keyword s/Any}
   :witan/doc           s/Str
   (s/optional-key :witan/param-schema) {s/Any s/Any}
   (s/optional-key :witan/exported?) s/Bool})

(def WorkflowModelMetaData
  "Schema for the Witan workflow model metadata"
  {:witan/name          s/Keyword
   :witan/version       s/Str
   :witan/doc           s/Str})

(def WorkflowStatement
  [(s/one s/Keyword "ingress") (s/one (s/if keyword?
                                        s/Keyword
                                        [(s/one s/Keyword "predicate")
                                         (s/one s/Keyword "predicate-true")
                                         (s/one s/Keyword "predicate-false")]) "egress")])

(def WorkflowModel
  "Schema for the Witan workflow model metadata"
  [WorkflowStatement])



(defmacro defworkflowfn
  "Macro for defining a workflow function"
  [name & body] ;; metadata args &body
  (let [doc      (when (string? (first body)) (first body))
        metadata (if doc (second body) (first body))
        body     (if doc (drop 2 body) (next body))
        args     (first body)
        body     (next body)
        doc      (or doc "No docs")
        metadata (assoc metadata :witan/doc doc)
        {:keys [witan/input-schema
                witan/output-schema
                witan/param-schema]} metadata]
    `(defn ~(with-meta name
              (assoc (meta name)
                     :witan/workflowfn
                     (s/validate WorkflowFnMetaData metadata)))
       ~doc
       [inputs# & params#]
       (let [params'# (when ~param-schema
                        (select-schema-keys ~param-schema (first params#)))
             inputs'# (select-schema-keys ~input-schema inputs#)
             result#  ((fn ~args ~@body) inputs'# params'#)
             result'# (select-schema-keys ~output-schema result#)]
         (merge inputs# result'#)))))

(defmacro defworkflowpred
  "Macro for defining a workflow predicate"
  [name & body] ;; metadata args &body
  (let [doc      (when (string? (first body)) (first body))
        metadata (if doc (second body) (first body))
        body     (if doc (drop 2 body) (next body))
        args     (first body)
        body     (next body)
        doc      (or doc "No docs")
        metadata (assoc metadata :witan/doc doc)
        {:keys [witan/input-schema
                witan/param-schema]} metadata]
    `(defn ~(with-meta name
              (assoc (meta name)
                     :witan/workflowpred
                     (s/validate WorkflowPredicateMetaData metadata)))
       ~doc
       [inputs# & params#]
       (let [params'# (when ~param-schema
                        (select-schema-keys ~param-schema (first params#)))
             inputs'# (select-schema-keys ~input-schema inputs#)
             result#  ((fn ~args ~@body) inputs'# params'#)]
         (boolean result#)))))

(defmacro defworkflowmodel
  "Macro for defining a workflow model"
  [name & body] ;; metadata args &body
  (let [doc      (when (string? (first body)) (first body))
        metadata (if doc (second body) (first body))
        body     (if doc (drop 2 body) (next body))
        doc      (or doc "No docs")
        metadata (assoc metadata :witan/doc doc)]
    `(def ~(with-meta name
             (assoc (meta name)
                    :witan/workflowmodel
                    (s/validate WorkflowModelMetaData metadata)))
       ~doc
       ~@body)))



(defmacro merge->
  [data & forms]
  `(apply merge ~@(map list (repeat '->) (repeat data) forms)))

(defmacro do-while->
  "Macro which threads data to forms whilst predicate. Guaranteed
   to execute once."
  [predicate data & forms]
  `(loop [x# ~data]
     (let [result# (-> x# ~@forms)]
       (if (-> result# ~predicate)
         (recur result#)
         result#))))
