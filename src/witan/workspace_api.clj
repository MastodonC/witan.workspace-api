(ns witan.workspace-api
  (:require [schema.core :as s]))

(defn select-schema-keys
  "Like select-keys but deduces keys from a schema and performs validation"
  [schema m]
  (when-not (map? schema) (throw (Exception. "Schema must be a map")))
  (let [has-any? (fn [x] (some #(= % s/Keyword) x))
        in-keys  (if (-> schema keys has-any?) [] (-> schema keys))
        result (if (seq in-keys) (select-keys m (vec in-keys)) m)]
    (s/validate schema result)))

(def WorkflowFnMetaData
  "Schema for the Witan workflow function metadata"
  {:witan/name          s/Keyword
   :witan/version       s/Str
   :witan/input-schema  {s/Any s/Any}
   :witan/output-schema {s/Any s/Any}
   :witan/doc           s/Str
   (s/optional-key :witan/param-schema) {s/Any s/Any}
   (s/optional-key :witan/exported?) s/Bool})

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
       (let [params'# (when (and (first params#) ~param-schema)
                        (select-schema-keys ~param-schema (first params#)))
             inputs'# (select-schema-keys ~input-schema inputs#)
             result#  ((fn ~args ~@body) inputs'# params'#)
             result'# (select-schema-keys ~output-schema result#)]
         (merge inputs# result'#)))))

(defmacro merge->
  "Macro sending x to multiple forms and then merging the results
  TODO: Make this way more resiliant to inline functions and other macros (such as threading)"
  [data & forms]
  (loop [forms forms, result `(apply merge)]
    (if forms
      (let [form (first forms)
            result (concat result (list `(-> ~data ~form)))]
        (recur (next forms) result))
      result)))

(defmacro do-while->
  "Macro which threads data to forms whilst predicate. Guaranteed
   to execute once."
  [predicate data & forms]
  `(loop [x# ~data]
     (let [result# (-> x# ~@forms)]
       (if (-> result# ~predicate)
         (recur result#)
         result#))))
