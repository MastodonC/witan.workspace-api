(ns witan.workspace-api
  (:require [schema.core :as s]))

(defn select-schema-keys
  "Like select-keys but deduces keys from a schema and performs validation"
  [schema m]
  (when-not (map? schema) (throw (Exception. "Schema must be a map")))
  (let [has-any? (fn [x] (some #(= (type %) schema.core.AnythingSchema) x))
        in-keys  (if (-> schema keys has-any?) [] (-> schema keys))
        result (if (seq in-keys) (select-keys m (vec in-keys)) m)]
    (s/validate schema result)))

(def WorkflowFnMetaData
  "Schema for the Witan workflow function metadata"
  {:witan/name          s/Keyword
   :witan/version       s/Str
   :witan/input-schema  {s/Any s/Any}
   :witan/output-schema {s/Any s/Any}
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

(defn ns-workflowfns
  "Fetches exported workflowfns from a ns"
  [ns-sym]
  (->> ns-sym
       (ns-publics)
       (filter #(let [m (-> % second meta)]
                  (and (contains? m :witan/workflowfn)
                       (-> m :witan/workflowfn :witan/exported?))))
       (mapv second)))

(defworkflowfn rename-keys
  "Helper fn for running tests that require inputs or outputs to be renamed."
  {:witan/name          :_
   :witan/exported?     false
   :witan/version       ""
   :witan/input-schema  {s/Any s/Any}
   :witan/output-schema {s/Any s/Any}
   :witan/param-schema  {s/Any s/Keyword}}
  [inputs renames]
  (clojure.set/rename-keys inputs renames))
