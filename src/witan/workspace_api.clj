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
  [name meta-data args & body]
  (let [{:keys [witan/input-schema
                witan/output-schema
                witan/param-schema]} meta-data]
    `(defn ~(with-meta name
              (assoc (meta name)
                     :witan/workflowfn
                     (s/validate WorkflowFnMetaData meta-data)))
       [inputs# & params#]
       (let [params'# (when (and (first params#) ~param-schema)
                        (select-schema-keys ~param-schema (first params#)))
             inputs'# (select-schema-keys ~input-schema inputs#)
             result#  ((fn ~args ~@body) inputs'# params'#)
             result'# (select-schema-keys ~output-schema result#)]
         (merge inputs# result'#)))))

(defn ns-workflowfns
  [ns-sym]
  (->> ns-sym
       (ns-publics)
       (filter #(let [m (-> % second meta)]
                  (and (contains? m :witan/workflowfn)
                       (-> m :witan/workflowfn :witan/exported?))))
       (map second)))

;; Helper fn for running tests that require inputs or outputs to be renamed.
(defworkflowfn rename-keys
  {:witan/name          :_
   :witan/exported?     false
   :witan/version       ""
   :witan/input-schema  {s/Any s/Any}
   :witan/output-schema {s/Any s/Any}
   :witan/param-schema  {s/Any s/Keyword}}
  [inputs renames]
  (clojure.set/rename-keys inputs renames))
