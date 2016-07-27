(ns witan.workspace-api
  (:require [schema.core :as s]
            [clojure.set]))

(def ^:private counter (atom 0))
(def ^:private logger (agent nil))
(def  logging-fn (atom identity))

(defn set-api-logging! [log] (if (fn? log)
                               (do
                                 (reset! counter 0)
                                 (reset! logging-fn
                                         (fn [msg]
                                           (send logger
                                                 (fn [_]
                                                   (log (str (swap! counter inc) " " msg)))))))
                               (throw (Exception. "Must be a function"))))

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

(def FnTypeEnum
  (s/enum :function
          :predicate
          :input
          :output))

(def ContractBase
  {:witan/name          s/Keyword
   :witan/impl          s/Keyword
   :witan/version       s/Str
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
   :witan/version s/Str
   :witan/type    FnTypeEnum
   (s/optional-key :witan/params) {s/Keyword s/Any}})

(def ModelMetaData
  "Schema for the Witan workflow model metadata"
  {:witan/name          s/Keyword
   :witan/version       s/Str
   :witan/doc           s/Str
   :witan/type          (s/eq :model)})

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

(def Model
  "Schema for the Witan workflow model metadata"
  (s/constrained {:workflow [WorkflowStatement]
                  :catalog  [CatalogEntry]} model-valid?))

(defn carve-body
  [body]
  (let [doc      (when (string? (first body)) (first body))
        metadata (if doc (second body) (first body))
        body     (if doc (drop 2 body) (next body))
        doc      (or doc (:witan/doc metadata) "No docs")
        metadata (assoc metadata :witan/doc doc)]
    [doc metadata body]))

(defn assign-meta
  [name kw schema metadata]
  (alter-meta! name assoc kw (s/validate schema metadata)))

(defn create-impl-kw
  [name]
  (->> name (str *ns* "/") (keyword)))

(defmacro defworkflowfn
  "Macro for defining a workflow function"
  [name & body]
  (let [[doc metadata [args & body]] (carve-body body)
        metadata (assoc metadata
                        :witan/impl (create-impl-kw name)
                        :witan/type :function)
        {:keys [witan/input-schema
                witan/output-schema
                witan/param-schema]} metadata]
    `(let [select-params# ~(if param-schema
                             `(partial select-schema-keys ~param-schema)
                             `(constantly nil))
           actual-fn# (fn ~args ~@body)]
       (defn ~name
         ~doc
         [inputs# & params#]
         (@logging-fn (str "witan.workspace-api -> calling fn:" (:witan/name ~metadata)))
         (try
           (let [params'# (select-params# (first params#))
                 inputs'# (select-schema-keys ~input-schema inputs#)
                 result#  (actual-fn# inputs'# params'#)
                 _#       (@logging-fn (str "witan.workspace-api <- finished fn:" (:witan/name ~metadata)))
                 result'# (select-schema-keys ~output-schema result#)]
             (merge inputs# result'#))
           (catch Exception e# (@logging-fn (str "witan.workspace-api !! Exception in fn" (:witan/name ~metadata) "-" e#))
                  (throw e#))))
       (assign-meta #'~name :witan/metadata WorkflowFnMetaData ~metadata))))

(defmacro defworkflowpred
  "Macro for defining a workflow predicate"
  [name & body]
  (let [[doc metadata [args & body]] (carve-body body)
        metadata (assoc metadata
                        :witan/type :predicate
                        :witan/impl (create-impl-kw name))
        {:keys [witan/input-schema
                witan/param-schema]} metadata]
    `(let [select-params# ~(if param-schema
                             `(partial select-schema-keys ~param-schema)
                             `(constantly nil))
           actual-fn# (fn ~args ~@body)]
       (defn ~name
         ~doc
         [inputs# & params#]
         (@logging-fn (str "witan.workspace-api -> calling pred:" (:witan/name ~metadata)))
         (try
           (let [params'# (select-params# (first params#))
                 inputs'# (select-schema-keys ~input-schema inputs#)
                 result#  (actual-fn# inputs'# params'#)
                 _#       (@logging-fn (str "witan.workspace-api <- finished pred:" (:witan/name ~metadata)))]
             (boolean result#))
           (catch Exception e# (@logging-fn (str "witan.workspace-api !! Exception in pred" (:witan/name ~metadata) "-" e#))
                  (throw e#))))
       (assign-meta #'~name :witan/metadata WorkflowPredicateMetaData ~metadata))))

(defmacro defworkflowinput
  "Macro for defining a workflow input"
  [name & body] ;; metadata args &body
  (let [[doc metadata [args & body]] (carve-body body)
        metadata (assoc metadata
                        :witan/impl (create-impl-kw name)
                        :witan/type :input)
        {:keys [witan/output-schema
                witan/param-schema]} metadata]
    `(let [select-params# ~(if param-schema
                             `(partial select-schema-keys ~param-schema)
                             `(constantly nil))
           actual-fn# (fn ~args ~@body)]
       (defn ~name
         ~doc
         [inputs# & params#] ;; input field will always be nil, we leave it there for uniformity
         (@logging-fn (str "witan.workspace-api -> calling input:" (:witan/name ~metadata)))
         (try
           (let [params'# (select-params# (first params#))
                 result#  (actual-fn# nil params'#)
                 _#       (@logging-fn (str "witan.workspace-api <- finished input:" (:witan/name ~metadata)))
                 result'# (select-schema-keys ~output-schema result#)]
             result'#)
           (catch Exception e# (@logging-fn (str "witan.workspace-api !! Exception in input" (:witan/name ~metadata) "-" e#))
                  (throw e#))))
       (assign-meta #'~name :witan/metadata WorkflowInputMetaData ~metadata))))

(defmacro defworkflowoutput
  "Macro for defining a workflow output"
  [name & body]
  (let [[doc metadata [args & body]] (carve-body body)
        metadata (assoc metadata
                        :witan/impl (create-impl-kw name)
                        :witan/type :output)
        {:keys [witan/input-schema
                witan/param-schema]} metadata]
    `(let [select-params# ~(if param-schema
                             `(partial select-schema-keys ~param-schema)
                             `(constantly nil))
           actual-fn# (fn ~args ~@body)]
       (defn ~name
         ~doc
         [inputs# & params#]
         (@logging-fn (str "witan.workspace-api -> calling output:" (:witan/name ~metadata)))
         (try
           (let [params'# (select-params# (first params#))
                 inputs'# (select-schema-keys ~input-schema inputs#)
                 result#  (actual-fn# inputs'# params'#)
                 _#       (@logging-fn (str "witan.workspace-api <- finished output:" (:witan/name ~metadata)))]
             result#)
           (catch Exception e# (@logging-fn (str "witan.workspace-api !! Exception in output" (:witan/name ~metadata) "-" e#))
                  (throw e#))))
       (assign-meta #'~name :witan/metadata WorkflowOutputMetaData ~metadata))))

(defmacro defmodel
  "Macro for defining a workflow model"
  [name & body] ;; metadata args &body
  (let [[doc metadata body] (carve-body body)
        metadata (assoc metadata :witan/type :model)
        _ (s/validate Model (first body))]
    `(do
       (def ~name
         ~doc
         ~@body)
       (assign-meta #'~name
                    :witan/metadata
                    ModelMetaData ~metadata))))

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
