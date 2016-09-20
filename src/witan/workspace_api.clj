(ns witan.workspace-api
  (:require [schema.core :as s]
            [witan.workspace-api.schema :refer :all]
            [clojure.set]))

(def ^:private counter (atom 0))
(def ^:private logger (agent nil))
(def  logging-fn (atom identity))

(defn set-api-logging!
  [log]
  (if (fn? log)
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
         (@logging-fn (str "witan.workspace-api -> calling fn: " (:witan/name ~metadata)))
         (try
           (let [params'# (select-params# (first params#))
                 inputs'# (select-schema-keys ~input-schema inputs#)
                 result#  (actual-fn# inputs'# params'#)
                 result'# (select-schema-keys ~output-schema result#)
                 merged#   (merge inputs# result'#)]
             (@logging-fn (str "witan.workspace-api <- finished fn: " (:witan/name ~metadata)))
             merged#)
           (catch Throwable e# (@logging-fn (str "witan.workspace-api !! Exception in fn" (:witan/name ~metadata) "-" e#))
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
         (@logging-fn (str "witan.workspace-api -> calling pred: " (:witan/name ~metadata)))
         (try
           (let [params'# (select-params# (first params#))
                 inputs'# (select-schema-keys ~input-schema inputs#)
                 result#  (actual-fn# inputs'# params'#)
                 result'# (boolean result#)]
             (@logging-fn (str "witan.workspace-api <- finished pred: " (:witan/name ~metadata)))
             result'#)
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
         (@logging-fn (str "witan.workspace-api -> calling input: " (:witan/name ~metadata)))
         (try
           (let [params'# (select-params# (first params#))
                 result#  (actual-fn# nil params'#)
                 result'# (select-schema-keys ~output-schema result#)]
             (@logging-fn (str "witan.workspace-api <- finished input: " (:witan/name ~metadata)))
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
         (@logging-fn (str "witan.workspace-api -> calling output: " (:witan/name ~metadata)))
         (try
           (let [params'# (select-params# (first params#))
                 inputs'# (select-schema-keys ~input-schema inputs#)
                 result#  (actual-fn# inputs'# params'#)]
             (@logging-fn (str "witan.workspace-api <- finished output: " (:witan/name ~metadata)))
             result#)
           (catch Exception e# (@logging-fn (str "witan.workspace-api !! Exception in output" (:witan/name ~metadata) "-" e#))
                  (throw e#))))
       (assign-meta #'~name :witan/metadata WorkflowOutputMetaData ~metadata))))

(defmacro defmodel
  "Macro for defining a workflow model"
  [name & body] ;; metadata args &body
  (let [[doc metadata body] (carve-body body)
        metadata (assoc metadata :witan/type :model)]
    `(do
       (s/validate Model ~@body)
       (def ~name
         ~doc
         ~@body)
       (assign-meta #'~name
                    :witan/metadata
                    ModelMetaData ~metadata))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; refactored input

(defmacro definput
  [fname {:keys [witan/version
                 witan/schema
                 witan/key
                 witan/name] :as args}]
  `(defworkflowinput ~fname
     "made-with-make-input-macro"
     {:witan/name ~name
      :witan/version ~version
      :witan/output-schema {~key ~schema}
      :witan/param-schema {:src s/Str
                           :fn  (s/pred fn?)}}
     [inputs# params#]
     (let [fn#  (:fn params#)
           src# (:src params#)]
       {~key (fn# src# ~schema)})))

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
