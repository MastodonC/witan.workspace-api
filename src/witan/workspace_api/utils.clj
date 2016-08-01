(ns witan.workspace-api.utils)

(defn assert!
  [p msg]
  (when-not (p) (throw (Exception. msg))))

(defmacro map-fn-meta
  [& ss]
  `[~@(for [s ss]
        `(-> #'~s meta :witan/metadata))])

(defmacro map-model-meta
  [& ss]
  `[~@(for [s ss]
        `(assoc ~s :metadata (-> #'~s meta :witan/metadata)))])
