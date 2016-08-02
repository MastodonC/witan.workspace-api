(ns witan.workspace-api.utils)

(defn property-holds? [x p msg]
  (if (p x)
    x
    (throw (Exception. msg))))

(defmacro map-fn-meta
  [& ss]
  `[~@(for [s ss]
        `(-> #'~s meta :witan/metadata))])

(defmacro map-model-meta
  [& ss]
  `[~@(for [s ss]
        `(assoc ~s :metadata (-> #'~s meta :witan/metadata)))])
