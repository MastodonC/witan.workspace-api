(ns witan.workspace-api.utils)

(defmacro map-meta
  [& ss]
  `[~@(for [s ss]
        `(-> #'~s meta :witan/metadata))])
