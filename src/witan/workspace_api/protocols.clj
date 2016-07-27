(ns witan.workspace-api.protocols)

(defprotocol IModelLibrary
  (available-fns [this])
  (available-models [this]))
