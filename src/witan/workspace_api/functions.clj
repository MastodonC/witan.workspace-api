(ns witan.workspace-api.functions
  (:require [schema.core :as s]
            [clojure.set]
            [witan.workspace-api :refer [defworkflowfn]]))

(defworkflowfn rename-keys
  "Helper fn for running tests that require inputs or outputs to be renamed."
  {:witan/name          :witan.workspace-api/rename-keys
   :witan/exported?     true
   :witan/version       "1.0"
   :witan/input-schema  {s/Keyword s/Any}
   :witan/output-schema {s/Keyword s/Any}
   :witan/param-schema  {s/Keyword s/Keyword}}
  [inputs renames]
  (clojure.set/rename-keys inputs renames))
