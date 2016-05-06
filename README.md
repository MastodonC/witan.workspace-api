# witan.workspace-api

A helper library for writing models in the style of Witan workspaces.

## Notes on Model authoring

This library is designed to help ...

## API

### defworkflowfn

Use this macro to define functions that represents 'nodes' or 'modules' in a workflow. It should also be used for 'sub-functions' as a way to preserve the essence of passing data through a workflow.

```clojure
(require '[witan.workspace-api :refer [defnworkflowfn]]
         '[schema.core :as s])

(defnworkflowfn my-function
  "This is a docstring"
  {:witan/name :my-namespace/function-name
   :witan/version "1.0"
   :witan/input-schema {:number s/Num}
   :witan/output-schema {:new-number s/Num}
   :witan/param-schema {:multiplier s/Num} ;; optional, defaults to nil
   :witan/exported? false ;; optional, defaults to false.}
   [inputs params]
   (prn "Thanks for calling my-function")
   {:new-number (inc (:number inputs))})
```

## License

Copyright © 2016 MastodonC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
