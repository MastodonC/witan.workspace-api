# witan.workspace-api

A helper library for writing models in the style of Witan workspaces.

## Notes on Model authoring

This library is designed to help ...

## API

### defworkflowfn

Use this macro to define functions that represents 'nodes' in a workflow. It should also be used for 'sub-functions' as a way to preserve the essence of passing data through a workflow.

```clojure
(require '[witan.workspace-api :refer [defworkflowfn]]
         '[schema.core :as s])
   
(defworkflowfn my-function
  "This is a docstring" ;; optional
  {:witan/name          :my-namespace/function-name
   :witan/version       "1.0"
   :witan/input-schema  {:number s/Num}
   :witan/output-schema {:new-number s/Num}
   :witan/param-schema  {:multiplier s/Num} ;; optional, defaults to nil
   :witan/exported?     false}              ;; optional, defaults to false
  [inputs params]
  {:new-number (* (:number inputs) (:multiplier params))})
   
;; Use as a normal function - `params` optional if not used
(my-function {:number 4} {:multiplier 3))
=> {:number 4 :new-number 12}
```

The macro wraps a standard function to provide some extra functionality:
* It adds the metadata specified in the map (using `with-meta`).
* It controls the arguments so that the fn only receives keys that match the schema. For this reason, schemas *must* be a map.
* Whilst only providing what's specified in the _input schema_, the fn will only accept, as a return value, a map that validates against the _output schema_. Extraneous keys will be culled.
* The return value is merged with the original input map, so that it will accrete over time.

### merge->

Use this macro to simulate a graph merge. Threads initial data into the first arg of each form, then merges the results. Forms *should* be workflow fns (at the least, they must return a map). 

```clojure
(require '[witan.workspace-api :refer [defworkflowfn merge->]]

;; assumes you have some workflow fns defined

(merge-> {:number 1}
         (my-function-1) ;; returns {:output-1 (inc number)}
         (my-function-2-with-params {:foo "bar"})) ;; returns {:output-2 (str number foo)}
=> {:number 1 :output-1 2 :output-2 "1bar"}

```


## License

Copyright © 2016 MastodonC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
