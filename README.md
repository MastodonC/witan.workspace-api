# witan.workspace-api

A helper library for writing models in the style of Witan workspaces.

## Defining Functions, Perficates and Models

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

* It controls the arguments so that the fn only receives keys that match the schema. For this reason, schemas *must* be a map.
* Whilst only providing what's specified in the _input schema_, the fn will only accept, as a return value, a map that validates against the _output schema_. Extraneous keys will be culled.
* The return value is merged with the original input map, so that it will accrete over time.

### defworkflowpred

Use this macro to define a workflow predicate, used to control the flow of a loop (for example, using `do-while->`). They are structurally similar to workflow functions but only the truthiness of the return value is considered.

```clojure
(require '[witan.workspace-api :refer [defworkflowpred]]
         '[schema.core :as s])

(defworkflowpred my-predicate?
  "This is a docstring" ;; optional
  {:witan/name         :my-namespace/predicate-name
   :witan/version      "1.0"
   :witan/input-schema {:number s/Num}
   :witan/param-schema {:value s/Num}}
  [{:keys [number]} {:keys [value]}]
  (< number value))

  ;; In a do-while-> loop. The initial data (`{:number 1}`) is passed to the predicate via thread-first.

  (do-while-> (my-predicate? {:value 10})
         {:number 1}
         (inc-loop) ;; returns {:number (inc number)}
         (double-loop)) ;; returns {:number (* 2 number)}
=> {:number 10}
```

### defworkflowmodel

Use this macro to define a workflow for a model. All model workflows defined this way will be exported and be made available upon request.

```clojure
(require '[witan.workspace-api :refer [defworkflowmodel]]
         '[schema.core :as s])

(defworkflowmodel my-model
  "This is a docstring" ;; optional
  {:witan/name          :my-namespace/model-name
   :witan/version       "1.0"}
  [[:in    :task1]
   [:task1 :task2]
   [:task3 [:continue? :task4 :task1]]
   [:task4 :out]])
```

Models consist of tuples that are either...
* Two keywords, corresponding to the 'from' to 'to' nodes in a graph. E.g. `[:in :task1]`
* A keyword ('from') and a 3-element tuple - predicate name, 'to' node and 'else' node. E.g. `[:task3 [:continue? :task4 :task1]]`

There is no restriction of the polarity of the predicate, but it's recommended the predicate checks the propagation (rather than termination) of the loop. This follows the semantics of 'do while' rather than 'do until'.

### defworkflowinput

Use this macro to define the schema for a required input into a model.

```clojure
(require '[witan.workspace-api :refer [defworkflowmodel]]
         '[schema.core :as s])

(def AnInputSchema
  {:number s/Num})

(defworkflowinput an-input
  {:witan/name :an-input
   :witan/version "1.0"
   :witan/doc "doc"
   :witan/input-schema AnInputSchema})
```

### defworkflowoutput

Use this macro to define the schema for a models outputs.

```clojure
(require '[witan.workspace-api :refer [defworkflowmodel]]
         '[schema.core :as s])

(def AnOutputSchema
  {:number s/Num})

(defworkflowoutput an-output
  {:witan/name :an-output
   :witan/version "1.0"
   :witan/doc "doc"
   :witan/output-schema AnOutputSchema})
```

## Helper Functions

### defworkflowinput

Use this macro to define the schema for a required input into a model.

```clojure
(require '[witan.workspace-api :refer [defworkflowmodel]]
         '[schema.core :as s])

(def AnInputSchema
  {:number s/Num})

(defworkflowinput an-input
  {:witan/name :an-input
   :witan/version "1.0"
   :witan/doc "doc"
   :witan/input-schema AnInputSchema})
```

### defworkflowoutput

Use this macro to define the schema for a models outputs.

```clojure
(require '[witan.workspace-api :refer [defworkflowmodel]]
         '[schema.core :as s])

(def AnOutputSchema
  {:number s/Num})

(defworkflowoutput an-output
  {:witan/name :an-output
   :witan/version "1.0"
   :witan/doc "doc"
   :witan/output-schema AnOutputSchema})
```

### merge->

Use this macro to simulate a graph merge. Initial data is threaded as the first arg of each form, then the results are merged. Forms *should* be workflow fns (at the least, they must return a map).

```clojure
(require '[witan.workspace-api :refer [defworkflowfn merge->]]

;; assumes you have some workflow fns defined

(merge-> {:number 1}
         (my-function-1) ;; returns {:output-1 (inc number)}
         (my-function-2-with-params {:foo "bar"})) ;; returns {:output-2 (str number foo)}
=> {:number 1 :output-1 2 :output-2 "1bar"}

```

Be aware that if a workflow fn outputs the same key as an input, this will likely cause trouble as other forms will also be returning that key and the merge will be unpredictable.

### do-while->

Use this macro to simulate a loop which executes at least once. First arg is a predicate which receives the data prior to each iteration. Initial data is threaded as the first arg of each form. Forms *should* be workflow fns.

```clojure
(require '[witan.workspace-api :refer [defworkflowfn do-while->]]

;; assumes you have some workflow fns defined

(do-while-> (number-lt-10)
         {:number 1}
         (inc-loop) ;; returns {:number (inc number)}
         (double-loop) ;; returns {:number (* 2 number)}
=> {:number 10}
```

## Notes on Model authoring

This library is designed to help develop models at both the workspace/workflow level and also the levels beneath. To this end, the `defworkflowfn` and associated macros (e.g. `merge->`) are designed to assist in creating and simulating graph-compute-like behaviour. For example, where to simulate a diamond workflow, the following should suffice:

```clojure
;    1
;   / \
;  2a 2b
;   \ /
;    3

(-> input
    (function-one)
    (merge-> (function-two-a)
             (function-two-b))
    (function-three))
```

## License

Copyright © 2016 MastodonC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
