(ns witan.datasets
  (:require [clojure.core.matrix.dataset :as ds]
            [clojure.core.matrix :as cm]
            [clojure.core.reducers :as r]))

(defn safe-divide
  [[d dd]]
  (if (zero? dd)
    0
    (/ d dd)))

(defn juxt-r
  [& fns]
  (fn 
    ([] 
     (map #(%) fns))   
    ([x]
     (map #(%1 %2) fns x))
    ([a x]
     (map #(%1 %2 %3) fns a x))))

(defn map-vals
  [f m]
  (zipmap (keys m)
          (map f (vals m))))

(def rollup-fns
  {:max [identity max identity]
   :min [identity min identity]
   :sum [identity + identity]
   :count [(constantly 1) + identity]
   :mean [(juxt identity (constantly 1))
          (juxt-r + +)
          safe-divide]})

(defn rollup
  "Returns a dataset that uses the given summary function (or function
  identifier keyword) to rollup the given column based on a set of
  group-by columns. You can provide a keyword identifier of a set of
  built-in functions including:
  :max -- the maximum value of the data in each group
  :min -- the minimum value of the data in each group
  :sum -- the sum of the data in each group
  :count -- the number of elements in each group
  :mean -- the mean of the data in each group
  or you can supply your own triple of [transformer, accumulator, finalizer]"
  [summary-fun col-name group-by data]  
  (let [sub-data (ds/select-columns data (conj group-by col-name))
        column-dexs (into {} (map-indexed #(vector %2 %1) (:column-names sub-data)))
        grouper (fn [values]
                  (mapv (fn [col] 
                          (nth values (column-dexs col))) 
                        group-by))
        [xf accumulate finalizer] (if (keyword? summary-fun)
                                    (rollup-fns summary-fun)
                                    summary-fun)]
    (->>
     (cm/rows sub-data)
     (r/map (fn transform
              ([values]
               [(grouper values) 
                (xf (nth values (column-dexs col-name)))])))
     (r/fold
      (fn combiner 
        ([] {})
        ([l r]
         (merge-with accumulate
                     l r)))
      (fn reducer
        ([] {})
        ([a [group value]]
         (update a
                 group
                 #(if %
                    (accumulate % value)
                    (accumulate value))))))
     (r/map (fn [[g v]]             
              (conj g (finalizer v))))
     (into [])
     (ds/dataset (conj group-by col-name)))))
