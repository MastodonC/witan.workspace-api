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
  [data summary-fun col-name group-by]  
  (let [sub-data (ds/select-columns data (conj group-by col-name))
        column-dexs (into {} (map-indexed #(vector %2 %1) (:column-names sub-data)))
        grouper (fn [values]
                  (mapv (fn [col] 
                          (nth values (column-dexs col))) 
                        group-by))
        [xf accumulate finalizer] (if (keyword? summary-fun)
                                    (rollup-fns summary-fun)
                                    summary-fun)]
    (->> sub-data
         cm/rows
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

(defn add-derived-column
  "Adds or replaces a column in the dataset, with values (apply derive-fn (select-columns src-col-name (rows dataset))).
   If derived-col-name already exists within the dataset, the column is replaced with the new values."
  [dataset derived-col-name src-col-names derive-fn]
  (let [derived-col-name-present ((set (ds/column-names dataset)) derived-col-name)]
    ((if derived-col-name-present ds/replace-column ds/add-column)
     dataset derived-col-name
     (apply (partial map derive-fn)
            (map #(ds/column dataset %) src-col-names)))))

(defn row-count
  "This should be added to core.matrix, and probably to the dataset protocol"
  [dataset]
  (first (:shape dataset)))

(defn build-index [indexer inverse-indexer dataset]
  (->> dataset
       cm/rows
       (r/fold
        (fn combiner
          ([] {})
          ([l r]
           (merge l r)))
        (fn reducer
          ([] {})
          ([a row]
           (assoc a
                  (indexer row)
                  (inverse-indexer row)))))))

(defn column-values-fn
  [dataset columns]
  (let [col-indexes (map (partial ds/column-index dataset) columns)] 
    (fn [row]
      (map (partial nth row) col-indexes))))

(defn join-
  "Right joins the two datasets by the values found in columns, where the left side of the join is target. 
  Implementation assumes the cost of converting the left dataset to rows and then using fold to join right, will be
  justified by the size of the data. Potential improve could be to detect the data size and if small perform the join by
  creating new columns for the dataset, rather than growing the rows."
  [target src [t-columns s-columns] {:keys [empty-cell] :or {:empty-cell nil}}]
  (let [src-indexer (column-values-fn src s-columns)
        unindexed-cols (remove (set s-columns) (ds/column-names src))
        inverse-indexer (column-values-fn src unindexed-cols)
        dex (build-index src-indexer inverse-indexer src)
        t-indexer (column-values-fn target t-columns)
        unmatched-index (repeat (count unindexed-cols) empty-cell)]
    (->> target
         cm/rows
         (r/fold
          (fn combiner
            ([] [])
            ([l r]
             (concat l r)))
          (fn reducer
            ([] [])
            ([a row]
             (conj a
                   (concat row
                           (get dex (t-indexer row) unmatched-index))))))
         (ds/dataset (concat (ds/column-names target) 
                             unindexed-cols)))))

(defn left-join
  [left right columns & options]
  (join- left right 
        (if (vector? (first columns)) columns (repeat 2 columns))
        (apply hash-map options)))

(defn right-join
  [left right columns & options]
  (join- right left 
        (if (vector? (first columns)) columns (repeat 2 columns))
        (apply hash-map options)))

(defn join
  "Performs a right-join"
  [left right columns & options]
  (apply (partial right-join left right columns) options))

(defn filter-dataset
  "Filters the given dataset to rows where filter-fn returns truthy when called with the values in filter-columns"
  [dataset filter-columns filter-fn]
  (let [filter-indexes (map #(ds/column-index dataset %) filter-columns)
        filter-on (fn [column-values]
                    (map #(get column-values %) filter-indexes))
        empty-data (mapv (constantly []) (ds/column-names dataset))]
    (->> dataset
         .columns
         (apply (partial mapv vector))
         (r/fold
          (fn combiner
            ([] empty-data)
            ([l r]
             (mapv concat l r)))
          (fn reducer
            ([] empty-data)
            ([a column-values]
             (if (apply filter-fn (filter-on column-values))
               (mapv #(conj %1 %2) a column-values)
               a))))
         (zipmap (ds/column-names dataset))
         ds/dataset)))
