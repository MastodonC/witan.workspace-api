(ns witan.workspace-api.onyx)

;; https://github.com/onyx-platform/onyx/blob/686ca2a8bb1fed8cd3a6dbe63d75923460f46888/src/onyx/peer/operation.clj#L27
(defn kw->fn [kw]
  (try
    (let [user-ns (symbol (namespace kw))
          user-fn (symbol (name kw))]
      (or (ns-resolve user-ns user-fn)
          (throw (Exception.))))
    (catch Throwable e
      (throw (ex-info (str "Could not resolve symbol on the classpath, did you require the file that contains the symbol " kw "?") {:kw kw})))))

(defn default-fn-wrapper
  "This function exists to act as a 'wrapper' for when workflow functions are to
   be used in an Onyx job, the original reason being that workflow functions expect
   [state params] whereas Onyx functions expect [params state]. Unfortunately we
   have to do an additional ns-resolve."
  ([fn segment]
   ((kw->fn fn) segment))
  ([fn params segment]
   ((kw->fn fn) segment params)))

(defn default-pred-wrapper
  "This function exists to act as a 'wrapper' for when workflow predicates are to
   be used in an Onyx job"
  ([_ _ segment _ fn]
   ((kw->fn fn) segment))
  ([_ _ segment _ fn params]
   ((kw->fn fn) segment params)))
