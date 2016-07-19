(defproject witan.workspace-api "0.1.10"
  :description "A helper library for writing models in the style of Witan workspaces."
  :url "https://github.com/MastodonC/witan.workspace-api"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [net.mikera/core.matrix "0.51.0"]
                 [prismatic/schema "1.1.2"]]
  :repositories [["releases" {:url "https://clojars.org/repo"
                              :creds :gpg}]
                 ["snapshots" {:url "https://clojars.org/repo"
                               :creds :gpg}]])
