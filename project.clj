(defproject org.iplantc/kameleon "2.0.0-SNAPSHOT"
  :description "Library for interacting with backend relational databases."
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [korma "0.3.0-RC4"]
                 [postgresql "9.0-801.jdbc4"]
                 [slingshot "0.10.3"]]
  :plugins [[lein-marginalia "0.7.1"]]
  :manifest {"db-version" "1.8.4:20131114.01"})
