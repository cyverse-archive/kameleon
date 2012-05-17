(defproject org.iplantc/kameleon "0.0.1-SNAPSHOT"
  :description "Library for interacting with backend relational databases."
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [korma "0.3.0-beta7"]
                 [postgresql "9.0-801.jdbc4"]
                 [slingshot "0.10.2"]]
  :plugins [[lein-marginalia "0.7.0"]])