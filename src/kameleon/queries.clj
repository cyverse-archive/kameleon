(ns kameleon.queries
  (:use [kameleon.core]
        [kameleon.entities]
        [korma.core]))

(defn find-public-root-category-ids
  "Finds all of the public workspaces in the database."
  []
  (map #(:category_id %)
       (select workspace
               (join template_group)
               (fields :root_analysis_group_id
                       [:template_group.id :category_id])
               (where {:is_public true}))))
