(ns kameleon.queries
  (:use [kameleon.core]
        [kameleon.entities]
        [korma.core]
        [slingshot.slingshot :only [throw+]]))

(defn find-public-root-category-ids
  "Finds the root category identifiers for all of the public workspaces in the
   database."
  []
  (map #(:category_id %)
       (select workspace
               (join template_group)
               (fields [:template_group.id :category_id])
               (where {:is_public true}))))

(defn- load-categories
  "Loads one or more categories based on a condition that will be placed in the
   where clause of the query."
  [condition]
  (select analysis_group_listing
          (fields :hid :id :name :description :is_public)
          (where condition)))

(defn- load-required-category
  "Attempts to load a category from the database and throws an exception if the
   category is not found.  This method should only be applied to fields that are
   guaranteed to be unique in the database."
  [condition]
  (let [categories (load-categories condition)]
    (if (empty? categories)
      (throw+ {:type ::category-not-found :condition condition})
      (first categories))))

(defn- load-subcategory-ids
  "Loads all of the subcategory identifiers for a category."
  [id]
  (map #(:subgroup_id %)
       (select "template_group_group"
               (fields :subgroup_id)
               (where {:parent_group_id id}))))

(defn load-ids-in-category-tree
  "Loads all of the internal identifiers of categories in a category tree
   rooted at the category with the given internal identifier."
  [id]
  (loop [ids [id] result []]
    (let [subcat-ids (flatten (map load-subcategory-ids ids))]
      (if (empty? subcat-ids)
        result
        (recur subcat-ids (concat result subcat-ids))))))

(defn- count-apps-in-category
  "Counts the number of apps that are directly associated with a category."
  [category]
  (:count (first (select "template_group_template"
                         (fields [(sqlfn count "*") :count])
                         (where {:template_group_id (:hid category)})))))

(defn- count-apps
  "Counts the number of apps that are associated with a category or any of its
   descendents."
  [category subcats]
  (apply + (count-apps-in-category category)
         (map #(:template_count %) subcats)))

(defn- recursively-load-category
  "Recursively loads a category and all of its descendents."
  [condition]
  (let [category (load-required-category condition)
        subcat-ids (load-subcategory-ids (:hid category))
        subcats (doall (map #(recursively-load-category {:hid %}) subcat-ids))
        app-count (count-apps category subcats)]
    (dissoc (assoc category :groups subcats :template_count app-count) :hid)))

(defn load-category-tree
  "Loads the category tree rooted at the category with the given identifier."
  [id]
  (recursively-load-category {:id id}))

(defn- load-apps
  "Loads the apps with the given internal identifiers."
  [ids]
  (select analysis_listing
          (where {:hid [in ids]})
          (order :name :ASC)))

(defn- get-app-ids-in-categories
  "loads the internal identifiers of the apps in a list of categories."
  [ids]
  (into #{} (map #(:template_id %)
                 (select "template_group_template"
                         (fields :template_id)
                         (where {:template_group_id [in ids]})))))

(defn load-apps-in-category
  "Lists all of the apps in a category or any of its descendents."
  [id]
  (let [hid (:hid (load-required-category {:id id}))]
    (load-apps (get-app-ids-in-categories (load-ids-in-category-tree hid)))))

(defn current-db-version
  "Determines the current database version."
  []
  (-> (select version
              (fields [:version])
              (order :version :DESC)
              (limit 1))
      ffirst
      val))

(defn check-db-version
  "Verifies that the current database version is the same as the version that
   is compatible with this version of Kameleon."
  []
  (let [current    (current-db-version)
        compatible (compatible-db-version)]
    (when (not= current compatible)
      (throw+ {:type ::incorrect-db-version
               :current current
               :compatible compatible}))))
