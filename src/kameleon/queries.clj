(ns kameleon.queries
  (:use [kameleon.core]
        [kameleon.entities]
        [korma.core]
        [slingshot.slingshot :only [throw+]]))

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

(defn add-query-offset
  "Returns select_query with an OFFSET clause added if offset_val is 0 or more;
   otherwise the original select_query is returned."
  [select_query offset_val]
  (if (>= offset_val 0)
    (-> select_query
      (offset offset_val))
    select_query))

(defn add-query-limit
  "Returns select_query with a LIMIT clause added if limit_val is more than 0;
   otherwise the original select_query is returned."
  [select_query limit_val]
  (if (> limit_val 0)
    (-> select_query
      (limit limit_val))
    select_query))

(defn add-query-sorting
  "Returns select_query with an ORDER BY clause added if sort-field is not nil;
   otherwise the original select_query is returned."
  [select_query sort-field sort-dir]
  (if (not (nil? sort-field))
    (let [sort-dir (if (= sort-dir :DESC)
                     sort-dir
                     :ASC)]
      (-> select_query
          (order sort-field sort-dir)))
    select_query))

(defn get-collaborators
  "Gets the list of collaborators for a given fully qualified username."
  [username]
  (map :username
       (select collaborators
               (fields :collaborator.username)
               (join users)
               (join collaborator)
               (where {:users.username username}))))

(defn get-user-id
  "Gets the internal user identifier for a fully qualified username.  A new
   entry will be added if the user doesn't already exist in the database."
  [username]
  (let [id (:id (first (select users (where {:username username}))))]
    (if (nil? id)
      (:id (insert users (values {:username username})))
      id)))

(defn get-user-ids
  "Gets the internal user identifiers for each username in a collection of
   fully qualified usernames.  Entries will be added for users that don't exist
   in the database."
  [usernames]
  (map get-user-id usernames))

(defn get-username
  "Gets the username for the user with the given identifier."
  [user-id]
  (when-not (nil? user-id)
    (:username (first (select users (where {:id user-id}))))))

(defn- add-collaboration
  "Adds a collaboration to the database if the collaboration doesn't exist
   already."
  [user-id collaborator-id]
  (let [results (select collaborators
                        (where {:user_id user-id
                                :collaborator_id collaborator-id}))]
    (when (empty? results)
      (insert collaborators
              (values {:user_id user-id
                       :collaborator_id collaborator-id})))))

(defn add-collaborators
  "Adds collaborators for a given fully qualified username."
  [username collaborators]
  (let [user-id (get-user-id username)
        collaborator-ids (get-user-ids collaborators)]
    (dorun (map (partial add-collaboration user-id) collaborator-ids))))

(defn- user-id-subquery
  "Performs a subquery for a user ID."
  [username]
  (subselect users
             (fields :id)
             (where {:username username})))

(defn- remove-collaboration
  "Removes a collaboration from the database if it exists."
  [user collab]
  (delete collaborators
          (where
           {:user_id         [= (user-id-subquery user)]
            :collaborator_id [= (user-id-subquery collab)]})))

(defn remove-collaborators
  "Removes collaborators for a given fully qualified username."
  [username collaborators]
  (dorun (map (partial remove-collaboration username) collaborators)))

(defn fetch-workspace-by-user-id
  "Gets the workspace for the given user_id."
  [user_id]
  (first (select workspace (where {:user_id user_id}))))

(defn create-workspace
  "Creates a workspace database entry for the given user ID."
  [user_id]
  (insert workspace (values {:user_id user_id})))

(defn set-workspace-root-app-group
  "Sets the given root-app-group ID in the given workspace, and returns a map of
   the workspace with its new group ID."
  [workspace_id root_app_group_id]
  (update workspace
          (set-fields {:root_analysis_group_id root_app_group_id})
          (where {:id workspace_id})))

(defn property-types-for-tool-type
  "Lists the valid property types for the tool type with the given identifier."
  ([tool-type-id]
     (property-types-for-tool-type (select* property_type) tool-type-id))
  ([base-query tool-type-id]
     (select base-query
             (join :tool_type_property_type
                   {:tool_type_property_type.property_type_id
                    :property_type.hid})
             (where {:tool_type_property_type.tool_type_id tool-type-id}))))

(defn get-tool-type-by-name
  "Searches for the tool type with the given name."
  [tool-type-name]
  (first (select tool_types
                 (where {:name tool-type-name}))))

(defn get-tool-type-by-component-id
  "Searches for the tool type associated with the given deployed component."
  [component-id]
  (first (select deployed_components
                 (fields :tool_types.id :tool_types.name :tool_types.label
                         :tool_types.description)
                 (join tool_types)
                 (where {:deployed_components.id component-id}))))

(defn get-or-create-user
  "Gets a user from the database, creating the user if necessary."
  [username]
  (if-let [user (first (select users (where {:username username})))]
    user
    (insert users (values {:username username}))))

(defn get-or-create-workspace-for-user
  "Gets a workspace from the database, creating it if necessary."
  [username]
  (let [user-id (:id (get-or-create-user username))]
    (if-let [workspace (first (select workspace (where {:user_id user-id})))]
      workspace
      (insert workspace (values {:user_id user-id})))))

(defn get-public-user-id
  "Gets the user ID for the public user."
  []
  (:id (get-or-create-user "<public>")))
