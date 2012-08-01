(ns kameleon.app-groups
  (:use [kameleon.core]
        [kameleon.entities]
        [korma.core]
        [slingshot.slingshot :only [throw+]])
  (:import [java.util UUID]))

(defn get-app-group-hierarchy
  "Gets the app group hierarchy rooted at the node with the given identifier."
  [root-id]
  (select (sqlfn :analysis_group_hierarchy root-id)))

(defn- get-root-app-group-ids
  "Gets the internal identifiers for all app groups associated with workspaces
   that satisfy the given condition."
  [condition]
  (map :app_group_id
       (select workspace
               (fields [:root_analysis_group_id :app_group_id])
               (where condition))))

(defn get-visible-root-app-group-ids
  "Gets the list of internal root app group identifiers that are visible to the
   user with the given workspace identifier."
  [workspace-id]
  (concat (get-root-app-group-ids {:id workspace-id})
          (get-root-app-group-ids {:is_public true})))

(defn load-root-app-groups-for-all-users
  "Gets the list of all root app group ids."
  []
  (select workspace
          (fields [:workspace.root_analysis_group_id :app_group_id]
                  [:workspace.id :workspace_id]
                  :users.username)
          (join users)))

(defn get-app-group
  "Retrieves an App Group by its ID."
  [app_group_id]
  (first (select analysis_group_listing
                 (fields :id :name :description :is_public)
                 (where {:id app_group_id}))))

(defn create-app-group
  "Creates a database entry for a template_group, with an UUID and the given
   workspace_id and name, and returns a map of the group with its new hid."
  [workspace_id name]
  (insert template_group (values [{:id (-> (UUID/randomUUID) (.toString))
                                   :workspace_id workspace_id
                                   :name name}])))

(defn add-subgroup
  "Adds a subgroup to a parent group, which should be listed at the given index
   position of the parent's subgroups."
  [parent_group_id index subgroup_id]
  (insert :template_group_group
          (values {:parent_group_id parent_group_id
                   :subgroup_id subgroup_id
                   :hid index})))

