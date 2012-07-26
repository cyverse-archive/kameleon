(ns kameleon.app-groups
  (:use [kameleon.core]
        [kameleon.entities]
        [korma.core]
        [slingshot.slingshot :only [throw+]]))

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
