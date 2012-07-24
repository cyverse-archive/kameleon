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
  (let [get-id   (fn [nm] (:id (first (select users (where {:username nm})))))
        add-user (fn [nm] (insert users (values {:username nm})) (get-id nm))
        id     (get-id username)]
    (if (nil? id)
      (add-user username)
      id)))

(defn get-user-ids
  "Gets the internal user identifiers for each username in a collection of
   fully qualified usernames.  Entries will be added for users that don't exist
   in the database."
  [usernames]
  (map get-user-id usernames))

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
