(ns kameleon.app-listing
  (:use [korma.core]
        [kameleon.core]
        [kameleon.entities]
        [kameleon.queries])
  (:require [clojure.tools.logging :as log]))

(defn- get-group-hid-subselect
  "Gets a subselect that fetches the hid for the given template_group ID."
  [app_group_id]
  (subselect :template_group
             (fields :hid)
             (where {:id app_group_id})))

(defn- get-all-group-ids-subselect
  "Gets a subselect that fetches the template_group and its subgroup IDs with
   the stored procedure app_group_hierarchy_ids."
  [app_group_id]
  (subselect
    (sqlfn :app_group_hierarchy_ids
           (get-group-hid-subselect app_group_id))))

(defn- get-fav-group-id-subselect
  "Gets a subselect that fetches the ID for the Favorites group at the given
   index under the template_group with the given ID."
  [workspace_root_group_id favorites_group_index]
  (subselect
    :template_group_group
    (fields :subgroup_id)
    (where {:parent_group_id workspace_root_group_id
            :hid favorites_group_index})))

(defn- get-is-fav-sqlfn
  "Gets a sqlfn that retuns true if the App ID in its subselect is found in the
   Favorites group with the ID returned by get-fav-group-id-subselect."
  [workspace_root_group_id favorites_group_index]
  (let [fav_group_id_subselect (get-fav-group-id-subselect
                                 workspace_root_group_id
                                 favorites_group_index)]
    (sqlfn* :exists
            (subselect
              :template_group_template
              (where {:template_group_template.template_id
                      :analysis_listing.hid})
              (where {:template_group_template.template_group_id
                      fav_group_id_subselect})))))

(defn- get-base-analysis-listing-query
  "Gets a select for some base fields in the analysis_listing."
  []
  (->
    (select* analysis_listing)
    (fields :id
            :name
            :description
            :integrator_name
            :integrator_email
            :integration_date
            :edited_date
            [:wikiurl :wiki_url]
            :average_rating
            :is_public
            :step_count
            :deleted
            :disabled
            :overall_job_type)))

(defn get-analysis-listing-query
  "Gets a query for all of the apps in an app group and all of its descendents."
  [base_listing_query app_group_id]
  (-> base_listing_query
    (join :inner :template_group_template
          (= :template_group_template.template_id :analysis_listing.hid))
    (where {:template_group_template.template_group_id
            [in (get-all-group-ids-subselect app_group_id)]
            :deleted false})))

(defn count-apps-in-group-for-user
  "Counts all of the apps in an app group and all of its descendents."
  [app_group_id]
  (let [listing_query (get-analysis-listing-query
                        (select* analysis_listing)
                        app_group_id)]
    ;; Excecute the query and return the results
    (log/debug "count-apps-in-group-for-user::count_query:"
               (sql-only (select (aggregate listing_query (count :*) :total))))
    (:total (first
              (select (aggregate listing_query (count :*) :total))))))

(defn get-apps-in-group-for-user
  "Lists all of the apps in an app group and all of its descendents, using the
   given workspace (as returned by fetch-workspace-by-user-id) to mark
   whether each app is a favorite and to include the user's rating in each app."
  [app_group_id workspace favorites_group_index params]
  (let [user_id (:user_id workspace)
        workspace_root_group_id (:root_analysis_group_id workspace)
        row_offset (try (Integer/parseInt (:offset params)) (catch Exception e 0))
        row_limit (try (Integer/parseInt (:limit params)) (catch Exception e -1))
        sortField (keyword (:sortField params))
        sortDir (keyword (:sortDir params))

        ;; Bind the final query
        listing_query (get-analysis-listing-query
                        (get-base-analysis-listing-query)
                        app_group_id)
    
        ;; Bind is_favorite subqueries
        is_fav_subselect (get-is-fav-sqlfn
                           workspace_root_group_id
                           favorites_group_index)

        ;; Add user's is_favorite column
        listing_query (-> listing_query
                        (fields [is_fav_subselect :is_favorite]))

        ;; Join the user's ratings
        listing_query (-> listing_query
                        (fields [:ratings.rating :user_rating]
                                :ratings.comment_id)
                        (join ratings
                              (and (= :ratings.transformation_activity_id
                                      :analysis_listing.hid)
                                   (= :ratings.user_id
                                      user_id))))

        ;; Add limits and sorting, if required
        listing_query (add-query-limit listing_query row_limit)
        listing_query (add-query-offset listing_query row_offset)
        listing_query (add-query-sorting listing_query sortField sortDir)]

    ;; Excecute the query and return the results
    (log/debug "get-apps-in-group-for-user::listing_query:"
               (sql-only (select listing_query)))
    (select listing_query)))
