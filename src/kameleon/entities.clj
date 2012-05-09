(ns kameleon.entities
  (:use [kameleon.core]
        [korma.core]))

(declare users workspace template_group transformation_activity
         integration_data deployed_components deployed_component_data_files
         transformation_steps)

;; Users who have logged into the DE.
(defentity users
  (entity-fields :username)
  (has-one workspace {:fk :user_id}))

;; The workspaces of users who have logged into the DE.
(defentity workspace
  (entity-fields :is_public)
  (belongs-to users {:fk :user_id})
  (belongs-to template_group {:fk :root_analysis_group_id}))

;; An app group.
(defentity template_group
  (pk :hid)
  (entity-fields :id :name :description)
  (belongs-to workspace)
  (many-to-many template_group :template_group_group
                {:lfk :parent_group_id
                 :rfk :subgroup_id})
  (many-to-many transformation_activity :template_group_template
                {:lfk :template_group_id
                 :rfk :template_id}))

;; An app.
(defentity transformation_activity
  (pk :hid)
  (entity-fields :id :name :location :description :type :deleted :wikiurl
                 :integration_date :disabled :edited_date)
  (belongs-to workspace)
  (belongs-to integration_data)
  (many-to-many template_group :template_group_template
                {:lfk :template_id
                 :rfk :template_group_id})
  (many-to-many transformation_steps :transformation_task_steps
                {:lfk :transformation_task_id
                 :rfk :transformation_step_id}))

;; Information about who integrated an app or a deployed component.
(defentity integration_data
  (entity-fields :integrator_name :integrator_email)
  (has-many transformation_activity)
  (has-many deployed_components))

;; Information about a deployed tool.
(defentity deployed_components
  (pk :hid)
  (entity-fields :id :name :location :type :description :version :attribution)
  (belongs-to integration_data)
  (has-many deployed_component_data_files {:fk :deployed_component_id}))

;; Test data files for use with deployed components.
(defentity deployed_component_data_files
  (entity-fields :filename :input_file)
  (belongs-to deployed_components {:fk :deployed_component_id}))

;; A list of steps within an app.
(defentity transformation_steps
  (entity-fields :name :guid :description))
