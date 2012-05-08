(ns kameleon.entities
  (:use [kameleon.core]
        [korma.core]))

(declare workspace template_group analyses)

;; Users who have logged into the DE.
(defentity users
  (entity-fields :username))

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
  (many-to-many analyses :template_group_template
                {:lfk :template_group_id
                 :rfk :template_id}))

;; An app.
(defentity analyses
  (pk :hid)
  (table :transformation_activity)
  (entity-fields :id :name :location :description))
