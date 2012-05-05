(ns kameleon.entities
  (:use [korma.core]))

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
  (has-many template_group_template))

;; TODO: figure out how to do a many-to-many relationship with a join table.
