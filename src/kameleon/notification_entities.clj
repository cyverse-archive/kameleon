(ns kameleon.notification-entities
  (:use [kameleon.core]
        [korma.core]))

(declare notifications analysis_execution_statuses email_notification_messages)

;; The notifications themselves.
(defentity notifications
  (entity-fields :uuid :type :username :subject :seen :deleted :date_created :message)
  (has-many email_notification_messages {:fk :notification_id}))

;; The most recent status seen by the notification agent for every job that it's seen.
(defentity analysis_execution_statuses
  (entity-fields :uuid :status :date_modified))

;; Records of email messages sent in response to notifications.
(defentity email_notification_messages
  (entity-fields :template :address :date_sent :payload)
  (belongs-to notifications {:fk :notification_id}))
