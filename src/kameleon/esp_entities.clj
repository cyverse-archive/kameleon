(ns kameleon.esp-entities
  (:use kameleon.core
        korma.core))

(declare event_sources events)

(defentity event-sources
  (table :event_sources)
  (entity-fields [:source_uuid :uuid]
                 [:unique_tag :tag]))

(defentity events
  (entity-fields [:event_uuid :uuid]
                 [:event_sources_id :source-id]
                 [:event_data :data])
  (belongs-to event-sources))

(defentity partitioners
  (entity-fields :part_key :last_run_time))