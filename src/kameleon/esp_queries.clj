(ns kameleon.esp-queries
  (:use kameleon.core
        kameleon.esp-entities
        kameleon.utils
        korma.core))

(defn dispatcher [action & args] action)

(defmulti act-on-event-sources dispatcher :default nil)

(defn merge-field
  [merged-map lookup-map lookup-name]
  (if (contains? lookup-map lookup-name)
    (assoc merged-map lookup-name (get lookup-map lookup-name))
    merged-map))

(defn convert-map
  [opt-fields cv-key]
  (if (contains? opt-fields cv-key)
    (assoc opt-fields cv-key (str->pg-uuid (get opt-fields cv-key)))
    opt-fields))

(defn convert-uuids
  [opt-fields]
  (-> opt-fields
      (convert-map :source_uuid)
      (convert-map :event_uuid)))

(defn event-source-where
  [query opt-fields]
  (if (zero? (count (keys opt-fields)))
    query
    (where query (-> {}
                     (merge-field opt-fields :tag)
                     (merge-field opt-fields :source_uuid)
                     (merge-field opt-fields :source_data)))))

(defn query-fields
  [query seq-fields]
  (if (zero? (count seq-fields))
    query
    (apply (partial fields query) seq-fields)))

(defn insert-event-source
  [src-uuid tag data]
  (insert event-sources
          (values {:tag tag
                   :source_data data
                   :source_uuid (str->pg-uuid src-uuid)})))

(defn query-event-sources
  [es-fields & {:as opt-fields}]
  (-> (select* event-sources)
      (query-fields es-fields)
      (event-source-where (convert-uuids opt-fields))
      (select)))

(defn event-source-id
  [src-uuid]
  (-> (query-event-sources [:id] :source_uuid src-uuid)
      first
      :id))

(defn events-where
  [query opt-fields]
  (if (zero? (count (keys opt-fields)))
    query
    (where query (-> {}
                     (merge-field opt-fields :event_uuid)
                     (merge-field opt-fields :event_sources_id)
                     (merge-field opt-fields :event_data)))))

(defn insert-event
  [ev-uuid src-uuid data]
  (insert events (values {:event_uuid (str->pg-uuid ev-uuid)
                          :event_sources_id (event-source-id src-uuid)
                          :event_data data})))

(defn query-events
  [event-fields & {:as opt-fields}]
  (-> (select* events)
      (query-fields event-fields)
      (events-where (convert-uuids opt-fields))
      (select)))
