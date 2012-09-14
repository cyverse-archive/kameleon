(ns kameleon.esp-queries
  (:use kameleon.core
        kameleon.esp-entities
        korma.core))

(defn dispatcher [action & args] action)

(defmulti act-on-event-sources dispatcher :default nil)

(defn merge-field
  [merged-map lookup-map lookup-name]
  (if (contains? lookup-map lookup-name)
    (assoc merged-map lookup-name (get lookup-map lookup-name))
    merged-map))

(defn event-source-where
  [query opt-fields]
  (if (zero? (count (keys opt-fields)))
    query
    (where query (-> {}
                     (merge-field opt-fields :tag)
                     (merge-field opt-fields :source_uuid)
                     (merge-field opt-fields :source_data)))))

(defn event-source-fields
  [query es-fields]
  (if (zero? (count es-fields))
    query
    (apply (partial fields query) es-fields)))

(defn insert-event-source
  [src-uuid tag data]
  (insert event-sources
          (values {:tag tag :source_data data :source_uuid src-uuid})))

(defn query-event-sources
  [es-fields & {:as opt-fields}]
  (-> (select* event-sources)
      (event-source-fields es-fields)
      (event-source-where opt-fields)
      (select)))

