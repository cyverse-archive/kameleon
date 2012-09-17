(ns kameleon.esp-queries
  (:use kameleon.core
        kameleon.esp-entities
        kameleon.utils
        korma.core))

(defn merge-field
  "Utility function that looks in lookup-map for lookup-key and merges that
   key/value pair into merged-map if it exists in lookup-map. Used to
   generate the Korma where clauses."
  [merged-map lookup-map lookup-key]
  (if (contains? lookup-map lookup-key)
    (assoc merged-map lookup-key (get lookup-map lookup-key))
    merged-map))

(defn convert-map
  "Utility function that converts the value associated with cv-key in
   opt-fields into a PostgreSQL compatible UUID."
  [opt-fields cv-key]
  (if (contains? opt-fields cv-key)
    (assoc opt-fields cv-key (str->pg-uuid (get opt-fields cv-key)))
    opt-fields))

(defn convert-uuids
  "Utility function. Looks calls (convert-map) for the :source_uuid and
   :event_uuid keys."
  [opt-fields]
  (-> opt-fields
      (convert-map :source_uuid)
      (convert-map :event_uuid)))

(defn event-source-where
  "Generates the (where) clause in a Korma query. Calls (merge-field) for
   :tag :source_uuid and :source_data. This is where those fields in the
   where map get added if they're needed."
  [query opt-fields]
  (if (zero? (count (keys opt-fields)))
    query
    (where query (-> {}
                     (merge-field opt-fields :tag)
                     (merge-field opt-fields :source_uuid)
                     (merge-field opt-fields :source_data)))))

(defn query-fields
  "Generates the (fields) part of a Korma query. seq-fields should be formatted
   like a normal (fields) seqence of fields."
  [query seq-fields]
  (if (zero? (count seq-fields))
    query
    (apply (partial fields query) seq-fields)))

(defn insert-event-source
  "Inserts a new event source into the database. src-uuid is the uuid for the
   new event source. tag is the string tag used to look up one or more event
   sources. data is a string representing the event source."
  [src-uuid tag data]
  (insert event-sources
          (values {:tag tag
                   :source_data data
                   :source_uuid (str->pg-uuid src-uuid)})))

(defn query-event-sources
  "Allows you to query the event_sources table in the database. Here are some
   example calls:

       (query-event-sources [:id] :tag \"test-tag\")
       (query-event-sources [:id :source_uuid] :tag \"test-tag\")
       (query-event-sources [:id :source_data] :tag \"test-tag\")

   The first param is a vector of fields to return. The rest of the
   parameters are key-value pairs that form the where clause in the query.
   Though it's not shown in the examples above, you can include multiple
   key-value pairs for the where clause. Also, you can alias the fields that are
   returned just like you can with Korma queries.

   String uuids are converted to PostgreSQL UUIDs. Do not call (str->pg-uuid)
   on them before passing them in."
  [es-fields & {:as opt-fields}]
  (-> (select* event-sources)
      (query-fields es-fields)
      (event-source-where (convert-uuids opt-fields))
      (select)))

(defn event-source-id
  "Helper method that queries the database for the id associated with the
   given event source uuid."
  [src-uuid]
  (-> (query-event-sources [:id] :source_uuid src-uuid)
      first
      :id))

(defn events-where
  "Generates the where clause for a Korma query on the events table. Takes
   in the query object and a map containing the optional fields to restrict the
   query on. Similar to the (event-sources-where) function."
  [query opt-fields]
  (if (zero? (count (keys opt-fields)))
    query
    (where query (-> {}
                     (merge-field opt-fields :event_uuid)
                     (merge-field opt-fields :event_sources_id)
                     (merge-field opt-fields :event_data)))))

(defn insert-event
  "Inserts an event into the database. ev-uuid is a string containing the uuid
   to associate with the event. scr-uuid is a string containing the uuid of the
   event_source associate with this event. event_data is a string containing
   arbitrary event data."
  [ev-uuid src-uuid data]
  (insert events (values {:event_uuid (str->pg-uuid ev-uuid)
                          :event_sources_id (event-source-id src-uuid)
                          :event_data data})))

(defn query-events
  "Allows you to query the events table in the database. Here are some example
   calls:

       (query-events [:id] :event_uuid \"fakeuuid\")
       (query-events [:id :event_uuid] :event_data \"fake_event_data\")

   The first param is a vector of the fields that the result rows should
   contain. The rest of the parameters are key-value pairs forming the where
   clause in the query. The fields can be aliased just like normal Korma queries
   and multiple key-value pairs can be included to form the where clause.

   String uuids are converted to PostgreSQL UUIDS. Do not call (str->pg-uuid)
   on them before passing them in."
  [event-fields & {:as opt-fields}]
  (-> (select* events)
      (query-fields event-fields)
      (events-where (convert-uuids opt-fields))
      (select)))
