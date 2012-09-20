(ns kameleon.esp-queries
  (:use kameleon.core
        kameleon.utils
        kameleon.esp-entities
        korma.core
        korma.db)
  (:require [clojure.data.json :as json]))

(defn merge-field
  "Utility function that looks in lookup-map for lookup-key and merges that
   key/value pair into merged-map if it exists in lookup-map. Used to
   generate the Korma where clauses."
  ([merged-map lookup-map lookup-key]
     (merge-field merged-map lookup-map lookup-key lookup-key))
  
  ([merged-map lookup-map lookup-key merge-as-key]
     (if (contains? lookup-map lookup-key)
       (assoc merged-map merge-as-key (get lookup-map lookup-key))
       merged-map)))

(defn xform-result
  "Utility function. It takes in a result map from a query, a key for a field in
   the result map, and a function that should be applied to the value
   associated with the key in the result map. This is used in other locations
   to turn java.util.UUID instances into strings, for instance."
  [res-map key new-val-func]
  (if (contains? res-map key)
    (assoc res-map key (new-val-func (get res-map key)))
    res-map))

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

(defn xform-es-map
  "Cleans up a result map for an event_source query by turning the :source_uuid,
   :date_modified, and :date_created fields into strings."
  [es-map]
  (-> es-map
      (xform-result :source_uuid #(str %1))
      (xform-result :date_modified #(str %1))
      (xform-result :date_created #(str %1))
      (xform-result :source_data #(json/read-json %1))))

(defn clean-es-map
  "Removes the :id field from the es-map."
  [es-map]
  (-> es-map
      (dissoc :id)))

(defn insert-event-source
  "Inserts a new event source into the database. src-uuid is the uuid for the
   new event source. tag is the string tag used to look up one or more event
   sources. data is a string representing the event source."
  [src-uuid tag data]
  (-> (transaction
       (insert event-sources
               (values {:tag tag
                        :source_data data
                        :source_uuid (str->pg-uuid src-uuid)})))
      xform-es-map
      clean-es-map))

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
   on them before passing them in.

   Note: This will never return the :id field. If you need the :id field, use
   Korma's normal (select)."
  [es-fields & {:as opt-fields}]
  (map
   #(-> %1 xform-es-map clean-es-map)
   (-> (select* event-sources)
       (query-fields es-fields)
       (event-source-where (convert-uuids opt-fields))
       (select))))

(defn event-source-id
  "Helper method that queries the database for the id associated with the
   given event source uuid."
  [src-uuid]
  (-> (select event-sources
              (fields :id)
              (where {:source_uuid [= (str->pg-uuid src-uuid)]}))
      first
      :id))

(defn events-where
  "Generates the where clause for a Korma query on the events table. Takes
   in the query object and a map containing the optional fields to restrict the
   query on. Similar to the (event-sources-where) function."
  [query opt-fields]
  (if (zero? (count (keys opt-fields)))
    query
    (where query
           (-> {}
               (merge-field opt-fields :event_uuid)
               (merge-field opt-fields :source_uuid :event_sources.source_uuid)
               (merge-field opt-fields :event_data)
               (merge-field opt-fields :event_type)))))

(defn xform-event-map
  "Takes in a result map for an event query and cleans it up. It does this by
   (dissoc)ing any fields that are database specific (namely :event_sources_id
   and :id) and by turning the :event_uuid, :source_uuid, :date_created, and
   :date_modified fields into strings."
  [ev-map]
  (println ev-map)
  (-> ev-map
      (xform-result :event_uuid #(str %1))
      (xform-result :source_uuid #(str %1))
      (xform-result :date_modified #(str %1))
      (xform-result :date_created #(str %1))
      (xform-result :event_data #(json/read-json %1))))

(defn clean-event-map
  "Removes the :event_sources_id and :id fields from ev-map."
  [ev-map]
  (-> ev-map
      (dissoc :event_sources_id :id)))

(defn insert-event
  "Inserts an event into the database. ev-uuid is a string containing the uuid
   to associate with the event. scr-uuid is a string containing the uuid of the
   event_source associate with this event. event_data is a string containing
   arbitrary event data."
  [ev-uuid src-uuid ev-type data]
  (-> (transaction
       (insert events (values {:event_uuid (str->pg-uuid ev-uuid)
                               :event_sources_id (event-source-id src-uuid)
                               :event_data data
                               :event_type ev-type})))
      xform-event-map
      clean-event-map))

(defn xform-ef
  "Takes in a field name that normally gets passed to the (fields) part of a
   Korma query and translates it to :event_sources.source_uuid if it's
   currently set to :source_uuid. Otherwise it gets left alone."
  [ef]
  (cond
   (vector? ef)
   (if (= (first ef) :source_uuid)
     (apply (partial vector :event_sources.source_uuid) (rest ef))
     ef)

   (= ef :source_uuid)
   :event_sources.source_uuid

   :else
   ef))

(defn xform-event-fields
  "Maps xform-ef to every value of event-fields."
  [event-fields]
  (map xform-ef event-fields))

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
   on them before passing them in.

   Note: This function will never return the :id or :event_sources_id fields.
   If you need either of those fields, use Korma's regular (select)."
  [event-fields & {:as opt-fields}]
  (into
   []
   (map
    #(-> %1 xform-event-map clean-event-map)
    (-> (select* events)
        (with event-sources (fields :source_uuid))
        (query-fields (xform-event-fields event-fields))
        (events-where (convert-uuids opt-fields))
        (select)))))

(defn clean-part-map
  [part-map]
  (-> part-map (dissoc :id)))

(defn xform-part-map
  [part-map]
  (-> part-map
      (xform-result :date_modified #(str %1))
      (xform-result :date_created #(str %1))
      (xform-result :last_run_time #(str %1))))

(defn parts-where
  "Generates the where clause for a Korma query on the events table. Takes
   in the query object and a map containing the optional fields to restrict the
   query on. Similar to the (event-sources-where) function."
  [query opt-fields]
  (if (zero? (count (keys opt-fields)))
    query
    (where query
           (-> {}
               (merge-field opt-fields :last_run_time)
               (merge-field opt-fields :part_key)))))

(defn insert-partitioner
  [part-key last-run-time]
  (->
   (insert partitioners
           (values {:part_key part-key
                    :last_run_time (str->timestamp last-run-time)}))
   xform-part-map
   clean-part-map))

(defn query-partitioners
  [part-fields & {:as opt-fields}]
  (into
   []
   (map
    #(-> %1 xform-part-map clean-part-map)
    (-> (select* partitioners)
        (query-fields part-fields)
        (parts-where opt-fields)
        (select)))))

(defn xform-update-map
  [up-map]
  (-> up-map (xform-result :last_run_time str->timestamp)))

(defn update-partitioners
  [update-fields & {:as opt-fields}]
  (into
   []
   (map
    #(-> %1 xform-part-map clean-part-map)
    (flatten [(-> (update* partitioners)
                  (set-fields (xform-update-map update-fields))
                  (parts-where opt-fields)
                  (select))]))))
