(ns kameleon.core
  (:use [clojure.pprint :only [pprint]]
        [slingshot.slingshot :only [throw+]])
  (:require [korma.config :as conf]
            [korma.sql.engine :as eng]
            [korma.sql.fns :as sfns]
            [korma.sql.utils :as utils]
            [clojure.set :as set]
            [clojure.string :as string]
            [korma.db :as db]
            [korma.core :as kc])
  (:import [java.io InputStreamReader PushbackReader]
           [java.net URL]
           [java.util.jar Manifest]))

(def ^:private project-clj-location
  "The location of the project.clj file within the JAR file."
  "/META-INF/leiningen/org.iplantc/kameleon/project.clj")

(defn many-to-many-keys
  "Determines and returns the keys needed for a many-to-many relationship."
  [parent child {:keys [join-table lfk rfk]}]
  {:lpk (kc/raw (eng/prefix parent (:pk parent)))
   :lfk (kc/raw (eng/prefix {:table (name join-table)} lfk))
   :rfk (kc/raw (eng/prefix {:table (name join-table)} rfk))
   :rpk (kc/raw (eng/prefix child (:pk child)))
   :join-table join-table})

(defn db-keys-and-foreign-ent
  "Determines and returns the database keys and foreign entity for a
   relationship."
  [type ent sub-ent opts]
  (condp = type
    :many-to-many [(many-to-many-keys ent sub-ent opts) sub-ent]))

(defn kameleon-create-rel
  "Creates a relationship.  This is just like korma.core/create-rel
   except that it supports many-to-many relationships."
  [ent sub-ent type opts]
  (let [[db-keys foreign-ent] (db-keys-and-foreign-ent type ent sub-ent opts)
        opts (when (:fk opts)
               {:fk (kc/raw (eng/prefix foreign-ent (:fk opts)))})]
    (merge {:table (:table sub-ent)
            :alias (:alias sub-ent)
            :rel-type type}
           db-keys
           opts)))

(defn kameleon-rel
  "Creates a relationship and associates it with an entity.  This is just like
   korma.core/rel except that it supports many-to-many relationships."
  [ent sub-ent type opts]
  (let [var-name (-> sub-ent meta :name)
        cur-ns *ns*]
    (assoc-in ent [:rel (name var-name)]
              (delay
               (let [resolved (ns-resolve cur-ns var-name)
                     sub-ent (when resolved (deref sub-ent))]
                 (when-not (map? sub-ent)
                   (throw+ {:type ::undefined-entity
                            :entity (name var-name)}))
                 (kameleon-create-rel ent sub-ent type opts))))))

(defn default-fk-name
  "Determines the default name to use for a foreign key."
  [ent]
  (if (string? ent)
    (keyword (str ent "_id"))
    (keyword (str (:table ent) "_id"))))

(defmacro many-to-many
  "Add a many-to-many relation for the given entity.  It is assumed that a join
   table is used to implement the relationship and that the foreign keys are in
   the join table."
  [ent sub-ent join-table & [opts]]
  `(kameleon-rel ~ent (var ~sub-ent) :many-to-many
                 (assoc ~opts
                   :join-table ~join-table
                   :lfk (:lfk ~opts (default-fk-name ~ent))
                   :rfk (:rfk ~opts (default-fk-name ~sub-ent)))))

(defn add-joins
  "Adds join clauses to a query for a relationship.  If the relationship uses a
   join table then two clauses will be added.  Otherwise, only one clause will
   be added."
  [query ent rel]
  (if-let [join-table (:join-table rel)]
    (-> query
        (kc/join* :left join-table (sfns/pred-= (:lpk rel) (:lfk rel)))
        (kc/join* :left ent (sfns/pred-= (:rfk rel) (:rpk rel))))
    (kc/join* query :left ent (sfns/pred-= (:pk rel) (:fk rel)))))

(defmacro kameleon-join
  "Adds join clauses to a query for a relationship.  This is identical to
   korma.core/join except that it support many-to-many relationships."
  ([query ent]
     `(let [q# ~query
            e# ~ent
            rel# (kc/get-rel (:ent q#) e#)]
        (add-joins q# e# rel#)))
  ([query table clause]
     `(kc/join* ~query :left ~table (eng/pred-map ~(eng/parse-where clause))))
  ([query type table clause]
     `(kc/join* ~query ~type ~table (eng/pred-map ~(eng/parse-where clause)))))

(defn- force-prefix
  "Copied from korma.core because it's private."
  [ent fields]
  (for [field fields]
    (if (vector? field)
      [(utils/generated (eng/prefix ent (first field))) (second field)]
      (eng/prefix ent field))))

(defn- add-aliases
  "Copied from korma.core because it's private."
  [query as]
  (update-in query [:aliases] set/union as))

(defn- merge-query
  "Copied from korma.core because it's private."
  [query neue]
  (let [merged (reduce #(kc/merge-part % neue %2)
                       query
                       [:fields :group :order :where :params :joins :post-queries])]
    (-> merged
        (add-aliases (:aliases neue)))))

(defn- sub-query
  "Copied from korma.core because it's private."
  [query sub-ent func]
  (let [neue (kc/select* sub-ent)
        neue (eng/bind-query neue (func neue))
        neue (-> neue
                 (update-in [:fields] #(force-prefix sub-ent %))
                 (update-in [:order] #(force-prefix sub-ent %))
                 (update-in [:group] #(force-prefix sub-ent %)))]
    (merge-query query neue)))

(defn- with-many-to-many
  "Defines the post-query to be used to obtain entities in a many-to-many
   relationship with entities in the current query."
  [rel query ent func opts]
  (let [{:keys [lfk rfk rpk join-table]} rel
        pk (get-in query [:ent :pk])
        table (keyword (eng/table-alias ent))]
    (kc/post-query
     query (partial
            map
            #(assoc % table
                    (kc/select ent
                            (kc/join :inner join-table (= rfk rpk))
                            (func)
                            (kc/where {lfk (get % pk)})))))))

(defn- with-later-fn
  "Returns a function to be used to obtain entities in a relationship with
   entities in the current query lazily.  This also allows the entities to be
   retrieved as separate objects.  This function is used for has-many
   relationships."
  [table ent func known unknown]
  (partial
   map #(assoc % table
               (kc/select ent (func)
                          (kc/where {unknown (get % known)})))))

(defn- with-one-later-fn
  "Returns a function to be used to obtain entities in a relationship with
   entities in the current query lazily.  This also allows the entities to be
   retrieved as separate objects.  This function is used for has-one and
   belongs-to relationships."
  [table ent func known unknown]
  (partial
   map #(assoc % table
               (first (kc/select ent (func)
                                 (kc/where {unknown (get % known)}))))))

(defn- with-later
  "Defines the post-query to be used to obtain entities in a has-many
   relationship with entities in the current query lazily.  This also allows
   the entities to be retrieved as separate objects."
  [rel query ent func opts]
  (let [fk (:fk rel)
        pk (get-in query [:ent :pk])
        table (keyword (eng/table-alias ent))]
    (kc/post-query query (with-later-fn table ent func pk fk))))

(defn- with-one-later
  "Defines the post-query to be used to obtain entities in has-one or
   belongs-to relationships with entities in the current query lazily.  This
   also allows the entities to be retrieved as separate objects."
  [rel query ent func opts]
  (let [fk (:fk rel)
        pk (get-in query [:ent :pk])
        table (keyword (eng/table-alias ent))]
    (kc/post-query query (with-one-later-fn table ent func pk fk))))

(defn- with-now
  "Defines the join to be used to obtain entries in has-one or belongs-to
   relationships with entities in the current query eagerly.  In this case,
   columns in the joined tables are included in the primary entity object."
  [rel query ent func opts]
  (let [table (if (:alias rel)
                [(:table ent) (:alias ent)]
                (:table ent))
        query (kc/join query table (= (:pk rel) (:fk rel)))]
    (sub-query query ent func)))

(defn- with-has-one
  "Defines the post-query or join to be used to obtain entities in a has-one
   relationship with entities in the current query.  If the :later option is
   enabled then the entities will be retrieved lazily and will be returned as
   separate objects.  Otherwise, the entities will be retrieved eagerly and the
   columns in the entities will be included in the objects associated with the
   entities in the query."
  [rel query ent func opts]
  (if (:later opts)
    (with-one-later rel query ent func opts)
    (with-now rel query ent func opts)))

;; FIXME: this will only work with the default naming strategy.
(defn- extract-field-keyword
  "Extracts the field keyword from a generated field name.  This method is
   broken in that it will only work with the default naming strategy.
   Fortunately, we're using the default naming strategy."
  [field]
  (let [{:keys [delimiters]} (or eng/*bound-options* @conf/options)
        [begin end] delimiters
        quoted-name (last (string/split (get field :korma.sql.utils/generated) #"[.]"))
        regex (re-pattern (str "^" begin "|" end "$"))]
    (keyword (string/replace quoted-name regex ""))))

(defn- with-belongs-to-later
  "Defines the post-query to be used to obtain entities in a belongs-to
   relationship with entities in the current query.  This also allowd the
   entities to be retrieved as separate objects."
  [rel query ent func opts]
  (let [fk (extract-field-keyword (:fk rel))
        pk (:pk rel)
        table (keyword (eng/table-alias ent))]
    (kc/post-query query (with-one-later-fn table ent func fk pk))))

(defn- with-belongs-to
  "Defines the post-query or join to be used to obtain entities in a belongs-to
   relationship with entities in the current query.  If the :later option is
   enabled then the entities will be retrieved lazily and will be returned as
   separate objects.  Otherwise, the entities will be retrieved eagerly and the
   columns in the entities will be included in the objects associated with the
   entities in the query."
  [rel query ent func opts]
  (if (:later opts)
    (with-belongs-to-later rel query ent func opts)
    (with-now rel query ent func opts)))

(def ^:private with-handlers
  {:has-many with-one-later
   :many-to-many with-many-to-many
   :has-one with-has-one
   :belongs-to with-belongs-to})

(defn kameleon-with*
  "Allows related entities to be fetched along with the query results.  This
   function is identical to korma.core/with* except that it also supports
   many-to-many relationships and lazy loading of objects in has-one and
   belongs-to relationships."
  [query sub-ent func opts]
  (let [rel (kc/get-rel (:ent query) sub-ent)
        handler-fn (get with-handlers (:rel-type rel))]
    (when (nil? handler-fn)
      (throw+ {:type ::no-relationship :table (:table sub-ent)}))
    (handler-fn rel query sub-ent func opts)))

(defmacro kameleon-with
  "Allows related entities to be fetched along with the query results.  This
   macro is identical to korma.core/with except that it also supports
   many-to-many relationships."
  [query ent & body]
  `(kameleon-with* ~query ~ent (fn [q#]
                                 (-> q#
                                     ~@body)) {}))

(defmacro with-object
  "Allows related entities to be fetched along with the query results.  This
   macro is identical to korma.core/with except that it also supports lazy
   loading of entities in has-one and belongs-to relationships."
  [query ent & body]
  `(kameleon-with* ~query ~ent (fn [q#]
                                 (-> q#
                                     ~@body)) {:later true}))

(defn- get-location
  "Gets the location of the source file containing this source file."
  []
  (.. (class get-location)
      getProtectionDomain
      getCodeSource
      getLocation))

(defn- read-project-clj
  "Reads the project.clj file from the JAR file containing this source file."
  []
  (let [location (get-location)]
    (when-not (nil? location)
      (-> (str "jar:" location "!" project-clj-location)
          (URL.)
          (.openStream)
          (InputStreamReader.)
          (PushbackReader.)
          (read)))))

(defn compatible-db-version
  "Retrieves the database version that the current version of Kameleon is
   compatible with.  The database version is stored in this project's
   MANIFEST.MF file."
  []
  (let [attrs (read-project-clj)]
    (get-in (into {} (map vec (partition 2 (drop 3 attrs))))
            [:manifest "db-version"])))

;; This was a failed attempt to import vars from korma.core into this namespace
;; so that they're also exportable from this namespace to other namespaces.  It
;; partially works, but I ran into problems with dynamic vars not being declared
;; dynamic, and I was afraid we might run into some weird duplication bugs.
;; I'm going to leave this out for the time being, and only add it back in if
;; it appears to be necessary to make client code useable.

;; (def ^{:private true} current-ns-vars
;;   (hash-set (keys (ns-map *ns*))))

;; (defn- not-in-current-ns
;;   [name]
;;   (not (current-ns-vars name)))

;; (defn- alias-ns-publics
;;   [ns-sym]
;;   (dorun
;;    (map
;;     (fn [[name sym]] (eval (list 'def name sym)))
;;     (filter not-in-current-ns (ns-publics ns-sym)))))

;; (alias-ns-publics 'korma.core)
