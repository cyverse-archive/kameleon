(ns kameleon.entities
  (:use [kameleon.core]
        [korma.core]))

(declare users workspace template_group transformation_activity
         integration_data deployed_components deployed_component_data_files
         transformation_steps transformations output_mapping input_mapping
         template inputs outputs info_type data_formats multiplicity
         property_group property property_type)

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

;; Steps within an app.
(defentity transformation_steps
  (entity-fields :name :guid :description)
  (belongs-to transformations {:fk :transformation_id})
  (has-many output_mapping {:fk :source})
  (has-many input_mapping {:fk :target}))

;; Transformations applied to steps within an app.
(defentity transformations
  (entity-fields :name :description :template_id))

;; A table that maps outputs from one step to inputs to another set.  Two
;; entities are associated with a single table here for convenience.  when I
;; have more time, I'd like to try to improve the relation handling in Korma
;; so that multiple relationships with the same table work correctly.
(defentity output_mapping
  (pk :hid)
  (table :input_output_mapping :output_mapping))
(defentity input_mapping
  (pk :hid)
  (table :input_output_mapping :input_mapping))

;; Data object mappings can't be implemeted as entities until Korma supports
;; composite primary keys.  In the meantime, we'll have to deal with this table
;; in code.

;; A template defines an interface to a tool that can be called.
(defentity template
  (pk :hid)
  (entity-fields :id :name :description :label :type :component_id)
  (many-to-many inputs :template_input
                {:lfk :template_id
                 :rfk :input_id})
  (many-to-many outputs :template_output
                {:lfk :template_id
                 :rfk :output_id})
  (many-to-many property_group :template_property_group
                {:lfk :template_id
                 :rfk :property_group_id}))

;; Input and output definitions.  Once again, two entities are associated with
;; the same table to allow us to define multiple relationships between the same
;; two tables.
(defentity inputs
  (pk :hid)
  (table :dataobjects :inputs)
  (entity-fields :id :name :label :orderd :switch :description :required
                 :retain :is_implicit)
  (belongs-to info_type {:fk :info_type})
  (belongs-to data_formats {:fk :data_format})
  (belongs-to multiplicity {:fk :multiplicity}))
(defentity outputs
  (pk :hid)
  (table :dataobjects :outputs)
  (entity-fields :id :name :label :orderd :switch :description :required
                 :retain :is_implicit)
  (belongs-to info_type {:fk :info_type})
  (belongs-to data_formats {:fk :data_format})
  (belongs-to multiplicity {:fk :multiplicity}))
(defentity data_object
  (pk :hid)
  (table :dataobjects :data_object)
  (entity-fields :id :name :label :orderd :switch :description :required
                 :retain :is_implicit)
  (belongs-to info_type {:fk :info_type})
  (belongs-to data_formats {:fk :data_format})
  (belongs-to multiplicity {:fk :multiplicity}))

;; The type of information stored in a data object.
(defentity info_type
  (pk :hid)
  (entity-fields :id :name :label :description :deprecated :display_order))

;; The format of the data in a data object.
(defentity data_formats
  (entity-fields :guid :name :label :display_order))

;; An input or output multiplicity definition.
(defentity multiplicity
  (pk :hid)
  (entity-fields :id :name :label :description :type_name))

;; A group of properties.
(defentity property_group
  (pk :hid)
  (entity-fields :id :name :description :label :group_type :is_visible)
  (many-to-many property :property_group_property
                {:lfk :property_group_id
                 :rfk :property_id}))

;; A single property.
(defentity property
  (pk :hid)
  (entity-fields :id :name :description :label :defalut_value :is_visible
                 :ordering :omit_if_blank)
  (belongs-to data_object {:fk :dataobject_id})
  (belongs-to property_type {:fk :property_type}))

;; The type of a single property.
(defentity property_type
  (pk :hid)
  (entity-fields :id :name :description :label :deprecated :display_order))
