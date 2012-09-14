(ns kameleon.utils
  (use korma.core))

(defn str->pg-uuid
  [uuid-str]
  (raw (format "CAST('%s' AS uuid)" (name uuid-str))))