(ns kameleon.utils
  (use korma.core))

(defn str->pg-uuid
  [uuid-str]
  (raw (format "CAST('%s' AS uuid)" (name uuid-str))))

(defn str->timestamp
  ([ts-str]
     (str->timestamp ts-str "YYYY-MM-DD HH24:MI:SS:MS"))
  ([ts-str fmt-str]
     (raw (format "to_timestamp('%s', '%s')" (name ts-str) (name fmt-str)))))
