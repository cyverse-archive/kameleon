(ns kameleon.sql-reader
  (:use [clojure.java.io :only [file reader]]
        [slingshot.slingshot :only [throw+]]))

(def test-file
  "/Users/dennis/src/iplant/ua/de-database-schema/src/main/data/01_data_formats.sql")

(defn char-seq
  [rdr]
  (let [c (.read rdr)]
    (if-not (< c 0)
      (lazy-seq (cons (char c) (char-seq rdr)))
      '())))

(declare c-comment-end-candidate c-comment c-comment-candidate line-comment
         line-comment-candidate single-quoted-string double-quoted-string base)

(defn c-comment-end-candidate
  [res [c & cs] lv]
  (cond (nil? c)                (throw+ {:type ::unterminated-c-comment})
        (and (= c \/) (> lv 1)) #(c-comment res cs (dec lv))
        (= c \/)                #(base res cs)
        :else                   #(c-comment res cs lv)))

(defn c-comment
  [res [c & cs] lv]
  (cond (nil? c) (throw+ {:type ::unterminated-c-comment})
        (= c \/) #(c-comment-candidate res cs (inc lv))
        (= c \*) #(c-comment-end-candidate res cs lv)
        :else    #(c-comment res cs lv)))

(defn c-comment-candidate
  [res [c & cs] lv]
  (cond (nil? c) (conj res \/)
        (= c \*) #(c-comment res cs lv)
        (> lv 1) #(c-comment res cs (dec lv))
        :else    #(base (conj res \/ c) cs)))

(defn line-commment
  [res [c & cs]]
  (cond (nil? c)       res
        (= c \newline) #(base res cs)
        :else          #(line-comment res cs)))

(defn line-comment-candidate
  [res [c & cs]]
  (cond (nil? c) (conj res \-)
        (= c \-) #(line-comment res cs)
        :else    #(base (conj res \- c) cs)))

(defn single-quoted-string
  [res [c & cs]]
  (cond (nil? c) (throw+ {:type ::unterminated-string})
        (= c \') #(base (conj res c) cs)
        :else    #(single-quoted-string (conj res c) cs)))

(defn double-quoted-string
  [res [c & cs]]
  (cond (nil? c) (throw+ {:type ::unterminated-quoted-name})
        (= c \") #(base (conj res c) cs)
        :else    #(double-quoted-string (conj res c) cs)))

(defn base
  [res [c & cs]]
  (cond (nil? c) res
        (= c \;) res
        (= c \/) #(c-comment-candidate res cs 1)
        (= c \-) #(line-comment-candidate res cs)
        (= c \') #(single-quoted-string (conj res c) cs)
        (= c \") #(double-quoted-string (conj res c) cs)
        :else    #(base (conj res c) cs)))

(defn sql-statements
  [filename]
  (with-open [rdr (reader filename)]
    (loop [res [] stmt (trampoline #(base [] (char-seq rdr)))]
      (if (empty? stmt)
        res
        (recur (cons (apply str stmt) res)
               (trampoline #(base [] (char-seq rdr))))))))
