(ns synth-db.datomic-migrate-test
  (:use synth-db.util.datomic-util
        synth-db.util.db-util)
  )

(def table-name-value "CUSTOMER")
;; Derby Related Code
(def db-info {:dbtype :derby
              :host "192.168.33.130"
              :port "1530"
              :db "CRM"
              :user "CRM"
              :password "CRM"
              :schema-name "CRM"
              :table-name table-name-value})

(def cols (get-columns db-info))

;;get columns
(def cols (into [] (map #(get-attr-map %1) cols)))

;;get table names
(def tables (get-tables db-info))

;;get table data
(def table-data (get-data db-info))

;;; DATOMIC RELATED

(def datomic-uri "datomic:free://192.168.33.161:4334/test101")

(def datomic-condata (create-connection datomic-uri))

;; create attributes
(d-transact (:connection datomic-condata) cols)

(defn get-datomic-map [value table-name tempid]
  (if (first (rest value))
    (get-insert-value-map
      {(str ":" table-name "/" (.substring (str (first value)) 1)) (first (rest value))}
      tempid
      )
    {}
    )
  )

(defn get-row [row-value]
  (let [temp-id (get-temp-id)
        table-name-value-lc (.toLowerCase table-name-value)
        data-row-value (into {} (map #(get-datomic-map %1 table-name-value-lc temp-id) row-value))]
    data-row-value)
  )

(def data-map (into [] (map #(get-row %1) table-data)))
(println (str "Data-Map" data-map))

(println "transacting datamap...")
(d-transact (:connection datomic-condata) data-map)

(println "Migration Completed")
