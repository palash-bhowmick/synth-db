(ns synth-db.datomic-migrate-test
  (:use synth-db.util.datomic-util
        synth-db.util.db-util)
  )

(def datomic-uri "datomic:free://192.168.33.161:4334/test134")
(def datomic-condata (create-connection datomic-uri))

;;; DATOMIC RELATED
(defn get-row [row-value table-name-value]
  (let [temp-id (get-temp-id)
        table-name-value-lc (.toLowerCase table-name-value)
        data-row-value (into {} (map #(get-datomic-map %1 table-name-value-lc temp-id) row-value))]
    data-row-value)
  )

(defn add-to-datomic [columns table-data table-name]
  ;; create attributes
  (println (str "Adding Attributes " columns))
  (d-transact (:connection datomic-condata) columns)
  (let [data-map (into [] (map #(get-row %1 table-name) table-data))]
    (println (str "datamap - " data-map))
    (d-transact (:connection datomic-condata) data-map)
    )
  )

;; Derby Related Code
(defn replicate-table-datomic [db-info table-name]
  (println (str "Migrating Table " table-name "..."))
  (let [db-info (merge {:table-name table-name} db-info)
        cols (get-columns db-info)
        cols (into [] (map #(get-attr-map %1) cols))
        table-data (get-data db-info)]
    (add-to-datomic cols table-data table-name)
    )
  )

(defn replicate-schema [db-info]
  ;;get table names
  (println (str "db info " db-info))
  (let [table-names (into [] (get-tables db-info))]
    (println (str "table names " table-names))
    (doall (map #(replicate-table-datomic db-info %1) table-names))
    )
  )

(let [db-info {:dbtype :derby
               :host "192.168.33.161"
               :port "1530"
               :db "CRM"
               :user "CRM"
               :password "CRM"
               :schema-name "CRM"
               }]
  (replicate-schema db-info))

(println "Migration Completed")
