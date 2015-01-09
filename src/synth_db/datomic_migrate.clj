(ns synth-db.datomic-migrate
  (:require [synth-db.util.datomic-util :as dutil]
            [synth-db.util.db-util :as db-util])
  )

;;; DATOMIC RELATED
(defn get-row [row-value table-name-value]
  (let [temp-id (dutil/get-temp-id)
        table-name-value-lc (.toLowerCase table-name-value)
        data-row-value (into {} (map #(dutil/get-datomic-map %1 table-name-value-lc temp-id) row-value))]
    data-row-value)
  )

(defn add-to-datomic [columns table-data table-name]
  ;; create attributes
  (println (str "Adding " (count columns) " Attributes.."))
  (dutil/d-transact columns)
  (let [data-map (into [] (map #(get-row %1 table-name) table-data))]
    (println (str "Adding " (count data-map) " rows.."))
    (dutil/d-transact data-map)
    )
  )

;; Derby Related Code
(defn replicate-table-datomic [db-info table-name]
  (println (str "Migrating Table " table-name "..."))
  (let [db-info (merge {:table-name table-name} db-info)
        cols (db-util/get-columns db-info)
        cols (into [] (map #(dutil/get-attr-map %1) cols))
        table-data (db-util/get-data db-info)]
    (add-to-datomic cols table-data table-name)
    )
  )

(defn replicate-schema [db-info]
  ;;get table names
  (println (str "db info " db-info))
  (let [table-names (into [] (db-util/get-tables db-info))]
    (println (str "table names " table-names))
    (doall (map #(replicate-table-datomic db-info %1) table-names))
    )
  )

(defn init
  [datomic-uri]
  (dutil/init datomic-uri))
