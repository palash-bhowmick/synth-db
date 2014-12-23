(ns synth-db.datomic-migrate-test
  (:use synth-db.util.datomic-util
        synth-db.util.db-util)
  )

(def table-name-value "SUPPLIER_DETAILS")
(def db-info {:dbtype :derby
              :host "localhost"
              :port "1530"
              :db "CRM"
              :user "CRM"
              :password "CRM"
              :schema-name "CRM"
              :table-name table-name-value})

(def cols (get-columns db-info))

(def cols (into [] (map #(get-attr-map %1) cols)))

(def conn-string "datomic:free://192.168.33.161:4334/test3")

(def con-data (create-connection conn-string))

;; create attributes
(d-transact (:connection con-data) cols)

(def table-data (get-data db-info))

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
(d-transact (:connection con-data) data-map)
