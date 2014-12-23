(ns
  ^{:author Tanmoy}
  synth-db.util.datomic-util
  (:require [datomic.api :as d])
  )

(defn create-connection
  [connection-url]
  (d/create-database connection-url)
  (let [uri connection-url
        conn (d/connect uri)
        dbcon (d/db conn)]
    {:connection conn
     :db dbcon})
  )


(defn get-attr-map [attr-map]
  (merge {:db/id (d/tempid ":db.part/db")
          :db/valueType :db.type/string
          :db/cardinality :db.cardinality/one
          :db.install/_attribute :db.part/db}
    attr-map
    )
  )

(defn get-insert-value-map [attr-map tempid]
  (merge {:db/id tempid}
    attr-map
    )
  )

(defn get-temp-id []
  (d/tempid ":db.part/user"))


(defn d-transact [con attr-map-list]
  (d/transact con attr-map-list)
  )

(defn executeQuery [query db]
  (d/q query
    db
    ))
