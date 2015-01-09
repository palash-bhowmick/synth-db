(ns
  ^{:author Tanmoy}
  synth-db.util.datomic-util
  (:require [datomic.api :as d]
            [synth-db.util.enlibra-util :as enlibra])
  )

(def connection-url nil)
(def conn nil)
(def db nil)

(defn create-connection
  [connection-url]
  (d/create-database connection-url)
  (let [uri connection-url
        conn (d/connect uri)
        dbcon (d/db conn)]
    {:connection conn
     :db dbcon})
  )

(defn init
  [& datomic-uri]
  (if (= connection-url nil)
    (if (= datomic-uri nil) (throw (IllegalArgumentException. "Specify DATOMIC URL. ")) (def connection-url (first datomic-uri))))
  (if (= conn nil)
    (let [create-conn (create-connection connection-url)]
      (def conn (:connection create-conn))
      (def db (:db create-conn))
      ))
  )

(defn get-conn []
  (init)
  conn)

(defn get-db []
  (init)
  db)

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

(defn get-datomic-map [value table-name tempid]
  (if (first (rest value))
    (get-insert-value-map
      {(str ":table." (enlibra/get-formatted-name table-name) "/" (enlibra/get-formatted-name (name (first value)))) (first (rest value))}
      tempid
      )
    {}
    )
  )

(defn get-temp-id []
  (d/tempid ":db.part/user"))


(defn d-transact [attr-map-list]
  (d/transact (get-conn) attr-map-list)
  )

(defn executeQuery [query]
  (d/q query
    (get-db)
    ))
