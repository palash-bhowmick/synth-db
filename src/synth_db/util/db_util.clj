(ns
  ^{:author Tanmoy}
  syth-db.util.db-util
  (:require [clojure.java.jdbc :as jdbc])
  )

(def driver-classes {"derby" "org.apache.derby.jdbc.ClientDriver"})

(def data-type {"VARCHAR" :db.type/string
                "NUMERIC" :db.type/bigdec})

(defn get-table-meta-data
  [db-spec catalog schema-name table-name]
  (let [conn (jdbc/get-connection db-spec)
        rs (.getColumns (.getMetaData conn) catalog schema-name table-name "%")]
    rs))

(defn get-column-vector
  [table-meta-data table-name-lc]
  (def mylist [])
  (while (.next table-meta-data)
    (def mylist (conj mylist
                  {:db/valueType (get-in data-type [(.getString table-meta-data 6)])
                   :db/ident (str ":" table-name-lc "/" (.toLowerCase (.getString table-meta-data 4)))
                   }
                  )
      )
    )
  mylist
  )
(defn get-columns
  [{:keys [host port db dbtype catalog schema-name table-name ssl?]
    :or {host "localhost", port 1527, db "", dbtype "", catalog "", schema-name "", table-name "", ssl? true} ;todo: ssl
    :as opts}]

  (if (== (.length table-name) 0)
    (throw (IllegalArgumentException. "Table Name Not Specified ....."))
    )

  (let [db-spec (merge {:classname (get-in driver-classes [dbtype])
                        :subprotocol dbtype
                        :subname (str "//" host ":" port "/" db ";ssl=basic")
                        :ssl "basic"
                        :make-pool? true
                        } opts)

        table-meta-data (get-table-meta-data db-spec catalog schema-name table-name)
        table-name-lc (.toLowerCase table-name)
        column-list (get-column-vector table-meta-data table-name-lc)
        ]
    column-list
    )
  )

(defn get-data
  [{:keys [host port db dbtype catalog schema-name table-name ssl?]
    :or {host "localhost", port 1527, db "", dbtype "", catalog "", schema-name "", table-name "", ssl? true} ;todo: ssl
    :as opts}]

  (if (== (.length table-name) 0)
    (throw (IllegalArgumentException. "Table Name Not Specified ....."))
    )

  (let [db-spec
        (merge {:classname (get-in driver-classes [dbtype])
                :subprotocol dbtype
                :subname (str "//" host ":" port "/" db ";ssl=basic")
                :ssl "basic"
                :make-pool? true
                } opts)
        query (if (> (.length schema-name) 0)
                (str "SELECT * FROM " schema-name "." table-name) (str "SELECT * FROM " table-name))
        qs (jdbc/query db-spec [query]
             )]
    qs
    )
  )
