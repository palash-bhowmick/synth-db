(ns
  ^{:author Tanmoy}
  synth-db.util.db-util
  (:require [clojure.java.jdbc :as jdbc])
  )

(def driver-classes {:derby "org.apache.derby.jdbc.ClientDriver"})

(def data-type {"VARCHAR" :db.type/string
                "NUMERIC" :db.type/bigdec
                "VARCHAR2" :db.type/string
                "NUMBER" :db.type/bigdec
                "DECIMAL" :db.type/bigdec
                "INTEGER" :db.type/bigdec})

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
                   :db/ident (str ":table." table-name-lc "/" (.replaceAll (.toLowerCase (.getString table-meta-data 4)) " " "_"))
                   }
                  )
      )
    )
  mylist
  )
(defn get-db-spec [{:keys [host port db dbtype catalog schema-name table-name ssl?]
                    :or {host "localhost", port 1527, db "", dbtype "", catalog "", schema-name "", table-name "", ssl? true} ;todo: ssl
                    :as opts}]
  (merge {:classname ((:dbtype opts) driver-classes)
          :subprotocol (name (:dbtype opts))
          :subname (str "//" (:host opts) ":" (:port opts) "/" (:db opts) ";ssl=basic")
          :ssl "basic"
          :make-pool? true
          } opts)
  )

(defn get-tables
  [{:keys [host port db dbtype catalog schema-name table-name ssl?]
    :or {host "localhost", port 1527, db "", dbtype "", catalog "", schema-name "", table-name "", ssl? true} ;todo: ssl
    :as opts}]
  (def mylist [])
  (let [db-spec (get-db-spec opts)
        table-meta-data (get-table-meta-data db-spec "" schema-name "%")
        ]
    (while (.next table-meta-data)
      (def mylist (conj mylist
                    (.getString table-meta-data 3)
                    )
        )
      )
    )
  (into #{} mylist)
  )

(defn get-columns
  [opts]
  (if (empty? (:table-name opts)) (throw (IllegalArgumentException. "Table Name Not Specified.")))

  (let [opts (get-db-spec opts)
        table-name (:table-name opts)
        schema-name (:schema-name opts)
        catalog (:catalog opts)
        table-meta-data (get-table-meta-data opts catalog schema-name table-name)
        table-name-lc (.toLowerCase table-name)
        column-list (get-column-vector table-meta-data table-name-lc)
        ]
    column-list
    )
  )

(defn get-data
  [opts]
  (if (empty? (:table-name opts)) (throw (IllegalArgumentException. "Table Name Not Specified.")))
  (let [opts (get-db-spec opts)
        table-name (:table-name opts)
        schema-name (:schema-name opts)
        query (if (> (.length schema-name) 0)
                (str "SELECT * FROM " schema-name "." table-name) (str "SELECT * FROM " table-name))
        qs (jdbc/query opts [query])]
    qs))
