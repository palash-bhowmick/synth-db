(ns
  ^{:author Tanmoy}
  synth-db.util.db-util
  (:require [clojure.java.jdbc :as jdbc]
            [synth-db.util.enlibra-util :as enlibra])
  )

(def driver-classes {:derby "org.apache.derby.jdbc.ClientDriver"})

(def data-type {"VARCHAR" :db.type/string
                "NUMERIC" :db.type/bigdec
                "VARCHAR2" :db.type/string
                "NUMBER" :db.type/bigdec
                "DECIMAL" :db.type/bigdec
                "INTEGER" :db.type/bigdec})

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

(defn get-primary-keys
  [opts]
  (if (empty? (:table-name opts)) (throw (IllegalArgumentException. "Table Name Not Specified.")))
  (let [opts (get-db-spec opts)
        conn (jdbc/get-connection opts)
        catalog (:catalog opts)
        schema-name (:schema-name opts)
        table-name (:table-name opts)
        ]
    (into-array
      (map #(str (% :column_name))
        (resultset-seq (->
                         conn
                         (.getMetaData)
                         (.getPrimaryKeys catalog schema-name table-name)
                         )
          )))
    ))

(defn get-tables
  [opts]
  (let [opts (get-db-spec opts)
        connection (jdbc/get-connection opts)
        schema-name (:schema-name opts)
        catalog (:catalog opts)
        ]
    (into #{}
      (map #(str (% :table_name))
        (resultset-seq (->
                         connection
                         (.getMetaData)
                         ; Params are catalog, schemapattern, tablenamepattern, Array of String
                         (.getTables catalog schema-name "%" (into-array ["TABLE"]))
                         )
          )))
    ))

(defn get-relevant-pk
  [primary-keys]
  ;RETURNS the key only if it is simple primary key
  (if (= (alength primary-keys) 1)
    (str (first primary-keys))
    ""))

(defn get-column-map [type-name column-name table-name-lc primary-key]
  (let [map-ele {:db/valueType (get-in data-type [type-name])
                 :db/ident (str ":table." table-name-lc "/" (enlibra/get-formatted-name column-name))}]
    (if (= column-name primary-key)
      (merge {:db/unique :db.unique/value} map-ele)
      map-ele))
  )

(defn get-columns
  [opts]
  (if (empty? (:table-name opts)) (throw (IllegalArgumentException. "Table Name Not Specified.")))
  ;Returns a Vector of map elements in accordance with datomic, for new field attributes along with unique - keyword
  ;  associated with single primary key attribute
  (let [opts (get-db-spec opts)
        table-name (:table-name opts)
        schema-name (:schema-name opts)
        catalog (:catalog opts)
        connection (jdbc/get-connection opts)
        table-name-lc (enlibra/get-formatted-name table-name)
        primary-keys (get-primary-keys opts)
        primary-key (get-relevant-pk primary-keys)
        ]
    (into []
      (map #(get-column-map (% :type_name) (% :column_name) table-name-lc primary-key)
        (resultset-seq (->
                         connection
                         (.getMetaData)
                         ; Params are catalog, schemapattern, tablenamepattern, columnnamepattern
                         (.getColumns catalog schema-name table-name "%")
                         )
          )))
    ))

(defn get-data
  [opts]
  (if (empty? (:table-name opts)) (throw (IllegalArgumentException. "Table Name Not Specified.")))
  (let [opts (get-db-spec opts)
        table-name (:table-name opts)
        schema-name (:schema-name opts)
        query (if (> (.length schema-name) 0)
                (str "SELECT * FROM \"" schema-name "\".\"" table-name "\"") (str "SELECT * FROM \"" table-name "\""))
        qs (jdbc/query opts [query] :identifiers synth-db.util.enlibra-util/get-formatted-name)]
    qs))
