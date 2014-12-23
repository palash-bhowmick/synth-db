(ns
  ^{:author Tanmoy}
  synth-db.util.datomic-util
  (:require [datomic.api :as d])
  )



(defn create-connection
  [connection-url]
  (let [uri connection-url
        conn (d/connect uri)
        dbcon (d/db conn)]
    {:connection conn
     :db dbcon})
  )

;(def me [{:db/id (d/tempid ":db.part/db")
;          :db/ident ":document/uri"
;          :db/doc "Document URI"
;          ;  :db/index true
;          ;  :db/unique :db.unique/identity
;          :db/valueType :db.type/string
;          :db/cardinality :db.cardinality/one
;          :db.install/_attribute :db.part/db}])



(defn get-attr-map [attr-map]
  (merge {:db/id (d/tempid ":db.part/db")
          :db/valueType :db.type/string
          :db/cardinality :db.cardinality/one
          :db.install/_attribute :db.part/db}
    attr-map
    )
  )

(defn create-entities
  ;  [attrs conn]
  [attr-map-list conn]
  (println (str "create entities called with " attr-map-list))
  (def attrs [])
  (for [x attr-map-list]
    (def attrs
      (conj attrs
        (get-attr-map {:db/ident (:db/ident x)
                       :db/valueType (:db/valueType x)})))
    )
  (println (str "attrs afetr merging " attrs))
  (d/transact conn attrs)
  )

(defn d-transact [con attr-map-list]
  (d/transact con attr-map-list)
  )
;(def attrs (conj []
;             (get-attr-map {:db/ident ":test/someAttr1"})
;             (get-attr-map {:db/ident ":test/someAttr2"})
;             (get-attr-map {:db/ident ":test/someAttr3"})
;             (get-attr-map {:db/ident ":test/someAttr4"})
;             ))

(def attrs (into [] (map #(get-attr-map %1) [{:db/ident ":test/someAttr11"} {:db/ident ":test/someAttr12"}])))



;(d/transact (:connection conn-data) attrs)







