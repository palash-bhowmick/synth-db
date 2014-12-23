(ns synth-db.core-test
  (:require [clojure.test :refer :all]
            [synth-db.core :refer :all]
            [datomic.api :as d]
            )
  )

(def db-uri-base "datomic:mem://")

(defn scratch-conn
  "Create a connection to an anonymous, in-memory database."
  []
  (let [uri (str db-uri-base (d/squuid))]
    (d/delete-database uri)
    (d/create-database uri)
    (d/connect uri)))

(def conn (scratch-conn))

;; transaction input and result are data
@(d/transact
  conn
  [[:db/add
    (d/tempid :db.part/user)
    :db/doc
    "Hello world"]])

;; point in time db value
(def db (d/db conn))

;; query input and result are data
(def q-result (d/q '[:find ?e .
                    :where [?e :db/doc "Hello world"]]
                  db))

;; entity is a navigable view over data
(def ent (d/entity db q-result))

;; entities are lazy, so...
(d/touch ent)

;; schema itself is data
(def doc-ent (d/entity db :db/doc))

(d/touch doc-ent)

(deftest a-test-2
  (testing "Datomic Connection"
    ;;expect "Hello world"
    (is (= "Hello world" (:db/doc ent)))))

