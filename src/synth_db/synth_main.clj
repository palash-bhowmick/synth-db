(ns synth-db.synth-main
  (:require [synth-db.datomic-migrate :as synthdb])
  (:gen-class :main true)
  )

(defn load-config [filename]
  (with-open [r (clojure.java.io/reader filename)]
    (read (java.io.PushbackReader. r))))

(defn -main [& args]
  (let [config-map (load-config "config.clj")
        db-info (:db-info config-map)
        datomic-uri (:datomic-uri config-map)]
    (synthdb/init datomic-uri)
    (synthdb/replicate-schema db-info)
    (println (format "Migration Completed"))
    )
  )