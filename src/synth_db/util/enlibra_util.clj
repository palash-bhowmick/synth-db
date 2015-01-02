(ns
  ^{:author Tanmoy}
  synth-db.util.enlibra-util
  )

(defn get-formatted-name [name]
  (clojure.string/lower-case
    (clojure.string/replace
      (clojure.string/trim
        (clojure.string/replace name #"([a-z])([A-Z])" "$1 $2")) #"\s+|_+" "-")
    )
  )
