{
  ;; datomic URI
  :datomic-uri "datomic:<type>://$host:4334/$dbname"
  ;; Derby Database parameter
  :db-info {
             :dbtype :derby
             :host "$host"
             :port "$port"
             :db "$Database"
             :user "$username"
             :password "$password"
             :schema-name "$schemaname"
             }
  }
