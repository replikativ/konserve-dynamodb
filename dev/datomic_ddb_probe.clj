;; Use ../datahike's :datomic alias plus software.amazon.awssdk/dynamodb 2.31.45.
;; Requires the matching local Pro transactor and a dedicated DynamoDB Local table.
(require '[datomic.api :as d])
(import '[java.net URI]
        '[software.amazon.awssdk.auth.credentials AwsBasicCredentials StaticCredentialsProvider]
        '[software.amazon.awssdk.regions Region]
        '[software.amazon.awssdk.services.dynamodb DynamoDbClient]
        '[software.amazon.awssdk.services.dynamodb.model ScanRequest DescribeTableRequest])

(def table "datomic-local-study")
(def client (-> (DynamoDbClient/builder)
                (.endpointOverride (URI/create "http://localhost:18000"))
                (.region Region/US_EAST_1)
                (.credentialsProvider (StaticCredentialsProvider/create
                                       (AwsBasicCredentials/create "dummy" "dummy")))
                .build))

(defn scan-items []
  (loop [start {} items []]
    (let [request (-> (cond-> (-> (ScanRequest/builder) (.tableName table)
                                  (.consistentRead true))
                        (seq start) (.exclusiveStartKey start))
                      .build)
          response (.scan client ^ScanRequest request)
          items (into items (.items response))]
      (if (empty? (.lastEvaluatedKey response))
        items
        (recur (.lastEvaluatedKey response) items)))))

(defn snapshot [label]
  (let [items (scan-items)
        payloads (keep #(get % "v") items)
        sizes (sort (map (fn [v]
                           (if-let [s (.s v)]
                             (alength (.getBytes ^String s "UTF-8"))
                             (.remaining (.asByteBuffer (.b v)))))
                         payloads))
        chunk-counts (keep (fn [item]
                             (when-let [n (get item "__n")]
                               (or (.n n) (.s n)))) items)]
    (prn {:snapshot label :rows (count items)
          :attribute-shapes (frequencies (map #(vec (sort (keys %))) items))
          :payload-types (frequencies (map #(if (.s %) :string :binary) payloads))
          :payload-bytes-total (reduce + 0 sizes)
          :payload-bytes-max (last sizes)
          :largest-payloads (vec (take-last 8 sizes))
          :chunk-counts (frequencies chunk-counts)})))

(try
  (let [desc (.table (.describeTable client
                                     (-> (DescribeTableRequest/builder) (.tableName table) .build)))]
    (prn {:key-schema (mapv #(vector (.attributeName %) (.keyTypeAsString %)) (.keySchema desc))
          :global-secondary-indexes (count (.globalSecondaryIndexes desc))
          :local-secondary-indexes (count (.localSecondaryIndexes desc))}))
  (snapshot :before)
  (doseq [n [48 128 1024]]
    (let [db-name (str "bytes-" n "-" (java.util.UUID/randomUUID))
          uri (str "datomic:ddb-local://localhost:18000/" table "/" db-name
                   "?aws_access_key_id=dummy&aws_secret_key=dummy")
          _ (d/create-database uri)
          conn (d/connect uri)]
      (try
        @(d/transact conn [{:db/ident :payload :db/valueType :db.type/bytes
                            :db/cardinality :db.cardinality/one}])
        (let [rng (java.util.Random. 42)
              data (mapv (fn [_]
                           (let [value (byte-array 4096)]
                             (.nextBytes rng value)
                             {:db/id (d/tempid :db.part/user) :payload value}))
                         (range n))
              tx @(d/transact conn data)
              t (d/basis-t (:db-after tx))]
          (prn {:entries n :status :transacted
                :count (d/q '[:find (count ?e) . :where [?e :payload]] (:db-after tx))})
          (snapshot [:after-tx n])
          (d/request-index conn)
          (let [indexed (deref (d/sync-index conn t) 120000 ::timeout)]
            (prn {:entries n :index-status (if (= ::timeout indexed) :timeout :indexed)}))
          (snapshot [:after-index n]))
        (finally (d/release conn)))))
  (finally (.close client)))
(shutdown-agents)
(System/exit 0)
