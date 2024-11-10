(ns konserve-dynamodb.core
  "DynamoDB based konserve backend."
  (:require [konserve.impl.defaults :refer [connect-default-store]]
            [konserve.impl.storage-layout :refer [PBackingStore PBackingBlob PBackingLock -delete-store]]
            [konserve.utils :refer [async+sync *default-sync-translation*]]
            [superv.async :refer [go-try-]]
            [taoensso.timbre :refer [info trace]])
  (:import [software.amazon.awssdk.services.dynamodb DynamoDbClient]
           [software.amazon.awssdk.services.dynamodb.model
            PutItemRequest PutItemResponse
            GetItemRequest
            DeleteItemRequest DeleteItemResponse
            ScanRequest ScanResponse
            AttributeValue
            CreateTableRequest DescribeTableRequest
            DeleteTableRequest
            AttributeDefinition
            KeySchemaElement KeyType ProvisionedThroughput
            ScalarAttributeType
            ResourceNotFoundException]
           [software.amazon.awssdk.auth.credentials StaticCredentialsProvider AwsBasicCredentials]
           [software.amazon.awssdk.core SdkBytes]
           [java.util HashMap Map]
           [java.io ByteArrayInputStream]))

(comment (set! *warn-on-reflection* true))

(defn dynamodb-client
  "Creates a new DynamoDB client using the provided options, with explicit credential handling."
  [opts]
  (let [builder (DynamoDbClient/builder)]
    (when (:region opts)
      (.region builder (software.amazon.awssdk.regions.Region/of (:region opts))))
    (when (:access-key opts)
      (let [credentials (StaticCredentialsProvider/create
                         (AwsBasicCredentials/create (:access-key opts) (:secret opts)))]
        (.credentialsProvider builder credentials)))
    (.build builder)))

(defn create-dynamodb-table
  [client table-name {:keys [read-capacity write-capacity]}]
  (let [attribute-definition (-> (AttributeDefinition/builder)
                                 (.attributeName "Key")
                                 (.attributeType ScalarAttributeType/S)
                                 .build)
        key-schema (-> (KeySchemaElement/builder)
                       (.attributeName "Key")
                       (.keyType KeyType/HASH)
                       .build)
        provisioned-throughput (-> (ProvisionedThroughput/builder)
                                   (.readCapacityUnits read-capacity)
                                   (.writeCapacityUnits write-capacity)
                                   .build)
        create-table-request (-> (CreateTableRequest/builder)
                                 (.tableName table-name)
                                 (.attributeDefinitions (into-array AttributeDefinition [attribute-definition]))
                                 (.keySchema (into-array KeySchemaElement [key-schema]))
                                 (.provisionedThroughput provisioned-throughput)
                                 .build)]
    (.createTable client create-table-request)))

(defn table-exists? [^DynamoDbClient client ^String table-name]
  (try
    (.describeTable client (-> (DescribeTableRequest/builder)
                               (.tableName table-name)
                               .build))
    true
    (catch ResourceNotFoundException _
      false)))

(defn delete-dynamodb-table [^DynamoDbClient client ^String table-name]
  (.deleteTable client (-> (DeleteTableRequest/builder)
                           (.tableName table-name)
                           .build)))

(defn ^PutItemResponse put-item [^DynamoDbClient client ^String table-name ^HashMap item]
  (.putItem client (-> (PutItemRequest/builder)
                       (.tableName table-name)
                       (.item item)
                       (.build))))

(defn ^Map get-item [^DynamoDbClient client ^String table-name ^HashMap key]
  (try
    (.item (.getItem client (-> (GetItemRequest/builder)
                                (.tableName table-name)
                                (.key key)
                                (.build))))
    (catch Exception _
      nil)))

(defn ^DeleteItemResponse delete-item [^DynamoDbClient client ^String table-name ^HashMap key]
  (.deleteItem client (-> (DeleteItemRequest/builder)
                          (.tableName table-name)
                          (.key key)
                          (.build))))

(defn ^ScanResponse scan-table [^DynamoDbClient client ^String table-name]
  (.scan client (-> (ScanRequest/builder)
                    (.tableName table-name)
                    (.build))))

(defrecord DynamoDBBlob [^DynamoDbClient table ^String key data fetched-object]
  PBackingBlob
  (-sync [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (let [{:keys [^bytes header ^bytes meta ^bytes value]} @data]
                           (if (and header meta value)
                             (put-item (:client table)
                                       (:table table)
                                       (HashMap. {"Key" (.build (.s (AttributeValue/builder) key))
                                                  "Header" (.build (.b (AttributeValue/builder) (SdkBytes/fromByteArray header)))
                                                  "Meta" (.build (.b (AttributeValue/builder) (SdkBytes/fromByteArray meta)))
                                                  "Value" (.build (.b (AttributeValue/builder) (SdkBytes/fromByteArray value)))}))
                             (throw (ex-info "Updating a row is only possible if header, meta, and value are set."
                                             {:data @data})))
                           (reset! data {})))))
  (-close [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-get-lock [_ env]
    (if (:sync? env) true (go-try- true)))
  (-read-header [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (when-not @fetched-object
                   (reset! fetched-object (get-item (:client table) (:table table) (HashMap. {"Key" (.build (.s (AttributeValue/builder) key))}))))
                 (.asByteArray (.b (.get @fetched-object "Header"))))))
  (-read-meta [_ _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (.asByteArray (.b (.get @fetched-object "Meta"))))))
  (-read-value [_ _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (.asByteArray (.b (.get @fetched-object "Value"))))))
  (-read-binary [_ _meta-size locked-cb env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [obj ^bytes (.asByteArray (.b (.get @fetched-object "Value")))]
                   (locked-cb {:input-stream
                               (ByteArrayInputStream. obj)
                               :size (alength obj)})))))
  (-write-header [_ header env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :header header))))
  (-write-meta [_ meta env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :meta meta))))
  (-write-value [_ value _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value value))))
  (-write-binary [_ _meta-size blob env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value blob)))))

(extend-protocol PBackingLock
  Boolean
  (-release [_ env]
    (if (:sync? env) nil (go-try- nil))))

(defrecord DynamoDBStore [client table]
  PBackingStore
  (-create-blob [this store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (DynamoDBBlob. this store-key (atom {}) (atom nil)))))
  (-delete-blob [_ store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (delete-item client table (HashMap. {"Key" (.build (.s (AttributeValue/builder) store-key))})))))
  (-blob-exists? [_ store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (not (empty? (get-item client table (HashMap. {"Key" (.build (.s (AttributeValue/builder) store-key))})))))))
  (-copy [_ from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [item (get-item client table (HashMap. {"Key" (.build (.s (AttributeValue/builder) from))}))]
                   (when item
                     (put-item client table (HashMap. {"Key" (.build (.s (AttributeValue/builder) to))
                                                       "Header" (.get item "Header")
                                                       "Meta" (.get item "Meta")
                                                       "Value" (.get item "Value")})))))))
  (-atomic-move [_ from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [item (get-item client table (HashMap. {"Key" (.build (.s (AttributeValue/builder) from))}))]
                   (when item
                     (put-item client table (HashMap. {"Key" (.build (.s (AttributeValue/builder) to))
                                                       "Header" (.get item "Header")
                                                       "Meta" (.get item "Meta")
                                                       "Value" (.get item "Value")}))
                     (delete-item client table (HashMap. {"Key" (.build (.s (AttributeValue/builder) from))})))))))
  (-create-store [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (info "DynamoDB table setup complete."))))
  (-sync-store [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-delete-store [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (try
                   (delete-dynamodb-table client table)
                   (info "DynamoDB store deleted.")
                   (catch ResourceNotFoundException _
                     (info "DynamoDB table does not exist."))))))
  (-migratable [_ _key _store-key env]
    (if (:sync? env) nil (go-try- nil)))
  (-migrate [_ _migration-key _key-vec _serializer _read-handlers _write-handlers env]
    (if (:sync? env) nil (go-try- nil)))
  (-keys [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [response (scan-table client table)]
                   (->> (.items response)
                        (map #(.s (.get % "Key")))
                        doall))))))

(defn connect-store [dynamodb-spec & {:keys [opts]
                                      :as params}]
  (let [complete-opts (merge {:sync? true :read-capacity 50 :write-capacity 50} opts)
        client (dynamodb-client dynamodb-spec)
        table-name (:table dynamodb-spec)
        _ (when-not (table-exists? client table-name)
            (create-dynamodb-table client table-name complete-opts))
        backing (DynamoDBStore. client table-name)
        config (merge {:opts               complete-opts
                       :config             {:sync-blob? true
                                            :in-place? false
                                            :lock-blob? true}
                       :default-serializer :FressianSerializer
                       :buffer-size        (* 1024 1024)}
                      (dissoc params :opts :config))]
    (connect-default-store backing config)))

(defn delete-store [dynamodb-spec & {:keys [opts]}]
  (let [complete-opts (merge {:sync? true} opts)
        backing (DynamoDBStore. (dynamodb-client dynamodb-spec) (:table dynamodb-spec))]
    (-delete-store backing complete-opts)))

(defn release
  "Release the store connection."
  [store env]
  (async+sync (:sync? env) *default-sync-translation*
              (go-try- (.close (:client (:backing store))))))

(comment
  ;; Testing and usage example:
  (require '[konserve.core :as k]
           '[clojure.core.async :refer [<!!]])

  ;; DynamoDB configuration
  (def dynamodb-spec {:region "us-west-2"
                      :table "konserve-dynamodb2"
                      :access-key (System/getenv "AWS_ACCESS_KEY_ID")
                      :secret (System/getenv "AWS_SECRET_ACCESS_KEY")})

  ;; Connect to the store
  (def store (<!! (connect-store dynamodb-spec :opts {:sync? false})))

  ;; Test inserting and retrieving data
  (time (<!! (k/assoc-in store ["foo"] {:foo "baz"} {:sync? false})))

  (<!! (k/get-in store ["foo"] nil {:sync? false}))

  ;; Check if a key exists
  (<!! (k/exists? store "foo" {:sync? false}))

  ;; Update data
  (time (k/assoc-in store ["bar"] 42 {:sync? true}))

  (k/update-in store ["bar"] inc {:sync? true})

  (k/get-in store ["bar"] nil {:sync? true})

  ;; Remove data
  (k/dissoc store ["bar"] {:sync? true})

  ;; List keys
  (k/keys store {:sync? true})

  ;; Release the store connection
  (release store {:sync? true})

  (delete-store dynamodb-spec :opts {:sync? true}))
