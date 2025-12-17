(ns konserve-dynamodb.core
  "DynamoDB based konserve backend."
  (:require
   [konserve.impl.defaults :refer [connect-default-store]]
   [konserve.impl.storage-layout :refer [PBackingStore PBackingBlob PBackingLock
                                         PMultiWriteBackingStore PMultiReadBackingStore
                                         -delete-store]]
   [konserve.utils :refer [async+sync *default-sync-translation*]]
   [superv.async :refer [go-try-]]
   [taoensso.timbre :refer [info trace warn]])
  (:import
   (java.io ByteArrayInputStream)
   (java.net URI)
   (java.util HashMap Map ArrayList)
   (software.amazon.awssdk.auth.credentials AwsBasicCredentials StaticCredentialsProvider)
   (software.amazon.awssdk.core SdkBytes)
   (software.amazon.awssdk.services.dynamodb DynamoDbClient)
   (software.amazon.awssdk.services.dynamodb.model
    AttributeDefinition
    AttributeDefinition$Builder
    AttributeValue
    BatchGetItemRequest
    BatchGetItemResponse
    CreateTableRequest
    CreateTableRequest$Builder
    Delete
    DeleteItemRequest
    DeleteItemResponse
    DeleteTableRequest
    DescribeTableRequest
    DescribeTableResponse
    GetItemRequest
    KeysAndAttributes
    KeySchemaElement
    KeySchemaElement$Builder
    KeyType
    ProvisionedThroughput
    ProvisionedThroughput$Builder
    Put
    PutItemRequest
    PutItemResponse
    ResourceNotFoundException
    ScalarAttributeType
    ScanRequest
    ScanResponse
    TableDescription
    TransactWriteItem
    TransactWriteItemsRequest
    TransactWriteItemsResponse)))

(set! *warn-on-reflection* true)

(defn dynamodb-client
  "Creates a new DynamoDB client using the provided options, with explicit credential handling."
  [opts]
  (let [builder (DynamoDbClient/builder)]
    (when (:endpoint opts)
      (.endpointOverride builder (URI/create (:endpoint opts))))
    (when (:region opts)
      (.region builder (software.amazon.awssdk.regions.Region/of (:region opts))))
    (when (:access-key opts)
      (let [credentials (StaticCredentialsProvider/create
                         (AwsBasicCredentials/create (:access-key opts) (:secret opts)))]
        (.credentialsProvider builder credentials)))
    (.build builder)))

(defn table-exists?
  [^DynamoDbClient client ^String table-name]
  (try
    (let [^DescribeTableRequest request (-> (DescribeTableRequest/builder)
                                            (.tableName table-name)
                                            .build)]
      (= ^String (.tableStatusAsString ^TableDescription (.table ^DescribeTableResponse (.describeTable client request))) "ACTIVE"))
    (catch ResourceNotFoundException _
      false)))

(defn create-dynamodb-table
  [^DynamoDbClient client table-name {:keys [read-capacity write-capacity]}]
  (let [^AttributeDefinition$Builder attribute-definition-builder (AttributeDefinition/builder)
        ^AttributeDefinition attribute-definition (-> attribute-definition-builder
                                                      (.attributeName "Key")
                                                      (.attributeType ScalarAttributeType/S)
                                                      .build)
        ^KeySchemaElement$Builder key-schema-builder (KeySchemaElement/builder)
        ^KeySchemaElement key-schema (-> key-schema-builder
                                         (.attributeName "Key")
                                         (.keyType KeyType/HASH)
                                         .build)
        ^ProvisionedThroughput$Builder provisioned-throughput-builder (ProvisionedThroughput/builder)
        ^ProvisionedThroughput provisioned-throughput (-> provisioned-throughput-builder
                                                          (.readCapacityUnits read-capacity)
                                                          (.writeCapacityUnits write-capacity)
                                                          .build)

        ^CreateTableRequest$Builder create-table-request-builder
        (CreateTableRequest/builder)

        ;; Explicitly assign the builder at each step with type hints
        ^CreateTableRequest$Builder create-table-request-builder
        (.tableName create-table-request-builder table-name)

        ^CreateTableRequest$Builder create-table-request-builder
        (.attributeDefinitions create-table-request-builder
                               ^"[Lsoftware.amazon.awssdk.services.dynamodb.model.AttributeDefinition;"
                               (into-array AttributeDefinition [attribute-definition]))

        ^CreateTableRequest$Builder create-table-request-builder
        (.keySchema create-table-request-builder
                    ^"[Lsoftware.amazon.awssdk.services.dynamodb.model.KeySchemaElement;"
                    (into-array KeySchemaElement [key-schema]))

        ^CreateTableRequest$Builder create-table-request-builder
        (.provisionedThroughput create-table-request-builder provisioned-throughput)

        ^CreateTableRequest create-table-request
        (.build create-table-request-builder)]
    (.createTable client create-table-request)
    (while (not (table-exists? client table-name))
      (Thread/sleep 2000))))

(defn delete-dynamodb-table
  [^DynamoDbClient client ^String table-name]
  (let [^DeleteTableRequest request (-> (DeleteTableRequest/builder)
                                        (.tableName table-name)
                                        .build)]
    (.deleteTable client request)))

(defn ^PutItemResponse put-item
  [^DynamoDbClient client ^String table-name ^HashMap item]
  (let [^PutItemRequest request (-> (PutItemRequest/builder)
                                    (.tableName table-name)
                                    (.item item)
                                    .build)]
    (.putItem client request)))

(defn ^TransactWriteItemsResponse transact-write-items
  "Execute multiple write operations in a single, atomic transaction.
   Limited to 100 items per transaction by DynamoDB."
  [^DynamoDbClient client ^ArrayList transact-items]
  (let [^TransactWriteItemsRequest request (-> (TransactWriteItemsRequest/builder)
                                               (.transactItems transact-items)
                                               .build)]
    (.transactWriteItems client request)))

(defn ^Map get-item
  [^DynamoDbClient client ^String table-name ^HashMap key ^java.lang.Boolean consistent-read?]
  (try
    (let [^GetItemRequest request (-> (GetItemRequest/builder)
                                      (.tableName table-name)
                                      (.key key)
                                      (.consistentRead consistent-read?)
                                      .build)]
      (into {} (.item (.getItem client request))))
    (catch Exception _
      nil)))

(defn ^DeleteItemResponse delete-item
  [^DynamoDbClient client ^String table-name ^HashMap key]
  (let [^DeleteItemRequest request (-> (DeleteItemRequest/builder)
                                       (.tableName table-name)
                                       (.key key)
                                       .build)]
    (.deleteItem client request)))

(defn ^ScanResponse scan-table
  [^DynamoDbClient client ^String table-name]
  (let [^ScanRequest request (-> (ScanRequest/builder)
                                 (.tableName table-name)
                                 .build)]
    (.scan client request)))

(defn ^BatchGetItemResponse batch-get-items
  "Fetch multiple items in a single BatchGetItem call.
   Returns a map of {store-key -> item-map} for found items.
   Limited to 100 items per request by DynamoDB."
  [^DynamoDbClient client ^String table-name store-keys ^Boolean consistent-read?]
  (when (seq store-keys)
    (let [;; Build list of keys to fetch
          keys-list (java.util.ArrayList.)
          _ (doseq [store-key store-keys]
              (let [key-map (doto (java.util.HashMap.)
                              (.put "Key" (.build (.s (AttributeValue/builder) store-key))))]
                (.add keys-list key-map)))
          ;; Build KeysAndAttributes with consistency setting
          ^KeysAndAttributes keys-and-attrs (-> (KeysAndAttributes/builder)
                                                (.keys keys-list)
                                                (.consistentRead consistent-read?)
                                                .build)
          ;; Build request items map
          request-items (doto (java.util.HashMap.)
                          (.put table-name keys-and-attrs))
          ;; Build and execute request
          ^BatchGetItemRequest request (-> (BatchGetItemRequest/builder)
                                           (.requestItems request-items)
                                           .build)
          ^BatchGetItemResponse response (.batchGetItem client request)]
      response)))

(defn- attribute-value-s
  [^String s]
  (.build (.s (AttributeValue/builder) s)))

(defn- attribute-value-b
  [^bytes b]
  (.build (.b (AttributeValue/builder) (SdkBytes/fromByteArray b))))

(defrecord DynamoDBBlob
           [table ^String key data ^clojure.lang.Atom fetched-object]

  PBackingBlob

  (-sync
    [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (let [{:keys [^bytes header ^bytes meta ^bytes value]} @data]
                           (if (and header meta value)
                             (put-item (:client table)
                                       (:table table)
                                       (hash-map "Key" (attribute-value-s key)
                                                 "Header" (attribute-value-b header)
                                                 "Meta" (attribute-value-b meta)
                                                 "Value" (attribute-value-b value)))
                             (throw (ex-info "Updating a row is only possible if header, meta, and value are set."
                                             {:data @data})))
                           (reset! data {})))))

  (-close
    [_ env]
    (if (:sync? env) nil (go-try- nil)))

  (-get-lock
    [_ env]
    (if (:sync? env) true (go-try- true)))

  (-read-header
    [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (when-not @fetched-object
                   (reset! fetched-object (get-item (:client table)
                                                    (:table table)
                                                    (hash-map "Key" (attribute-value-s key))
                                                    (:consistent-read? table))))
                 (let [^Map fetched-obj @fetched-object
                       ^AttributeValue attr-value (.get fetched-obj "Header")
                       ^SdkBytes sdk-bytes (.b attr-value)
                       ^bytes byte-array (.asByteArray sdk-bytes)]
                   byte-array))))

  (-read-meta
    [_ _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [^Map fetched-obj @fetched-object
                       ^AttributeValue attr-value (.get fetched-obj "Meta")
                       ^SdkBytes sdk-bytes (.b attr-value)
                       ^bytes byte-array (.asByteArray sdk-bytes)]
                   byte-array))))

  (-read-value
    [_ _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [^Map fetched-obj @fetched-object
                       ^AttributeValue attr-value (.get fetched-obj "Value")
                       ^SdkBytes sdk-bytes (.b attr-value)
                       ^bytes byte-array (.asByteArray sdk-bytes)]
                   byte-array))))

  (-read-binary
    [_ _meta-size locked-cb env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [^Map fetched-obj @fetched-object
                       ^AttributeValue attr-value (.get fetched-obj "Value")
                       ^SdkBytes sdk-bytes (.b attr-value)
                       ^bytes obj (.asByteArray sdk-bytes)]
                   (locked-cb {:input-stream
                               (ByteArrayInputStream. obj)
                               :size (alength obj)})))))

  (-write-header
    [_ header env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :header header))))

  (-write-meta
    [_ meta env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :meta meta))))

  (-write-value
    [_ value _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value value))))

  (-write-binary
    [_ _meta-size blob env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value blob)))))

(extend-protocol PBackingLock
  Boolean
  (-release [_ env]
    (if (:sync? env) nil (go-try- nil))))

(defrecord DynamoDBStore
           [^DynamoDbClient client ^String table ^java.lang.Boolean consistent-read?]

  PBackingStore

  (-create-blob
    [this store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (DynamoDBBlob. this store-key (atom {}) (atom nil)))))

  (-delete-blob
    [_ store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (delete-item client table (hash-map "Key" (attribute-value-s store-key))))))

  (-blob-exists?
    [_ store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (boolean (seq (get-item client
                                                 table
                                                 (hash-map "Key" (attribute-value-s store-key))
                                                 consistent-read?))))))

  (-copy
    [_ from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [item (get-item client table (hash-map "Key" (attribute-value-s from)) consistent-read?)]
                   (when item
                     (put-item client table (assoc item "Key" (attribute-value-s to))))))))

  (-atomic-move
    [_ from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [item (get-item client table (hash-map "Key" (attribute-value-s from)) consistent-read?)]
                   (when item
                     (put-item client table (assoc item "Key" (attribute-value-s to)))
                     (delete-item client table (hash-map "Key" (attribute-value-s from))))))))

  (-create-store
    [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (info "DynamoDB table setup complete."))))

  (-sync-store
    [_ env]
    (if (:sync? env) nil (go-try- nil)))

  (-delete-store
    [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (try
                   (delete-dynamodb-table client table)
                   (info "DynamoDB store deleted.")
                   (catch ResourceNotFoundException _
                     (info "DynamoDB table does not exist."))))))

  (-migratable
    [_ _key _store-key env]
    (if (:sync? env) nil (go-try- nil)))

  (-migrate
    [_ _migration-key _key-vec _serializer _read-handlers _write-handlers env]
    (if (:sync? env) nil (go-try- nil)))

  (-keys
    [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (let [^ScanResponse response (scan-table client table)]
                   (->> (.items response)
                        (map (fn [^Map item]
                               (let [^AttributeValue attr-value (.get item "Key")]
                                 (.s attr-value))))
                        doall)))))

  ;; Implementation for atomic multi-key writes
  PMultiWriteBackingStore
  (-multi-write-blobs
    [_ store-key-values env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (if (> (count store-key-values) 100)
          ;; DynamoDB TransactWriteItems API has a limit of 100 items per transaction
                   (throw (ex-info "DynamoDB TransactWriteItems exceeds item limit (max 100)"
                                   {:type :not-supported
                                    :reason "Too many items for a single transaction"
                                    :item-count (count store-key-values)}))
                   (try
            ;; Create a transact write items request with all our items
                     (let [^ArrayList transact-items (ArrayList.)
                           _ (doseq [[store-key data] store-key-values]
                               (let [{:keys [header meta value]} data
                            ;; Create a map with the item data
                                     item-map (doto (HashMap.)
                                                (.put "Key" (attribute-value-s store-key))
                                                (.put "Header" (attribute-value-b header))
                                                (.put "Meta" (attribute-value-b meta))
                                                (.put "Value" (attribute-value-b value)))
                            ;; Create a Put request for this item
                                     ^Put put (.build (.item (.tableName (Put/builder) table) item-map))
                            ;; Create a TransactWriteItem with the Put request
                                     ^TransactWriteItem transact-write-item (.build (.put (TransactWriteItem/builder) put))]
                        ;; Add to our list of TransactWriteItems
                                 (.add transact-items transact-write-item)))
                  ;; Execute the transaction
                           _ (transact-write-items client transact-items)
                  ;; If we get here, all writes succeeded
                  ;; Create a result map with all keys mapping to true
                           results (into {} (map (fn [[store-key _]] [store-key true]) store-key-values))]
                       results)
            ;; Handle any transaction errors
                     (catch Exception e
                       (warn "TransactWriteItems failed:" (.getMessage e))
                       (throw (ex-info "DynamoDB TransactWriteItems failed"
                                       {:type :not-supported
                                        :reason "Transaction failed"
                                        :cause e}))))))))

  (-multi-delete-blobs
    [_ store-keys env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (if (empty? store-keys)
                   {}
                   (if (> (count store-keys) 100)
                     ;; DynamoDB TransactWriteItems API has a limit of 100 items per transaction
                     (throw (ex-info "DynamoDB TransactWriteItems exceeds item limit (max 100)"
                                     {:type :not-supported
                                      :reason "Too many items for a single transaction"
                                      :item-count (count store-keys)}))
                     (try
                       ;; First check which keys exist using BatchGetItem
                       (let [^BatchGetItemResponse response (batch-get-items client table store-keys consistent-read?)
                             ;; Get the items that were found
                             found-items (when response
                                           (get (.responses response) table))
                             existing-keys (when found-items
                                             (into #{}
                                                   (map (fn [^java.util.Map item]
                                                          (let [^AttributeValue key-attr (.get item "Key")]
                                                            (.s key-attr)))
                                                        found-items)))
                             existing-keys (or existing-keys #{})]
                         ;; Only delete if there are existing keys
                         (when (seq existing-keys)
                           (let [^ArrayList transact-items (ArrayList.)
                                 _ (doseq [store-key existing-keys]
                                     (let [;; Create key map for delete
                                           key-map (doto (HashMap.)
                                                     (.put "Key" (attribute-value-s store-key)))
                                           ;; Create Delete request
                                           ^Delete delete-req (.build (.key (.tableName (Delete/builder) table) key-map))
                                           ;; Create TransactWriteItem with Delete
                                           ^TransactWriteItem transact-write-item (.build (.delete (TransactWriteItem/builder) delete-req))]
                                       (.add transact-items transact-write-item)))]
                             (transact-write-items client transact-items)))
                         ;; Return map showing which keys existed
                         (reduce (fn [acc k]
                                   (assoc acc k (contains? existing-keys k)))
                                 {}
                                 store-keys))
                       (catch Exception e
                         (warn "TransactWriteItems (delete) failed:" (.getMessage e))
                         (throw (ex-info "DynamoDB TransactWriteItems (delete) failed"
                                         {:type :not-supported
                                          :reason "Transaction failed"
                                          :cause e})))))))))

  PMultiReadBackingStore
  (-multi-read-blobs
    [this store-keys env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (if (empty? store-keys)
                   {}
                   (if (> (count store-keys) 100)
                     ;; DynamoDB BatchGetItem API has a limit of 100 items per request
                     (throw (ex-info "DynamoDB BatchGetItem exceeds item limit (max 100)"
                                     {:type :not-supported
                                      :reason "Too many items for a single request"
                                      :item-count (count store-keys)}))
                     (try
                       (let [^BatchGetItemResponse response (batch-get-items client table store-keys consistent-read?)
                             ;; Get the items that were found for our table
                             found-items (when response
                                           (get (.responses response) table))]
                         ;; Build sparse map of store-key -> DynamoDBBlob with pre-populated data
                         (if found-items
                           (reduce (fn [acc ^java.util.Map item]
                                     (let [^AttributeValue key-attr (.get item "Key")
                                           store-key (.s key-attr)
                                           ;; Pre-populate fetched-object with the item data (eager loading)
                                           blob (DynamoDBBlob. this store-key (atom {}) (atom (into {} item)))]
                                       (assoc acc store-key blob)))
                                   {}
                                   found-items)
                           {}))
                       (catch Exception e
                         (warn "BatchGetItem failed:" (.getMessage e))
                         (throw (ex-info "DynamoDB BatchGetItem failed"
                                         {:type :not-supported
                                          :reason "Batch read failed"
                                          :cause e}))))))))))

(defn connect-store
  [dynamodb-spec & {:keys [opts]
                    :as params}]
  (let [complete-opts (merge {:sync? true :read-capacity 5 :write-capacity 5} opts)
        ^DynamoDbClient client (dynamodb-client dynamodb-spec)
        ^String table-name (:table dynamodb-spec)
        ^java.lang.Boolean consistent-read? (or (:consistent-read? dynamodb-spec) false)
        _ (when-not (table-exists? client table-name)
            (create-dynamodb-table client table-name complete-opts))
        backing (DynamoDBStore. client table-name consistent-read?)
        config (merge {:opts               complete-opts
                       :config             {:sync-blob? true
                                            :in-place? true
                                            :no-backup? true
                                            :lock-blob? true}
                       :default-serializer :FressianSerializer
                       :buffer-size        (* 1024 1024)}
                      (dissoc params :opts :config))]
    (connect-default-store backing config)))

(defn delete-store
  [dynamodb-spec & {:keys [opts]}]
  (let [complete-opts (merge {:sync? true} opts)
        backing (DynamoDBStore. (dynamodb-client dynamodb-spec)
                                (:table dynamodb-spec)
                                (or (:consistent-read? dynamodb-spec) false))]
    (-delete-store backing complete-opts)))

(defn release
  "Release the store connection."
  [store env]
  (async+sync (:sync? env) *default-sync-translation*
              (go-try-
               (let [^DynamoDBStore backing (:backing store)
                     ^DynamoDbClient client (:client backing)]
                 (.close client)))))

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
