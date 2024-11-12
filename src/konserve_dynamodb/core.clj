(ns konserve-dynamodb.core
  "DynamoDB based konserve backend."
  (:require
    [konserve.impl.defaults :refer [connect-default-store]]
    [konserve.impl.storage-layout :refer [PBackingStore PBackingBlob PBackingLock -delete-store]]
    [konserve.utils :refer [async+sync *default-sync-translation*]]
    [superv.async :refer [go-try-]]
    [taoensso.timbre :refer [info trace]])
  (:import
    (java.io
      ByteArrayInputStream)
    (java.net
      URI)
    (java.util
      HashMap
      Map)
    (software.amazon.awssdk.auth.credentials
      AwsBasicCredentials
      StaticCredentialsProvider)
    (software.amazon.awssdk.core
      SdkBytes)
    (software.amazon.awssdk.services.dynamodb
      DynamoDbClient)
    (software.amazon.awssdk.services.dynamodb.model
      AttributeDefinition
      AttributeDefinition$Builder
      AttributeValue
      CreateTableRequest
      CreateTableRequest$Builder
      DeleteItemRequest
      DeleteItemResponse
      DeleteTableRequest
      DescribeTableRequest
      GetItemRequest
      KeySchemaElement
      KeySchemaElement$Builder
      KeyType
      ProvisionedThroughput
      ProvisionedThroughput$Builder
      PutItemRequest
      PutItemResponse
      ResourceNotFoundException
      ScalarAttributeType
      ScanRequest
      ScanResponse)))


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
    (.createTable client create-table-request)))


(defn table-exists?
  [^DynamoDbClient client ^String table-name]
  (try
    (let [^DescribeTableRequest request (-> (DescribeTableRequest/builder)
                                            (.tableName table-name)
                                            .build)]
      (.describeTable client request)
      true)
    (catch ResourceNotFoundException _
      false)))


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


(defn ^Map get-item
  [^DynamoDbClient client ^String table-name ^HashMap key]
  (try
    (let [^GetItemRequest request (-> (GetItemRequest/builder)
                                      (.tableName table-name)
                                      (.key key)
                                      .build)]
      (.item (.getItem client request)))
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
                    (reset! fetched-object (get-item (:client table) (:table table) (hash-map "Key" (attribute-value-s key)))))
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
  [^DynamoDbClient client ^String table]

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
                (go-try- (boolean (seq (get-item client table (hash-map "Key" (attribute-value-s store-key))))))))


  (-copy
    [_ from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                  (let [item (get-item client table (hash-map "Key" (attribute-value-s from)))]
                    (when item
                      (put-item client table (assoc item "Key" (attribute-value-s to))))))))


  (-atomic-move
    [_ from to env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                  (let [item (get-item client table (hash-map "Key" (attribute-value-s from)))]
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
                         doall))))))


(defn connect-store
  [dynamodb-spec & {:keys [opts]
                    :as params}]
  (let [complete-opts (merge {:sync? true :read-capacity 50 :write-capacity 50} opts)
        ^DynamoDbClient client (dynamodb-client dynamodb-spec)
        ^String table-name (:table dynamodb-spec)
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


(defn delete-store
  [dynamodb-spec & {:keys [opts]}]
  (let [complete-opts (merge {:sync? true} opts)
        backing (DynamoDBStore. (dynamodb-client dynamodb-spec) (:table dynamodb-spec))]
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
