(ns konserve-dynamodb.core
  "DynamoDB based konserve backend."
  (:require
   [clojure.core.async :refer [<!!]]
   [konserve.protocols :as protocols]
   [konserve-dynamodb.layout :as layout]
   [konserve.impl.defaults :refer [connect-default-store normalize-store-config]]
   [konserve.impl.storage-layout :refer [PBackingStore PBackingBlob PBackingLock
                                         PMultiWriteBackingStore PMultiReadBackingStore
                                         PReadMissSafe store-key-not-found-ex
                                         -delete-store]]
   [konserve.utils :refer [async+sync *default-sync-translation*]]
   [konserve.store :as store]
   [superv.async :refer [go-try-]]
   [replikativ.logging :as log])
  (:import
   (java.io ByteArrayInputStream)
   (java.net URI)
   (java.nio ByteBuffer)
   (java.security MessageDigest)
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
    BatchWriteItemRequest
    PutRequest
    WriteRequest
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
    PutItemRequest$Builder
    ConditionalCheckFailedException
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
  ;; A missing item is an empty map. Service failures must reach the caller;
  ;; treating throttling or authentication errors as absence can corrupt updates.
  (let [^GetItemRequest request (-> (GetItemRequest/builder)
                                    (.tableName table-name)
                                    (.key key)
                                    (.consistentRead consistent-read?)
                                    .build)]
    (into {} (.item (.getItem client request)))))

(defn ^DeleteItemResponse delete-item
  [^DynamoDbClient client ^String table-name ^HashMap key]
  (let [^DeleteItemRequest request (-> (DeleteItemRequest/builder)
                                       (.tableName table-name)
                                       (.key key)
                                       .build)]
    (.deleteItem client request)))

(defn ^ScanResponse scan-table
  ([client table-name] (scan-table client table-name nil))
  ([client table-name start-key] (scan-table client table-name start-key false))
  ([^DynamoDbClient client ^String table-name start-key consistent-read?]
   (let [request (-> (cond-> (-> (ScanRequest/builder) (.tableName table-name) (.consistentRead (boolean consistent-read?)))
                       (seq start-key) (.exclusiveStartKey ^Map start-key))
                     .build)]
     (.scan client ^ScanRequest request))))

(defn- batch-read-backoff!
  [attempt]
  ;; Full jitter, capped at one second; at most eight retries per batch.
  (Thread/sleep (long (rand-int (inc (min 1000 (* 25 (bit-shift-left 1 attempt))))))))

(defn ^BatchGetItemResponse batch-get-items
  "Fetch up to 100 distinct keys, retrying partial responses with bounded backoff.
   Returns an aggregated BatchGetItemResponse. Exhaustion throws rather than
   presenting unprocessed keys as missing."
  [^DynamoDbClient client ^String table-name store-keys ^Boolean consistent-read?]
  (when (seq store-keys)
    (let [keys-list (mapv (fn [store-key]
                            {"Key" (.build (.s (AttributeValue/builder) store-key))})
                          (distinct store-keys))
          ^KeysAndAttributes keys-and-attrs (-> (KeysAndAttributes/builder)
                                                (.keys ^java.util.Collection keys-list)
                                                (.consistentRead consistent-read?)
                                                .build)]
      (loop [pending {table-name keys-and-attrs}
             items []
             attempt 0]
        (let [request (-> (BatchGetItemRequest/builder)
                          (.requestItems ^Map pending)
                          .build)
              ^BatchGetItemResponse response (.batchGetItem client ^BatchGetItemRequest request)
              items (into items (get (.responses response) table-name))
              remaining (.unprocessedKeys response)]
          (if (empty? remaining)
            (-> (BatchGetItemResponse/builder)
                (.responses ^Map {table-name items})
                .build)
            (if (>= attempt 8)
              (throw (ex-info "DynamoDB batch read retries exhausted"
                              {:type :konserve.dynamodb/batch-read-incomplete
                               :table table-name
                               :attempts (inc attempt)
                               :unprocessed-count (reduce + (map #(count (.keys ^KeysAndAttributes %))
                                                                 (vals remaining)))}))
              (do
                (batch-read-backoff! attempt)
                (recur remaining items (inc attempt))))))))))

(defn batch-write-fragments!
  "Stage at most 25 immutable 300 KiB fragments. Even with base64 and envelope
   overhead this stays below BatchWriteItem's 16 MiB wire limit. Retry only
   UnprocessedItems; callers must not publish manifests until this succeeds."
  [^DynamoDbClient client table items]
  (when (> (count items) 25)
    (throw (ex-info "Fragment batch exceeds 25 items" {:type :konserve.dynamodb/invalid-fragment-batch})))
  (when (seq items)
    (let [requests (mapv (fn [item]
                           (when-not (and (layout/fragment-key? (layout/string-of (get item "Key")))
                                          (= #{"Key" "Fragment"} (set (keys item)))
                                          (<= (alength ^bytes (layout/bytes-of (get item "Fragment"))) layout/fragment-size))
                             (throw (ex-info "Invalid staged fragment" {:type :konserve.dynamodb/invalid-fragment-batch})))
                           (let [^PutRequest put (-> (PutRequest/builder) (.item ^Map item) .build)]
                             (-> (WriteRequest/builder) (.putRequest put) .build))) items)]
      (loop [pending {table requests} attempt 0]
        (let [request (-> (BatchWriteItemRequest/builder) (.requestItems ^Map pending) .build)
              remaining (.unprocessedItems (.batchWriteItem client ^BatchWriteItemRequest request))]
          (when (seq remaining)
            (if (>= attempt 8)
              (throw (ex-info "DynamoDB fragment write retries exhausted"
                              {:type :konserve.dynamodb/batch-write-incomplete
                               :table table :attempts (inc attempt)
                               :unprocessed-count (reduce + (map count (vals remaining)))}))
              (do
                (batch-read-backoff! attempt)
                (recur remaining (inc attempt))))))))))

(defn- stage-fragments!
  [client table plans]
  ;; Fresh generation keys only; logical publication remains a separate atomic
  ;; step after EVERY staging batch has completed successfully.
  (doseq [items (partition-all 25 (mapcat :fragments plans))]
    (batch-write-fragments! client table items)))

(defn- hydrate-item
  [client table item]
  (if-let [{:keys [generation chunks length digest]} (layout/manifest item)]
    (let [buf (ByteBuffer/allocate (int length))]
      ;; Strong reads for freshly staged immutable fragments. Missing fragments
      ;; are corruption/incomplete storage, never a missing logical key.
      (doseq [indices (partition-all 16 (range chunks))]
        (let [keys (mapv #(layout/fragment-key generation %) indices)
              response (batch-get-items client table keys true)
              found (into {} (map (fn [row] [(layout/string-of (get row "Key")) row]))
                          (get (.responses ^BatchGetItemResponse response) table))]
          (doseq [[i key] (map vector indices keys)]
            (let [attr (get (get found key) "Fragment")
                  bytes (when attr (layout/bytes-of attr))
                  expected (min layout/fragment-size (- length (* i layout/fragment-size)))]
              (when-not (and bytes (= expected (alength ^bytes bytes)))
                (throw (ex-info "Missing or invalid DynamoDB blob fragment"
                                {:type :konserve.dynamodb/incomplete-blob
                                 :key (layout/string-of (get item "Key"))
                                 :fragment key})))
              (.put buf ^bytes bytes)))))
      (let [payload (.array buf)]
        (when-not (MessageDigest/isEqual ^bytes digest (layout/digest payload))
          (throw (ex-info "DynamoDB blob checksum mismatch"
                          {:type :konserve.dynamodb/corrupt-blob
                           :key (layout/string-of (get item "Key"))})))
        (assoc (merge {"Key" (get item "Key")} (layout/unpack payload))
               ::condition-meta (get item "Meta"))))
    item))

(defn- attribute-value-s
  [^String s]
  (.build (.s (AttributeValue/builder) s)))

(defn- attribute-value-b
  [^bytes b]
  (.build (.b (AttributeValue/builder) (SdkBytes/fromByteArray b))))

(defn put-item-conditional
  "PutItem only if the stored `Meta` attribute is still `expected-meta` — or, when
   that is `::absent`, only if the item does not exist. True on success, false
   when DynamoDB refuses.

   DynamoDB evaluates the condition itself, atomically with the write, which is
   what makes this backing's guarantee `:global`: it holds against every writer
   anywhere, not merely those sharing a filesystem or a heap.

   The condition compares the META attribute rather than a separate version
   column. konserve's revision lives inside the serialized metadata, so the meta
   bytes ARE the revision as far as this item is concerned — and comparing them
   needs no schema change, no second attribute to keep in step, and no migration
   for existing tables. Meta is small; the value is not compared and not sent.

   Expression attribute NAMES for both `Key` and `Meta`: `KEY` is a DynamoDB
   reserved word, and a literal in an expression would be rejected."
  [^DynamoDbClient client ^String table-name ^HashMap item expected-meta]
  (let [absent? (= ::absent expected-meta)
        ^PutItemRequest$Builder builder (-> (PutItemRequest/builder)
                                            (.tableName table-name)
                                            (.item item)
                                            (.expressionAttributeNames
                                             (if absent?
                                               (HashMap. {"#k" "Key"})
                                               (HashMap. {"#m" "Meta"})))
                                            (.conditionExpression
                                             (if absent?
                                               "attribute_not_exists(#k)"
                                               "#m = :m")))
        ^PutItemRequest request (-> (if absent?
                                      builder
                                      (.expressionAttributeValues
                                       builder
                                       (HashMap. {":m" (attribute-value-b expected-meta)})))
                                    .build)]
    (try
      (.putItem client request)
      true
      (catch ConditionalCheckFailedException _
        false))))

(defrecord DynamoDBBlob
           [table ^String key data ^clojure.lang.Atom fetched-object]

  PBackingBlob

  (-sync
    [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (let [{:keys [^bytes header ^bytes meta ^bytes value]} @data
                               expected-revision (:expected-revision env)
                               plan (when (and header meta value)
                                      (layout/plan key @data layout/item-limit (true? (:overflow-write? table))))
                               item (:item plan)]
                           (if (and header meta value)
                             (if expected-revision
                               ;; FENCED. konserve has already compared the revision
                               ;; it read against the caller's; the condition closes
                               ;; the window BETWEEN that read and this write, which
                               ;; is the half no counter can do. Both together are
                               ;; the compare-and-set.
                               ;;
                               ;; What was read is remembered by `-read-header` and
                               ;; looked up here, because `-sync` runs on a DIFFERENT
                               ;; blob record than the read did — `update-blob`
                               ;; creates its own. No entry means no read happened,
                               ;; which for a fenced write is create-if-absent.
                               (let [cache (:read-cache table)
                                     expected (get @cache key ::absent)]
                                 (try
                                   (stage-fragments! (:client table) (:table table) [plan])
                                   (when-not (put-item-conditional (:client table) (:table table)
                                                                   item expected)
                                     (throw (ex-info (str "Conditional write rejected: the stored item is not "
                                                          "the one this write was derived from.")
                                                     {:type :konserve/revision-mismatch
                                                      :key key
                                                      :expected expected-revision})))
                                   (finally
                                     ;; Whatever happened, this read is spent.
                                     (swap! cache dissoc key))))
                               (do
                                 (stage-fragments! (:client table) (:table table) [plan])
                                 (put-item (:client table) (:table table) item)))
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
                 (when (contains? @fetched-object "Format")
                   (swap! fetched-object #(hydrate-item (:client table) (:table table) %)))
                 (let [^Map fetched-obj @fetched-object
                       ^AttributeValue attr-value (get fetched-obj "Header")]
                   ;; PReadMissSafe: an absent item is an empty map (GetItem returns
                   ;; no Item), so there is no "Header". Signal not-found; io-operation's
                   ;; read-first path converts it to the caller's :not-found.
                   (when (nil? attr-value)
                     (throw (store-key-not-found-ex key)))
                   ;; Remember the META for a fenced `-sync`, and only for one. The
                   ;; read preceding a conditional write carries `:expected-revision`
                   ;; in its env, so we can tell — caching on every read would hold
                   ;; metadata for every key a store ever touched.
                   (when (:expected-revision env)
                     (when-let [^AttributeValue m (or (get @fetched-object ::condition-meta)
                                                      (get @fetched-object "Meta"))]
                       (swap! (:read-cache table) assoc key (.asByteArray ^SdkBytes (.b m)))))
                   (.asByteArray ^SdkBytes (.b attr-value))))))

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
           [^DynamoDbClient client ^String table ^java.lang.Boolean consistent-read? read-cache]

  ;; DynamoDB evaluates the condition — see `put-item-conditional` — so konserve
  ;; adds no mechanism of its own: no sidecar blob, no lock it would take.
  ;; Declared rather than inferred from the domain, since how far a guarantee
  ;; reaches and who evaluates it are separate questions.
  protocols/PSelfConditionalWrite

  protocols/PConditionalWrite
  ;; `:global`. The comparison happens inside DynamoDB, atomically with the write.
  (-conditional-write-domain [_] :global)

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
                (go-try-
                 (when-not (table-exists? client table)
                   (create-dynamodb-table client table env))
                 (log/info :konserve.dynamodb/table-created "DynamoDB table created."))))

  (-store-exists?
    [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (table-exists? client table))))

  (-sync-store
    [_ env]
    (if (:sync? env) nil (go-try- nil)))

  (-delete-store
    [_ env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try-
                 (try
                   (delete-dynamodb-table client table)
                   (log/info :konserve.dynamodb/store-deleted "DynamoDB store deleted.")
                   (catch ResourceNotFoundException _
                     (log/info :konserve.dynamodb/table-not-found "DynamoDB table does not exist."))))))

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
                 (loop [start nil result []]
                   (let [response (scan-table client table start)
                         keys (map #(layout/string-of (get % "Key")) (.items response))
                         result (into result (remove layout/fragment-key? keys))
                         next-key (.lastEvaluatedKey response)]
                     (if (seq next-key) (recur next-key result) result))))))

  ;; Implementation for atomic multi-key writes
  PMultiWriteBackingStore
  (-multi-write-blobs
    [this store-key-values env]
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
                     (let [;; Per-item budgets keep the complete publication under 4 MiB.
                           ;; Large payloads are staged outside this transaction; every
                           ;; logical key still changes atomically in the single request.
                           budget (min layout/item-limit
                                       (quot layout/transaction-limit (max 1 (count store-key-values))))
                           plans (mapv (fn [[key data]] (layout/plan key data budget (true? (:overflow-write? this)))) store-key-values)
                           _ (stage-fragments! client table plans)
                           ^ArrayList transact-items (ArrayList.)
                           _ (doseq [{:keys [item]} plans]
                               (let [^Put request (-> (Put/builder) (.tableName table) (.item ^Map item) .build)]
                                 (.add transact-items
                                       (-> (TransactWriteItem/builder)
                                           (.put request)
                                           .build))))
                           _ (when (seq plans) (transact-write-items client transact-items))
                  ;; If we get here, all writes succeeded
                  ;; Create a result map with all keys mapping to true
                           results (into {} (map (fn [[store-key _]] [store-key true]) store-key-values))]
                       results)
            ;; Handle any transaction errors
                     (catch Exception e
                       (log/warn :konserve.dynamodb/transact-write-failed {:message (.getMessage e)})
                       (throw (ex-info "DynamoDB TransactWriteItems failed"
                                       {:type :konserve.dynamodb/transaction-failed
                                        :reason "Transaction failed"}
                                       e))))))))

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
                         (log/warn :konserve.dynamodb/transact-delete-failed {:message (.getMessage e)})
                         (throw (ex-info "DynamoDB TransactWriteItems (delete) failed"
                                         {:type :konserve.dynamodb/transaction-failed
                                          :reason "Transaction failed"}
                                         e)))))))))

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
                         (log/warn :konserve.dynamodb/batch-get-failed {:message (.getMessage e)})
                         (throw (ex-info "DynamoDB BatchGetItem failed"
                                         (merge {:type :konserve.dynamodb/batch-read-failed}
                                                (ex-data e))
                                         e))))))))))

;; DynamoDB reads are read-miss-safe: -create-blob only constructs a DynamoDBBlob
;; (no side effect), and -read-header throws store-key-not-found-ex when GetItem
;; returns no item (empty map, no "Header"). So io-operation skips the -blob-exists?
;; probe — a read is one GetItem, and update-in/assoc-in/bassoc drop their probe too.
(extend-type DynamoDBStore
  PReadMissSafe)

(defn connect-store
  [dynamodb-spec & {:keys [opts]
                    :as params}]
  (let [overflow-write? (get dynamodb-spec :overflow-write? false)
        _ (when-not (boolean? overflow-write?)
            (throw (ex-info ":overflow-write? must be boolean" {:overflow-write? overflow-write?})))
        complete-opts (merge {:sync? true :read-capacity 5 :write-capacity 5} opts)
        ^DynamoDbClient client (dynamodb-client dynamodb-spec)
        ^String table-name (:table dynamodb-spec)
        ^java.lang.Boolean consistent-read? (or (:consistent-read? dynamodb-spec) false)
        backing (assoc (DynamoDBStore. client table-name consistent-read? (atom {}))
                       :konserve/max-multi-write-items 100
                       :overflow-write? overflow-write?)
        config (merge {:opts               complete-opts
                       :config             {:sync-blob? true
                                            :in-place? true
                                            :no-backup? true
                                            :lock-blob? true}
                       :buffer-size        (* 1024 1024)}
                      (dissoc params :opts :config))
        ;; `:config` IS forwarded now. It used to be dissoc'd, so the literal
        ;; default always won and a caller could not configure compression or
        ;; encryption at all -- the blob header carried a 0 whatever they
        ;; asked for. Merged onto the defaults rather than replacing them, so
        ;; a partial `:config` keeps the rest.
        ;;
        ;; Normalised BEFORE our own serializer default is filled: emitting
        ;; `:default-serializer` would trip konserve's deprecation warning on
        ;; every connect whatever the caller passed, and filling first would
        ;; let it occupy the slot and silently drop a caller's older spelling.
        config (-> config
                   (assoc :config (merge {:sync-blob? true
                                          :in-place? true
                                          :no-backup? true
                                          :lock-blob? true}
                                         (:config params)))
                   normalize-store-config
                   (update-in [:config :encoding]
                              #(merge {:serializer :FressianSerializer} %)))]
    (connect-default-store backing config)))

(defn delete-store
  [dynamodb-spec & {:keys [opts]}]
  (let [complete-opts (merge {:sync? true} opts)
        backing (DynamoDBStore. (dynamodb-client dynamodb-spec)
                                (:table dynamodb-spec)
                                (or (:consistent-read? dynamodb-spec) false)
                                (atom {}))]
    (-delete-store backing complete-opts)))

(defn store-exists?
  [dynamodb-spec & {:keys [opts]}]
  (let [complete-opts (merge {:sync? true} opts)
        backing (DynamoDBStore. (dynamodb-client dynamodb-spec)
                                (:table dynamodb-spec)
                                (or (:consistent-read? dynamodb-spec) false)
                                (atom {}))]
    (konserve.impl.storage-layout/-store-exists? backing complete-opts)))

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

;; =============================================================================
;; Multimethod Registration for konserve.store dispatch
;; =============================================================================

(defmethod store/-connect-store :dynamodb
  [{:keys [region table access-key secret consistent-read?] :as config} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (let [dynamodb-spec (dissoc config :backend)
                     exists (store-exists? dynamodb-spec :opts opts)]
                 (when-not (if (:sync? opts) exists (<!! exists))
                   (throw (ex-info (str "DynamoDB table does not exist: " table)
                                   {:table table :region region :config config})))
                 (connect-store dynamodb-spec :opts (assoc opts :sync? true) :config (:config config))))))

(defmethod store/-create-store :dynamodb
  [{:keys [region table read-capacity write-capacity] :as config} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (let [dynamodb-spec (dissoc config :backend)
                     client (dynamodb-client dynamodb-spec)
                     exists (table-exists? client table)]
                 (when exists
                   (throw (ex-info (str "DynamoDB table already exists: " table)
                                   {:table table :region region :config config})))
                 ;; Create the table
                 (create-dynamodb-table client table {:read-capacity (or read-capacity 5)
                                                      :write-capacity (or write-capacity 5)})
                 (connect-store dynamodb-spec :opts (assoc opts :sync? true) :config (:config config))))))

(defmethod store/-store-exists? :dynamodb
  [{:keys [region table] :as config} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (let [dynamodb-spec (dissoc config :backend)]
                 (store-exists? dynamodb-spec)))))

(defmethod store/-delete-store :dynamodb
  [{:keys [region table] :as config} opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (let [dynamodb-spec (dissoc config :backend)]
                 (delete-store dynamodb-spec)))))

(defmethod store/-release-store :dynamodb
  [_config _store opts]
  ;; DynamoDB doesn't require explicit release, return proper async type
  (if (:sync? opts) nil (go-try- nil)))
