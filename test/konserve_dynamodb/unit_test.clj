(ns konserve-dynamodb.unit-test
  "Deterministic service failures; no AWS credentials or emulator required."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve-dynamodb.core :as dynamo]
            [konserve-dynamodb.layout :as layout]
            [konserve.impl.storage-layout :as sl])
  (:import [java.lang.reflect Proxy InvocationHandler]
           [java.util HashMap]
           [software.amazon.awssdk.services.dynamodb DynamoDbClient]
           [software.amazon.awssdk.services.dynamodb.model
            AttributeValue BatchGetItemResponse BatchWriteItemResponse TransactWriteItemsResponse GetItemResponse KeysAndAttributes]))

(defn- fake-client [f]
  (Proxy/newProxyInstance
   (.getClassLoader DynamoDbClient)
   (into-array Class [DynamoDbClient])
   (reify InvocationHandler
     (invoke [_ _ method args]
       (f (.getName method) (first args))))))

(defn- key-item [k]
  {"Key" (-> (AttributeValue/builder) (.s k) .build)})

(defn- batch-response [items unprocessed]
  (-> (BatchGetItemResponse/builder)
      (.responses ^java.util.Map {"test" items})
      (.unprocessedKeys ^java.util.Map unprocessed)
      .build))

(deftest get-item-distinguishes-missing-and-failed
  (let [failure (ex-info "network failure" {:sentinel true})
        key (HashMap. (key-item "a"))]
    (is (= {} (dynamo/get-item
               (fake-client (fn [_ _] (.build (GetItemResponse/builder))))
               "test" key true)))
    (is (identical? failure
                    (try
                      (dynamo/get-item (fake-client (fn [_ _] (throw failure)))
                                       "test" key true)
                      (catch Exception e e))))))

(deftest partial-batch-read-retries-only-unprocessed-keys
  (let [requests (atom [])
        remaining (-> (KeysAndAttributes/builder)
                      (.keys ^java.util.Collection [(key-item "b")])
                      (.consistentRead true)
                      .build)
        client (fake-client
                (fn [_ request]
                  (swap! requests conj request)
                  (if (= 1 (count @requests))
                    (batch-response [(key-item "a")] {"test" remaining})
                    (batch-response [(key-item "b")] {}))))]
    (with-redefs-fn {#'dynamo/batch-read-backoff! (fn [_])}
      (fn []
        (let [response (dynamo/batch-get-items client "test" ["a" "b" "missing" "a"] true)]
          (is (= #{"a" "b"}
                 (set (map #(.s (get % "Key")) (get (.responses response) "test"))))))
        (is (= 2 (count @requests)))
        (is (= 3 (count (.keys (get (.requestItems (first @requests)) "test")))))
        (is (= {"test" remaining} (.requestItems (second @requests))))))))

(deftest incomplete-batch-is-an-error-through-backing-store
  (let [calls (atom 0)
        client (fake-client
                (fn [_ request]
                  (swap! calls inc)
                  (batch-response [] (.requestItems request))))
        backing (dynamo/->DynamoDBStore client "test" true (atom {}))]
    (with-redefs-fn {#'dynamo/batch-read-backoff! (fn [_])}
      (fn []
        (let [error (try (sl/-multi-read-blobs backing ["a"] {:sync? true})
                         (catch Exception e e))]
          (is (= :konserve.dynamodb/batch-read-incomplete (:type (ex-data error))))
          (is (= 1 (:unprocessed-count (ex-data error))))
          (is (some? (ex-cause error)))
          (is (= 9 @calls)))))))

(deftest transaction-errors-are-not-capability-errors
  (let [failure (ex-info "Item size has exceeded the maximum allowed size" {})
        client (fake-client (fn [_ _] (throw failure)))
        backing (dynamo/->DynamoDBStore client "test" true (atom {}))
        error (try
                (sl/-multi-write-blobs backing
                                       {"a" {:header (byte-array 1)
                                             :meta (byte-array 1)
                                             :value (byte-array 1)}}
                                       {:sync? true})
                (catch Exception e e))]
    (is (= :konserve.dynamodb/transaction-failed (:type (ex-data error))))
    (is (identical? failure (ex-cause error)))))

(deftest partial-fragment-batch-retries-before-publication
  (let [calls (atom [])
        requests (atom [])
        client (fake-client
                (fn [method request]
                  (swap! calls conj method)
                  (case method
                    "batchWriteItem"
                    (do (swap! requests conj request)
                        (-> (BatchWriteItemResponse/builder)
                            (.unprocessedItems ^java.util.Map
                             (if (= 1 (count @requests))
                               {"test" [(last (get (.requestItems request) "test"))]} {}))
                            .build))
                    "transactWriteItems" (.build (TransactWriteItemsResponse/builder)))))
        backing (assoc (dynamo/->DynamoDBStore client "test" true (atom {})) :overflow-write? true)]
    (with-redefs-fn {#'dynamo/batch-read-backoff! (fn [_])}
      (fn []
        (sl/-multi-write-blobs backing {"node" {:header (byte-array 1) :meta (byte-array 1)
                                                :value (byte-array (* 700 1024))}} {:sync? true})))
    (is (= ["batchWriteItem" "batchWriteItem" "transactWriteItems"] @calls))
    (is (= 3 (count (get (.requestItems (first @requests)) "test"))))
    (is (= [(last (get (.requestItems (first @requests)) "test"))]
           (get (.requestItems (second @requests)) "test")))))

(deftest exhausted-fragment-batch-never-publishes
  (let [calls (atom [])
        client (fake-client (fn [method request]
                              (swap! calls conj method)
                              (is (= "batchWriteItem" method))
                              (-> (BatchWriteItemResponse/builder)
                                  (.unprocessedItems (.requestItems request)) .build)))
        backing (assoc (dynamo/->DynamoDBStore client "test" true (atom {})) :overflow-write? true)]
    (with-redefs-fn {#'dynamo/batch-read-backoff! (fn [_])}
      (fn []
        (let [error (try (sl/-multi-write-blobs backing
                                                {"node" {:header (byte-array 1) :meta (byte-array 1)
                                                         :value (byte-array (* 700 1024))}} {:sync? true})
                         (catch Exception e e))]
          (is (= :konserve.dynamodb/batch-write-incomplete (:type (ex-data (ex-cause error)))))
          (is (= 9 (count @calls))))))))

(deftest fragment-staging-is-bounded
  (let [sizes (atom [])
        client (fake-client (fn [_ request]
                              (swap! sizes conj (count (get (.requestItems request) "test")))
                              (.build (BatchWriteItemResponse/builder))))
        generation (str (java.util.UUID/randomUUID))
        items (map (fn [i] {"Key" (layout/string-attr (layout/fragment-key generation i))
                            "Fragment" (layout/binary-attr (byte-array 1))}) (range 60))]
    (#'dynamo/stage-fragments! client "test" [{:fragments items}])
    (is (= [25 25 10] @sizes))))
