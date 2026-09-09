(ns konserve-dynamodb.core-test
  (:require
   [clojure.core.async :refer [<!!]]
   [clojure.test :refer [deftest testing is]]
   [konserve-dynamodb.core :as dynamo]
   [konserve.impl.storage-layout :as sl]
   [konserve.store :as store]
   [konserve.core :as k]
   [konserve.compliance-test :refer [compliance-test
                                     conditional-write-compliance-test]])
  (:import [java.util UUID]))

;; Local DynamoDB configuration (docker run -p 8000:8000 amazon/dynamodb-local)
;; Or use: docker-compose up -d
(def dynamodb-spec
  {:endpoint "http://localhost:8000"
   :region "us-west-2"  ; required but ignored locally
   :table "konserve-dynamodb-test"
   :access-key "dummy"  ; required but ignored locally
   :secret "dummy"
   :id (UUID/randomUUID)  ; Unique store identifier
   })

(deftest dynamodb-compliance-sync-test
  (let [spec (assoc dynamodb-spec :backend :dynamodb :table "konserve-dynamodb-sync-test")]
    ;; Clean up first
    (try (store/delete-store spec {:sync? true}) (catch Exception _))
    (Thread/sleep 1000)  ; Wait for table deletion

    ;; Create and test
    (let [st (store/create-store spec {:sync? true})]
      (Thread/sleep 1000)  ; Wait for table to be ready
      (testing "Compliance test with synchronous store"
        (compliance-test st))
      (dynamo/release st {:sync? true})
      (store/delete-store spec {:sync? true}))))

(deftest dynamodb-conditional-write-test
  (testing "the `:expected-revision` contract against DynamoDB Local.

            konserve's shared contract, called rather than restated — a backend
            that restates it drifts."
    (let [spec (assoc dynamodb-spec :backend :dynamodb :table "konserve-dynamodb-cas-test")]
      (try (store/delete-store spec {:sync? true}) (catch Exception _))
      (Thread/sleep 1000)
      (let [st (store/create-store spec {:sync? true})]
        (Thread/sleep 1000)
        (try
          (is (= :global (k/conditional-write-domain st))
              "DynamoDB evaluates the condition atomically with the write")
          (conditional-write-compliance-test st)
          (finally
            (dynamo/release st {:sync? true})
            (store/delete-store spec {:sync? true})))))))

(deftest dynamodb-concurrent-fenced-counter-test
  (testing "concurrent increments converge when the caller fences and retries, and
            no update is lost.

            The contract alone cannot establish this: single-threaded, konserve's
            own `check-revision!` catches a stale token without the storage ever
            being asked to compare anything — measured on konserve-redis and
            konserve-gcs, where the contract still passed with the condition
            removed. Only a concurrent test tells an honest fence from a claimed
            one."
    (let [spec (assoc dynamodb-spec :backend :dynamodb :table "konserve-dynamodb-conc-test")]
      (try (store/delete-store spec {:sync? true}) (catch Exception _))
      (Thread/sleep 1000)
      (let [init (store/create-store spec {:sync? true})
            _ (Thread/sleep 1000)
            _ (k/assoc-in init [:counter] 0 {:sync? true})
            _ (dynamo/release init {:sync? true})
            threads 4 per-thread 8
            expected (* threads per-thread)
            conflicts (atom 0)
            unexpected (atom [])
            fs (doall
                (for [_ (range threads)]
                  (future
                    (let [st (store/connect-store spec {:sync? true})]
                      (try
                        (dotimes [_ per-thread]
                          (loop [tries 0]
                            (let [rev (k/revision st :counter {:sync? true})
                                  r (try (k/update-in st [:counter] (fnil inc 0)
                                                      {:sync? true :expected-revision rev})
                                         ::ok
                                         (catch Exception e (or (:type (ex-data e)) ::other)))]
                              (cond
                                (= ::ok r) :done
                                (= :konserve/revision-mismatch r)
                                (do (swap! conflicts inc)
                                    (if (< tries 500)
                                      (recur (inc tries))
                                      (swap! unexpected conj :retries-exhausted)))
                                :else (swap! unexpected conj r)))))
                        (finally (dynamo/release st {:sync? true})))))))]
        (doseq [f fs] @f)
        (let [fin (store/connect-store spec {:sync? true})]
          (is (empty? @unexpected) (str "unexpected failures: " (pr-str @unexpected)))
          (is (= expected (k/get-in fin [:counter] nil {:sync? true}))
              "every increment must survive")
          (is (pos? @conflicts)
              (str "the threads must actually have contended (" @conflicts "); "
                   "a run with none shows the fence held but not that it was needed"))
          (dynamo/release fin {:sync? true}))
        (store/delete-store spec {:sync? true})))))

(deftest dynamodb-compliance-async-test
  (let [spec (assoc dynamodb-spec :backend :dynamodb :table "konserve-dynamodb-async-test")]
    ;; Clean up first
    (try (<!! (store/delete-store spec {:sync? false})) (catch Exception _))
    (Thread/sleep 1000)  ; Wait for table deletion

    ;; Create and test
    (let [st (<!! (store/create-store spec {:sync? false}))]
      (Thread/sleep 1000)  ; Wait for table to be ready
      (testing "Compliance test with asynchronous store"
        (compliance-test st))
      (<!! (dynamo/release st {:sync? false}))
      (<!! (store/delete-store spec {:sync? false})))))

(deftest dynamodb-read-miss-safe-marker-test
  (testing "DynamoDB backing implements PReadMissSafe (io-operation skips the -blob-exists? probe on reads)"
    (let [spec (assoc dynamodb-spec :backend :dynamodb :table "konserve-dynamodb-marker-test")]
      (try (store/delete-store spec {:sync? true}) (catch Exception _))
      (Thread/sleep 500)
      (let [st (store/create-store spec {:sync? true})]
        (Thread/sleep 500)
        (is (satisfies? sl/PReadMissSafe (:backing st)))
        (dynamo/release st {:sync? true})
        (store/delete-store spec {:sync? true})))))

(deftest dynamodb-compression-through-store-dispatch-test
  (let [spec (assoc dynamodb-spec :backend :dynamodb
                    :table "konserve-dynamodb-encoding-test")
        compressed (assoc spec :config {:encoding {:compressor {:type :lz4}}})
        value (apply str (repeat 20000 "compressible-value"))]
    (try
      (let [st (store/create-store compressed {:sync? true})]
        (try
          ;; Larger than a normal small item; confirm the public dispatch path
          ;; actually installs compression, including in transactional writes.
          (k/assoc-in st [:single] value {:sync? true})
          (k/multi-assoc st {:multi value} {:sync? true})
          (let [items (.items (dynamo/scan-table (:client (:backing st)) (:table spec)))
                data-items (filter #(contains? % "Header") items)]
            (is (seq data-items))
            (is (every? #(pos? (aget (.asByteArray (.b (get % "Header"))) 2)) data-items)))
          (finally (dynamo/release st {:sync? true}))))
      ;; Reconnect also forwards the encoding settings for new writes.
      (let [st (<!! (store/connect-store compressed {:sync? false}))]
        (try
          (is (= value (k/get st :single nil {:sync? true})))
          (is (= value (k/get st :multi nil {:sync? true})))
          (k/assoc-in st [:reconnected] value {:sync? true})
          (let [items (.items (dynamo/scan-table (:client (:backing st)) (:table spec)))]
            (is (every? #(pos? (aget (.asByteArray (.b (get % "Header"))) 2))
                        (filter #(contains? % "Header") items))))
          (finally (dynamo/release st {:sync? true}))))
      ;; Reading compressed blobs must work without re-specifying compression.
      (let [st (store/connect-store spec {:sync? true})]
        (try
          (is (= value (k/get st :reconnected nil {:sync? true})))
          (finally (dynamo/release st {:sync? true}))))
      (finally (store/delete-store spec {:sync? true})))))

(deftest dynamodb-local-item-size-boundary-test
  (let [spec (assoc dynamodb-spec :backend :dynamodb
                    :table "konserve-dynamodb-size-test")
        st (store/create-store spec {:sync? true})
        client (:client (:backing st))
        item (fn [n]
               (java.util.HashMap.
                {"Key" (-> (software.amazon.awssdk.services.dynamodb.model.AttributeValue/builder)
                           (.s "size") .build)
                 "Value" (-> (software.amazon.awssdk.services.dynamodb.model.AttributeValue/builder)
                             (.b (software.amazon.awssdk.core.SdkBytes/fromByteArray (byte-array n)))
                             .build)}))
        ;; UTF-8 attribute names and key: 3 + 4 + 5 = 12 bytes.
        max-value (- (* 400 1024) 12)]
    (try
      (is (some? (dynamo/put-item client (:table spec) (item max-value))))
      (is (thrown-with-msg? software.amazon.awssdk.services.dynamodb.model.DynamoDbException
                            #"(?i)size"
                            (dynamo/put-item client (:table spec) (item (inc max-value)))))
      (finally
        (dynamo/release st {:sync? true})
        (store/delete-store spec {:sync? true})))))
