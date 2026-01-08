(ns konserve-dynamodb.core-test
  (:require
   [clojure.core.async :refer [<!!]]
   [clojure.test :refer [deftest testing]]
   [konserve-dynamodb.core :as dynamo]
   [konserve.store :as store]
   [konserve.compliance-test :refer [compliance-test]])
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
