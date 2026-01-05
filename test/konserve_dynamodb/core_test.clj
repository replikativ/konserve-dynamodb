(ns konserve-dynamodb.core-test
  (:require
   [clojure.core.async :refer [<!!]]
   [clojure.test :refer [deftest testing]]
   [konserve-dynamodb.core :as dynamo]
   [konserve.store :as store]
   [konserve.compliance-test :refer [compliance-test]]))

;; Local DynamoDB configuration (docker run -p 8000:8000 amazon/dynamodb-local)
;; Or use: docker-compose up -d
(def dynamodb-spec
  {:endpoint "http://localhost:8000"
   :region "us-west-2"  ; required but ignored locally
   :table "konserve-dynamodb-test"
   :access-key "dummy"  ; required but ignored locally
   :secret "dummy"})

(deftest dynamodb-compliance-sync-test
  (let [spec (assoc dynamodb-spec :backend :dynamodb :table "konserve-dynamodb-sync-test" :opts {:sync? true})]
    ;; Clean up first
    (try (store/delete-store spec) (catch Exception _))
    (Thread/sleep 1000)  ; Wait for table deletion

    ;; Create and test
    (let [st (store/create-store spec)]
      (Thread/sleep 1000)  ; Wait for table to be ready
      (testing "Compliance test with synchronous store"
        (compliance-test st))
      (dynamo/release st {:sync? true})
      (store/delete-store spec))))

(deftest dynamodb-compliance-async-test
  (let [spec (assoc dynamodb-spec :backend :dynamodb :table "konserve-dynamodb-async-test" :opts {:sync? false})]
    ;; Clean up first
    (try (<!! (store/delete-store spec)) (catch Exception _))
    (Thread/sleep 1000)  ; Wait for table deletion

    ;; Create and test
    (let [st (<!! (store/create-store spec))]
      (Thread/sleep 1000)  ; Wait for table to be ready
      (testing "Compliance test with asynchronous store"
        (compliance-test st))
      (<!! (dynamo/release st {:sync? false}))
      (<!! (store/delete-store spec)))))
