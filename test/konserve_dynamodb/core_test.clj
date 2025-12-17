(ns konserve-dynamodb.core-test
  (:require
   [clojure.core.async :refer [<!!]]
   [clojure.test :refer [deftest testing]]
   [konserve-dynamodb.core :refer [connect-store release delete-store]]
   [konserve.compliance-test :refer [compliance-test]]))

;; Local DynamoDB configuration (docker run -p 8000:8000 amazon/dynamodb-local)
(def dynamodb-spec
  {:endpoint "http://localhost:8000"
   :region "us-west-2"  ; required but ignored locally
   :table "konserve-dynamodb-test"
   :access-key "dummy"  ; required but ignored locally
   :secret "dummy"})

(deftest dynamodb-compliance-sync-test
  (let [dynamodb-spec (assoc dynamodb-spec :table "konserve-dynamodb-sync-test")
        _     (delete-store dynamodb-spec :opts {:sync? true})
        _ (Thread/sleep 1000)  ; Local DynamoDB is faster
        store (connect-store dynamodb-spec :opts {:sync? true})]
    (Thread/sleep 1000)
    (testing "Compliance test with synchronous store"
      (compliance-test store))
    (release store {:sync? true})
    (delete-store dynamodb-spec :opts {:sync? true})))

(deftest dynamodb-compliance-async-test
  (let [dynamodb-spec (assoc dynamodb-spec :table "konserve-dynamodb-async-test")
        _     (<!! (delete-store dynamodb-spec :opts {:sync? false}))
        _ (Thread/sleep 1000)  ; Local DynamoDB is faster
        store (<!! (connect-store dynamodb-spec :opts {:sync? false}))]
    (Thread/sleep 1000)
    (testing "Compliance test with asynchronous store"
      (compliance-test store))
    (<!! (release store {:sync? false}))
    (<!! (delete-store dynamodb-spec :opts {:sync? false}))))
