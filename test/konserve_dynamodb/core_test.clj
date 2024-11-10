(ns konserve-dynamodb.core-test
  (:require [clojure.test :refer [deftest testing]]
            [clojure.core.async :refer [<!!]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve-dynamodb.core :refer [connect-store release delete-store]]
            [konserve.core :as k]))

(def dynamodb-spec {:region "us-west-2"
                    :table "konserve-dynamodb-test"
                    :access-key (System/getenv "AWS_ACCESS_KEY_ID")
                    :secret (System/getenv "AWS_SECRET_ACCESS_KEY")})

(deftest dynamodb-compliance-sync-test
  (let [dynamodb-spec (assoc dynamodb-spec :table "konserve-dynamodb-sync-test")
        _     (delete-store dynamodb-spec :opts {:sync? true})
        _ (Thread/sleep 10000)
        store (connect-store dynamodb-spec :opts {:sync? true})]
    (Thread/sleep 10000)
    (testing "Compliance test with synchronous store"
      (compliance-test store))
    (release store {:sync? true})
    (delete-store dynamodb-spec :opts {:sync? true})))

(deftest dynamodb-compliance-async-test
  (let [dynamodb-spec (assoc dynamodb-spec :table "konserve-dynamodb-async-test")
        _     (<!! (delete-store dynamodb-spec :opts {:sync? false}))
        _ (Thread/sleep 10000)
        store (<!! (connect-store dynamodb-spec :opts {:sync? false}))]
    (Thread/sleep 10000)
    (testing "Compliance test with asynchronous store"
      (compliance-test store))
    (<!! (release store {:sync? false}))
    (<!! (delete-store dynamodb-spec :opts {:sync? false}))))
