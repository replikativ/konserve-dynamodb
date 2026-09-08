(ns konserve-dynamodb.maintenance-test
  (:require [clojure.test :refer [deftest is testing]]
            [konserve-dynamodb.core :as dynamo]
            [konserve-dynamodb.layout :as layout]
            [konserve-dynamodb.maintenance :as maintenance]
            [konserve-dynamodb.overflow-test :as fixture]
            [konserve.impl.storage-layout :as sl]
            [konserve.core :as k]
            [konserve.store :as store])
  (:import [java.util Arrays HashMap]))

(def opts {:sync? true})
(def offline {:dry-run? false :quiescent? true})

(deftest overflow-rollout-switch
  (fixture/with-store
    (fn [writer]
      (let [spec (::fixture/spec writer)
            reader (store/connect-store (dissoc spec :overflow-write?) opts)
            large (fixture/random-bytes (* 1100 1024))]
        (try
          (k/assoc-in writer [:node] large opts)
          (is (false? (-> reader :backing :overflow-write?)))
          (is (Arrays/equals large ^bytes (k/get-in reader [:node] nil opts)))
          (testing "disabled writes fail before any fragment is staged"
            (with-redefs [dynamo/batch-write-fragments! (fn [& _] (throw (AssertionError. "unexpected write")))]
              (is (= :konserve.dynamodb/overflow-disabled
                     (:type (ex-data (try (k/assoc-in reader [:node] large opts)
                                          (catch Exception e e))))))))
          (is (Arrays/equals large ^bytes (k/get-in reader [:node] nil opts)))
          (k/assoc-in reader [:small] :ok opts)
          (is (= :ok (k/get-in writer [:small] nil opts)))
          (testing "an oversized batch cannot partially publish when disabled"
            (let [data (fixture/raw-data (* 300 1024))]
              (with-redefs [dynamo/batch-write-fragments! (fn [& _] (throw (AssertionError. "unexpected staging")))
                            dynamo/transact-write-items (fn [& _] (throw (AssertionError. "unexpected publication")))]
                (let [error (try (sl/-multi-write-blobs (:backing reader)
                                                        (zipmap (map str (range 20)) (repeat data)) opts)
                                 (catch Exception e e))]
                  (is (= :konserve.dynamodb/overflow-disabled (:type (ex-data (ex-cause error)))))))))
          (finally (dynamo/release reader opts)))))))

(deftest reclaim-offline-with-shared-generations-and-retry
  (fixture/with-store
    (fn [st]
      (let [spec (::fixture/spec st) backing (:backing st)
            client (:client backing) table (:table backing)
            data (fixture/raw-data (* 1100 1024))]
        (sl/-multi-write-blobs backing {"deleted" data "live" data} opts)
        (sl/-delete-blob backing "deleted" opts)
        ;; Copies can share a generation; the remaining alias must keep it alive.
        (dynamo/put-item client table (HashMap. (assoc (fixture/raw-item backing "live")
                                                       "Key" (layout/string-attr "alias"))))
        (sl/-multi-write-blobs backing {"live" (fixture/raw-data 1)} opts)
        ;; An interrupted staging write left one fragment, with no manifest.
        (let [plan (layout/plan "abandoned" data layout/item-limit)]
          (dynamo/put-item client table (first (:fragments plan))))
        (let [report (maintenance/collect-fragments! spec)]
          (is (= 5 (:orphan-fragments report)))
          (is (= 4 (:referenced-fragments report)))
          (is (= 1 (:live-manifests report)))
          (is (pos? (:reclaimable-bytes report)))
          (is (> (:scanned-bytes report) (* 2 1024 1024)))
          (is (zero? (:deleted-fragments report)))
          (is (= report (maintenance/collect-fragments! spec))))
        (testing "deletion requires an explicit offline assertion"
          (is (= :konserve.dynamodb/quiescence-required
                 (:type (ex-data (try (maintenance/collect-fragments! spec {:dry-run? false})
                                      (catch Exception e e)))))))
        (testing "interrupted deletion reports progress and can be retried"
          (let [delete! dynamo/delete-item calls (atom 0)]
            (with-redefs [dynamo/delete-item (fn [& args]
                                               (when (= 2 (swap! calls inc))
                                                 (throw (ex-info "injected delete failure" {})))
                                               (apply delete! args))]
              (let [error (try (maintenance/collect-fragments! spec offline) (catch Exception e e))]
                (is (= :konserve.dynamodb/reclamation-interrupted (:type (ex-data error))))
                (is (= 1 (:deleted-fragments (ex-data error))))))))
        (is (= 4 (:deleted-fragments (maintenance/collect-fragments! spec offline))))
        (is (zero? (:orphan-fragments (maintenance/collect-fragments! spec))))
        (is (Arrays/equals ^bytes (:value data) ^bytes (fixture/blob-value backing "alias")))
        (sl/-delete-blob backing "alias" opts)
        (is (= 4 (:deleted-fragments (maintenance/collect-fragments! spec offline))))
        (is (zero? (:deleted-fragments (maintenance/collect-fragments! spec offline))))))))

(deftest reclamation-fails-closed
  (doseq [fault [:unknown-manifest :missing-fragment :scan-failure]]
    (fixture/with-store
      (fn [st]
        (let [spec (::fixture/spec st) backing (:backing st)
              data (fixture/raw-data (* 1100 1024))
              client (:client backing) table (:table backing)
              scan! dynamo/scan-table scans (atom 0) deletes (atom 0)]
          (sl/-multi-write-blobs backing {"live" data "orphan" data} opts)
          (sl/-delete-blob backing "orphan" opts)
          (case fault
            :unknown-manifest
            (dynamo/put-item client table
                             (HashMap. (assoc (fixture/raw-item backing "live") "Format" (layout/string-attr "future-v2"))))
            :missing-fragment
            (let [manifest (layout/manifest (fixture/raw-item backing "live"))]
              (dynamo/delete-item client table {"Key" (layout/string-attr (layout/fragment-key (:generation manifest) 0))}))
            :scan-failure nil)
          (with-redefs [dynamo/delete-item (fn [& _] (swap! deletes inc))
                        dynamo/scan-table (fn [& args]
                                            (is (true? (last args)) "maintenance scans must be strong")
                                            (when (and (= fault :scan-failure) (= 2 (swap! scans inc)))
                                              (throw (ex-info "injected scan failure" {})))
                                            (apply scan! args))]
            (is (thrown? Exception (maintenance/collect-fragments! spec offline)))
            (is (zero? @deletes))))))))
