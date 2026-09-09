(ns konserve-dynamodb.datahike-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :as async]
            [datahike.api :as d]
            [datahike.writing :as writing]
            [konserve.core :as k]
            [konserve.store :as ks]
            [konserve-dynamodb.core :as dynamo]
            [konserve-dynamodb.maintenance :as maintenance])
  (:import [java.util UUID Random]))

(deftest ordered-commit-batching
  (doseq [sync? [true false]
          store [{:backing {:konserve/max-multi-write-items 2}}
                 {:frontend-store {:backing {:konserve/max-multi-write-items 3}}
                  :backend-store {:backing {:konserve/max-multi-write-items 2}}}]
          fail? [false true]]
    (let [writes [[:a 1] [:b 2] [:c 3] [:cid 4] [:head 5]]
          metas (zipmap [:a :b :c :cid] (repeat {:immutable? true}))
          calls (atom [])
          failure (ex-info "injected second batch failure" {})]
      (with-redefs [k/multi-assoc
                    (fn [_ batch metadata opts]
                      (swap! calls conj [(vec batch) metadata])
                      (let [result (if (and fail? (= 2 (count @calls))) failure :ok)]
                        (if (:sync? opts)
                          (if (instance? Exception result) (throw result) result)
                          (async/go result))))]
        (let [result (try (let [r (writing/write-commit-kvs! store writes metas sync?)]
                            (if sync? r (async/<!! r)))
                          (catch Exception e e))]
          (is (= (if fail? 2 3) (count @calls)))
          (is (= (if fail? (take 4 writes) writes) (mapcat first @calls)))
          (is (= metas (into {} (mapcat second @calls))))
          (when fail? (is (some #(identical? failure %) (take-while some? (iterate ex-cause result)))))))))
  (testing "unlimited stores retain one batch"
    (let [calls (atom [])]
      (with-redefs [k/multi-assoc (fn [_ writes _ _] (swap! calls conj writes))]
        (writing/write-commit-kvs! {} [[:a 1] [:head 2]] {} true)
        (is (= [[[:a 1] [:head 2]]] @calls))))))

(defn config [fuse?]
  {:store {:backend :dynamodb :endpoint "http://localhost:8000"
           :region "us-west-2" :access-key "dummy" :secret "dummy"
           :table (str "datahike-overflow-" (UUID/randomUUID))
           :id (UUID/randomUUID) :consistent-read? true :overflow-write? true}
   :schema-flexibility :write :keep-history? true
   :value-caps :default :fuse-index-roots? fuse?})

(def schema [{:db/ident :payload :db/valueType :db.type/bytes
              :db/cardinality :db.cardinality/one}])

(defn values [n size]
  (let [rng (Random. 42)]
    (mapv (fn [_] (let [value (byte-array size)]
                    (.nextBytes rng value) {:payload value})) (range n))))

(defn payload-count [db]
  (d/q '[:find (count ?e) . :where [?e :payload]] db))

(deftest fused-and-unfused-overflow
  (doseq [[fuse? n] [[true 48] [false 128]]]
    (let [cfg (config fuse?) conn (atom nil)]
      (try
        (d/create-database cfg)
        (reset! conn (d/connect cfg))
        (d/transact @conn schema)
        (d/transact @conn (values n 4096))
        (is (= n (payload-count @@conn)))
        (d/release @conn)
        (reset! conn (d/connect cfg))
        (is (= n (payload-count @@conn)) "reopening must reconstruct persisted fragments")
        (is (= n (payload-count (d/history @@conn))))
        (finally
          (when @conn (d/release @conn))
          (ks/delete-store (:store cfg) {:sync? true}))))))

(deftest commit-exceeding-one-hundred-nodes
  (let [cfg (assoc (config false) :index-config {:branching-factor 8})
        conn (atom nil)
        publish! dynamo/transact-write-items
        sizes (atom [])]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (d/transact @conn schema)
      (testing "a failed second batch preserves the durable old head"
        (let [calls (atom 0)]
          (with-redefs [dynamo/transact-write-items
                        (fn [client items]
                          (when (= 2 (swap! calls inc))
                            (throw (ex-info "injected second publication failure" {})))
                          (publish! client items))]
            (is (thrown? Exception (d/transact @conn (values 512 128)))))
          (is (= 2 @calls)))
        (d/release @conn)
        (reset! conn (d/connect cfg))
        (is (empty? (d/q '[:find ?e :where [?e :payload]] @@conn))))
      (with-redefs [dynamo/transact-write-items
                    (fn [client items]
                      (swap! sizes conj (count items))
                      (publish! client items))]
        (d/transact @conn (values 512 128)))
      (is (> (reduce + @sizes) 100))
      (is (every? #(<= % 100) @sizes))
      (d/release @conn)
      (reset! conn (d/connect cfg))
      (is (= 512 (payload-count @@conn)))
      (finally
        (when @conn (d/release @conn))
        (ks/delete-store (:store cfg) {:sync? true})))))

(deftest offline-reclamation-preserves-datahike-history
  (let [cfg (config true) conn (atom nil)]
    (try
      (d/create-database cfg)
      (reset! conn (d/connect cfg))
      (d/transact @conn schema)
      (d/transact @conn (values 48 4096))
      (let [first-t (:max-tx @@conn)]
        (d/transact @conn (values 48 4096))
        (d/release @conn)
        (reset! conn nil)
        (let [report (maintenance/collect-fragments! (:store cfg)
                                                     {:dry-run? false :quiescent? true})]
          (is (pos? (:deleted-fragments report))))
        (reset! conn (d/connect cfg))
        (is (= 96 (payload-count @@conn)))
        (is (= 96 (payload-count (d/history @@conn))))
        (is (= 48 (payload-count (d/as-of @@conn first-t)))))
      (finally
        (when @conn (d/release @conn))
        (ks/delete-store (:store cfg) {:sync? true})))))

(deftest lifecycle-awaits-store-release
  (doseq [action [:create :release]]
    (let [cfg {:store {:backend :memory :id (UUID/randomUUID)}}
          conn (when (= action :release)
                 (d/create-database cfg)
                 (d/connect cfg))
          original-release ks/release-store
          entered (promise) gate (promise) finished (promise)]
      (try
        (with-redefs [ks/release-store
                      (fn [spec st & [opts]]
                        (deliver entered true)
                        (let [close! (fn [] @gate
                                       (original-release spec st {:sync? true})
                                       (deliver finished true)
                                       :released)]
                          (if (:sync? opts) (close!) (async/thread (close!)))))]
          (let [work (future (if (= action :create) (d/create-database cfg) (d/release conn)))]
            (try
              (is (= true (deref entered 5000 ::timeout)))
              (is (= ::pending (deref work 50 ::pending))
                  "public lifecycle call must wait for resource release")
              (deliver gate true)
              (is (not= ::timeout (deref work 5000 ::timeout)))
              (is (realized? finished))
              (finally (deliver gate true)))))
        (finally
          (deliver gate true)
          (ks/delete-store (:store cfg) {:sync? true}))))))
