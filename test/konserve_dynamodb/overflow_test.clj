(ns konserve-dynamodb.overflow-test
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [konserve.store :as store]
            [konserve.impl.storage-layout :as sl]
            [konserve-dynamodb.core :as dynamo]
            [konserve-dynamodb.layout :as layout])
  (:import [java.util UUID Random Arrays]))

(def sync-opts {:sync? true})

(defn random-bytes [n]
  (doto (byte-array n) (->> (.nextBytes (Random. 42)))))

(defn with-store [f]
  (let [spec {:backend :dynamodb :endpoint "http://localhost:8000"
              :region "us-west-2" :access-key "dummy" :secret "dummy"
              :consistent-read? true :overflow-write? true :id (UUID/randomUUID)
              :table (str "overflow-test-" (UUID/randomUUID))}
        st (store/create-store spec sync-opts)]
    (try (f (assoc st ::spec spec))
         (finally (dynamo/release st sync-opts)
                  (store/delete-store spec sync-opts)))))

(defn raw-item [backing key]
  (dynamo/get-item (:client backing) (:table backing)
                   {"Key" (layout/string-attr key)} true))

(defn blob-value [backing key]
  (let [blob (sl/-create-blob backing key sync-opts)]
    (sl/-read-header blob sync-opts)
    (sl/-read-value blob 0 sync-opts)))

(defn raw-data [n]
  {:header (byte-array [1 2 3]) :meta (byte-array [4 5]) :value (random-bytes n)})

(deftest physical-layout-bounds
  (doseq [n [0 100 (- layout/item-limit 30) layout/item-limit (* 2 1024 1024)]]
    (let [data (raw-data n)
          {:keys [item fragments]} (layout/plan "key" data layout/item-limit)]
      (is (<= (layout/item-size item) layout/item-limit))
      (is (every? #(<= (layout/item-size %) layout/item-limit) fragments))
      (when (seq fragments)
        (let [out (layout/unpack (layout/pack data))]
          (is (Arrays/equals ^bytes (:value data) ^bytes (layout/bytes-of (get out "Value")))))))))

(deftest logical-overflow-roundtrip-and-cas
  (with-store
    (fn [st]
      (let [large (random-bytes (* 1100 1024))]
        (k/assoc-in st [:large] large sync-opts)
        (is (Arrays/equals large ^bytes (k/get-in st [:large] nil sync-opts)))
        (let [rev (k/revision st :large sync-opts)]
          (k/assoc-in st [:large] :small (assoc sync-opts :expected-revision rev)))
        (is (= :small (k/get-in st [:large] nil sync-opts)))
        (let [rev (k/revision st :large sync-opts)]
          (k/assoc-in st [:large] large (assoc sync-opts :expected-revision rev)))
        (let [rev (k/revision st :large sync-opts)]
          (k/assoc-in st [:large] large (assoc sync-opts :expected-revision rev)))
        (is (Arrays/equals large ^bytes (k/get-in st [:large] nil sync-opts)))
        (k/bassoc st :binary large sync-opts)
        (is (Arrays/equals large ^bytes (k/bget st :binary
                                                (fn [{:keys [input-stream]}] (.readAllBytes input-stream))
                                                sync-opts)))
        ;; The fragments make the physical scan span several 1 MiB pages.
        (is (= #{:large :binary} (set (map :key (k/keys st sync-opts)))))
        (k/dissoc st :large sync-opts)
        (is (= ::missing (k/get-in st [:large] ::missing sync-opts)))))))

(deftest overflow-publication-and-retained-generations
  (with-store
    (fn [st]
      (let [backing (:backing st)
            old (raw-data (* 1100 1024))
            put! dynamo/put-item]
        (sl/-multi-write-blobs backing {"node" old} sync-opts)
        (let [manifest (raw-item backing "node")
              generation (:generation (layout/manifest manifest))
              fragment-key (layout/fragment-key generation 0)]
          (testing "a failed staging write leaves the old logical value readable"
            (with-redefs [dynamo/batch-write-fragments!
                          (fn [client table items]
                            (put! client table (first items))
                            (throw (ex-info "injected partial staging failure" {})))]
              (is (thrown? Exception (sl/-multi-write-blobs backing {"node" (raw-data (* 1200 1024))} sync-opts))))
            (is (= manifest (raw-item backing "node"))))
          (testing "overwriting does not invalidate a reader holding the old manifest"
            (sl/-multi-write-blobs backing {"node" (raw-data 20)} sync-opts)
            (let [hydrated (#'dynamo/hydrate-item (:client backing) (:table backing) manifest)]
              (is (Arrays/equals ^bytes (:value old) ^bytes (layout/bytes-of (get hydrated "Value"))))))
          (testing "corruption and missing fragments are errors, not absent values"
            (let [fragment (raw-item backing fragment-key)
                  payload (layout/bytes-of (get fragment "Fragment"))]
              (aset-byte payload 0 (unchecked-byte (bit-xor 1 (aget payload 0))))
              (put! (:client backing) (:table backing)
                    (java.util.HashMap. (assoc (into {} fragment) "Fragment" (layout/binary-attr payload))))
              (is (= :konserve.dynamodb/corrupt-blob
                     (:type (ex-data (try (#'dynamo/hydrate-item (:client backing) (:table backing) manifest)
                                          (catch Exception e e)))))))
            (dynamo/delete-item (:client backing) (:table backing) {"Key" (layout/string-attr fragment-key)})
            (is (= :konserve.dynamodb/incomplete-blob
                   (:type (ex-data (try (#'dynamo/hydrate-item (:client backing) (:table backing) manifest)
                                        (catch Exception e e))))))))))))

(deftest large-atomic-batch
  (with-store
    (fn [st]
      (let [backing (:backing st)
            data (raw-data (* 300 1024))
            values (into {} (map (fn [i] [(str "node-" i) data]) (range 20)))
            publish! dynamo/transact-write-items
            publications (atom [])]
        (with-redefs [dynamo/transact-write-items
                      (fn [client items]
                        (swap! publications conj (mapv #(.item (.put %)) items))
                        (publish! client items))]
          (sl/-multi-write-blobs backing values sync-opts))
        (is (= 1 (count @publications)))
        (is (= 20 (count (first @publications))))
        (is (<= (reduce + (map layout/item-size (first @publications))) layout/transaction-limit))
        (doseq [key (keys values)]
          (is (Arrays/equals ^bytes (:value data) ^bytes (blob-value backing key))))
        (let [before (into {} (map (fn [key] [key (raw-item backing key)]) (keys values)))]
          (with-redefs [dynamo/transact-write-items (fn [& _] (throw (ex-info "injected publication failure" {})))]
            (is (thrown? Exception (sl/-multi-write-blobs backing values sync-opts))))
          (is (= before (into {} (map (fn [key] [key (raw-item backing key)]) (keys values))))))
        (testing "101-key atomic writes fail before staging"
          (with-redefs [dynamo/batch-write-fragments! (fn [& _] (throw (AssertionError. "unexpected staging")))]
            (is (= :not-supported
                   (:type (ex-data (try (sl/-multi-write-blobs (:backing st)
                                                               (zipmap (map str (range 101)) (repeat (raw-data 1)))
                                                               sync-opts)
                                        (catch Exception e e))))))))))))

(deftest competing-writer-during-overflow-staging
  (with-store
    (fn [st]
      (let [peer (store/connect-store (::spec st) sync-opts)
            large (random-bytes (* 1100 1024))
            conditional! dynamo/put-item-conditional]
        (try
          (k/assoc-in st [:node] large sync-opts)
          (let [revision (k/revision st :node sync-opts)]
            (with-redefs [dynamo/put-item-conditional
                          (fn [& args]
                            ;; A separate peer commits after the first peer read,
                            ;; before DynamoDB evaluates its physical CAS token.
                            (k/assoc-in peer [:node] :winner sync-opts)
                            (apply conditional! args))]
              (is (= :konserve/revision-mismatch
                     (:type (ex-data (try
                                       (k/assoc-in st [:node] large
                                                   (assoc sync-opts :expected-revision revision))
                                       (catch Exception e e))))))))
          (is (= :winner (k/get-in st [:node] nil sync-opts)))
          (finally (dynamo/release peer sync-opts)))))))
