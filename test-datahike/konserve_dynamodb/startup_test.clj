(ns konserve-dynamodb.startup-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :as async]
            [datahike.api :as d]
            [datahike.store :as ds]
            [datahike.query :as query]
            [datahike.versioning :as versioning]
            [datahike-lmdb.core]
            [konserve.core :as k]
            [konserve.store :as ks]
            [konserve.memory :as memory]
            [konserve.tiered :as tiered]
            [konserve-dynamodb.core :as dynamo]
            [konserve-dynamodb.datahike-test :as fixture])
  (:import [java.nio.file Files]))

(def opts {:sync? true})

(defn ready [store sync?]
  (let [result (ds/ready-store {:backend :tiered :startup-policy :heads :branch :feature
                                :frontend-config {:backend :memory} :backend-config {:backend :memory}
                                :opts {:sync? sync?}} store)
        result (if sync? result (async/<!! result))]
    (when (instance? Throwable result) (throw result))
    result))

(defn head [cid crypto?] {:meta {:datahike/commit-id cid} :config {:crypto-hash? crypto?}})

(deftest authoritative-head-refresh-and-node-invalidation
  (doseq [sync? [true false] crypto? [true false]]
    (let [front (memory/new-mem-store (atom {}) opts)
          back (memory/new-mem-store (atom {}) opts)
          st (tiered/connect-tiered-store front back :opts opts)
          keys! k/keys]
      (k/assoc front :feature (head :old crypto?) opts)
      (k/assoc front :node :cached opts)
      (k/assoc back :branches #{:db :feature} opts)
      (k/assoc back :feature (head :new crypto?) opts)
      (k/assoc back :node (if crypto? :cached :replacement) opts)
      (k/assoc back :new-node :new opts)
      (with-redefs [k/keys (fn [store options]
                             (is (identical? front store) "only the frontend may be enumerated")
                             (keys! store options))]
        (ready st sync?))
      (is (= (head :new crypto?) (k/get st :feature nil opts)))
      (is (= #{:db :feature} (k/get st :branches nil opts)))
      (is (= (if crypto? :cached ::missing) (k/get front :node ::missing opts)))
      (is (= ::missing (k/get front :new-node ::missing opts)))
      (is (= :new (k/get st :new-node nil opts)) "uncached nodes load on demand")
      (when-not crypto?
        (is (= :replacement (k/get st :node nil opts)) "recycled addresses must not return old cached data"))
      (testing "an unchanged head preserves the cache without enumeration"
        (with-redefs [k/keys (fn [& _] (throw (AssertionError. "unexpected cache scan")))]
          (ready st sync?)))
      (testing "an absent remote head removes a stale frontend head"
        (k/dissoc back :feature opts)
        (k/assoc back :branches #{:db} opts)
        (ready st sync?)
        (is (= ::missing (k/get st :feature ::missing opts)))
        (is (= #{:db} (k/get st :branches nil opts)))))))

(deftest failed-head-read-does-not-change-cache
  (let [front (memory/new-mem-store (atom {}) opts)
        back (memory/new-mem-store (atom {}) opts)
        st (tiered/connect-tiered-store front back :opts opts)
        get! k/get]
    (k/assoc front :feature (head :old false) opts)
    (k/assoc front :node :old opts)
    (with-redefs [k/get (fn [store key not-found options]
                          (if (and (identical? store back) (= key :feature))
                            (throw (ex-info "injected backend failure" {}))
                            (get! store key not-found options)))]
      (is (thrown? Exception (ready st true))))
    (is (= (head :old false) (k/get front :feature nil opts)))
    (is (= :old (k/get front :node nil opts))))
  (let [front (memory/new-mem-store (atom {}) opts)
        back (memory/new-mem-store (atom {}) opts)]
    (is (thrown? Exception (ready (tiered/connect-tiered-store front back :write-policy :frontend-only :opts opts) true)))))

(defn payload-count [db]
  (binding [query/*query-result-cache?* false] (fixture/payload-count db)))

(deftest lmdb-cache-refreshes-named-branch-and-observes-deletion
  (let [direct (fixture/config false)
        remote (:store direct)
        path (str (Files/createTempDirectory "datahike-startup-" (make-array java.nio.file.attribute.FileAttribute 0)))
        frontend {:backend :lmdb :id (:id remote) :path (str path "/cache")}
        cached (assoc direct :branch :feature
                      :store {:backend :tiered :id (:id remote) :startup-policy :heads
                              :frontend-config frontend :backend-config remote})
        conn (atom nil)]
    (try
      (d/create-database direct)
      (reset! conn (d/connect direct))
      (d/transact @conn fixture/schema)
      (d/transact @conn (fixture/values 128 4096))
      (versioning/branch! @conn :db :feature)
      (d/release @conn)
      (reset! conn nil)
      (with-redefs [dynamo/scan-table (fn [& _] (throw (AssertionError. "remote scan during heads startup")))]
        (reset! conn (d/connect cached))
        (is (= 128 (payload-count @@conn)))
        (d/release @conn)
        (reset! conn (d/connect (assoc direct :branch :feature)))
        (d/transact @conn (fixture/values 1 4096))
        (d/release @conn)
        (reset! conn (d/connect cached))
        (is (= 129 (payload-count @@conn)) "another writer's head must replace the persisted frontend head")
        (d/release @conn)
        (reset! conn (d/connect direct))
        (is (= 128 (payload-count @@conn)) "the default branch differs from the selected branch")
        (versioning/delete-branch! @conn :feature)
        ;; delete-branch! removes only the directory entry; GC later removes
        ;; the stored head. Exercise that physical absence as well.
        (k/dissoc (:store @@conn) :feature opts)
        (d/release @conn)
        (reset! conn nil)
        (is (thrown? Exception (d/connect cached))))
      (let [front (ks/connect-store frontend opts)]
        (try
          (is (= ::missing (k/get front :feature ::missing opts)))
          (is (= #{:db} (k/get front :branches nil opts)))
          (finally (ks/release-store frontend front opts))))
      (finally
        (when @conn (d/release @conn))
        (ks/delete-store remote opts)
        (ks/delete-store frontend opts)
        (clojure.java.io/delete-file path true)))))
