(ns konserve-dynamodb.local-bench
  "Local-only end-to-end Datahike benchmark. Added delay applies per DynamoDB
   data-plane SDK request, including retries. JVM startup is excluded."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [datahike.api :as d]
            [datahike.query :as query]
            [konserve.store :as ks]
            [datahike-lmdb.core]
            [konserve-dynamodb.core :as dynamo]
            [konserve-dynamodb.layout :as layout])
  (:import [java.lang.reflect Proxy InvocationHandler InvocationTargetException]
           [java.nio.file Files]
           [java.util UUID Random]
           [software.amazon.awssdk.services.dynamodb DynamoDbClient]))

(def data-methods #{"getItem" "batchGetItem" "putItem" "batchWriteItem" "transactWriteItems" "deleteItem" "scan"})

(defn item-bytes [items] (reduce + 0 (map layout/item-size items)))

(defn traffic [method request response]
  (case method
    "getItem" {:read-bytes (layout/item-size (.item response))}
    "batchGetItem" {:read-bytes (item-bytes (mapcat val (.responses response)))}
    "scan" {:read-bytes (item-bytes (.items response))}
    "putItem" {:write-bytes (layout/item-size (.item request))
               :fragment-puts (if (contains? (.item request) "Fragment") 1 0)
               :manifest-puts (if (contains? (.item request) "Format") 1 0)}
    "batchWriteItem" (let [items (map #(-> % .putRequest .item) (mapcat val (.requestItems request)))]
                       {:write-bytes (item-bytes items) :fragment-puts (count items)})
    "transactWriteItems" (let [items (keep #(some-> % .put .item) (.transactItems request))]
                           {:write-bytes (item-bytes items)
                            :manifest-puts (count (filter #(contains? % "Format") items))})
    {}))

(defn instrument-client [^DynamoDbClient client active stats delay-ms]
  (Proxy/newProxyInstance
   (.getClassLoader DynamoDbClient) (into-array Class [DynamoDbClient])
   (reify InvocationHandler
     (invoke [_ _ method args]
       (let [name (.getName method)
             measured? (and @active (contains? data-methods name))]
         (when measured?
           (swap! stats update-in [:requests name] (fnil inc 0))
           (when (pos? delay-ms) (Thread/sleep (long delay-ms))))
         (let [response (try (.invoke method client args)
                             (catch InvocationTargetException e (throw (.getCause e))))]
           (when measured?
             (swap! stats #(merge-with + % (traffic name (first args) response))))
           response))))))

(defn measure [active stats f]
  (reset! stats {})
  (reset! active true)
  (let [start (System/nanoTime)]
    (try
      (let [value (f)]
        [value (assoc @stats :ms (/ (- (System/nanoTime) start) 1e6))])
      (finally (reset! active false)))))

(def schema [{:db/ident :payload :db/valueType :db.type/bytes :db/cardinality :db.cardinality/one}])
(def workloads {:inline {:entries 48 :size 128 :random? true}
                :large {:entries 128 :size 4096 :random? true}
                :compressible {:entries 128 :size 4096 :random? false}})

(defn payloads [{:keys [entries size random?]}]
  (let [rng (Random. 42)]
    (mapv (fn [_] (let [bytes (byte-array size)]
                    (when random? (.nextBytes rng bytes))
                    {:payload bytes})) (range entries))))

(defn query-count [db]
  (binding [query/*query-result-cache?* false]
    (d/q '[:find (count ?e) . :where [?e :payload]] db)))

(defn run-case [{:keys [workload fusion? compression? tier? delay-ms startup-policy] :or {startup-policy :heads} :as scenario}]
  (let [id (UUID/randomUUID)
        directory (.toFile (Files/createTempDirectory "konserve-ddb-bench-" (make-array java.nio.file.attribute.FileAttribute 0)))
        frontend {:backend :lmdb :path (str (io/file directory "cache")) :id id}
        remote {:backend :dynamodb :endpoint "http://localhost:8000" :region "us-west-2"
                :access-key "dummy" :secret "dummy" :table (str "local-bench-" id) :id id
                :consistent-read? true :overflow-write? true
                :config (if compression? {:encoding {:compressor {:type :lz4}}} {})}
        store (if tier? {:backend :tiered :id id :frontend-config frontend :backend-config remote
                         :write-policy :write-through :read-policy :frontend-first :startup-policy startup-policy} remote)
        config {:store store :schema-flexibility :write :keep-history? true
                :value-caps :default :fuse-index-roots? fusion?}
        conn (atom nil) stats (atom {}) active (atom false)
        make-client dynamo/dynamodb-client
        entries (:entries (workloads workload))]
    (with-redefs [dynamo/dynamodb-client (fn [spec] (instrument-client (make-client spec) active stats delay-ms))]
      (try
        (d/create-database config)
        (reset! conn (d/connect config))
        (d/transact @conn schema)
        (let [[_ commit] (measure active stats #(d/transact @conn (payloads (workloads workload))))
              _ (d/release @conn)
              _ (reset! conn nil)
              ;; Empty the persistent frontend only for the cold-cache phase.
              _ (when tier? (ks/delete-store frontend {:sync? true}))
              [_ cold-connect] (measure active stats #(reset! conn (d/connect config)))
              [n first-query] (measure active stats #(query-count @@conn))
              [warm-n warm-query] (measure active stats #(query-count @@conn))
              _ (when-not (= entries n warm-n) (throw (ex-info "Incorrect query result" {:expected entries :actual [n warm-n]})))
              _ (d/release @conn)
              _ (reset! conn nil)
              [_ cached-connect] (measure active stats #(reset! conn (d/connect config)))
              [cached-n cached-query] (measure active stats #(query-count @@conn))]
          (when-not (= entries cached-n) (throw (ex-info "Incorrect cached query result" {:actual cached-n})))
          (assoc scenario :startup-policy (if tier? startup-policy :direct) :phases {:commit commit :cold-connect cold-connect :first-query first-query
                                                                                     :warm-query warm-query :cached-connect cached-connect :cached-query cached-query}))
        (finally
          (reset! active false)
          (when @conn (d/release @conn))
          (ks/delete-store store {:sync? true})
          (doseq [file (reverse (file-seq directory))] (io/delete-file file true)))))))

(defn -main [& [options]]
  (let [{:keys [repetitions delays output selected-workloads startup-policy]
         :or {repetitions 3 delays [0 10] output "/tmp/konserve-local-bench.edn"
              selected-workloads [:inline :large :compressible] startup-policy :heads}} (if options (edn/read-string options) {})
        scenarios (for [workload selected-workloads fusion? [false true] compression? [false true]
                        tier? [false true] delay-ms delays]
                    {:workload workload :fusion? fusion? :compression? compression? :tier? tier? :delay-ms delay-ms :startup-policy startup-policy})]
    (when-not (and (pos-int? repetitions) (every? workloads selected-workloads)
                   (every? #(and (integer? %) (<= 0 % 1000)) delays))
      (throw (ex-info "Invalid benchmark options" {})))
    ;; Exercise both storage paths before collecting measurements. The benchmark
    ;; measures warm-JVM peer reconnect, not fresh-process or Lambda startup.
    (doseq [tier? [false true]]
      (run-case {:workload :large :fusion? true :compression? true :tier? tier? :delay-ms 0}))
    (with-open [writer (io/writer output)]
      (doseq [repetition (range repetitions)
              scenario (if (even? repetition) scenarios (reverse scenarios))]
        (let [result (assoc (run-case scenario) :repetition repetition)]
          (.write writer (str (pr-str result) "\n"))
          (.flush writer)
          (println (pr-str (assoc scenario :repetition repetition :status :ok))))))
    (shutdown-agents)
    (System/exit 0)))
