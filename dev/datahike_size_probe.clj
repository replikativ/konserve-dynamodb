;; Run against DynamoDB Local only, with the sibling Datahike checkout:
;; clojure -Sdeps '{:deps {org.replikativ/datahike {:local/root "../datahike"}}}' -M dev/datahike_size_probe.clj
(require '[datahike.api :as d]
         '[konserve-dynamodb.core :as dynamo]
         '[konserve.store :as ks])

(defn item-bytes [item]
  ;; This backing uses only String and Binary attributes. Count UTF-8 bytes,
  ;; not the base64 representation used on the wire.
  (reduce + (for [[k v] item]
              (+ (alength (.getBytes ^String k "UTF-8"))
                 (if-let [s (.s v)]
                   (alength (.getBytes ^String s "UTF-8"))
                   (alength (.asByteArray (.b v))))))))

(defn probe [fuse? n]
  (let [cfg {:store {:backend :dynamodb :endpoint "http://localhost:8000"
                     :region "us-west-2" :access-key "dummy" :secret "dummy"
                     :table (str "cap-probe-" (java.util.UUID/randomUUID))
                     :id (java.util.UUID/randomUUID) :consistent-read? true :overflow-write? true}
             :schema-flexibility :write :keep-history? true
             :value-caps :default :fuse-index-roots? fuse?}
        conn (atom nil)
        max-item-bytes (atom 0)
        transact! dynamo/transact-write-items]
    (with-redefs [dynamo/transact-write-items
                  (fn [client items]
                    (doseq [item items :when (.put item)]
                      (swap! max-item-bytes max (item-bytes (.item (.put item)))))
                    (transact! client items))]
      (try
        (d/create-database cfg)
        (reset! conn (d/connect cfg))
        (d/transact @conn [{:db/ident :payload :db/valueType :db.type/bytes
                            :db/cardinality :db.cardinality/one}])
        (let [rng (java.util.Random. 42)]
          (d/transact @conn (mapv (fn [_]
                                    (let [value (byte-array 4096)]
                                      (.nextBytes rng value)
                                      {:payload value}))
                                  (range n))))
        {:fusion fuse? :entries n :status :ok :max-item-bytes @max-item-bytes}
        (catch Exception e
          (let [cause (last (take-while some? (iterate ex-cause e)))]
            {:fusion fuse? :entries n :status :failed
             :max-item-bytes @max-item-bytes :cause (.getMessage cause)}))
        (finally
          (when @conn (try (d/release @conn) (catch Exception _)))
          (ks/delete-store (:store cfg) {:sync? true}))))))

(doseq [[fuse? n] [[true 48] [false 48] [false 128]]]
  (prn (probe fuse? n)))
(shutdown-agents)
(System/exit 0)
