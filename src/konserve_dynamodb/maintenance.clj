(ns konserve-dynamodb.maintenance
  "Offline physical-fragment reclamation. All users of the table must be stopped
   for deletion; this is not an online garbage collector or a distributed lock."
  (:require [konserve-dynamodb.core :as dynamo]
            [konserve-dynamodb.layout :as layout])
  (:import [java.util UUID]
           [software.amazon.awssdk.services.dynamodb DynamoDbClient]))

(defn- invalid! [message key]
  (throw (ex-info message {:type :konserve.dynamodb/unsafe-reclamation :key key})))

(defn- fragment-info [item key]
  (let [[_ generation index] (re-matches #"konserve\.fragment/([0-9a-f-]+)/([0-9]+)" key)]
    (when-not (and generation
                   (try (= generation (str (UUID/fromString generation))) (catch Exception _ false))
                   (try (= index (str (Long/parseLong index))) (catch Exception _ false))
                   (= #{"Key" "Fragment"} (set (keys item)))
                   (some? (some-> (get item "Fragment") .b)))
      (invalid! "Unrecognized item in reserved fragment namespace" key))
    {:generation generation :bytes (layout/item-size item)}))

(defn- inventory [client table]
  ;; Retain only keys/sizes and manifest references, never whole fragment payloads.
  ;; Finish and validate the complete scan before issuing any deletion.
  (loop [start nil result {:scanned-items 0 :scanned-bytes 0 :manifests [] :fragments {}}]
    (let [response (dynamo/scan-table client table start true)
          result (reduce
                  (fn [acc item]
                    (let [key (layout/string-of (get item "Key"))
                          acc (-> acc (update :scanned-items inc)
                                  (update :scanned-bytes + (layout/item-size item)))]
                      (cond
                        (layout/fragment-key? key)
                        (assoc-in acc [:fragments key] (fragment-info item key))

                        (contains? item "Format")
                        (update acc :manifests conj (assoc (layout/manifest item) :key key))

                        ;; An unfamiliar logical layout might reference fragments.
                        ;; Abort rather than treating it as an unreferenced value.
                        (= #{"Key" "Header" "Meta" "Value"} (set (keys item))) acc

                        :else (invalid! "Unrecognized logical item layout" key))))
                  result (.items response))
          next-key (.lastEvaluatedKey response)]
      (if (seq next-key) (recur next-key result) result))))

(defn collect-fragments!
  "Scan a dedicated Konserve table and report unreferenced fragment bytes.
   Defaults to {:dry-run? true}. To delete, pass
   {:dry-run? false :quiescent? true}, after stopping ALL readers and writers of
   ALL databases in the table. Keep them stopped until this call finishes and
   restart readers afterward. :quiescent? is the caller's assertion, not a lock.

   Uses strong paginated scans, validates all manifests and referenced fragment
   presence before deleting, and rescans on every call. A failed deletion can be
   retried. Reports raw item bytes, not billed storage or an AWS cost estimate.
   Does not remove logical nodes; run Datahike's logical GC separately."
  ([spec] (collect-fragments! spec {}))
  ([spec {:keys [dry-run? quiescent?] :or {dry-run? true}}]
   (when-not (boolean? dry-run?)
     (throw (ex-info ":dry-run? must be boolean" {:dry-run? dry-run?})))
   (when (and (not dry-run?) (not (true? quiescent?)))
     (throw (ex-info "Offline deletion requires all table readers/writers stopped and :quiescent? true"
                     {:type :konserve.dynamodb/quiescence-required})))
   (with-open [^DynamoDbClient client (dynamo/dynamodb-client spec)]
     (let [{:keys [manifests fragments scanned-items scanned-bytes]} (inventory client (:table spec))
           referenced (into #{} (mapcat (fn [{:keys [generation chunks]}]
                                          (map #(layout/fragment-key generation %) (range chunks)))) manifests)
           _ (doseq [key referenced]
               (when-not (contains? fragments key)
                 (invalid! "A live manifest references a missing fragment" key)))
           orphans (apply dissoc fragments referenced)
           report {:dry-run? dry-run? :scanned-items scanned-items :scanned-bytes scanned-bytes
                   :live-manifests (count manifests) :referenced-fragments (count referenced)
                   :orphan-fragments (count orphans)
                   :reclaimable-bytes (reduce + 0 (map :bytes (vals orphans)))}
           deleted (atom 0)]
       (when-not dry-run?
         (try
           (doseq [key (keys orphans)]
             (dynamo/delete-item client (:table spec) {"Key" (layout/string-attr key)})
             (swap! deleted inc))
           (catch Exception e
             (throw (ex-info "Fragment reclamation interrupted; rescan and retry while still offline"
                             (assoc report :type :konserve.dynamodb/reclamation-interrupted
                                    :deleted-fragments @deleted) e)))))
       (assoc report :deleted-fragments @deleted)))))
