(ns konserve-dynamodb.bench-report
  (:require [clojure.edn :as edn]
            [clojure.string :as str]))

(defn median [xs]
  (let [xs (vec (sort xs)) n (count xs)]
    (if (odd? n) (nth xs (quot n 2))
        (/ (+ (nth xs (dec (quot n 2))) (nth xs (quot n 2))) 2.0))))

(defn sum-phases [sample phases f]
  (reduce + 0 (map #(f (get-in sample [:phases %])) phases)))

(defn request-count [phase] (reduce + 0 (vals (:requests phase))))

(defn -main [input output]
  (let [samples (mapv edn/read-string (remove str/blank? (str/split-lines (slurp input))))
        groups (group-by #(select-keys % [:workload :fusion? :compression? :tier? :delay-ms]) samples)
        dimensions [:workload :fusion? :compression? :tier?]
        render-row
        (fn [[scenario rows]]
          (let [metric (fn [phases f] (median (map #(sum-phases % phases f) rows)))
                number #(format "%.1f" (double %))]
            (str "| " (name (:workload scenario)) " | "
                 (str/join " | " (map #(if (% scenario) "yes" "no") [:fusion? :compression? :tier?]))
                 " | " (count rows) " | "
                 (str/join " | "
                           (map number [(metric [:commit] :ms)
                                        (metric [:cold-connect :first-query] :ms)
                                        (metric [:cached-connect :cached-query] :ms)
                                        (metric [:warm-query] :ms)
                                        (metric [:commit] request-count)
                                        (metric [:cold-connect :first-query] request-count)
                                        (metric [:cached-connect :cached-query] request-count)
                                        (/ (metric [:commit] #(get % :write-bytes 0)) 1024.0)])) " |\n")))
        report
        (str "# Local Datahike/DynamoDB benchmark\n\n"
             "Each cell is a median over independent databases. Durations are milliseconds. "
             "Cold means an empty persistent frontend; cached means a reopened peer with "
             "the populated LMDB frontend retained. Both run in an already started JVM.\n\n"
             "Requests are actual DynamoDB data-plane SDK calls; KiB counts raw written "
             "item attributes, excluding wire encoding and service overhead. These are "
             "DynamoDB Local results with **added** delay per request, not AWS latency "
             "or billing estimates. Control-plane requests are excluded from counts/delay, "
             "but their elapsed time remains in the connect duration.\n\n"
             "Fixtures: inline = 48 × 128-byte random values; large = 128 × 4,096-byte "
             "random values; compressible = 128 × 4,096-byte zero-filled values. Random "
             "values can still repeat across indexes and compress in a fused record. "
             "History is enabled and query-result caching is disabled. Two unrecorded warm-ups exercise direct and LMDB paths.\n\n"
             (apply str
                    (for [delay (sort (distinct (map :delay-ms samples)))]
                      (str "## Added request delay: " delay " ms\n\n"
                           "| Fixture | Fusion | LZ4 | LMDB | Samples | Commit ms | Cold connect + query ms | Cached connect + query ms | Warm query ms | Commit requests | Cold requests | Cached requests | Commit KiB |\n"
                           "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n"
                           (apply str (map render-row (sort-by (fn [[scenario _]] (mapv scenario dimensions))
                                                               (filter #(= delay (:delay-ms (key %))) groups))))))))]
    (spit output report)
    (println "Wrote" output "from" (count samples) "samples")))
