# konserve-dynamodb

A [DynamoDB](https://aws.amazon.com/dynamodb/) backend for [konserve](https://github.com/replikativ/konserve). 

## Usage

Add to your dependencies:

[![Clojars Project](http://clojars.org/org.replikativ/konserve-dynamodb/latest-version.svg)](http://clojars.org/org.replikativ/konserve-dynamodb)

### Configuration

``` clojure
(require '[konserve-dynamodb.core]  ;; Registers the :dynamodb backend
         '[konserve.core :as k])

(def config
  {:backend :dynamodb
   :region "us-west-1"
   :table "konserve-demo"
   :id #uuid "550e8400-e29b-41d4-a716-446655440000"
   ;; Optional:
   :access-key "your-access-key"
   :secret "your-secret"
   :consistent-read? false  ;; Default: eventual consistency
   :overflow-write? false  ;; Enable after every reader supports overflow
   :x-ray? false})          ;; Enable AWS X-Ray tracing

(def store (k/create-store config {:sync? true}))
```

For API usage (assoc-in, get-in, delete-store, etc.), see the [konserve documentation](https://github.com/replikativ/konserve).

## Implementation Details

### Multi-key Operations

This backend supports atomic multi-key writes and deletes (`multi-assoc`, `multi-dissoc`) and batched reads (`multi-get`). Batched reads do not provide an atomic snapshot.

**All multi-key operations are limited to 100 logical keys.** DynamoDB limits
physical items to 400 KiB and transactions to 4 MiB. Writes exceeding the byte
budget use the overflow layout below. Arbitrary writes of more than 100 keys
still fail; they are never silently split into separate transactions. Multi-key
deletes remain subject to DynamoDB's aggregate transaction size limit.

| Operation | DynamoDB API | Atomicity | Limit |
|-----------|--------------|-----------|-------|
| `multi-assoc` | TransactWriteItems | Atomic (all-or-nothing) | 100 items |
| `multi-get` | BatchGetItem | Eventual/Strong consistency* | 100 items |
| `multi-dissoc` | TransactWriteItems | Atomic (all-or-nothing) | 100 items |

*Consistency for reads depends on the `:consistent-read?` option (default: `false` for eventual consistency).

Partial batch reads retry only unprocessed keys, with jittered exponential backoff
and at most eight retries. If keys remain unprocessed, the operation throws
`:konserve.dynamodb/batch-read-incomplete`; it does not return them as missing.
Single-item service errors also propagate to the caller.

### Compression and Datahike item sizes

Set `:config {:encoding {:compressor {:type :lz4}}}` in the store configuration
to compress values on both create and reconnect. Existing blobs record their
encoding in their headers and remain readable when the write configuration changes.
Compression can reduce item sizes, but does not guarantee that values fit.

Strict encoded-entry caps multiplied by fanout can bound a node, but Datahike's
current caps are too permissive for DynamoDB, especially with fused roots. This
backend now fragments oversized encoded blobs without changing their logical
addresses or Datahike's tree structure.

### Overflow layout and compatibility

Overflow reads are always supported. **Overflow writes default to disabled**;
set `:overflow-write? true` in the DynamoDB store spec after upgrading every
reader. Disabled oversized writes fail before staging fragments with
`:konserve.dynamodb/overflow-disabled` (the cause of a multi-write error).
Small writes retain the existing single-item format. Enabled oversized blobs are stored
as immutable 300 KiB fragments plus a versioned manifest at their logical key.
The envelope contains the full header, metadata, and encoded value. Reads fetch
fragments in batches with strong consistency and verify their lengths and SHA-256
checksum. Missing or corrupt fragments throw instead of appearing as absent keys.

Fragments are staged in `BatchWriteItem` requests of at most 25 items before
publishing the manifest. Unprocessed items retry with bounded exponential jitter
(up to eight retries); exhaustion raises `:konserve.dynamodb/batch-write-incomplete`.
Publication waits for every staging batch to complete. A failed staging operation
leaves the old logical value intact. Conditional writes compare the prior physical
metadata token when publishing, so a competing writer still causes rejection.
Multi-key writes lower the per-item budget when needed, stage their fragments,
then publish **all logical keys in one DynamoDB transaction**.

**Upgrade every reader before writing overflow values.** Existing inline data
remains readable, but older backend releases cannot read the new manifests.
Physical keys beginning with `konserve.fragment/` are reserved; paginated logical
key enumeration excludes them.

Old and abandoned fragments remain after overwrite or logical deletion so readers
holding older manifests remain safe. Offline reclamation is available below;
there is no online collector, so sustained workloads need maintenance windows.

The backing advertises `:konserve/max-multi-write-items 100`. The accompanying
Datahike change uses this limit to stage ordered commit dependencies in bounded
batches and publish the branch head last, while holding its existing GC guard.
A failed prefix leaves the old head intact. This does not add distributed writer
fencing or change the atomicity contract of arbitrary Konserve multi-key writes.
Tiered stores can retain complete logical blobs in their frontend cache.

Table creation currently uses provisioned capacity (5 read and 5 write capacity
units by default). To use an on-demand table, create/configure it separately and
connect to it; the backend does not yet expose billing-mode configuration.

## Datahike tiered startup

This section requires the companion Datahike changes in `store.cljc`,
`connector.cljc`, and `writing.cljc`; upgrading konserve-dynamodb alone does not
add the startup policy or bounded Datahike commit batching. The backend's overflow
support is independent of those changes.

For a write-through LMDB frontend, select the new startup policy in Datahike's
store configuration:

```clojure
{:store {:backend :tiered
         :id store-id
         :startup-policy :heads
         :write-policy :write-through
         :read-policy :frontend-first
         :frontend-config {:backend :lmdb :id store-id :path cache-path}
         :backend-config (assoc dynamodb-config :id store-id
                                :consistent-read? true :overflow-write? true)}}
```

Load `datahike-lmdb.core` for the LMDB node codecs. On connect, `:heads` fetches
`:branches` and the selected branch head from the backend, replaces cached copies,
and removes a cached head when it is absent remotely. Nodes load on demand through
Konserve's existing frontend-first path. This avoids enumerating DynamoDB's table.
The refresh follows the backend's consistency setting; use strong DynamoDB reads
for current committed heads. It is a startup refresh, not continuous subscription
or distributed writer fencing.

A non-content-addressed database may recycle node addresses. When its selected
head changes, this policy clears the frontend's logical cache before exposing the
new head; enumeration is confined to the frontend. An unchanged head preserves
that cache. With `:crypto-hash? true`, immutable nodes remain cached across head
changes. Frontends must be dedicated caches, not stores of independent data.

`:startup-policy :eager` remains the default full populate-missing sync. `:heads`
requires `:write-through` writes and `:frontend-first` reads; it cannot safely
replace frontend-only edits or populate an offline-only reader on demand.

## Offline fragment reclamation

```clojure
(require '[konserve-dynamodb.maintenance :as maintenance])

;; Dry-run is the default. Reports counts and raw reclaimable item bytes.
(maintenance/collect-fragments! config)

;; Stop ALL readers and writers for ALL databases sharing this table first.
;; Keep them stopped throughout deletion; reconnect readers afterward.
(maintenance/collect-fragments! config {:dry-run? false :quiescent? true})
```

`:quiescent?` is your assertion, not a lock acquired by the collector. A scan is
not a transaction snapshot, even with strong reads; concurrent writers or readers
holding old manifests make deletion unsafe. A dry-run during activity is only
provisional. Each invocation rescans; it never applies a stale saved report.

The collector completes a strongly consistent, paginated scan and validates
manifest versions and referenced-fragment presence before deleting any orphan.
Unknown layouts and scan failures abort before deletion. Shared generations are
retained while any logical manifest references them. An interrupted deletion
reports confirmed progress and can be retried while the table remains offline.

Reports include `:orphan-fragments`, `:reclaimable-bytes`, and
`:deleted-fragments`. Bytes exclude service storage overhead and are not a cost
estimate. The scan reads the whole table and holds key/reference metadata in
memory; deletes are sequential. This collects physical fragments only: run
Datahike's logical GC separately if obsolete logical nodes also need removal.

## Local benchmarks

With sibling `../datahike`, `../datahike-lmdb`, and `../konserve-lmdb` checkouts and DynamoDB Local on
port 8000:

```sh
clojure -M:bench-local '{:repetitions 3 :delays [0 10] :startup-policy :heads :output "/tmp/konserve-local-bench.edn"}'
```

The harness defaults to `:heads` startup; pass `:startup-policy :eager` for the
previous full-prefetch behavior. The matrix compares inline, large random, and compressible payloads; fusion,
compression, and LMDB tiering on/off; and added per-request delays. It records
commit, empty-cache reconnect/first query, warm query, and populated-cache
reconnect/query time, actual DynamoDB data-plane SDK request counts, and raw
item bytes. It uses disposable local tables and temporary LMDB directories.
Query-result caching is disabled; warm queries still benefit from loaded index nodes.
JVM startup, AWS latency, provisioned throughput, and billing are not measured.
Use the accompanying Datahike checkout changes: creation now awaits asynchronous
store release, and synchronous `d/release` waits for every tier to close. This
prevents immediate reconnects from racing cleanup of the previous LMDB handle.
The harness loads `datahike-lmdb.core` to install Datahike's node codecs in LMDB;
the generic `konserve-lmdb.store` registration alone does not encode PSS nodes.

Generate a Markdown table from the raw EDN samples:

```sh
clojure -Sdeps '{:paths ["dev"]}' -M -m konserve-dynamodb.bench-report /tmp/konserve-local-bench.edn /tmp/konserve-local-bench.md
```

See the [before/after results](doc/local-benchmark-optimized.md) and the
[original baseline](https://github.com/replikativ/konserve-dynamodb/blob/943d42827e23672a85e0e7b9b35b9da42ce80701/doc/local-benchmark.md) in
[research PR #8](https://github.com/replikativ/konserve-dynamodb/pull/8).

## Local testing

No AWS account is needed. Start DynamoDB Local with `docker compose up -d`, then
run `bin/run-unittests`. Integration tests explicitly use `http://localhost:8000`
and dummy credentials. Alternatively, run the official DynamoDB Local Java
archive with `-inMemory -sharedDb -disableTelemetry -port 8000`.

The optional sibling-Datahike integration tests cover fused/unfused overflow,
reopening, history, and failure during a commit larger than 100 nodes:
These tests and the local benchmark require the companion Datahike changes
described above; the standard backend suite does not require sibling checkouts.

```sh
clojure -Sdeps '{:deps {org.replikativ/datahike {:local/root "../datahike"}} :paths ["src" "test-datahike"]}' -M -e '(require (quote konserve-dynamodb.datahike-test)) (let [r (clojure.test/run-tests (quote konserve-dynamodb.datahike-test))] (System/exit (+ (:fail r) (:error r))))'
```

Run the complete backend and sibling-Datahike suite, including persistent LMDB
startup/freshness checks, with `bin/run-datahike-tests` (uses `:datahike-test`).

The service-failure tests do not require an emulator:

```sh
clojure -M:dev -e '(require (quote konserve-dynamodb.unit-test)) (let [r (clojure.test/run-tests (quote konserve-dynamodb.unit-test))] (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1)))'
```

DynamoDB Local checks API behavior and size limits, but ignores provisioned
throughput. These tests do not establish real AWS latency, throttling behavior,
billing, or point-in-time recovery. See [AWS local usage notes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.UsageNotes.html).

## Authentication

A [common
approach](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html)
to manage AWS credentials is to put them into the environment variables as
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to avoid storing them in plain
text or code files. Alternatively you can provide the credentials in the
`dynamodb-spec` as `:access-key` and `:secret`.

## License

Copyright © 2024-2026 Christian Weilbach

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
