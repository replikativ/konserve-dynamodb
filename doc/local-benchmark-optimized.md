# Batched writes and head-refresh startup

Measured 2026-09-08: **144 successful samples**, using the same fixtures and
three-repetition matrix as the [baseline](https://github.com/replikativ/konserve-dynamodb/blob/943d42827e23672a85e0e7b9b35b9da42ce80701/doc/local-benchmark.md).
[Raw samples](benchmarks/local-2026-09-08-optimized.edn) retain SDK calls and bytes
for every phase. This run uses batched fragment staging and `:startup-policy
:heads` for tiered stores. The historical baseline uses serial staging and eager
populate-missing startup. Query-result caching is disabled in both.

## Before and after

Representative medians with **10 ms added per data-plane request**, 128 random
4,096-byte values, history enabled, and compression disabled:

| Operation | Before ms | After ms | Before requests | After requests |
|---|---:|---:|---:|---:|
| Fused commit, direct DynamoDB | 332.9 | 126.5 | 15 | 2 |
| Unfused commit, direct DynamoDB | 245.0 | 88.0 | 9 | 2 |
| Fused populated-LMDB reconnect + query | 451.8 | 103.8 | 16 | 3 |
| Unfused populated-LMDB reconnect + query | 541.1 | 54.1 | 27 | 2 |
| Fused empty-LMDB reconnect + query | 545.0 | 106.6 | 19 | 3 |
| Unfused empty-LMDB reconnect + query | 635.3 | 94.9 | 32 | 4 |

The new matrix made **zero DynamoDB Scan requests**. Large commits now stage
fragments with one BatchWriteItem request in these fixtures, then publish logical
keys in one transaction. Logical byte volume is essentially unchanged; request
batching does not establish a reduction in billed storage or per-item capacity.

Head-refresh startup reads the branch directory and selected head, replaces stale
cached heads, and loads nodes on demand. An absent backend head removes the cached
copy. For non-content-addressed databases, a changed head invalidates the frontend
cache so recycled addresses cannot serve stale values. Content-addressed nodes
remain cached across head changes. The benchmark's populated-cache phases reopen
an **unchanged head**: changed-head invalidation cost is not measured here.

`:startup-policy :eager` remains the default. Enable `:heads` on a write-through,
frontend-first tiered store, and use `:consistent-read? true` on DynamoDB for current
committed heads. This refreshes on connect; it is not continuous synchronization
or distributed writer fencing. See [configuration and limits](../README.md).

## Validation and limits

The historical baseline and Datomic investigation are maintained separately in
[research PR #8](https://github.com/replikativ/konserve-dynamodb/pull/8).
The companion Datahike changes were uncommitted when measured; an exact Datahike
revision is not yet available, so this is not a fully pinned reproduction.

The combined suite passed **30 tests, 538 assertions, zero failures/errors**.
Coverage includes partial fragment retries, retry exhaustion without publication,
25-item staging boundaries, synchronous/asynchronous head refresh, named branches,
changed heads from another writer, physically deleted heads, and reused-address
cache invalidation. The existing atomicity, conditional-write, overflow,
reclamation, history and lifecycle tests also passed.

These are exploratory three-sample medians on DynamoDB Local in a warm JVM.
The databases fit in memory. The request-count reductions are directly measured;
the precise latency ratios are not production guarantees. AWS, Tigris, fresh-JVM
startup, throttling, and billing were not measured. No AWS resources were used.

Each cell is a median over independent databases. Durations are milliseconds. Cold means an empty persistent frontend; cached means a reopened peer with the populated LMDB frontend retained. Both run in an already started JVM.

Requests are actual DynamoDB data-plane SDK calls; KiB counts raw written item attributes, excluding wire encoding and service overhead. These are DynamoDB Local results with **added** delay per request, not AWS latency or billing estimates. Control-plane requests are excluded from counts/delay, but their elapsed time remains in the connect duration.

Fixtures: inline = 48 × 128-byte random values; large = 128 × 4,096-byte random values; compressible = 128 × 4,096-byte zero-filled values. Random values can still repeat across indexes and compress in a fused record. History is enabled and query-result caching is disabled. Two unrecorded warm-ups exercise direct and LMDB paths.

## Added request delay: 0 ms

| Fixture | Fusion | LZ4 | LMDB | Samples | Commit ms | Cold connect + query ms | Cached connect + query ms | Warm query ms | Commit requests | Cold requests | Cached requests | Commit KiB |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| compressible | no | no | no | 3 | 58.2 | 38.9 | 34.1 | 0.8 | 2.0 | 3.0 | 3.0 | 2063.7 |
| compressible | no | no | yes | 3 | 78.5 | 42.3 | 31.6 | 0.7 | 2.0 | 4.0 | 2.0 | 2064.2 |
| compressible | no | yes | no | 3 | 41.6 | 35.9 | 31.9 | 1.0 | 1.0 | 2.0 | 2.0 | 15.4 |
| compressible | no | yes | yes | 3 | 40.3 | 38.6 | 33.3 | 0.7 | 1.0 | 3.0 | 2.0 | 15.7 |
| compressible | yes | no | no | 3 | 119.1 | 60.2 | 63.4 | 0.6 | 2.0 | 2.0 | 2.0 | 4120.1 |
| compressible | yes | no | yes | 3 | 128.9 | 79.8 | 87.0 | 0.5 | 2.0 | 3.0 | 3.0 | 4120.6 |
| compressible | yes | yes | no | 3 | 24.2 | 19.2 | 19.0 | 0.5 | 1.0 | 1.0 | 1.0 | 25.4 |
| compressible | yes | yes | yes | 3 | 43.4 | 49.1 | 49.7 | 1.0 | 1.0 | 2.0 | 2.0 | 25.7 |
| inline | no | no | no | 3 | 19.8 | 26.3 | 24.5 | 0.8 | 1.0 | 2.0 | 2.0 | 32.7 |
| inline | no | no | yes | 3 | 24.8 | 30.1 | 31.8 | 1.1 | 1.0 | 3.0 | 2.0 | 33.3 |
| inline | no | yes | no | 3 | 35.5 | 30.1 | 26.8 | 0.8 | 1.0 | 2.0 | 2.0 | 30.3 |
| inline | no | yes | yes | 3 | 37.6 | 50.0 | 33.8 | 0.8 | 1.0 | 3.0 | 2.0 | 30.7 |
| inline | yes | no | no | 3 | 13.5 | 22.5 | 16.2 | 0.3 | 1.0 | 1.0 | 1.0 | 59.3 |
| inline | yes | no | yes | 3 | 21.6 | 32.5 | 29.5 | 0.5 | 1.0 | 2.0 | 2.0 | 59.8 |
| inline | yes | yes | no | 3 | 20.9 | 27.5 | 23.0 | 1.1 | 1.0 | 1.0 | 1.0 | 16.6 |
| inline | yes | yes | yes | 3 | 23.2 | 30.7 | 32.6 | 0.4 | 1.0 | 2.0 | 2.0 | 16.9 |
| large | no | no | no | 3 | 69.2 | 36.8 | 36.7 | 0.9 | 2.0 | 3.0 | 3.0 | 2063.7 |
| large | no | no | yes | 3 | 76.6 | 48.5 | 29.4 | 1.2 | 2.0 | 4.0 | 2.0 | 2064.2 |
| large | no | yes | no | 3 | 97.1 | 48.1 | 38.8 | 0.8 | 2.0 | 3.0 | 3.0 | 2062.8 |
| large | no | yes | yes | 3 | 100.4 | 52.0 | 26.2 | 0.8 | 2.0 | 4.0 | 2.0 | 2063.2 |
| large | yes | no | no | 3 | 100.7 | 55.1 | 57.2 | 0.5 | 2.0 | 2.0 | 2.0 | 4120.1 |
| large | yes | no | yes | 3 | 124.7 | 81.3 | 79.5 | 0.5 | 2.0 | 3.0 | 3.0 | 4120.6 |
| large | yes | yes | no | 3 | 128.3 | 64.9 | 60.9 | 0.5 | 2.0 | 2.0 | 2.0 | 4120.2 |
| large | yes | yes | yes | 3 | 137.7 | 75.2 | 85.8 | 0.5 | 2.0 | 3.0 | 3.0 | 4120.8 |

## Added request delay: 10 ms

| Fixture | Fusion | LZ4 | LMDB | Samples | Commit ms | Cold connect + query ms | Cached connect + query ms | Warm query ms | Commit requests | Cold requests | Cached requests | Commit KiB |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| compressible | no | no | no | 3 | 93.2 | 69.9 | 78.1 | 1.1 | 2.0 | 3.0 | 3.0 | 2063.7 |
| compressible | no | no | yes | 3 | 92.8 | 90.1 | 53.9 | 1.0 | 2.0 | 4.0 | 2.0 | 2064.2 |
| compressible | no | yes | no | 3 | 49.3 | 59.7 | 56.9 | 1.0 | 1.0 | 2.0 | 2.0 | 15.4 |
| compressible | no | yes | yes | 3 | 56.0 | 68.0 | 57.3 | 1.0 | 1.0 | 3.0 | 2.0 | 15.7 |
| compressible | yes | no | no | 3 | 133.8 | 85.5 | 88.6 | 0.4 | 2.0 | 2.0 | 2.0 | 4120.1 |
| compressible | yes | no | yes | 3 | 137.2 | 112.4 | 127.9 | 0.7 | 2.0 | 3.0 | 3.0 | 4120.6 |
| compressible | yes | yes | no | 3 | 34.6 | 37.0 | 36.7 | 0.7 | 1.0 | 1.0 | 1.0 | 25.4 |
| compressible | yes | yes | yes | 3 | 55.9 | 65.2 | 62.0 | 0.5 | 1.0 | 2.0 | 2.0 | 25.7 |
| inline | no | no | no | 3 | 29.6 | 48.3 | 44.4 | 0.6 | 1.0 | 2.0 | 2.0 | 32.7 |
| inline | no | no | yes | 3 | 31.6 | 68.1 | 52.7 | 1.2 | 1.0 | 3.0 | 2.0 | 33.3 |
| inline | no | yes | no | 3 | 50.9 | 50.9 | 44.4 | 0.6 | 1.0 | 2.0 | 2.0 | 30.3 |
| inline | no | yes | yes | 3 | 50.7 | 74.0 | 56.6 | 0.9 | 1.0 | 3.0 | 2.0 | 30.7 |
| inline | yes | no | no | 3 | 31.5 | 31.1 | 32.9 | 0.4 | 1.0 | 1.0 | 1.0 | 59.3 |
| inline | yes | no | yes | 3 | 31.6 | 61.3 | 58.1 | 0.5 | 1.0 | 2.0 | 2.0 | 59.8 |
| inline | yes | yes | no | 3 | 31.6 | 31.1 | 27.9 | 0.3 | 1.0 | 1.0 | 1.0 | 16.6 |
| inline | yes | yes | yes | 3 | 42.1 | 59.9 | 58.6 | 0.3 | 1.0 | 2.0 | 2.0 | 16.9 |
| large | no | no | no | 3 | 88.0 | 74.4 | 73.4 | 1.0 | 2.0 | 3.0 | 3.0 | 2063.7 |
| large | no | no | yes | 3 | 89.0 | 94.9 | 54.1 | 1.3 | 2.0 | 4.0 | 2.0 | 2064.2 |
| large | no | yes | no | 3 | 113.3 | 76.1 | 67.9 | 0.7 | 2.0 | 3.0 | 3.0 | 2062.8 |
| large | no | yes | yes | 3 | 124.7 | 99.1 | 56.8 | 1.0 | 2.0 | 4.0 | 2.0 | 2063.1 |
| large | yes | no | no | 3 | 126.5 | 78.6 | 82.9 | 0.5 | 2.0 | 2.0 | 2.0 | 4120.1 |
| large | yes | no | yes | 3 | 151.0 | 106.6 | 103.8 | 0.5 | 2.0 | 3.0 | 3.0 | 4120.6 |
| large | yes | yes | no | 3 | 139.1 | 86.7 | 86.6 | 0.6 | 2.0 | 2.0 | 2.0 | 4120.2 |
| large | yes | yes | yes | 3 | 163.9 | 112.0 | 105.6 | 0.5 | 2.0 | 3.0 | 3.0 | 4120.8 |
