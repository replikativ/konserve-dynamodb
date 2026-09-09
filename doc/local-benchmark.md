# Local Datahike/DynamoDB benchmark

These are historical research notes accompanying [implementation PR #7](https://github.com/replikativ/konserve-dynamodb/pull/7). Backend changes discussed below live in that PR; Datahike integration requires companion changes that were uncommitted when measured. The baseline implementation was not captured as a separate commit, so these samples are evidence rather than a fully pinned reproduction.

This is the **baseline before fragment batching and head-refresh startup**.
See the [implemented changes and new measurements](https://github.com/replikativ/konserve-dynamodb/blob/9a257e3/doc/local-benchmark-optimized.md).

Measured 2026-09-08 on DynamoDB Local 1.25.1: 48 configurations × 3 repetitions,
**144 successful samples**. [Raw samples](benchmarks/local-2026-09-08.edn) and
[benchmark harness](https://github.com/replikativ/konserve-dynamodb/blob/9a257e3/dev/konserve_dynamodb/local_bench.clj) are included.

The useful conclusions are about request structure, with timings as exploratory
measurements (three samples per configuration, small databases, one host):

- **Tiered startup is the largest avoidable read cost.** For large random values,
  fusion enabled, compression disabled, and 10 ms added delay, populated-cache
  reconnect plus query takes a median **451.8 ms / 16 remote requests** versus
  **77.7 ms / 2 requests** directly. `datahike.store/ready-store` eagerly scans and
  synchronizes on connect, even with persistent LMDB. LMDB prevents subsequent
  query misses but does not eliminate that startup work. These fixtures fit in
  memory and do not establish the benefit for large working sets.
- **Fusion trades writes for fewer read requests.** The same direct fixture
  commits in **332.9 ms / 15 requests** fused versus **245.0 ms / 9 requests**
  unfused. Fused commit records write about 4,120 KiB versus 2,064 KiB. Cold
  connect plus query is **86.3 ms** fused versus **82.1 ms** unfused: saving a
  request did not offset reading the larger fused record in this fixture.
- **Compression helps compressible data, not these large random values.** With
  fusion and 10 ms added delay, the zero-filled fixture falls from 4,120 KiB / 15
  commit requests to 25 KiB / 1 request with LZ4. The random fixture remains at
  about 4,120 KiB / 15 requests. Compression needs the overflow fallback.

The next bounded improvements to test are:

1. Stage immutable fragments using `BatchWriteItem`, retrying unprocessed items
   before publishing manifests. The measured 14-fragment fused commit could use
   one staging batch plus one publication transaction instead of 15 requests,
   absent retries. That is a request-count projection, not a measured speedup.
   AWS permits up to 25 operations and 16 MB on the wire; these 300 KiB fragments
   fit that budget even with base64 encoding. The batch itself need not be atomic
   because fresh fragments remain unreachable until publication.
   [AWS BatchWriteItem contract](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html).
2. Add a Datahike tiered-startup policy that refreshes mutable head metadata and
   loads immutable nodes on demand, instead of enumerating the complete table.
   It must preserve freshness of the branch head; simply skipping synchronization
   with `:frontend-first` reads would risk stale databases.

Neither improvement is implemented in these measurements. No Tigris backend,
AWS endpoint, fresh JVM startup, production throughput, or billing was measured.
An AWS experiment is deferred until these avoidable request patterns are addressed.

The harness uses `datahike-lmdb.core` for LMDB's Datahike codecs. The accompanying
Datahike lifecycle fixes await asynchronous release after creation and complete
all tier releases before synchronous public release returns. A partial earlier
run exposed an LMDB reconnect error while release completion was discarded; it
is excluded, as are early measurements that allowed query-result cache hits.
The complete corrected run had no failed cases.

Each cell is a median over independent databases. Durations are milliseconds. Cold means an empty persistent frontend; cached means a reopened peer with the populated LMDB frontend retained. Both run in an already started JVM.

Requests are actual DynamoDB data-plane SDK calls; KiB counts raw written item attributes, excluding wire encoding and service overhead. These are DynamoDB Local results with **added** delay per request, not AWS latency or billing estimates. Control-plane requests are excluded from counts/delay, but their elapsed time remains in the connect duration.

Fixtures: inline = 48 × 128-byte random values; large = 128 × 4,096-byte random values; compressible = 128 × 4,096-byte zero-filled values. Random values can still repeat across indexes and compress in a fused record. History is enabled and query-result caching is disabled. Two unrecorded warm-ups exercise direct and LMDB paths.

## Added request delay: 0 ms

| Fixture | Fusion | LZ4 | LMDB | Samples | Commit ms | Cold connect + query ms | Cached connect + query ms | Warm query ms | Commit requests | Cold requests | Cached requests | Commit KiB |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| compressible | no | no | no | 3 | 108.2 | 40.2 | 39.1 | 0.8 | 9.0 | 3.0 | 3.0 | 2063.7 |
| compressible | no | no | yes | 3 | 107.7 | 291.3 | 244.5 | 1.2 | 9.0 | 31.0 | 26.0 | 2064.1 |
| compressible | no | yes | no | 3 | 51.1 | 36.7 | 31.7 | 1.1 | 1.0 | 2.0 | 2.0 | 15.4 |
| compressible | no | yes | yes | 3 | 69.3 | 210.1 | 157.2 | 0.9 | 1.0 | 22.0 | 21.0 | 15.7 |
| compressible | yes | no | no | 3 | 194.8 | 60.8 | 58.8 | 0.8 | 15.0 | 2.0 | 2.0 | 4120.1 |
| compressible | yes | no | yes | 3 | 181.0 | 352.5 | 245.0 | 0.6 | 15.0 | 19.0 | 16.0 | 4120.6 |
| compressible | yes | yes | no | 3 | 32.0 | 26.5 | 30.9 | 0.4 | 1.0 | 1.0 | 1.0 | 25.4 |
| compressible | yes | yes | yes | 3 | 62.1 | 175.1 | 94.6 | 1.0 | 1.0 | 12.0 | 11.0 | 25.7 |
| inline | no | no | no | 3 | 14.5 | 33.8 | 29.2 | 0.8 | 1.0 | 2.0 | 2.0 | 32.7 |
| inline | no | no | yes | 3 | 35.1 | 151.7 | 135.6 | 0.7 | 1.0 | 22.0 | 21.0 | 33.2 |
| inline | no | yes | no | 3 | 45.9 | 35.4 | 50.1 | 0.7 | 1.0 | 2.0 | 2.0 | 30.3 |
| inline | no | yes | yes | 3 | 47.5 | 233.8 | 156.2 | 0.6 | 1.0 | 22.0 | 21.0 | 30.6 |
| inline | yes | no | no | 3 | 22.4 | 23.8 | 30.8 | 0.8 | 1.0 | 1.0 | 1.0 | 59.3 |
| inline | yes | no | yes | 3 | 24.8 | 93.2 | 82.2 | 0.9 | 1.0 | 12.0 | 11.0 | 59.8 |
| inline | yes | yes | no | 3 | 34.5 | 29.3 | 29.9 | 1.1 | 1.0 | 1.0 | 1.0 | 16.6 |
| inline | yes | yes | yes | 3 | 44.1 | 287.5 | 163.1 | 0.5 | 1.0 | 12.0 | 11.0 | 16.9 |
| large | no | no | no | 3 | 169.4 | 44.6 | 46.3 | 0.7 | 9.0 | 3.0 | 3.0 | 2063.7 |
| large | no | no | yes | 3 | 148.7 | 356.4 | 258.8 | 0.6 | 9.0 | 32.0 | 27.0 | 2064.1 |
| large | no | yes | no | 3 | 179.6 | 41.5 | 40.1 | 0.8 | 9.0 | 3.0 | 3.0 | 2062.8 |
| large | no | yes | yes | 3 | 146.0 | 360.6 | 255.2 | 1.1 | 9.0 | 31.0 | 26.0 | 2063.1 |
| large | yes | no | no | 3 | 172.8 | 59.1 | 60.5 | 0.5 | 15.0 | 2.0 | 2.0 | 4120.1 |
| large | yes | no | yes | 3 | 218.2 | 372.9 | 227.6 | 0.8 | 15.0 | 19.0 | 16.0 | 4120.5 |
| large | yes | yes | no | 3 | 244.9 | 70.2 | 72.6 | 1.4 | 15.0 | 2.0 | 2.0 | 4120.2 |
| large | yes | yes | yes | 3 | 219.2 | 347.3 | 263.3 | 0.9 | 15.0 | 19.0 | 16.0 | 4120.7 |

## Added request delay: 10 ms

| Fixture | Fusion | LZ4 | LMDB | Samples | Commit ms | Cold connect + query ms | Cached connect + query ms | Warm query ms | Commit requests | Cold requests | Cached requests | Commit KiB |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| compressible | no | no | no | 3 | 204.7 | 84.2 | 87.8 | 1.0 | 9.0 | 3.0 | 3.0 | 2063.7 |
| compressible | no | no | yes | 3 | 224.5 | 597.8 | 495.8 | 0.9 | 9.0 | 31.0 | 26.0 | 2064.1 |
| compressible | no | yes | no | 3 | 67.7 | 59.9 | 54.2 | 1.1 | 1.0 | 2.0 | 2.0 | 15.4 |
| compressible | no | yes | yes | 3 | 73.8 | 472.9 | 363.1 | 1.0 | 1.0 | 22.0 | 21.0 | 15.7 |
| compressible | yes | no | no | 3 | 323.8 | 81.0 | 80.8 | 0.6 | 15.0 | 2.0 | 2.0 | 4120.1 |
| compressible | yes | no | yes | 3 | 339.5 | 570.1 | 407.1 | 0.8 | 15.0 | 19.0 | 16.0 | 4120.5 |
| compressible | yes | yes | no | 3 | 62.3 | 44.1 | 32.3 | 0.9 | 1.0 | 1.0 | 1.0 | 25.4 |
| compressible | yes | yes | yes | 3 | 53.7 | 277.7 | 239.9 | 0.5 | 1.0 | 12.0 | 11.0 | 25.7 |
| inline | no | no | no | 3 | 31.7 | 53.2 | 52.7 | 1.2 | 1.0 | 2.0 | 2.0 | 32.7 |
| inline | no | no | yes | 3 | 32.8 | 431.9 | 391.6 | 1.0 | 1.0 | 22.0 | 21.0 | 33.2 |
| inline | no | yes | no | 3 | 72.3 | 60.5 | 48.5 | 0.7 | 1.0 | 2.0 | 2.0 | 30.3 |
| inline | no | yes | yes | 3 | 79.3 | 481.5 | 409.6 | 0.8 | 1.0 | 22.0 | 21.0 | 30.6 |
| inline | yes | no | no | 3 | 30.8 | 33.3 | 31.6 | 0.8 | 1.0 | 1.0 | 1.0 | 59.3 |
| inline | yes | no | yes | 3 | 33.2 | 239.7 | 187.1 | 0.6 | 1.0 | 12.0 | 11.0 | 59.8 |
| inline | yes | yes | no | 3 | 43.6 | 75.4 | 62.2 | 0.7 | 1.0 | 1.0 | 1.0 | 16.6 |
| inline | yes | yes | yes | 3 | 45.0 | 363.0 | 261.6 | 1.1 | 1.0 | 12.0 | 11.0 | 16.9 |
| large | no | no | no | 3 | 245.0 | 82.1 | 79.2 | 0.7 | 9.0 | 3.0 | 3.0 | 2063.7 |
| large | no | no | yes | 3 | 275.8 | 635.3 | 541.1 | 0.8 | 9.0 | 32.0 | 27.0 | 2064.1 |
| large | no | yes | no | 3 | 264.4 | 77.2 | 75.4 | 0.8 | 9.0 | 3.0 | 3.0 | 2062.8 |
| large | no | yes | yes | 3 | 244.5 | 686.7 | 558.6 | 0.9 | 9.0 | 32.0 | 27.0 | 2063.1 |
| large | yes | no | no | 3 | 332.9 | 86.3 | 77.7 | 0.9 | 15.0 | 2.0 | 2.0 | 4120.1 |
| large | yes | no | yes | 3 | 356.1 | 545.0 | 451.8 | 0.5 | 15.0 | 19.0 | 16.0 | 4120.5 |
| large | yes | yes | no | 3 | 366.2 | 87.9 | 90.0 | 0.6 | 15.0 | 2.0 | 2.0 | 4120.2 |
| large | yes | yes | yes | 3 | 384.2 | 635.3 | 435.2 | 0.8 | 15.0 | 19.0 | 16.0 | 4120.7 |
