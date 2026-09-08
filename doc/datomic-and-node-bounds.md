# Datomic Pro study and Datahike node bounds

Measured 2026-09-08, using Datahike's existing `:datomic` alias (peer 1.0.7622),
a matching Datomic Pro 1.0.7622 transactor, and DynamoDB Local 1.25.1. No AWS
account, cloud table, IAM role, or paid service was used.

## What the profile supports

`../datahike/deps.edn` supplies `com.datomic/peer` through `:datomic`. Existing
migration tests use `datomic:mem://`, which does not exercise DynamoDB storage.
The same profile can connect to a Pro transactor using `datomic:ddb-local://`.
It needs the additional provided-scope DynamoDB SDK, version 2.31.45 for this
peer release, and a separate transactor distribution.

Datomic Pro (formerly On-Prem) supports DynamoDB; the product name does not
restrict it to local disk or SQL. Datomic Local is a different product, and
Datomic Cloud is a different deployment architecture. Neither is necessary for
this experiment. Datomic explicitly documents DynamoDB Local support.

Sources: [storage setup](https://docs.datomic.com/operation/storage.html),
[peer connection URIs](https://docs.datomic.com/javadoc/datomic/Peer.html).

## Local reproduction

Download the matching distribution from:
`https://datomic-pro-downloads.s3.amazonaws.com/1.0.7622/datomic-pro-1.0.7622.zip`.
Run DynamoDB Local on port 18000 with `-inMemory -sharedDb -disableTelemetry`.
Create this transactor properties file in a temporary directory:

```properties
protocol=ddb-local
host=localhost
port=14334
aws-dynamodb-table=datomic-local-study
aws-dynamodb-override-endpoint=localhost:18000
memory-index-threshold=32m
memory-index-max=256m
object-cache-max=128m
```

In the unpacked Datomic distribution, using the actual properties paths:

```sh
export AWS_ACCESS_KEY_ID=dummy
export AWS_SECRET_ACCESS_KEY=dummy
export AWS_EC2_METADATA_DISABLED=true
bin/datomic ensure-transactor /tmp/local.properties /tmp/local-ready.properties
bin/transactor -Xmx1g -Xms256m /tmp/local-ready.properties
```

In the sibling Datahike checkout:

```sh
AWS_ACCESS_KEY_ID=dummy AWS_SECRET_ACCESS_KEY=dummy AWS_EC2_METADATA_DISABLED=true \
clojure -Sdeps '{:deps {software.amazon.awssdk/dynamodb {:mvn/version "2.31.45"}}}' \
  -M:datomic ../konserve-dynamodb/dev/datomic_ddb_probe.clj
```

The probe uses only localhost and dummy credentials. It creates three databases
in the dedicated local table, waits for indexing, and scans the physical items
with pagination. Stop the temporary transactor and in-memory emulator afterward
to discard the test data. `sync-index` has a two-minute timeout, explicitly
reported if indexing does not complete.

## Observations

- One table, a String partition key named `id`, no sort key, no GSIs or LSIs.
- Three distinct Datomic databases coexist in the same table.
- 48, 128, and 1,024 distinct 4,096-byte pseudorandom values each transact
  successfully in one transaction, with query counts matching the inputs.
  Explicit background indexing completes for all three databases.
- After the run, the table contained 729 items. Of these, 720 had a `v` payload;
  every observed `v` used DynamoDB String, not Binary.
- The largest observed `v` was 63,488 UTF-8 bytes (62 KiB). This is payload size,
  not the entire item's billed size.
- Physical records included `__n` chunk counts of 1, 4, 10, and 76. Seven records
  had more than one chunk. Large logical values therefore span multiple items.

A focused `javap -c -p` inspection of the versioned peer binary confirms that
`datomic.ddb-values/put-value` calls `datomic.io/bbuf->base128`, then splits the
result into chunks of `62 * 1024` characters. `chunk-key` derives continuation
keys using a `__` suffix and a chunk number; `get-value` includes chunk assembly
and parallel mapping. These are observed private implementation details of
1.0.7622, not a supported format contract or a recommendation to copy its codec.
The local ASCII payload measurements agree with the chunk constant.

This establishes that Datomic has storage-level fragmentation. It does NOT
establish that every multi-chunk value is an index node, why 62 KiB was chosen,
or the complete crash-recovery/GC protocol. Datomic's public documentation also
describes compressed segments, but compression alone is not its only answer to
large logical values. We did not benchmark production latency or billing.

## Correcting the earlier bound argument

A maximum entry count times a maximum encoded entry size absolutely can bound a
node. The previous experiment shows the current caps are too permissive for
DynamoDB; it does not show that a static bound is impossible.

For an unbuffered leaf, a conservative condition is:

```
node-overhead + F * Emax <= item-budget
```

`F` is maximum entries, `Emax` bounds the entire encoded datom, and the item budget
reserves space for the Konserve header/metadata, key, attribute names, and any
encoding overhead. Branches additionally need child addresses/counts/separators;
opt-in diff buffers need their own bound. The whole fused record needs:

```
record-overhead + sum(root-node-bounds) <= item-budget
```

Ignoring overhead just to show scale, 400 KiB / 512 is 800 bytes per entry;
six full fused roots leave about 133 bytes per entry. These are ceilings before
overhead, not proposed settings. Keeping 4 KiB byte values with fanout 64 uses
256 KiB of value payload per full leaf. That is a plausible starting point for
that value type, but the current 4,096-element double-array cap alone permits
32 KiB per value. String character caps, tuple slots, attribute identifiers,
per-attribute overrides, and exempt values also need accounting.

A strict backend-compatible encoded-datom cap could be checked during transaction
validation, before mutating the index. Alternatively, conservative per-type caps
can establish the same bound if every allowed representation is covered. This
need not require serializing each datom in a fresh encoder on every insertion;
a proven upper-bound estimator can avoid that overhead.

## Where each alternative belongs

1. **Static caps plus existing count-based splits.** Lowest structural change.
   Choose caps/fanout at database creation; validate values early. Existing large
   nodes require migration/rebuild, not just new validation settings. Worst-case
   allocation reduces fanout even when most actual values are tiny.
2. **Size-aware fusion.** Roots already exist independently. Choose which to
   embed before removing their separate pending writes and publishing the commit.
   This avoids splitting a tree node, but cannot fix an oversized individual node.
3. **Byte-aware tree splits.** Make the split decision within PSS while changing
   the tree, then update parents normally. The sibling PSS checkout has an
   `IBoundary` insertion-split seam, but it is not a complete byte-budget feature:
   bulk construction, replacement, deletion/merging, branches, diff buffers,
   persistence, and both JVM/CLJS paths need to preserve the bound.
4. **Konserve blob chunking.** Preserve one logical node address while the backing
   stores a manifest and several physical chunks. This is the closest match to
   the observed Datomic overflow handling and avoids changing tree topology.
   It still needs versioned immutable chunks, atomic manifest publication/CAS,
   complete reads, cleanup, and explicit handling of multi-key transaction limits.

A size check just before a DynamoDB request is a useful diagnostic, not a
transparent solution. It can reject the transaction, but cannot silently replace
one logical tree node with several addressed tree nodes. PSS's storage interface
returns one address, and parents already reference that address. Ordinary flushes
queue nodes in Datahike's `pending-writes` before physical I/O; streaming bulk
builds may have written children already. In either case, publication of the
mutable database head must remain last, so rejection must not publish a broken
new database state.

Petrus's other suggestions address independent questions: single-table key
namespacing, caching/DAX, and capacity/table-class cost tuning. Those do not
remove item limits. Datomic Cloud's DynamoDB coordination plus S3 index storage
is a useful architectural comparison, but is distinct from the Pro experiment
above: [Cloud architecture](https://docs.datomic.com/operation/architecture.html).

## What we understand well enough to apply

The 400 KiB boundary need not dictate Datahike's logical node topology. Datomic's
observed storage fragmentation is direct evidence for that separation. Our
implementation uses a different binary envelope and fresh fragment generations;
it does not copy the private Datomic wire format.

Datomic's chunk-level writer uses individual item writes, not one transaction
containing the entire logical value. A leading chunk is not itself the mutable
database head. We must distinguish assembling an immutable value from publishing
a reference that makes it reachable; the exact Datomic recovery and reclamation
protocol around those operations remains outside the scope of this inspection.

A separate, publicly documented advantage is background indexing: transactions
append log data while index-segment updates are amortized across indexing jobs.
Chunking alone does not give Datahike that write-cost or latency profile.
See [index model](https://docs.datomic.com/indexes/index-model.html),
[background indexing](https://docs.datomic.com/indexes/background-indexing.html),
and [segment caching](https://docs.datomic.com/operation/valcache.html).

With a local LMDB tier already serving warm reads, the remaining DynamoDB value
proposition is chiefly cold reads/cache misses and durable writes. This is a
workload hypothesis, not a measured advantage over Tigris. AWS latency/cost
measurements and safe fragment reclamation remain necessary before recommending
this layout for sustained production use.
