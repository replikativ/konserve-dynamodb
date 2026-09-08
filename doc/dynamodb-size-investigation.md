# Datahike size caps and DynamoDB Local

Investigation on 2026-09-08 using the sibling Datahike checkout, this backend,
Konserve 0.9.376, and DynamoDB Local 1.25.1. No AWS resources were created.

## Original reproduction (before overflow support)

Start DynamoDB Local on port 8000, then run:

```sh
clojure -Sdeps '{:deps {org.replikativ/datahike {:local/root "../datahike"}}}' -M dev/datahike_size_probe.clj
```

The probe uses dummy credentials, temporary uniquely named local tables, and
cleans them up. It uses `:value-caps :default`, `:keep-history? true`, the default
branching factor of 512, Fressian encoding without compression, and one byte
attribute. Each value contains 4,096 deterministic pseudorandom bytes. The
reported size is the largest item submitted to TransactWriteItems, counting
attribute names and String/Binary values in the backend's actual item layout.

| Fusion | Values | Largest item (bytes) | Result |
|---|---:|---:|---|
| Enabled | 48 | 792,403 | Rejected: item too large |
| Disabled | 48 | 197,915 | Succeeded |
| Disabled | 128 | 527,022 | Rejected: item too large |

DynamoDB's item limit is 409,600 bytes. A separate regression test confirms that
DynamoDB Local accepts exactly 409,600 bytes and rejects one byte more, including
the key and attribute names. These are counterexamples to the sufficiency of
per-value caps, not general performance measurements or a claim about every
Datahike schema. UUIDs and subsequent encoding changes can affect exact sizes.

## Implications for settings

Datahike's preset caps are opt-in. They permit 4,096 bytes per byte value,
4,096 elements per float/double array, and 4,096 characters per string. Array
lengths and string character counts are not serialized byte counts. Explicit
attribute `:db/maxLength` takes precedence; implicit defaults have exemptions
and apply under write-schema validation.

Existing cap overrides and a smaller `:index-config :branching-factor` can
mitigate a particular workload. A new DynamoDB preset could bundle these, but
would not by itself guarantee that the database record, metadata, or each node
fits. Compression likewise needs a fallback for incompressible input.

A static bound is possible with strict encoded-entry caps and a suitable fanout;
the current preset simply exceeds the available budget (see
[the follow-up Datomic study](datomic-and-node-bounds.md)). Another design is a
backend maximum-item capability, an encoded-byte budget
for nodes, and size-aware root fusion. The budget must include the key, header,
metadata, and attribute names after encoding. A fusion fallback must retain
separate writes for every omitted root; the current root-exclusion logic is
configuration-based. Batched root loading can reduce the extra read round trips.

## Backend findings

Implemented in this working tree:

- Propagate single-read service failures instead of returning a false absence.
- Retry only unprocessed batch-read keys, with bounded exponential jitter;
  exhaustion is an error, not a partial success. Deduplicate request keys.
- Preserve transaction errors as causes, without incorrectly labelling AWS
  rejections as unsupported multi-key operations. This misleading wrapping was
  observed in the actual size reproduction.
- Forward compression configuration through public create/connect dispatch.
  Keep the connection factory synchronous inside the outer async wrapper and
  make the existence probe respect the requested sync mode.

The subsequent implementation adds:

- Exact String/Binary item sizing and an immutable overflow-fragment layout.
  Single blobs and aggregate writes can exceed physical byte limits while
  publishing at most 100 logical items in one transaction.
- Paginated key enumeration that excludes internal fragments.
- Strong, batched fragment reads with length/checksum validation.
- A backing batch-limit capability consumed by Datahike's ordered commit path.
  Dependencies are staged in bounded batches; the branch head is published last.
  Arbitrary Konserve multi-key writes still preserve their single-transaction
  contract and reject more than 100 logical keys.

The original oversized fused and unfused fixtures now have integration tests
that transact, reopen, and query history. A low-fanout fixture exceeds 100 nodes
and injects failure into the second publication batch before reopening the old
head. See `test-datahike/konserve_dynamodb/datahike_test.clj` and the README command.
The reproduction table above records the old behavior, not the current expected
outcome of the probe.

Remaining work:

- Online fragment reclamation: old generations remain until offline maintenance.
  `konserve-dynamodb.maintenance/collect-fragments!` now supports dry-run reports
  and deletion while all table users are stopped. Logical GC is separate.
- Size-aware fusion/root prefetching and bounded parallel fragment staging could
  improve round trips; they are not prerequisites for oversized-node correctness.
- Billing-mode configuration: table creation still provisions 5 RCUs / 5 WCUs.
- Production throughput, failure recovery under real service faults, and costs.
  This change does not add distributed Datahike writer fencing.

## What still needs AWS

Local and simulated tests establish size rejection, compression round trips,
conditional writes, and response/error handling. DynamoDB Local ignores
provisioned throughput and does not reproduce production latency, partition
throttling, or PITR. A later bounded AWS experiment is useful for cost and
performance comparisons; it is not needed for these correctness fixes.

References: [AWS local limitations](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.UsageNotes.html),
[batch read semantics](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html),
[large-item strategies](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-use-s3-too.html).

## Validation of the overflow implementation

The combined backend and sibling-Datahike suite passed on DynamoDB Local:
**19 tests, 437 assertions, zero failures or errors** (2026-09-08).
This includes existing compliance and concurrent conditional writes, EDN/binary
blobs over 1 MiB, an atomic 20-key batch with 6 MiB of payload, failure during
fragment staging and manifest publication, old-generation reads, corrupt/missing
fragments, paginated logical key enumeration, and a competing peer between read
and conditional publication. Datahike tests cover both fusion modes, reopening,
history, and a commit larger than 100 nodes interrupted after its first batch.
Formatting checks and `git diff --check` passed for the changed source.


## Offline maintenance and rollout

Overflow writes now default to disabled (`:overflow-write? false`) while reads
always support manifests. Upgrade readers first, then explicitly enable writers.
Disabled writes fail during planning, before staging or publishing any item.

The offline collector completes a strong paginated scan before deletion. It
retains shared generations, aborts on unknown layouts or missing live fragments,
and reports progress if deletion is interrupted. Every invocation rescans.
`:quiescent? true` is an explicit caller assertion; it provides no online lock.
The backend/Datahike regression suite passed with these changes: 22 tests,
469 assertions, zero failures/errors. The local benchmark profile separately
loads the existing Datahike LMDB adapter to install domain codecs.


The benchmark also exposed two discarded asynchronous store-release results in
Datahike: database creation and the synchronous public release path. Creation
now awaits its release channel; public release explicitly requests synchronous
cleanup. Gated lifecycle tests demonstrate that neither call returns before
cleanup finishes. The expanded Datahike suite passed (5 tests, 53 assertions),
including current/history/as-of reads after offline fragment reclamation.

Benchmark queries explicitly disable Datahike's global query-result cache. This
keeps an earlier answer from surviving a reconnect and masking storage reads;
the ordinary index/page caches still apply. Initial partial runs before these
corrections are excluded from the final measurement artifact.

Final combined verification after the lifecycle fixes: **24 tests, 481
assertions, zero failures/errors**. Formatting checks and `git diff --check`
passed. The corrected local benchmark completed all 144 samples; see
[results and interpretation](local-benchmark.md). No AWS resources were used.


## Batched staging and head-refresh startup

Fragment staging now uses batches of at most 25 items, retries only unprocessed
items, and requires complete success before manifest publication. Datahike adds
`:startup-policy :heads` for write-through tiered caches. It refreshes the selected
head and branch directory without enumerating the authoritative store. Changed
non-crypto heads invalidate the frontend cache to handle address reuse safely.
The default `:eager` policy remains available.

Verification: **30 tests, 538 assertions, zero failures/errors**; formatting and
whitespace checks passed. The new 144-sample benchmark matrix completed with no
DynamoDB scans. See the [before/after measurements](local-benchmark-optimized.md).
