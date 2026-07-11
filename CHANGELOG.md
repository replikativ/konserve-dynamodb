# Changelog

All notable, user-visible changes to konserve-dynamodb are documented here.

## Unreleased

### Added
- **Read-miss-safe reads (one GetItem, no probe).** The DynamoDB backing implements
  konserve's `PReadMissSafe` and `-read-header` throws `store-key-not-found-ex` when
  GetItem returns no item (an absent key has no "Header"). On a konserve that supports
  the marker the redundant `-blob-exists?` GetItem probe is dropped, so a read is a
  single GetItem, and read-modify-write ops (`update-in` / `assoc-in` / `bassoc`) skip
  it too. Requires konserve `0.9.354`+.

### Changed
- konserve `0.9.342` → `0.9.354`.
- CI now runs the compliance suite (sync + async) against a DynamoDB Local service
  container, plus a smoke load-check that catches a stale konserve pin; the release is
  gated on both.
