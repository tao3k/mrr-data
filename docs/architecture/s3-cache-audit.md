# S3 and Kache integration audit — 2026-09-19

Scope: the production `ContentStore` / `RemoteContentStore` protocol, Kache local
adapter, OpenDAL S3 adapter, feature boundaries and executable local regressions.
This audit preserves physical-content ownership; it adds no semantic admission or
snapshot publication authority.

## Fixed: local reads materialized oversized blocks before checking the budget

Previously `read_through` called unbounded `ContentStore::get`, then checked
`max_bytes`. A large cached block could therefore consume memory even when the
caller requested a small limit. Existing-entry validation during `put` had the
same unbounded read path.

`ContentStore::get_bounded` is now a required adapter operation; `get` is the
unbounded convenience method. The coordinator calls the bounded operation.
Memory checks before cloning; filesystem and Kache inspect the opened file's
length, then limit the read to budget plus one byte to detect concurrent growth.
Existing-entry validation uses the incoming block length as its budget. No cache
engine or eviction policy was reimplemented.

Regression evidence includes memory and warm Kache limits with no remote
fallback, and a sparse 64 MiB file rejected against a 16-byte budget.

## Verified failure and concurrency semantics

The actual production adapters are exercised through the public protocol against
an ephemeral localhost S3 wire fixture:

- A response that sends its first body byte and then stalls hits the total
  deadline. No partial block is admitted into Kache.
- A PUT that persists the object but returns an error produces no success receipt
  and no local admission. Retrying verifies the existing immutable object.
- Two concurrent conditional publications of one CID both succeed with one remote
  object. A conflicting existing object remains unchanged and is rejected.
- Missing, denied, oversized and corrupt reads remain distinct; repeated warm
  reads and reads after reopening Kache make no remote GET.

## Boundaries retained

The local wire fixture requires a SigV4 authorization header but does not verify
its cryptographic signature. It does not certify a hosted provider or a TLS
handshake. Configuration supports custom trust roots and client identities;
no paid cloud account is a prerequisite for this integration work.

Local store calls are synchronous. Applications must account for blocking disk
I/O when choosing an executor. Concurrent cold misses can download the same block
more than once; Kache key contention is reported as cache-admission failure while
valid remote bytes remain available. Quota maintenance is explicit and follows
Kache's soft-limit/grace policy. These are documented policies, not newly claimed
single-flight or hard-quota guarantees.

A remote receipt covers one block only. It does not prove snapshot closure or
publish a discovery pointer; children-before-root coordination remains with the
snapshot publication caller.

## Local results

On macOS with Rust 1.95.0: content tests 19 passed; production adapter tests
10 passed; workspace tests 95 passed and one subprocess helper ignored (its parent
runs it); workspace Clippy, formatting, and all 14 feature dependency/compile
checks passed. These are local results, not a claim that remote CI has run.

## Reproduction

```sh
cargo test -p mrr-data-content --all-features --locked
cargo test -p mrr-data-cache --all-features --locked
cargo test --workspace --features mrr-data/car,mrr-data/filesystem,mrr-data/cache,mrr-data/s3 --locked
cargo clippy --workspace --features mrr-data/car,mrr-data/filesystem,mrr-data/cache,mrr-data/s3 --all-targets --locked -- -D warnings
python3 tools/check-features.py
```

The S3 suite requires permission to bind an ephemeral localhost socket; it uses
only test credentials. Native GraphAr acceptance remains in its separate CI job.
