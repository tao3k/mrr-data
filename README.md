# MRR Data

`mrr-data` is the proposed physical data, interchange, persistence, and
distribution plane for
[`meta-relational-reasoning`](https://github.com/tao3k/meta-relational-reasoning).
It is not another graph database, query language, or semantic authority.

The design has one governing rule:

> One semantic relation model, multiple physical forms.

MRR owns semantic identities, typed relations, generations, provenance,
lineage, queries, and admission. `mrr-data` is Arrow-first: its default feature
materializes those contracts as typed Apache Arrow batches. Additional physical
forms are explicitly selected:

- default `arrow`: typed in-memory and IPC interchange;
- optional `datafusion`: execution of the admitted single-hop binary-Entity slice;
- optional `graphar`: persistent Property Graph projection contracts;
- optional `graphar-native`: the admitted maintained GraphAr C++ data path;
- optional `ipfs`: CID/DAG-CBOR content identity and snapshot manifests;
- optional `content`: verified blocks, memory store and cache/remote protocol;
- optional `snapshot`: validated snapshot publication and cold restore;
- optional `car`: CARv1 packaging and bounded import;
- optional `filesystem`: local filesystem content store;
- optional `cache`: Kache local cache.
- optional `s3`: S3-compatible remote content adapter.

The dependency direction is one way:

```text
meta-relational-reasoning  <-  mrr-data(default: Arrow)
                                      \-> DataFusion (opt-in)
                                      \-> GraphAr (opt-in)
                                      \-> CID / CAR (opt-in)
```

MRR must not depend on this repository. Arrow schemas, GraphAr internal IDs,
file paths, CIDs, and CAR layout are physical concerns and must not leak into
MRR's semantic APIs.

## Current status

The public `mrr-data` facade now defaults to `arrow`. Its dependency-light
`mrr-data-profile` crate owns namespaces and numeric versions without importing
CID, DAG-CBOR, CAR, or GraphAr native dependencies. Content identity and local
packaging remain implemented but opt-in rather than part of the default graph.

The Arrow implementation provides relation-specific Arrow
schemas and lossless complete-Fact round trips. The
batch preserves `FactId`, `RelationId`, `GenerationId`, authority, provenance,
completeness, validity, field order, and every V1 value shape. Recursive List
and Record values use Arrow's native nested arrays rather than JSON. Invalid
facts are rejected before projection; deterministic Arrow IPC file export and
bounded import are covered by example-based tests, property tests, malformed
and mutated corpora, resource-limit contracts, and a fuzz target. The native
in-process path projects borrowed values into capacity-sized Arrow builders and
constructs one typed decoder tree per batch instead of cloning columns or
downcasting every cell. A 10,000-complete-Fact Scenario guards the native
RecordBatch round trip against multi-second regressions.

The optional `mrr-data-core` manifest engine implements the content identity
boundary: schema namespace
`mrr.data.snapshot` with a separate numeric version, canonical DAG-CBOR,
CIDv1/dag-cbor/SHA-256 roots, raw/SHA-256 child CIDs, and typed rejection of
unknown profiles or self-inconsistent descriptors. It consumes MRR's semantic
generation, source snapshot, and catalog digests rather than defining a second
semantic identity system. Its physical binder consumes MRR's admitted
`CatalogBoundQuery` and a verified `SnapshotBlock`, then rejects only physical
identity drift, unavailable GraphAr projection data, or unsupported engine
features. Query typing and result admission remain owned by MRR.
Its physical output bridge accepts only rows produced under the exact bound
engine profile. Engines keep their native synchronous or asynchronous
lifecycle and return storage-neutral columns and rows without an identity
envelope; `mrr-data-core` injects the immutable MRR query binding and returns a
`CandidateQueryResult` for MRR to admit. Arrow and GraphAr adapters therefore
cannot create a parallel execution framework or result-admission authority.
The optional `mrr-data-datafusion` adapter executes one deliberately narrow
slice: exactly one outgoing hop over a two-column, non-null `Entity` relation,
`RETURN ALL`, and direct endpoint projections. It builds DataFusion expressions
directly from admitted MRR IR rather than reparsing SQL, owns no async runtime,
and returns only storage-neutral physical output. Unsupported filters, path
shapes, projection expressions, aggregation, ordering, grouping, pagination,
and `DISTINCT` fail closed. The native GraphAr suite supplies both an Arrow
round trip and the maintained GraphAr reader to this same DataFusion plan, then
requires identical candidates and identical MRR admission receipts. This is
real engine parity for the declared slice, not a claim of a general GQL query
engine.

Native GraphAr publication also returns a `GraphArQuerySource`. This immutable,
database-neutral value pins the dataset root, canonical GraphAr metadata entry
point, admitted MRR relation and endpoint roles, stable semantic identity
properties, and row counts. Preparing an existing GraphAr source can project
the same descriptor without re-entering native storage. A downstream crate may
therefore depend on `mrr-data` plus DuckDB/DuckGQL, DataFusion, or another
engine and register the exact source without this repository importing a
database, emitting SQL, or defining another engine lifecycle. Database catalog,
indexes, statistics, CSR caches, and connections remain rebuildable downstream
state; GraphAr remains the durable physical graph projection and MRR remains
the semantic authority.

With the explicit `ipfs,graphar` feature composition, downstream code can
call `admit_graphar_query_source` before registering that descriptor. The
boundary rejects a source relation outside the MRR-owned query and re-hashes
the current GraphAr metadata against the exact projection manifest CID already
carried by `BoundDataQuery`. It still creates no database connection or engine
lifecycle.

The optional `mrr-data-content` crate also exposes a provider-neutral
[cache and remote content protocol](docs/architecture/content-cache-protocol.md):
verified read-through, explicit cache admission, and remote-acknowledged publication.
Enable `cache` for Kache and `s3` for the remote adapter (`cache,s3` for both). S3 credentials and TLS policy
belong to adapters; local integration tests require no cloud account.

The optional `mrr-data-content` crate provides packaging under `car` and disk
storage under `filesystem`. Its memory and
filesystem stores derive and verify every CID from an explicit `raw` or
`dag-cbor` codec. Snapshot archives use the upstream `fvm_ipld_car` CARv1
reader/writer, while MRR Data adds single-root admission, duplicate rejection,
full referenced-child closure and length checks, exact catalog verification,
typed import budgets, and validation-before-commit. CAR block order and extra
transport blocks may change without changing the snapshot root. A 512-extra-
block scenario guards against multi-second import regressions without reducing
the corpus. A slice-only frame preflight rejects declared block/count/aggregate
limits before the upstream reader allocates block payloads.

The optional `mrr-data-graphar` crate owns the graph specialization boundary. It
admits only relations
with exactly two ordered, non-null `Entity` fields, validates every fact against
its owning MRR relation, and produces physical-ID-free edge records preserving
`EntityId`, `FactId`, `RelationId`, `GenerationId`, authority, provenance,
completeness, and validity. N-ary, nullable-endpoint, scalar-endpoint, and
invalid-fact inputs fail with typed errors. Repartitioning cannot alter these
records because GraphAr row IDs, chunks, and adjacency offsets never enter the
contract. A deterministic physical vertex index then sorts and deduplicates the
semantic endpoint set, assigns dense GraphAr-local `i64` IDs, and retains the
bidirectional mapping. The physical ID is never treated as an `EntityId`;
native writers must persist the semantic identity as a property.

The opt-in `native-graphar` feature pins an immutable revision of the
`GTrunSec/incubator-graphar` fork maintained by this project. Apache GraphAr
PR #977 was closed intentionally; Apache merge is not a gate for using our
maintained extension. It writes vertex identity properties, relation/context
edge properties, adjacency chunks, and official metadata through
`graphar-rs`/GraphAr C++; it does not implement a private GraphAr serializer.
The native reader exports upstream Arrow tables through the Arrow C Stream
interface, so Rust borrows the official buffers instead of rebuilding every
row as owned strings. An isolated 10,000-edge ASP Rust Scenario measures the
official GraphAr reader and the Rust bridge on the identical property
projection, rejects bridge P95 above 125% of the official P95, and measures MRR
semantic admission separately.
Output is staged beside the destination and atomically renamed only after every
native builder and metadata write succeeds. Admission is owned by the pinned
fork revision, cross-platform CI, and executable writer/readback Scenarios.

The repository does not yet claim:

- general GraphAr import beyond the admitted binary-Entity profile;
- an IPFS network integration;
- general schema-bound query execution beyond the admitted DataFusion slice.
- a built-in DuckDB/DuckGQL dependency or database-owned source of truth.

The canonical proposal, invariants, V1 manifest boundary, and delivery gates
are in [RFC 0001](docs/architecture/0001-mrr-data-plane.org). Its M4 delivery plan
records local acceptance for S3/Kache snapshot publication and cold restore,
whole-operation budgets and cancellation, Kache integration under concurrent
consumers, and independent SigV4/TLS conformance. The reproducible
[local S3 runner](tools/s3-conformance/README.md) needs no cloud account.
Remote CI and hosted B2/R2 acceptance are separate from these local results.

## Intended V1 scope

V1 is deliberately Arrow-first and narrow:

1. lossless relation-specific Arrow round trips;
2. trusted in-process Arrow interchange and performance evidence;
3. opt-in DataFusion execution for the first exact MRR query slice;
4. a gated GraphAr projection for graph-shaped MRR relations;
5. optional deterministic snapshot manifests and local CID/CAR packaging;
6. remote distribution only after a concrete requirement is demonstrated.

There is no `mrr-ipfs` crate in the V1 plan. Content addressing does not imply
an IPFS daemon, a public gateway, or public publication.

## Development contract

`asp-rust` is pinned as a development dependency. One shared
`mrr-data-asp-rust-build-support` owner drives the parser-native workspace
policy for every member crate. Product crates reach it only through
`[dev-dependencies]`; their normal and build graphs stay free of ASP Rust.
Package gates compose into the existing unit suites, and the facade runs the
workspace gate once. There is no second style checker or one-process-per-gate
test harness in this repository.

The complete local gate is the same gate used by CI:

```sh
cargo fmt --all -- --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --manifest-path fuzz/Cargo.toml --all -- --check
cargo check --manifest-path fuzz/Cargo.toml --locked
cargo clippy --manifest-path fuzz/Cargo.toml --all-targets --locked -- -D warnings
cargo check -p mrr-data --locked
cargo check -p mrr-data --no-default-features --locked
cargo check -p mrr-data --no-default-features --features ipfs,graphar --locked
cargo check -p mrr-data --no-default-features --features datafusion --locked
cargo test -p mrr-data --no-default-features --locked
cargo test -p mrr-data --no-default-features --features ipfs,graphar --locked
```

Facade features:

| Feature | Default | Boundary |
|---|---:|---|
| `arrow` | yes | RecordBatch and bounded Arrow IPC |
| `datafusion` | no | single-hop binary-Entity physical query execution |
| `graphar` | no | semantic Property Graph projection |
| `graphar-native` | no | maintained GraphAr C++ writer/readback path |
| `ipfs` | no | CID/DAG-CBOR identity and immutable manifests; no IPFS node transport |
| `content` | no | verified blocks, memory store and protocol; implies `ipfs` |
| `snapshot` | no | complete Arrow snapshot publication/restore; implies `content`, not CAR |
| `transfer` | no | Tokio snapshot sessions: deadline, cancellation, retry budgets and blocking local adapter |
| `car` | no | CARv1 packaging/import; implies `content` |
| `filesystem` | no | local filesystem store; implies `content` |
| `cache` | no | content protocol and Kache local cache |
| `s3` | no | content protocol and S3 remote adapter |

`default = ["arrow"]` is the only default. `cache` and `s3` imply `content`
because their protocol addresses blocks by CID; neither enables CAR or the plain
filesystem store. `datafusion` implies Arrow but does not enable CID/DAG-CBOR.
Member crates also default to no optional providers: `mrr-data-core/ipfs`,
`mrr-data-content/snapshot`, `mrr-data-content/transfer`, `mrr-data-content/car`, `mrr-data-content/filesystem`, and
`mrr-data-cache/kache,s3,blocking` are selected explicitly. Plain `snapshot` does
not require Tokio; `transfer` enables the runtime boundary, with Kache and S3
still selected independently. The `content` feature now means
the base protocol; consumers of CAR/filesystem APIs must select those features.
Run `python3 tools/check-features.py` to compile each slice and check dependency
absence as well as presence.

CI runs this contract on both Ubuntu and macOS, verifies the declared Rust 1.95
MSRV on Ubuntu, then exercises the IPC import
boundary with a bounded ASan fuzz campaign on nightly Linux. Workspace lints
forbid unsafe Rust and enable Clippy's `all` and `pedantic` groups for every
member crate.

## North star

> Arrow-native by default, GraphAr when graph-scale, content-addressed only
> when explicitly requested.
