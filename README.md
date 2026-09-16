# MRR Data

`mrr-data` is the proposed physical data, interchange, persistence, and
distribution plane for
[`meta-relational-reasoning`](https://github.com/tao3k/meta-relational-reasoning).
It is not another graph database, query language, or semantic authority.

The design has one governing rule:

> One semantic relation model, multiple physical forms.

MRR owns semantic identities, typed relations, generations, provenance,
lineage, queries, and admission. `mrr-data` consumes those contracts and may
materialize them as:

- Apache Arrow record batches for typed in-memory and IPC interchange;
- Apache GraphAr datasets for large persistent Property Graph projections;
- content-addressed blocks and CAR archives for immutable packaging;
- filesystem or object-store objects for controlled distribution.

The dependency direction is one way:

```text
meta-relational-reasoning  <-  mrr-data  ->  Arrow / GraphAr / CID / CAR
```

MRR must not depend on this repository. Arrow schemas, GraphAr internal IDs,
file paths, CIDs, and CAR layout are physical concerns and must not leak into
MRR's semantic APIs.

## Current status

This repository now contains the M0 through M2 executable implementation
slices and the first M3 admission slice. M0 provides relation-specific Arrow
schemas and lossless complete-Fact round trips. The
batch preserves `FactId`, `RelationId`, `GenerationId`, authority, provenance,
completeness, validity, field order, and every V1 value shape. Recursive List
and Record values use Arrow's native nested arrays rather than JSON. Invalid
facts are rejected before projection; deterministic Arrow IPC file export and
bounded import are covered by example-based tests, property tests, malformed
and mutated corpora, resource-limit contracts, and a fuzz target.

`mrr-data-core` implements the M1 identity boundary: schema namespace
`mrr.data.snapshot` with a separate numeric version, canonical DAG-CBOR,
CIDv1/dag-cbor/SHA-256 roots, raw/SHA-256 child CIDs, and typed rejection of
unknown profiles or self-inconsistent descriptors. It consumes MRR's semantic
generation, source snapshot, and catalog digests rather than defining a second
semantic identity system.

`mrr-data-content` implements the M2 local packaging boundary. Its memory and
filesystem stores derive and verify every CID from an explicit `raw` or
`dag-cbor` codec. Snapshot archives use the upstream `fvm_ipld_car` CARv1
reader/writer, while MRR Data adds single-root admission, duplicate rejection,
full referenced-child closure and length checks, exact catalog verification,
typed import budgets, and validation-before-commit. CAR block order and extra
transport blocks may change without changing the snapshot root. A 512-extra-
block scenario guards against multi-second import regressions without reducing
the corpus. A slice-only frame preflight rejects declared block/count/aggregate
limits before the upstream reader allocates block payloads.

`mrr-data-graphar` begins M3 at the semantic boundary. It admits only relations
with exactly two ordered, non-null `Entity` fields, validates every fact against
its owning MRR relation, and produces physical-ID-free edge records preserving
`EntityId`, `FactId`, `RelationId`, `GenerationId`, authority, provenance,
completeness, and validity. N-ary, nullable-endpoint, scalar-endpoint, and
invalid-fact inputs fail with typed errors. Repartitioning cannot alter these
records because GraphAr row IDs, chunks, and adjacency offsets never enter the
contract. A deterministic physical vertex index then sorts and deduplicates the
semantic endpoint set, assigns dense GraphAr-local `i64` IDs, and retains the
bidirectional mapping. The physical ID is never treated as an `EntityId`;
upstream writers must persist the semantic identity as a property.

The opt-in `upstream-graphar` feature temporarily pins the exact commit behind
Apache GraphAr PR #977. It writes vertex identity properties, relation/context
edge properties, adjacency chunks, and official metadata through
`graphar-rs`/GraphAr C++; it does not implement a private GraphAr serializer.
Output is staged beside the destination and atomically renamed only after every
upstream builder and metadata write succeeds. This feature is provisional and
is not a release dependency until the upstream API is admitted.

The repository does not yet claim:

- GraphAr import or writer-to-reader round trips through the upstream runtime;
- an IPFS network integration;
- schema-bound query execution.

The canonical proposal, invariants, V1 manifest boundary, and delivery gates
are in [RFC 0001](docs/architecture/0001-mrr-data-plane.org).

## Intended V1 scope

V1 is deliberately local-first and narrow:

1. lossless relation-specific Arrow round trips;
2. a deterministic snapshot manifest and identity contract over that proven encoding;
3. CID/CAR packaging backed by memory and the local filesystem;
4. a gated GraphAr projection for the graph-shaped subset of MRR relations;
5. object-store support only after the local contract is stable.

There is no `mrr-ipfs` crate in the V1 plan. Content addressing does not imply
an IPFS daemon, a public gateway, or public publication.

## Development contract

`asp-rust` is pinned as a development dependency. One shared
`mrr-data-asp-rust-project-policy` Build Support owner drives the parser-native
workspace policy for every member crate. It rejects non-canonical source and
test layout during ordinary Cargo builds; there is no second style checker or
source-scanning test harness in this repository.

The complete local gate is the same gate used by CI:

```sh
cargo fmt --all -- --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --manifest-path fuzz/Cargo.toml --all -- --check
cargo check --manifest-path fuzz/Cargo.toml --locked
cargo clippy --manifest-path fuzz/Cargo.toml --all-targets --locked -- -D warnings
```

CI runs this contract on both Ubuntu and macOS, then exercises the IPC import
boundary with a bounded ASan fuzz campaign on nightly Linux. Workspace lints
forbid unsafe Rust and enable Clippy's `all` and `pedantic` groups for every
member crate.

## North star

> Zero-copy when hot, graph-native when large, content-addressed when durable.
