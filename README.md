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

This repository now contains the M0 executable implementation slice:
relation-specific Arrow schemas and lossless complete-Fact round trips. The
batch preserves `FactId`, `RelationId`, `GenerationId`, authority, provenance,
completeness, validity, field order, and every V1 value shape. Recursive List
and Record values use Arrow's native nested arrays rather than JSON. Invalid
facts are rejected before projection; deterministic Arrow IPC file export and
bounded import are covered by example-based tests, property tests, malformed
and mutated corpora, resource-limit contracts, and a fuzz target. It does not
yet claim:

- GraphAr export/import;
- stable snapshot manifests or CIDs;
- CAR packaging or an IPFS network integration;
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

`asp-rust` is pinned as a development dependency and drives the parser-native
workspace policy from each crate's `build.rs`. It rejects non-canonical source
and test layout during ordinary Cargo builds; there is no second style checker
or source-scanning test harness in this repository.

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
