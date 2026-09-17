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
- optional `graphar`: persistent Property Graph projection contracts;
- optional `graphar-native`: the admitted upstream GraphAr C++ writer;
- optional `content`: CID/DAG-CBOR manifests, CAR, and local content stores.

The dependency direction is one way:

```text
meta-relational-reasoning  <-  mrr-data(default: Arrow)
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
semantic identity system.

The optional `mrr-data-content` crate implements local packaging. Its memory and
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
upstream writers must persist the semantic identity as a property.

The opt-in `upstream-graphar` feature pins an immutable revision of the
`GTrunSec/incubator-graphar` fork maintained by this project. Apache GraphAr
PR #977 was closed intentionally; Apache merge is not a gate for using our
maintained extension. It writes vertex identity properties, relation/context
edge properties, adjacency chunks, and official metadata through
`graphar-rs`/GraphAr C++; it does not implement a private GraphAr serializer.
Output is staged beside the destination and atomically renamed only after every
native builder and metadata write succeeds. Admission is owned by the pinned
fork revision, cross-platform CI, and executable writer/readback Scenarios.

The repository does not yet claim:

- GraphAr import or writer-to-reader round trips through the upstream runtime;
- an IPFS network integration;
- schema-bound query execution.

The canonical proposal, invariants, V1 manifest boundary, and delivery gates
are in [RFC 0001](docs/architecture/0001-mrr-data-plane.org).

## Intended V1 scope

V1 is deliberately Arrow-first and narrow:

1. lossless relation-specific Arrow round trips;
2. trusted in-process Arrow interchange and performance evidence;
3. a gated GraphAr projection for graph-shaped MRR relations;
4. optional deterministic snapshot manifests and local CID/CAR packaging;
5. remote distribution only after a concrete requirement is demonstrated.

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
cargo check -p mrr-data --locked
cargo check -p mrr-data --no-default-features --locked
cargo check -p mrr-data --no-default-features --features content,graphar --locked
cargo test -p mrr-data --no-default-features --locked
cargo test -p mrr-data --no-default-features --features content,graphar --locked
```

Facade features:

| Feature | Default | Boundary |
|---|---:|---|
| `arrow` | yes | RecordBatch and bounded Arrow IPC |
| `graphar` | no | semantic Property Graph projection |
| `graphar-native` | no | upstream GraphAr C++ writer branch |
| `content` | no | manifest, CID/DAG-CBOR, CAR, local stores |

CI runs this contract on both Ubuntu and macOS, verifies the declared Rust 1.95
MSRV on Ubuntu, then exercises the IPC import
boundary with a bounded ASan fuzz campaign on nightly Linux. Workspace lints
forbid unsafe Rust and enable Clippy's `all` and `pedantic` groups for every
member crate.

## North star

> Arrow-native by default, GraphAr when graph-scale, content-addressed only
> when explicitly requested.
