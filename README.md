# MRR Data

`mrr-data` is the physical data plane for
[Meta Relational Reasoning](https://github.com/tao3k/meta-relational-reasoning).
MRR owns identities, relations, generations, queries and result admission;
this repository handles Arrow interchange, bounded physical execution,
snapshots and optional storage providers.

Arrow is the only default feature. DataFusion, GraphAr, content addressing,
Kache and S3 are selected explicitly for the workload that needs them.

## Start here

| Topic | Document |
|---|---|
| Architecture, ownership and feature status | [RFC 0001](docs/architecture/0001-mrr-data-plane.org) |
| Implemented APIs, feature contract and development commands | [Current implementation](docs/architecture/current-implementation.org) |
| CID, local cache, snapshot and S3 behavior | [Content cache protocol](docs/architecture/content-cache-protocol.org) |
| Distribution acceptance | [Snapshot distribution audit](docs/architecture/snapshot-distribution-audit.org) |
| Healthcare query status and next chain | [Healthcare property-path audit](docs/architecture/healthcare-property-path-audit.org) |
| Existing POO Flow static-edge consumer | [Integration guide](integrations/poo_flow/README.md) |

## Build and test

From the repository root:

```sh
cargo fmt --all -- --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```

The [development contract](docs/architecture/current-implementation.org)
lists the optional-feature, fuzz and platform gates used by CI.

## Boundaries

MRR remains the semantic authority. `mrr-data` owns physical representation,
verified transport and execution under MRR's admitted contracts. POO Flow
declares domain queries and consumes admitted receipts. The current
Healthcare two-hop executor is a physical slice; the original Healthcare GQL
and reasoning caller are tracked by the [Healthcare property-path audit](docs/architecture/healthcare-property-path-audit.org).
