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

This repository now contains the first executable implementation slice:
relation-specific Arrow schemas and lossless scalar-row round trips, with
typed rejection for nested shapes that are not yet admitted. It does not yet
claim:

- a complete MRR-to-Arrow mapping for List and Record shapes or full Fact context;
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

## North star

> Zero-copy when hot, graph-native when large, content-addressed when durable.
