# Healthcare query M5.1 implementation audit — 2026-09-20

M5.1 targets the existing POO Flow query without changing its text:

```gql
MATCH (s:Scenario)-[:HAS_CASE]->(c:Case)-[:HAS_EFFECTIVE_PROFILE]->(p:Profile)
WHERE s.identity = 'healthcare'
RETURN s.identity, c.id, p.identity
```

This record separates the physical execution slice delivered in this repository
from the cross-repository consumer acceptance that remains outstanding.

## Delivered physical slice

`mrr-data-datafusion::execute_property_path_query` consumes an already
catalog-bound MRR query and typed Arrow tables. Its admitted shape is deliberately
bounded to one path with one or two anonymous outgoing binary relations, exact
single node/relation types, `RETURN ALL`, string-property equality predicates,
and nullable string-property projections.

The adapter verifies the bound entity and relation catalog digests before
planning. It rejects duplicate or malformed entity identities, dangling relation
endpoints, substituted property columns, unknown properties, unsupported query
operators, and zero or exceeded resource budgets. Input rows and bytes,
conservative join cardinality, output cells, and DataFusion memory have separate
bounds. The external worker remains responsible for its wall-clock deadline and
cancellation.

Physical output is sorted deterministically after projection. `RETURN ALL`
multiplicity and null values remain intact. The adapter returns
`PhysicalQueryOutput`; it does not create semantic identity or admit its own
result. Tests pass the output through MRR's existing
`admit_query_result_candidate` boundary.

The focused suite covers the expected Healthcare-shaped three-column result,
shared Profiles, missing projected values, absent filter matches, duplicate edge
multiplicity, reordered input determinism, catalog/property substitution,
duplicate entities, dangling endpoints, input/join/output/memory budgets, and MRR
result admission.

## Producer and parser findings

The active sibling POO Flow checkout has the authoritative producer:
`ontology-scenario-reasoning-graph` creates Scenario, Case, and Profile nodes plus
`HAS_CASE` and `HAS_EFFECTIVE_PROFILE` edges from accepted Case composition
receipts. Node metadata carries entity kind and identity. This is the source to
project; a parallel Healthcare model in MRR Data would create duplicate authority.

That producer is currently present at local POO Flow revision
`f8be70a08224bb17f2ead7c9eddb78e185a9f8ef`. A clean fetch of that exact revision
from the public remote fails because the remote does not advertise the commit.
The public revision used by the existing static-edge acceptance,
`7f60e82b609ed2227ce3e71d17c5a1a351902e54`, does not contain the Healthcare
submodule content required for this query. The dirty sibling checkout was not
edited.

The fixed MRR revision exposes the parser-owned `mrr-frontends` query compiler and
the required gerbil-parser revision is obtainable. The host Homebrew Gerbil build
lacks `:std/srfi/1`, so it cannot build that native frontend. MRR CI instead uses
Gerbil revision `add922481cec81d05431db014bcb91b30485cf21`, which contains that
library. Reproducing that toolchain is dependency qualification; it is not proof
that the original GQL has compiled and executed.

## Evidence and remaining acceptance

Current local evidence for this source change:

- DataFusion package: 13 tests passed, including the ASP Rust policy gate.
- Workspace with transfer, CAR, filesystem, cache, S3, and POO Flow runtime:
  141 tests passed and 3 existing subprocess/external-service cases were ignored.
- Workspace/all-target Clippy with `-D warnings`: passed with zero warnings.
- All 18 facade feature-isolation cases: passed; Arrow remains the only default.
- Four maintained Python consumer contracts and two source-receipt freshness
  contracts: passed against the clean pinned M5.0 runtime source.

These results close the reusable physical execution layer only. M5.1 remains open
until one clean, remotely obtainable POO Flow revision supplies the real producer
and invocation, the original GQL bytes compile through the parser-owned frontend,
property-bearing snapshots use the existing Cache/S3 transport, and local/cold/
warm/reopened execution returns the same MRR-admitted result on Ubuntu and macOS.
The final acceptance must retain source, query, catalog, generation, and snapshot
bindings plus the negative cases named in the canonical roadmap.
