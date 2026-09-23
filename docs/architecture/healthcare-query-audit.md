# Healthcare M5.1 chain audit — 2026-09-23

The acceptance target is the unchanged, declared POO Flow query:

```gql
MATCH (s:Scenario)-[:HAS_CASE]->(c:Case)-[:HAS_EFFECTIVE_PROFILE]->(p:Profile)
WHERE s.identity = 'healthcare'
RETURN s.identity, c.id, p.identity
```

This audit starts from the squash-merged [mrr-data PR #1](https://github.com/tao3k/mrr-data/pull/1)
at `f53909884efcb9b345837c1fd2746d2cdcdd0c5b`. Its final
[CI run](https://github.com/tao3k/mrr-data/actions/runs/35804588191) passed all
nine jobs, including both platforms, the MSRV, GraphAr, S3/TLS, IPC fuzz and M4
acceptance. That run qualifies the existing physical slice and static-edge
consumer; it is not an M5.1 Healthcare execution receipt.

## Source and authority, checked at the current pins

| Boundary | Verified source | What it currently supplies |
|---|---|---|
| POO Flow Healthcare producer | Public `tao3k/lambda-episteme` `main` at `d758ad0dfa5c4b7c567b4c5f1562fba4ffc4ae3c` | `modules/ontology/funs.ss` projects accepted Case receipts to Scenario, Case and Profile nodes and `HAS_CASE` / `HAS_EFFECTIVE_PROFILE` edges. Node metadata contains kind and identity. |
| Original query owner | The same revision, `user-interface/scenarios/healthcare/reasoning.ss` and `reasoning/case-profile-relations.gql` | The exact file has SHA-256 `7a3a88a9ebd24cd738d426c0def633247d1a0fc13e9e37cca13bb23e90ba0c63`, matching the POO query declaration. `assurance.ss` checks declared query source paths; it calls the GQL non-authoritative analysis input for a later MRR Runtime. |
| Semantic and parser frontend | mrr-data pins MRR `fbbb753b8b28c8b22bd0e8131ace8dd42646b3fc` in both root and fuzz workspaces | `mrr-frontends::QueryFrontend::compile_with_receipt` can retain source and grammar digests. Its tests cover a simpler graph query; this exact Healthcare file has not been shown compiling and binding to a catalog. MRR owns that lowering and result admission. |
| Physical path | `mrr-data-datafusion::execute_property_path_query` | Already executes a catalog-bound one/two-edge path over typed in-memory Arrow entity/relation tables, with string equality and projections, limits and deterministic physical ordering. The tests construct MRR IR directly. |
| Snapshot and transport | `mrr-data-core::SnapshotManifest` plus `mrr-data-content` / `mrr-data-cache` | The V1 manifest inventories relation batches, coverage, lineage and optional GraphAr projection. It binds the entity catalog digest, but has no typed descriptor for entity-property batches. Cache/S3 closure verification therefore cannot yet publish and restore the tables consumed by the property executor. |
| Existing POO consumer | `integrations/poo_flow/mrr_data_resource.py` and `mrr-data-poo-flow` | The fixed `poo-flow.static-edges.v1` envelope accepts one relation of endpoint IDs and returns two-column admitted rows. It does not accept this source graph or return the three projected properties. |

The previous audit's unavailable-producer-revision finding is resolved: the
Healthcare producer and original query are now obtainable from public
`lambda-episteme` at the revision above. The checked-out POO Flow submodule has
uncommitted work; this audit used an isolated checkout of its public `main`.

The producer deduplicates each `(from, kind, to)` edge and rejects unaccepted or
cross-Scenario Case receipts. Its graph node ID is a kind-prefixed transport ID;
`s.identity`, `c.id` and `p.identity` must come from the producer's accepted
domain values or metadata, not from that ID's text. In particular, `c.id` must
be specified against the accepted Case receipt's `case-id` before projection.
The physical fixture's duplicate-edge and null-property cases are valuable
negative/semantic tests; they are not evidence that this producer emits them.

## Next delivery sequence

These are dependencies of one M5.1 acceptance claim, with one owner per contract.

1. **Fix the source contract and expected rows in POO Flow.** Pin the public
   producer revision and original GQL digest in the real reasoning invocation.
   Project the accepted Scenario/Case/Profile graph to typed entity properties
   and two typed relations. Define the independent expected rows from accepted
   Case receipts, including a distractor Scenario, shared Profiles, no match and
   missing-value behavior. Keep domain projection in `lambda-episteme`.
2. **Compile and bind through MRR.** Run the original file bytes through
   `mrr-frontends::QueryFrontend::compile_with_receipt`, then bind the resulting
   query to the Scenario/Case/Profile and relation catalogs. Retain the frontend
   source/grammar receipt, catalog digests and semantic generation. If lowering
   rejects this shape, fix that owned frontend; do not create a second GQL
   parser or hand-write the production IR.
3. **Add a versioned property snapshot in mrr-data.** Admit typed entity batches
   with explicit schema, row counts, CIDs and byte lengths in the immutable
   manifest. Include them in closure, preflight limits, publish and verified
   restore. Decode to the existing `EntityPropertyTable` and
   `BinaryRelationTable`, validate the restored catalogs, and call the existing
   physical executor and MRR result admission. Keep the V1 static-edge profile
   working independently.
4. **Connect the real reasoning caller.** Extend the existing bounded worker
   and POO Flow resource with a named Healthcare profile whose request and
   receipt bind source revision, exact query/grammar, catalog, generation and
   snapshot root. The caller supplies accepted graph projection and runtime
   credentials; the worker enforces budgets and a deadline. The current
   `healthcare-query-source-path` check is only declaration validation, not this
   invocation.
5. **Prove the chain on both platforms.** Execute the original caller and
   compare its independently specified rows with MRR-admitted output for local,
   cold S3, warm Kache and reopened-process reads. Record root, generation,
   query/catalog/admission digests, startup and execution time separately,
   remote operations/bytes and producer cardinalities. Exercise source/query/
   catalog/generation substitution, malformed properties, corruption,
   transport failure, cancellation and input/join/output/memory/deadline limits.
   Keep existing static-edge tests and the nine CI jobs green at the delivered
   revision.

The first implementation change belongs at the producer projection and original
GQL compile/bind boundary. A direct-IR test of `execute_property_path_query`
cannot establish either. M5.2 (`profile-impact.gql` and then the causal event
query) follows the admitted M5.1 path and needs its own domain semantics.
