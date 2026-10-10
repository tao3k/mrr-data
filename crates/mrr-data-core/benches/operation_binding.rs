//! Paired, same-process measurement of direct getters and borrowed bindings.

use std::hint::black_box;
use std::time::{Duration, Instant};

use meta_relational_reasoning::{
    EntityCatalog, ExternalRevisionIdentity, GenerationId, RelationCatalog, RelationField,
    RelationId, RelationSchema, RevisionBinding, SemanticSnapshot, ValueSchema,
};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, RelationDescriptor, SnapshotBlock,
    SnapshotManifest, SnapshotManifestRequest, SnapshotOperationBinding, SnapshotRowBinding,
    raw_cid,
};

const ROUNDS: usize = 40;
const ITERATIONS: usize = 100_000;

fn snapshot() -> SnapshotBlock {
    let generation = GenerationId::from_canonical_bytes("generation:benchmark").unwrap();
    let revision = RevisionBinding::admit(
        ExternalRevisionIdentity::new("benchmark", "source", "revision").unwrap(),
        generation,
    )
    .unwrap();
    let semantic = SemanticSnapshot::admit(generation, vec![revision]).unwrap();
    let relation_id = RelationId::from_canonical_bytes("relation:benchmark").unwrap();
    let schema = RelationSchema::new(
        relation_id,
        "Benchmark",
        vec![RelationField::new("value", ValueSchema::String, false).unwrap()],
        vec![],
    )
    .unwrap();
    let relations = RelationCatalog::admit(vec![schema]).unwrap();
    let entities = EntityCatalog::admit(vec![]).unwrap();
    let batch = BatchDescriptor::new(raw_cid(b"synthetic-arrow"), 1, 15).unwrap();
    let descriptor = RelationDescriptor::new(relation_id, 1, vec![batch]).unwrap();
    let coverage = CoverageDescriptor::new(CoverageKind::Unknown, raw_cid(b"coverage")).unwrap();
    let request =
        SnapshotManifestRequest::new(semantic, &relations, &entities, vec![descriptor], coverage);
    SnapshotBlock::encode(SnapshotManifest::admit(request).unwrap()).unwrap()
}

fn timed(mut operation: impl FnMut()) -> Duration {
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        operation();
    }
    start.elapsed()
}

fn report(name: &str, mut samples: Vec<Duration>) {
    samples.sort_unstable();
    let iterations = u128::try_from(ITERATIONS).expect("iteration count fits in u128");
    let hundredths = |duration: Duration| duration.as_nanos().saturating_mul(100) / iterations;
    let p50 = hundredths(samples[ROUNDS / 2]);
    let p95 = hundredths(samples[ROUNDS * 95 / 100]);
    println!(
        "{name}: p50_batch_avg={}.{:02}ns/op p95_batch_avg={}.{:02}ns/op",
        p50 / 100,
        p50 % 100,
        p95 / 100,
        p95 % 100
    );
}

fn main() {
    let snapshot = snapshot();
    let mut direct_snapshot = Vec::with_capacity(ROUNDS);
    let mut bound_snapshot = Vec::with_capacity(ROUNDS);

    for round in 0..ROUNDS {
        let direct = || {
            black_box(snapshot.cid());
            black_box(snapshot.manifest().semantic_snapshot().generation());
        };
        let bound = || {
            let view = SnapshotOperationBinding::new(black_box(&snapshot));
            black_box(view.root());
            black_box(view.generation());
        };
        if round % 2 == 0 {
            direct_snapshot.push(timed(direct));
            bound_snapshot.push(timed(bound));
        } else {
            bound_snapshot.push(timed(bound));
            direct_snapshot.push(timed(direct));
        }
    }
    println!("rounds={ROUNDS} iterations_per_round={ITERATIONS} profile=release");
    report("direct-snapshot", direct_snapshot);
    report("bound-snapshot", bound_snapshot);
    let relation = &snapshot.manifest().relations()[0];
    let child_cid = relation.batches()[0].cid();
    let mut row_binding = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        row_binding.push(timed(|| {
            black_box(
                SnapshotRowBinding::new(black_box(&snapshot), relation.relation_id(), child_cid, 0)
                    .unwrap(),
            );
        }));
    }
    report("bind-snapshot-row", row_binding);
}
