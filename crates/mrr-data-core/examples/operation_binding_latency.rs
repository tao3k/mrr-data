//! Paired, same-process measurement of direct getters and borrowed bindings.

use std::hint::black_box;
use std::time::{Duration, Instant};

use meta_relational_reasoning::{
    EntityCatalog, ExternalRevisionIdentity, GenerationId, RelationCatalog, RelationField,
    RelationId, RelationSchema, RevisionBinding, SemanticSnapshot, ValueSchema,
};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, PseudonymizationInputBinding,
    RelationDescriptor, SnapshotBlock, SnapshotManifest, SnapshotManifestRequest,
    SnapshotOperationBinding, raw_cid,
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
    let value_digest = [17; 32];
    let mut direct_snapshot = Vec::with_capacity(ROUNDS);
    let mut bound_snapshot = Vec::with_capacity(ROUNDS);
    let mut direct_token = Vec::with_capacity(ROUNDS);
    let mut bound_token = Vec::with_capacity(ROUNDS);

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
        let direct_pseudonymization = || {
            black_box(snapshot.cid());
            black_box(&value_digest);
            black_box("tenant-a:study-1");
        };
        let bound_pseudonymization = || {
            let view = PseudonymizationInputBinding::new(
                black_box(&snapshot),
                "patient_id",
                &value_digest,
                "tenant-a:study-1",
                "aes-siv",
            );
            black_box(view.source().root());
            black_box(view.value_digest());
            black_box(view.context());
        };
        if round % 2 == 0 {
            direct_snapshot.push(timed(direct));
            bound_snapshot.push(timed(bound));
            direct_token.push(timed(direct_pseudonymization));
            bound_token.push(timed(bound_pseudonymization));
        } else {
            bound_snapshot.push(timed(bound));
            direct_snapshot.push(timed(direct));
            bound_token.push(timed(bound_pseudonymization));
            direct_token.push(timed(direct_pseudonymization));
        }
    }
    println!("rounds={ROUNDS} iterations_per_round={ITERATIONS} profile=release");
    report("direct-snapshot", direct_snapshot);
    report("bound-snapshot", bound_snapshot);
    report("direct-pseudonymization", direct_token);
    report("bound-pseudonymization", bound_token);
}
