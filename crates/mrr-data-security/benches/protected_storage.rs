//! Matched local baseline for admission and complete protected staging.
//! Run with `cargo bench -p mrr-data-security --features protected-publish --bench protected_storage`.

use std::{hint::black_box, time::Instant};

use meta_relational_reasoning::{
    EntityCatalog, ExternalRevisionIdentity, GenerationId, RelationCatalog, RelationField,
    RelationId, RelationSchema, RevisionBinding, SemanticSnapshot, ValueSchema,
};
use mrr_data_content::{
    ContentBlock, ContentCodec, ContentStore, MemoryContentStore, SnapshotTransferLimits,
};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, RelationDescriptor, SnapshotBlock,
    SnapshotManifest, SnapshotManifestRequest, raw_cid,
};
use mrr_data_security::data_protection::{
    CurrentStorageState, EntityRef, ProtectedEnvelopeKey, ProtectedStage, ProtectionClaim,
    ProtectionIntent, RawStorageDestination, RawStorageTier, SourceLabel, StorageEffect,
    stage_protected_snapshot,
};

fn snapshot(
    children: usize,
) -> (
    SnapshotBlock,
    MemoryContentStore,
    RelationCatalog,
    EntityCatalog,
) {
    let generation = GenerationId::from_canonical_bytes("generation:protected-bench").unwrap();
    let revision = RevisionBinding::admit(
        ExternalRevisionIdentity::new("bench", "source", "revision").unwrap(),
        generation,
    )
    .unwrap();
    let semantic = SemanticSnapshot::admit(generation, vec![revision]).unwrap();
    let relation_id = RelationId::from_canonical_bytes("relation:protected-bench").unwrap();
    let schema = RelationSchema::new(
        relation_id,
        "Bench",
        vec![RelationField::new("value", ValueSchema::String, false).unwrap()],
        vec![],
    )
    .unwrap();
    let relations = RelationCatalog::admit(vec![schema]).unwrap();
    let entities = EntityCatalog::admit(vec![]).unwrap();
    let source = MemoryContentStore::default();
    let mut batches = Vec::new();
    for index in 0..children {
        let bytes = format!("synthetic-child-{index:04}-{}", "x".repeat(192));
        source
            .put(ContentBlock::new(ContentCodec::Raw, bytes.as_bytes()))
            .unwrap();
        batches
            .push(BatchDescriptor::new(raw_cid(bytes.as_bytes()), 1, bytes.len() as u64).unwrap());
    }
    source
        .put(ContentBlock::new(ContentCodec::Raw, b"coverage"))
        .unwrap();
    let descriptor = RelationDescriptor::new(relation_id, children as u64, batches).unwrap();
    let coverage = CoverageDescriptor::new(CoverageKind::Unknown, raw_cid(b"coverage")).unwrap();
    let request =
        SnapshotManifestRequest::new(semantic, &relations, &entities, vec![descriptor], coverage);
    let snapshot = SnapshotBlock::encode(SnapshotManifest::admit(request).unwrap()).unwrap();
    source
        .put(ContentBlock::new(ContentCodec::DagCbor, snapshot.bytes()))
        .unwrap();
    (snapshot, source, relations, entities)
}

fn quantiles(mut samples: Vec<u128>) -> (u128, u128) {
    samples.sort_unstable();
    let p50 = samples[samples.len() / 2];
    let p95 = samples[(samples.len() * 95 / 100).min(samples.len() - 1)];
    (p50, p95)
}

fn run_case(runtime: &tokio::runtime::Runtime, source_count: usize, children: usize) {
    let (snapshot, source, relations, entities) = snapshot(children);
    let owner = EntityRef {
        type_name: "Team",
        id: "analytics",
    };
    let labels: Vec<_> = (0..source_count)
        .map(|_| SourceLabel {
            resource: EntityRef {
                type_name: "Dataset",
                id: "orders",
            },
            owner,
            tenant: "tenant-a",
            restricted: true,
        })
        .collect();
    let owners = [owner];
    let intent = ProtectionIntent {
        storage: StorageEffect {
            operation_id: "bench-protected",
            subject: EntityRef {
                type_name: "Service",
                id: "publisher",
            },
            purpose: "archive",
            snapshot_root: snapshot.cid(),
            sources: &labels,
            destination: RawStorageDestination {
                resource: EntityRef {
                    type_name: "Bucket",
                    id: "archive",
                },
                tenant: "tenant-a",
                accepted_owners: &owners,
                accepts_restricted: true,
                tier: RawStorageTier::Remote,
            },
            policy_root: "policy-root-1",
            lineage_revision: "lineage-1",
        },
        profile: "aes-256-gcm-v1",
        key_ref: "key-tenant-a",
        key_version: "key-version-7",
        residency: "us-east-1",
    };
    let claim = ProtectionClaim {
        intent,
        epoch: 4,
        expires_at: 100,
        allowed: true,
    };
    let current = CurrentStorageState {
        policy_root: "policy-root-1",
        lineage_revision: "lineage-1",
        epoch: 4,
        now: 99,
    };
    let key = ProtectedEnvelopeKey::aes_256_gcm(&[7_u8; 32]).unwrap();
    let mut gate = Vec::with_capacity(10_000);
    for _ in 0..10_000 {
        let start = Instant::now();
        black_box(intent)
            .check_intent(black_box(&claim), black_box(current))
            .unwrap();
        gate.push(start.elapsed().as_nanos());
    }
    let mut stage = Vec::with_capacity(200);
    let mut outer_bytes = 0;
    for _ in 0..200 {
        let outbox = MemoryContentStore::default();
        let start = Instant::now();
        let prepared = runtime
            .block_on(stage_protected_snapshot(ProtectedStage {
                intent,
                claim: &claim,
                current,
                source: &source,
                outbox: &outbox,
                relations: &relations,
                entities: &entities,
                inner_limits: SnapshotTransferLimits::new(65_536, children + 2, 65_536, 131_072),
                max_outer_block_bytes: 65_536,
                max_outer_total_bytes: 131_072,
                key: &key,
            }))
            .unwrap();
        stage.push(start.elapsed().as_micros());
        outer_bytes = prepared.total_outer_bytes();
        black_box(prepared);
    }
    let (gate_p50, gate_p95) = quantiles(gate);
    let (stage_p50, stage_p95) = quantiles(stage);
    println!(
        "sources={source_count} children={children} gate_iterations=10000 stage_iterations=200 gate_ns_p50={gate_p50} gate_ns_p95={gate_p95} stage_us_p50={stage_p50} stage_us_p95={stage_p95} outer_bytes={outer_bytes}"
    );
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let requested = std::env::args()
        .skip(1)
        .find_map(|arg| arg.parse::<usize>().ok());
    for (sources, children) in [(1, 1), (16, 16), (64, 128)] {
        if requested.is_none_or(|value| value == sources) {
            run_case(&runtime, sources, children);
        }
    }
}
