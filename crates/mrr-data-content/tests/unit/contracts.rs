use std::{
    fs,
    time::{Duration, Instant},
};

use fvm_ipld_car::{Block, CarHeader, CarWriter};
use meta_relational_reasoning::{
    EntityCatalog, EntityId, EntitySchema, ExternalRevisionIdentity, GenerationId, RelationCatalog,
    RelationField, RelationId, RelationSchema, RevisionBinding, SemanticSnapshot, ValueSchema,
};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, RelationDescriptor, SnapshotBlock,
    SnapshotManifest, SnapshotManifestRequest, raw_cid,
};
use tempfile::tempdir;

use crate::{
    CarImportLimits, ContentBlock, ContentCodec, ContentError, ContentStore,
    FilesystemContentStore, ImportResource, MemoryContentStore, encode_snapshot_car,
    import_snapshot_car,
};

const ALPHA: &[u8] = b"alpha-arrow-ipc";
const BETA: &[u8] = b"beta-arrow-ipc";
const LINEAGE_A: &[u8] = b"lineage-a";
const LINEAGE_B: &[u8] = b"lineage-b";
const COVERAGE: &[u8] = b"coverage";

fn relation_id(name: &str) -> RelationId {
    RelationId::from_canonical_bytes(name).expect("relation identity")
}

fn relation_schema(name: &str) -> RelationSchema {
    RelationSchema::new(
        relation_id(name),
        name,
        vec![RelationField::new("value", ValueSchema::String, false).expect("field")],
        vec![],
    )
    .expect("relation schema")
}

fn catalogs() -> (RelationCatalog, EntityCatalog) {
    let relations = RelationCatalog::admit(vec![relation_schema("alpha"), relation_schema("beta")])
        .expect("relation catalog");
    let entity = EntitySchema::new(
        EntityId::from_canonical_bytes("entity:fixture").expect("entity identity"),
        "Fixture",
        vec![],
    )
    .expect("entity schema");
    let entities = EntityCatalog::admit(vec![entity]).expect("entity catalog");
    (relations, entities)
}

fn semantic_snapshot() -> SemanticSnapshot {
    let generation = GenerationId::from_canonical_bytes("generation:fixture").expect("generation");
    let revision = RevisionBinding::admit(
        ExternalRevisionIdentity::new("git", "repository:a", "commit:a").expect("revision"),
        generation,
    )
    .expect("binding");
    SemanticSnapshot::admit(generation, vec![revision]).expect("snapshot")
}

fn descriptor(name: &str, payload: &[u8], rows: u64) -> RelationDescriptor {
    RelationDescriptor::new(
        relation_id(name),
        rows,
        vec![BatchDescriptor::new(raw_cid(payload), rows, payload.len() as u64).unwrap()],
    )
    .unwrap()
}

fn fixture_with_alpha(
    alpha: &'static [u8],
) -> (
    SnapshotBlock,
    Vec<ContentBlock<'static>>,
    RelationCatalog,
    EntityCatalog,
) {
    let (relations, entities) = catalogs();
    let manifest = SnapshotManifest::admit(
        SnapshotManifestRequest::new(
            semantic_snapshot(),
            &relations,
            &entities,
            vec![descriptor("alpha", alpha, 2), descriptor("beta", BETA, 3)],
            CoverageDescriptor::new(CoverageKind::Complete, raw_cid(COVERAGE)).unwrap(),
        )
        .with_lineage_batch_cids(vec![raw_cid(LINEAGE_A), raw_cid(LINEAGE_B)]),
    )
    .unwrap();
    let snapshot = SnapshotBlock::encode(manifest).unwrap();
    let children = [alpha, BETA, LINEAGE_A, LINEAGE_B, COVERAGE]
        .into_iter()
        .map(|bytes| ContentBlock::new(ContentCodec::Raw, bytes))
        .collect();
    (snapshot, children, relations, entities)
}

fn fixture() -> (
    SnapshotBlock,
    Vec<ContentBlock<'static>>,
    RelationCatalog,
    EntityCatalog,
) {
    fixture_with_alpha(ALPHA)
}

fn generous_limits(archive_len: usize) -> CarImportLimits {
    CarImportLimits::new(archive_len as u64, 16, 4_096, 8_192)
}

#[test]
fn memory_and_filesystem_stores_round_trip_and_verify_blocks() {
    let raw = ContentBlock::new(ContentCodec::Raw, b"payload");
    let memory = MemoryContentStore::default();
    let cid = memory.put(raw).unwrap();
    assert_eq!(memory.get(&cid).unwrap(), b"payload");
    assert_eq!(memory.put(raw).unwrap(), cid);

    let directory = tempdir().unwrap();
    let filesystem = FilesystemContentStore::open(directory.path()).unwrap();
    assert_eq!(filesystem.put(raw).unwrap(), cid);
    assert_eq!(filesystem.get(&cid).unwrap(), b"payload");
    assert_eq!(filesystem.put(raw).unwrap(), cid);

    fs::write(filesystem.root().join(cid.to_string()), b"tampered").unwrap();
    assert!(matches!(
        filesystem.get(&cid),
        Err(ContentError::CidMismatch { .. })
    ));
}

#[test]
fn car_import_verifies_root_children_catalogs_and_commits_root_last() {
    let (snapshot, children, relations, entities) = fixture();
    let archive = encode_snapshot_car(&snapshot, &children).unwrap();
    let store = MemoryContentStore::default();
    let imported = import_snapshot_car(
        &archive,
        generous_limits(archive.len()),
        &relations,
        &entities,
        &store,
    )
    .unwrap();

    assert_eq!(imported.root(), snapshot.cid());
    assert_eq!(imported.manifest(), snapshot.manifest());
    assert_eq!(imported.block_count(), children.len() + 1);
    assert_eq!(store.get(snapshot.cid()).unwrap(), snapshot.bytes());
    for child in children {
        assert_eq!(store.get(&child.cid()).unwrap(), child.bytes());
    }
}

#[test]
fn repacking_changes_car_bytes_but_not_snapshot_identity() {
    let (snapshot, mut children, relations, entities) = fixture();
    let first = encode_snapshot_car(&snapshot, &children).unwrap();
    children.reverse();
    let second = encode_snapshot_car(&snapshot, &children).unwrap();
    assert_ne!(first, second);

    let first_import = import_snapshot_car(
        &first,
        generous_limits(first.len()),
        &relations,
        &entities,
        &MemoryContentStore::default(),
    )
    .unwrap();
    let second_import = import_snapshot_car(
        &second,
        generous_limits(second.len()),
        &relations,
        &entities,
        &MemoryContentStore::default(),
    )
    .unwrap();
    assert_eq!(first_import.root(), second_import.root());
    assert_eq!(first_import.root(), snapshot.cid());
}

#[test]
fn equivalent_semantic_generation_allows_distinct_physical_roots() {
    let (left, _, _, _) = fixture_with_alpha(ALPHA);
    let (right, _, _, _) = fixture_with_alpha(b"alpha-arrow-ipc-repacked");
    assert_ne!(left.cid(), right.cid());
    assert_eq!(
        left.manifest().semantic_snapshot().generation(),
        right.manifest().semantic_snapshot().generation()
    );
    assert_eq!(
        left.manifest().semantic_snapshot().digest(),
        right.manifest().semantic_snapshot().digest()
    );
}

#[test]
fn packaging_rejects_missing_duplicate_and_wrong_children() {
    let (snapshot, mut children, _, _) = fixture();
    children.pop();
    assert!(matches!(
        encode_snapshot_car(&snapshot, &children),
        Err(ContentError::MissingReferencedBlock(_))
    ));

    let (_, mut children, _, _) = fixture();
    children.push(children[0]);
    assert!(matches!(
        encode_snapshot_car(&snapshot, &children),
        Err(ContentError::DuplicateBlock(_))
    ));

    let (snapshot, mut children, _, _) = fixture();
    children[0] = ContentBlock::new(ContentCodec::Raw, b"alpha-arrow-ipc-expanded");
    assert!(matches!(
        encode_snapshot_car(&snapshot, &children),
        Err(ContentError::MissingReferencedBlock(_))
    ));

    let (relations, entities) = catalogs();
    let wrong_length = SnapshotManifest::admit(
        SnapshotManifestRequest::new(
            semantic_snapshot(),
            &relations,
            &entities,
            vec![
                RelationDescriptor::new(
                    relation_id("alpha"),
                    2,
                    vec![BatchDescriptor::new(raw_cid(ALPHA), 2, ALPHA.len() as u64 + 1).unwrap()],
                )
                .unwrap(),
                descriptor("beta", BETA, 3),
            ],
            CoverageDescriptor::new(CoverageKind::Complete, raw_cid(COVERAGE)).unwrap(),
        )
        .with_lineage_batch_cids(vec![raw_cid(LINEAGE_A), raw_cid(LINEAGE_B)]),
    )
    .unwrap();
    let wrong_length = SnapshotBlock::encode(wrong_length).unwrap();
    let (_, children, _, _) = fixture();
    assert!(matches!(
        encode_snapshot_car(&wrong_length, &children),
        Err(ContentError::ChildLengthMismatch { .. })
    ));
}

#[test]
fn import_enforces_each_configured_budget() {
    let (snapshot, children, relations, entities) = fixture();
    let archive = encode_snapshot_car(&snapshot, &children).unwrap();
    let cases = [
        (
            CarImportLimits::new(archive.len() as u64 - 1, 16, 4_096, 8_192),
            ImportResource::ArchiveBytes,
        ),
        (
            CarImportLimits::new(archive.len() as u64, 1, 4_096, 8_192),
            ImportResource::Blocks,
        ),
        (
            CarImportLimits::new(archive.len() as u64, 16, 4, 8_192),
            ImportResource::BlockBytes,
        ),
        (
            CarImportLimits::new(archive.len() as u64, 16, 4_096, 8),
            ImportResource::TotalBlockBytes,
        ),
    ];
    for (limits, resource) in cases {
        assert!(matches!(
            import_snapshot_car(
                &archive,
                limits,
                &relations,
                &entities,
                &MemoryContentStore::default(),
            ),
            Err(ContentError::LimitExceeded { resource: actual, .. }) if actual == resource
        ));
    }
}

#[test]
fn scenario_imports_512_extra_verified_blocks_within_five_seconds() {
    let (snapshot, mut children, relations, entities) = fixture();
    let extras: Vec<Vec<u8>> = (0..512_u16)
        .map(|index| format!("transport-extra-{index:04}").into_bytes())
        .collect();
    children.extend(
        extras
            .iter()
            .map(|bytes| ContentBlock::new(ContentCodec::Raw, bytes)),
    );
    let archive = encode_snapshot_car(&snapshot, &children).unwrap();

    let started = Instant::now();
    let imported = import_snapshot_car(
        &archive,
        CarImportLimits::new(archive.len() as u64, 1_024, 4_096, archive.len() as u64),
        &relations,
        &entities,
        &MemoryContentStore::default(),
    )
    .unwrap();
    let elapsed = started.elapsed();

    assert_eq!(imported.root(), snapshot.cid());
    assert_eq!(imported.block_count(), children.len() + 1);
    assert!(
        elapsed < Duration::from_secs(5),
        "512-block import took {elapsed:?}"
    );
    eprintln!(
        "mrr_data_content_import blocks={} archive_bytes={} elapsed_us={}",
        imported.block_count(),
        archive.len(),
        elapsed.as_micros()
    );
}

#[test]
fn import_rejects_tampering_duplicates_wrong_roots_and_catalogs() {
    let (snapshot, children, relations, entities) = fixture();
    let archive = encode_snapshot_car(&snapshot, &children).unwrap();

    let mut tampered = archive.clone();
    *tampered.last_mut().unwrap() ^= 1;
    assert!(matches!(
        import_snapshot_car(
            &tampered,
            generous_limits(tampered.len()),
            &relations,
            &entities,
            &MemoryContentStore::default(),
        ),
        Err(ContentError::Car(_))
    ));

    let duplicate = manual_car(
        vec![*snapshot.cid()],
        std::iter::once(Block {
            cid: *snapshot.cid(),
            data: snapshot.bytes().to_vec(),
        })
        .chain(children.iter().map(|child| Block {
            cid: child.cid(),
            data: child.bytes().to_vec(),
        }))
        .chain(std::iter::once(Block {
            cid: children[0].cid(),
            data: children[0].bytes().to_vec(),
        }))
        .collect(),
    );
    assert!(matches!(
        import_snapshot_car(
            &duplicate,
            generous_limits(duplicate.len()),
            &relations,
            &entities,
            &MemoryContentStore::default(),
        ),
        Err(ContentError::DuplicateBlock(_))
    ));

    let two_roots = manual_car(
        vec![*snapshot.cid(), *snapshot.cid()],
        vec![Block {
            cid: *snapshot.cid(),
            data: snapshot.bytes().to_vec(),
        }],
    );
    assert_eq!(
        import_snapshot_car(
            &two_roots,
            generous_limits(two_roots.len()),
            &relations,
            &entities,
            &MemoryContentStore::default(),
        ),
        Err(ContentError::RootCount { actual: 2 })
    );

    let wrong_relations = RelationCatalog::admit(vec![relation_schema("gamma")]).unwrap();
    assert!(matches!(
        import_snapshot_car(
            &archive,
            generous_limits(archive.len()),
            &wrong_relations,
            &entities,
            &MemoryContentStore::default(),
        ),
        Err(ContentError::Manifest(_))
    ));
}

fn manual_car(roots: Vec<cid::Cid>, blocks: Vec<Block>) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut writer = CarWriter::new(CarHeader::from(roots), &mut bytes).unwrap();
    for block in blocks {
        writer.write(block).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);
    bytes
}
