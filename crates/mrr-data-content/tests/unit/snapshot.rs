use crate::{
    ContentBlock, ContentCodec, ContentSource, ContentStore, MemoryContentStore,
    RemoteContentStore, RemoteError, RemoteFuture, SnapshotTransferError, SnapshotTransferLimits,
    publish_snapshot, restore_snapshot,
};
use cid::Cid;
use std::{collections::BTreeMap, sync::Mutex};

#[derive(Default)]
struct Remote {
    blocks: Mutex<BTreeMap<Cid, Vec<u8>>>,
    writes: Mutex<Vec<Cid>>,
    fail: Mutex<Option<Cid>>,
}
impl RemoteContentStore for Remote {
    fn get<'a>(&'a self, cid: &'a Cid, limit: usize) -> RemoteFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            let blocks = self.blocks.lock().unwrap();
            if blocks.get(cid).is_some_and(|b| b.len() > limit) {
                return Err(RemoteError::TooLarge);
            }
            Ok(blocks.get(cid).cloned())
        })
    }
    fn put<'a>(&'a self, block: ContentBlock<'a>) -> RemoteFuture<'a, ()> {
        Box::pin(async move {
            self.writes.lock().unwrap().push(block.cid());
            if *self.fail.lock().unwrap() == Some(block.cid()) {
                return Err(RemoteError::Unavailable);
            }
            self.blocks
                .lock()
                .unwrap()
                .insert(block.cid(), block.bytes().to_vec());
            Ok(())
        })
    }
}
pub(super) fn limits() -> SnapshotTransferLimits {
    SnapshotTransferLimits::new(100_000, 10, 100_000, 1_000_000)
}
pub(super) fn local(children: &[ContentBlock<'_>]) -> MemoryContentStore {
    let store = MemoryContentStore::default();
    for block in children {
        store.put(*block).unwrap();
    }
    store
}

#[tokio::test]
async fn snapshot_roundtrip_preserves_identity_and_publishes_root_last() {
    let (snapshot, children, relations, entities) = snapshot_fixture();
    let remote = Remote::default();
    let receipt = publish_snapshot(
        &local(&children),
        &remote,
        &snapshot,
        &relations,
        &entities,
        limits(),
    )
    .await
    .unwrap();
    assert_eq!(receipt.root(), snapshot.cid());
    assert_eq!(
        receipt.total_bytes(),
        snapshot.bytes().len() + children.iter().map(|b| b.bytes().len()).sum::<usize>()
    );
    let mut expected = snapshot.manifest().referenced_cids();
    expected.push(*snapshot.cid());
    assert_eq!(*remote.writes.lock().unwrap(), expected);
    let target = MemoryContentStore::default();
    let restored = restore_snapshot(
        &target,
        &remote,
        snapshot.cid(),
        &relations,
        &entities,
        limits(),
    )
    .await
    .unwrap();
    assert_eq!(restored.snapshot(), &snapshot);
    assert_eq!(restored.children().len(), children.len());
    for child in children {
        assert_eq!(restored.children()[&child.cid()], child.bytes());
    }
    let warm = restore_snapshot(
        &target,
        &remote,
        snapshot.cid(),
        &relations,
        &entities,
        limits(),
    )
    .await
    .unwrap();
    assert!(
        warm.sources()
            .values()
            .all(|source| *source == ContentSource::Local)
    );
}

#[tokio::test]
async fn property_snapshot_publishes_and_restores_entity_child_with_cold_warm_parity() {
    let (snapshot, children, relations, entities) = fixture_with(FixtureMode::Property);
    let property_cid = mrr_data_core::raw_cid(b"entity-property-batch");
    assert!(
        snapshot
            .manifest()
            .referenced_cids()
            .contains(&property_cid)
    );
    let remote = Remote::default();
    let publication = publish_snapshot(
        &local(&children),
        &remote,
        &snapshot,
        &relations,
        &entities,
        limits(),
    )
    .await
    .unwrap();
    assert_eq!(publication.root(), snapshot.cid());
    let target = MemoryContentStore::default();
    let cold = restore_snapshot(
        &target,
        &remote,
        snapshot.cid(),
        &relations,
        &entities,
        limits(),
    )
    .await
    .unwrap();
    assert_eq!(cold.children()[&property_cid], b"entity-property-batch");
    let warm = restore_snapshot(
        &target,
        &remote,
        snapshot.cid(),
        &relations,
        &entities,
        limits(),
    )
    .await
    .unwrap();
    assert_eq!(warm.snapshot(), cold.snapshot());
    assert_eq!(
        warm.children()[&property_cid],
        cold.children()[&property_cid]
    );
    assert!(
        warm.sources()
            .values()
            .all(|source| *source == ContentSource::Local)
    );
}

#[tokio::test]
async fn property_snapshot_rejects_wrong_entity_child_length_before_remote_write() {
    let (snapshot, children, relations, entities) = fixture_with(FixtureMode::WrongPropertyLength);
    let remote = Remote::default();
    assert!(
        publish_snapshot(
            &local(&children),
            &remote,
            &snapshot,
            &relations,
            &entities,
            limits(),
        )
        .await
        .is_err()
    );
    assert!(remote.writes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn incomplete_local_closure_and_budgets_cause_zero_remote_writes() {
    let (snapshot, children, relations, entities) = snapshot_fixture();
    let remote = Remote::default();
    assert!(
        publish_snapshot(
            &local(&children[..1]),
            &remote,
            &snapshot,
            &relations,
            &entities,
            limits()
        )
        .await
        .is_err()
    );
    for budget in [
        SnapshotTransferLimits::new(1, 10, 100_000, 1_000_000),
        SnapshotTransferLimits::new(100_000, 1, 100_000, 1_000_000),
        SnapshotTransferLimits::new(100_000, 10, 1, 1_000_000),
        SnapshotTransferLimits::new(100_000, 10, 100_000, snapshot.bytes().len()),
    ] {
        assert!(
            publish_snapshot(
                &local(&children),
                &remote,
                &snapshot,
                &relations,
                &entities,
                budget
            )
            .await
            .is_err()
        );
    }
    assert!(remote.writes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn child_and_root_failures_never_issue_a_receipt_and_retry_converges() {
    let (snapshot, children, relations, entities) = snapshot_fixture();
    for failed in [snapshot.manifest().referenced_cids()[0], *snapshot.cid()] {
        let remote = Remote::default();
        *remote.fail.lock().unwrap() = Some(failed);
        assert!(
            publish_snapshot(
                &local(&children),
                &remote,
                &snapshot,
                &relations,
                &entities,
                limits()
            )
            .await
            .is_err()
        );
        assert!(!remote.blocks.lock().unwrap().contains_key(snapshot.cid()));
        if failed != *snapshot.cid() {
            assert!(!remote.writes.lock().unwrap().contains(snapshot.cid()));
        }
        *remote.fail.lock().unwrap() = None;
        publish_snapshot(
            &local(&children),
            &remote,
            &snapshot,
            &relations,
            &entities,
            limits(),
        )
        .await
        .unwrap();
        assert_eq!(remote.blocks.lock().unwrap().len(), children.len() + 1);
    }
}

#[tokio::test]
async fn restore_rejects_missing_corrupt_and_substituted_blocks() {
    let (snapshot, children, relations, entities) = snapshot_fixture();
    let remote = Remote::default();
    let missing = restore_snapshot(
        &MemoryContentStore::default(),
        &remote,
        snapshot.cid(),
        &relations,
        &entities,
        limits(),
    )
    .await;
    assert_eq!(
        missing.unwrap_err(),
        SnapshotTransferError::MissingBlock(Box::new(*snapshot.cid()))
    );
    publish_snapshot(
        &local(&children),
        &remote,
        &snapshot,
        &relations,
        &entities,
        limits(),
    )
    .await
    .unwrap();
    let cid = children[0].cid();
    let saved = remote.blocks.lock().unwrap().remove(&cid).unwrap();
    assert_eq!(
        restore_snapshot(
            &MemoryContentStore::default(),
            &remote,
            snapshot.cid(),
            &relations,
            &entities,
            limits()
        )
        .await
        .unwrap_err(),
        SnapshotTransferError::MissingBlock(Box::new(cid))
    );
    remote
        .blocks
        .lock()
        .unwrap()
        .insert(cid, b"wrong child".to_vec());
    assert!(
        restore_snapshot(
            &MemoryContentStore::default(),
            &remote,
            snapshot.cid(),
            &relations,
            &entities,
            limits()
        )
        .await
        .is_err()
    );
    remote.blocks.lock().unwrap().insert(cid, saved);
    remote
        .blocks
        .lock()
        .unwrap()
        .insert(*snapshot.cid(), b"wrong root".to_vec());
    assert!(
        restore_snapshot(
            &MemoryContentStore::default(),
            &remote,
            snapshot.cid(),
            &relations,
            &entities,
            limits()
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn wrong_catalog_is_rejected_before_publication_or_restoration() {
    let (snapshot, children, relations, entities) = snapshot_fixture();
    let wrong = meta_relational_reasoning::RelationCatalog::admit(vec![
        meta_relational_reasoning::RelationSchema::new(
            meta_relational_reasoning::RelationId::from_canonical_bytes("other-relation").unwrap(),
            "other",
            vec![
                meta_relational_reasoning::RelationField::new(
                    "value",
                    meta_relational_reasoning::ValueSchema::String,
                    false,
                )
                .unwrap(),
            ],
            vec![],
        )
        .unwrap(),
    ])
    .unwrap();
    let remote = Remote::default();
    assert!(
        publish_snapshot(
            &local(&children),
            &remote,
            &snapshot,
            &wrong,
            &entities,
            limits()
        )
        .await
        .is_err()
    );
    assert!(remote.writes.lock().unwrap().is_empty());
    publish_snapshot(
        &local(&children),
        &remote,
        &snapshot,
        &relations,
        &entities,
        limits(),
    )
    .await
    .unwrap();
    assert!(
        restore_snapshot(
            &MemoryContentStore::default(),
            &remote,
            snapshot.cid(),
            &wrong,
            &entities,
            limits()
        )
        .await
        .is_err()
    );
}

// A real MRR manifest and CAR closure; its physical batch is an opaque fixture.
type SnapshotFixture = (
    mrr_data_core::SnapshotBlock,
    Vec<ContentBlock<'static>>,
    meta_relational_reasoning::RelationCatalog,
    meta_relational_reasoning::EntityCatalog,
);
pub(super) fn snapshot_fixture() -> SnapshotFixture {
    fixture_with(FixtureMode::Default)
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum FixtureMode {
    Default,
    WrongRelationLength,
    Graph,
    Property,
    WrongPropertyLength,
}

fn fixture_with(mode: FixtureMode) -> SnapshotFixture {
    use meta_relational_reasoning::{
        EntityCatalog, EntityId, EntitySchema, ExternalRevisionIdentity, GenerationId,
        RelationCatalog, RelationField, RelationId, RelationSchema, RevisionBinding,
        SemanticSnapshot, ValueSchema,
    };
    use mrr_data_core::{
        BatchDescriptor, CoverageDescriptor, CoverageKind, RelationDescriptor, SnapshotBlock,
        SnapshotManifest, SnapshotManifestRequest, raw_cid,
    };
    let relation = RelationId::from_canonical_bytes("probe:relation").unwrap();
    let relations = RelationCatalog::admit(vec![
        RelationSchema::new(
            relation,
            "probe",
            vec![RelationField::new("value", ValueSchema::String, false).unwrap()],
            vec![],
        )
        .unwrap(),
    ])
    .unwrap();
    let entities = EntityCatalog::admit(vec![
        EntitySchema::new(
            EntityId::from_canonical_bytes("probe:entity").unwrap(),
            "Probe",
            vec![],
        )
        .unwrap(),
    ])
    .unwrap();
    let generation = GenerationId::from_canonical_bytes("probe:generation").unwrap();
    let semantic = SemanticSnapshot::admit(
        generation,
        vec![
            RevisionBinding::admit(
                ExternalRevisionIdentity::new("git", "probe", "revision").unwrap(),
                generation,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let data = b"physical-batch";
    let coverage = b"coverage";
    let mut request = SnapshotManifestRequest::new(
        semantic,
        &relations,
        &entities,
        vec![
            RelationDescriptor::new(
                relation,
                1,
                vec![
                    BatchDescriptor::new(
                        raw_cid(data),
                        1,
                        data.len() as u64 + u64::from(mode == FixtureMode::WrongRelationLength),
                    )
                    .unwrap(),
                ],
            )
            .unwrap(),
        ],
        CoverageDescriptor::new(CoverageKind::Complete, raw_cid(coverage)).unwrap(),
    );
    if mode == FixtureMode::Graph {
        request = request.with_graph_projection(
            mrr_data_core::GraphProjectionDescriptor::new("1", raw_cid(b"graph manifest")).unwrap(),
        );
    }
    let property_data = b"entity-property-batch";
    if matches!(
        mode,
        FixtureMode::Property | FixtureMode::WrongPropertyLength
    ) {
        request = request.with_entities(vec![property_descriptor(
            entities.entities()[0].clone(),
            mode == FixtureMode::WrongPropertyLength,
        )]);
    }
    let manifest = SnapshotManifest::admit(request).unwrap();
    let mut children = vec![
        ContentBlock::new(ContentCodec::Raw, data),
        ContentBlock::new(ContentCodec::Raw, coverage),
    ];
    if matches!(
        mode,
        FixtureMode::Property | FixtureMode::WrongPropertyLength
    ) {
        children.push(ContentBlock::new(ContentCodec::Raw, property_data));
    }
    (
        SnapshotBlock::encode(manifest).unwrap(),
        children,
        relations,
        entities,
    )
}

fn property_descriptor(
    schema: meta_relational_reasoning::EntitySchema,
    wrong_length: bool,
) -> mrr_data_core::EntityDescriptor {
    use mrr_data_core::{BatchDescriptor, EntityDescriptor, raw_cid};
    let property_data = b"entity-property-batch";
    EntityDescriptor::new(
        schema,
        1,
        vec![
            BatchDescriptor::new(
                raw_cid(property_data),
                1,
                property_data.len() as u64 + u64::from(wrong_length),
            )
            .unwrap(),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn unsupported_graphs_and_wrong_declared_lengths_fail_closed() {
    for mode in [FixtureMode::WrongRelationLength, FixtureMode::Graph] {
        let (snapshot, children, relations, entities) = fixture_with(mode);
        let remote = Remote::default();
        let result = publish_snapshot(
            &local(&children),
            &remote,
            &snapshot,
            &relations,
            &entities,
            limits(),
        )
        .await;
        if mode == FixtureMode::Graph {
            assert_eq!(
                result.unwrap_err(),
                SnapshotTransferError::UnsupportedGraphProjection
            );
        } else {
            assert!(matches!(
                result,
                Err(SnapshotTransferError::Content(
                    crate::ContentError::ChildLengthMismatch { .. }
                ))
            ));
        }
        assert!(remote.writes.lock().unwrap().is_empty());
        // Simulate a foreign publisher bypassing the validated snapshot operation.
        for child in children {
            remote.put(child).await.unwrap();
        }
        remote
            .put(ContentBlock::new(ContentCodec::DagCbor, snapshot.bytes()))
            .await
            .unwrap();
        assert!(
            restore_snapshot(
                &MemoryContentStore::default(),
                &remote,
                snapshot.cid(),
                &relations,
                &entities,
                limits()
            )
            .await
            .is_err()
        );
    }
}

#[cfg(feature = "car")]
#[tokio::test]
async fn car_and_remote_restore_share_the_same_closure_validation() {
    let (snapshot, children, relations, entities) = snapshot_fixture();
    let remote = Remote::default();
    publish_snapshot(
        &local(&children),
        &remote,
        &snapshot,
        &relations,
        &entities,
        limits(),
    )
    .await
    .unwrap();
    let restored = restore_snapshot(
        &MemoryContentStore::default(),
        &remote,
        snapshot.cid(),
        &relations,
        &entities,
        limits(),
    )
    .await
    .unwrap();
    let borrowed: Vec<_> = restored
        .children()
        .iter()
        .map(|(cid, bytes)| ContentBlock::new(ContentCodec::from_cid(cid).unwrap(), bytes))
        .collect();
    let archive = crate::encode_snapshot_car(restored.snapshot(), &borrowed).unwrap();
    let imported = crate::import_snapshot_car(
        &archive,
        crate::CarImportLimits::new(1_000_000, 10, 100_000, 1_000_000),
        &relations,
        &entities,
        &MemoryContentStore::default(),
    )
    .unwrap();
    assert_eq!(imported.root(), snapshot.cid());
    assert_eq!(imported.manifest(), snapshot.manifest());
}

struct NoCache;
impl ContentStore for NoCache {
    fn get_bounded(&self, cid: &Cid, _: usize) -> Result<Vec<u8>, crate::ContentError> {
        Err(crate::ContentError::NotFound(Box::new(*cid)))
    }
    fn put(&self, _: ContentBlock<'_>) -> Result<Cid, crate::ContentError> {
        Err(crate::ContentError::LockPoisoned)
    }
}

#[tokio::test]
async fn restored_bytes_survive_cache_admission_failure() {
    let (snapshot, children, relations, entities) = snapshot_fixture();
    let remote = Remote::default();
    publish_snapshot(
        &local(&children),
        &remote,
        &snapshot,
        &relations,
        &entities,
        limits(),
    )
    .await
    .unwrap();
    let restored = restore_snapshot(
        &NoCache,
        &remote,
        snapshot.cid(),
        &relations,
        &entities,
        limits(),
    )
    .await
    .unwrap();
    assert_eq!(restored.snapshot(), &snapshot);
    for child in children {
        assert_eq!(restored.children()[&child.cid()], child.bytes());
    }
    assert!(
        restored
            .sources()
            .values()
            .all(|s| matches!(s, ContentSource::Remote(crate::CacheAdmission::Failed(_))))
    );
}

#[tokio::test]
async fn restore_enforces_count_root_child_and_total_budgets() {
    let (snapshot, children, relations, entities) = snapshot_fixture();
    let remote = Remote::default();
    publish_snapshot(
        &local(&children),
        &remote,
        &snapshot,
        &relations,
        &entities,
        limits(),
    )
    .await
    .unwrap();
    for budget in [
        SnapshotTransferLimits::new(1, 10, 100_000, 1_000_000),
        SnapshotTransferLimits::new(100_000, 1, 100_000, 1_000_000),
        SnapshotTransferLimits::new(100_000, 10, 1, 1_000_000),
        SnapshotTransferLimits::new(100_000, 10, 100_000, snapshot.bytes().len()),
    ] {
        assert!(
            restore_snapshot(
                &MemoryContentStore::default(),
                &remote,
                snapshot.cid(),
                &relations,
                &entities,
                budget
            )
            .await
            .is_err()
        );
    }
}
