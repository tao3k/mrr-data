//! Public immutable capture qualification, independent of query engine SDKs.
use crate::{
    BinaryEntityProjection, GraphArCaptureError, GraphArReadLimits, capture_graphar_snapshot,
    inventory_graphar_directory, write_graphar_dataset,
};
use meta_relational_reasoning::{
    Binding, Direction, EntityCatalog, EntityId, EntitySchema, EvidenceCompleteness, Expression,
    ExternalRevisionIdentity, Fact, FactId, FactProvenance, FactValidity, GenerationId,
    GraphPattern, MetaQueryIr, NodePattern, PathPattern, PathSegment, Projection, QueryId,
    QueryOperatorId, QueryResult, QueryTemplate, ReasoningBundle, ReasoningBundleDeclaration,
    RelationAuthority, RelationCatalog, RelationContext, RelationField, RelationId,
    RelationPattern, RelationSchema, RevisionBinding, SemanticSnapshot, SetQuantifier, Value,
    ValueSchema, bind_query_to_catalog,
};
use mrr_data_core::{
    BatchDescriptor, BoundDataQuery, CoverageDescriptor, CoverageKind, DataEngineProfile,
    GraphDatasetBinding, GraphDatasetInventory, GraphFileKind, GraphInventoryLimits,
    GraphProjectionDescriptor, RelationDescriptor, SnapshotBlock, SnapshotManifest,
    SnapshotManifestRequest, bind_data_query, raw_cid,
};

fn schema() -> RelationSchema {
    RelationSchema::new(
        RelationId::from_canonical_bytes("knows").unwrap(),
        "knows",
        vec![
            RelationField::new("source", ValueSchema::Entity, false).unwrap(),
            RelationField::new("destination", ValueSchema::Entity, false).unwrap(),
        ],
        vec![],
    )
    .unwrap()
}
fn fact() -> Fact {
    let entity = |name: &str| EntityId::from_canonical_bytes(name).unwrap();
    Fact::new(
        FactId::from_canonical_bytes("edge").unwrap(),
        schema().id(),
        vec![Value::Entity(entity("alice")), Value::Entity(entity("bob"))],
        RelationContext::new(
            GenerationId::from_canonical_bytes("generation").unwrap(),
            RelationAuthority::Entity(entity("owner")),
            FactProvenance::Source(entity("owner")),
            EvidenceCompleteness::Complete,
            FactValidity::Valid,
        )
        .unwrap(),
    )
}
fn query(inventory: &GraphDatasetInventory, generation: &str) -> BoundDataQuery {
    let generation = GenerationId::from_canonical_bytes(generation).unwrap();
    let snapshot = SemanticSnapshot::admit(
        generation,
        vec![
            RevisionBinding::admit(
                ExternalRevisionIdentity::new("git", "fixture", "revision").unwrap(),
                generation,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let node = EntityId::from_canonical_bytes("node").unwrap();
    let binding = |name: &str| Binding::new(name).unwrap();
    let op = |name: &str| QueryOperatorId::from_canonical_bytes(name).unwrap();
    let query_id = QueryId::from_canonical_bytes("query").unwrap();
    let query = MetaQueryIr::new(
        query_id,
        GraphPattern::new(
            op("graph"),
            vec![PathPattern::new(
                NodePattern::new(binding("source"), vec![node]),
                vec![PathSegment::new(
                    RelationPattern::new(
                        None,
                        vec![schema().id()],
                        Direction::Outgoing,
                        1,
                        Some(1),
                    )
                    .unwrap(),
                    NodePattern::new(binding("target"), vec![node]),
                )],
            )],
        )
        .unwrap(),
        vec![],
        QueryResult::returning(SetQuantifier::All).with_projections(vec![Projection::new(
            op("return"),
            Expression::Binding(binding("source")),
            binding("entity"),
        )]),
    )
    .unwrap();
    let bundle = ReasoningBundle::admit(ReasoningBundleDeclaration {
        relations: vec![schema()],
        entities: vec![EntitySchema::new(node, "Node", vec![]).unwrap()],
        query_templates: vec![QueryTemplate::new(query, vec![])],
        ..ReasoningBundleDeclaration::default()
    })
    .unwrap();
    let bound = bind_query_to_catalog(&bundle, query_id, &snapshot).unwrap();
    let relations = RelationCatalog::admit(vec![schema()]).unwrap();
    let entities =
        EntityCatalog::admit(vec![EntitySchema::new(node, "Node", vec![]).unwrap()]).unwrap();
    let metadata = inventory
        .files()
        .iter()
        .find(|f| f.path() == inventory.entry())
        .unwrap()
        .cid();
    let manifest = SnapshotManifest::admit(
        SnapshotManifestRequest::new(
            snapshot,
            &relations,
            &entities,
            vec![
                RelationDescriptor::new(
                    schema().id(),
                    1,
                    vec![BatchDescriptor::new(raw_cid(b"ipc"), 1, 3).unwrap()],
                )
                .unwrap(),
            ],
            CoverageDescriptor::new(CoverageKind::Complete, raw_cid(b"coverage")).unwrap(),
        )
        .with_graph_projection(GraphProjectionDescriptor::new("0.12.0", *metadata).unwrap()),
    )
    .unwrap();
    bind_data_query(
        &bound,
        &SnapshotBlock::encode(manifest).unwrap(),
        &DataEngineProfile::new("graphar-native", true, []).unwrap(),
    )
    .unwrap()
}
fn fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    BinaryEntityProjection,
    GraphDatasetInventory,
) {
    let parent = tempfile::tempdir().unwrap();
    let source = parent.path().join("dataset");
    let projection = BinaryEntityProjection::admit_catalog(
        &RelationCatalog::admit(vec![schema()]).unwrap(),
        schema().id(),
    )
    .unwrap();
    let receipt = write_graphar_dataset(
        &source,
        &projection,
        &[projection.project(&fact()).unwrap()],
    )
    .unwrap();
    (parent, source, projection, receipt.inventory().clone())
}
#[test]
fn capture_survives_source_deletion_and_rejects_semantic_generation_drift() {
    let (_parent, source, projection, inventory) = fixture();
    let limits = GraphInventoryLimits::default();
    let query = query(&inventory, "generation");
    let binding =
        GraphDatasetBinding::admit(&query, projection.relation_id(), &inventory, limits).unwrap();
    let captured = capture_graphar_snapshot(
        &source,
        &query,
        binding,
        &inventory,
        &projection,
        limits,
        GraphArReadLimits::new(2, 1),
    )
    .unwrap();
    std::fs::remove_dir_all(&source).unwrap();
    assert_eq!(captured.facts(&query).unwrap(), &[fact()]);
    assert_eq!(captured.vertex_count(), 2);
    let drifted = self::query(&inventory, "another-generation");
    assert!(captured.facts(&drifted).is_err());
}
#[test]
fn capture_refuses_changed_file_before_native_preparation() {
    let (_parent, source, projection, inventory) = fixture();
    let limits = GraphInventoryLimits::default();
    let query = query(&inventory, "generation");
    let binding =
        GraphDatasetBinding::admit(&query, projection.relation_id(), &inventory, limits).unwrap();
    let count = inventory
        .files()
        .iter()
        .find(|f| f.kind() == GraphFileKind::Count)
        .unwrap();
    std::fs::write(source.join(count.path()), [0u8; 8]).unwrap();
    assert!(
        capture_graphar_snapshot(
            &source,
            &query,
            binding,
            &inventory,
            &projection,
            limits,
            GraphArReadLimits::new(2, 1)
        )
        .is_err()
    );
}
#[test]
fn native_capture_never_follows_metadata_path_instructions() {
    let (_parent, source, projection, inventory) = fixture();
    std::fs::write(
        source.join(inventory.entry()),
        b"prefix: /nonexistent/external-source/
vertices: [../../external.yaml]
",
    )
    .unwrap();
    let limits = GraphInventoryLimits::default();
    let inventory = inventory_graphar_directory(&source, limits).unwrap();
    let query = query(&inventory, "generation");
    let binding =
        GraphDatasetBinding::admit(&query, projection.relation_id(), &inventory, limits).unwrap();
    let captured = capture_graphar_snapshot(
        &source,
        &query,
        binding,
        &inventory,
        &projection,
        limits,
        GraphArReadLimits::new(2, 1),
    )
    .unwrap();
    assert_eq!(captured.facts(&query).unwrap(), &[fact()]);
}
#[test]
fn capture_checks_fact_generation_even_with_matching_published_scope() {
    let (_parent, source, projection, inventory) = fixture();
    let limits = GraphInventoryLimits::default();
    let query = query(&inventory, "wrong-generation");
    let binding =
        GraphDatasetBinding::admit(&query, projection.relation_id(), &inventory, limits).unwrap();
    assert!(matches!(
        capture_graphar_snapshot(
            &source,
            &query,
            binding,
            &inventory,
            &projection,
            limits,
            GraphArReadLimits::new(2, 1)
        ),
        Err(GraphArCaptureError::SemanticScope)
    ));
}

#[cfg(feature = "backend")]
mod backend_qualification {
    use super::*;
    use mrr_data_backend::{
        AuthorityCapability, Backend, BackendConfig, BackendError, Lifecycle, MetadataProvider,
        ProviderCapabilities, StoredOutcome, StoredRevision, StoredWrite,
        providers::ProviderResult,
    };
    use mrr_data_content::{ContentRevision, PublishReceipt};
    use mrr_data_core::GraphFile;
    struct MetadataStub;
    impl MetadataProvider for MetadataStub {
        fn capabilities(&self) -> ProviderCapabilities {
            ProviderCapabilities {
                atomic_head_operation: true,
                durable_commit: true,
                historical_lookup: true,
                authority_versions: AuthorityCapability::Unsupported,
            }
        }
        fn open(&self) -> Result<(), BackendError> {
            Ok(())
        }
        fn close(&self) -> Result<(), BackendError> {
            Ok(())
        }
        fn commit(
            &self,
            _: &StoredWrite,
            _: Option<&PublishReceipt>,
            _: &mut dyn FnMut(Option<ContentRevision>) -> bool,
        ) -> ProviderResult<StoredOutcome> {
            panic!("capture qualification does not commit metadata")
        }
        fn recover(&self, _: &StoredWrite) -> ProviderResult<Option<StoredRevision>> {
            panic!("capture qualification does not recover metadata")
        }
    }
    #[tokio::test]
    async fn native_capture_retains_the_same_backend_until_final_handle_drop() {
        let (_parent, source, projection, inventory) = fixture();
        let limits = GraphInventoryLimits::default();
        let query = query(&inventory, "generation");
        let binding =
            GraphDatasetBinding::admit(&query, projection.relation_id(), &inventory, limits)
                .unwrap();
        let backend = Backend::open(
            BackendConfig::default(),
            MetadataStub,
            tokio::runtime::Handle::current(),
        )
        .await
        .unwrap();
        let bytes = usize::try_from(
            inventory
                .files()
                .iter()
                .map(GraphFile::byte_length)
                .sum::<u64>(),
        )
        .unwrap();
        let captured = crate::prepare_graphar_snapshot(
            &backend,
            crate::GraphArSnapshotRequest {
                source: source.clone(),
                query: query.clone(),
                binding,
                inventory,
                projection,
                inventory_limits: limits,
                read_limits: GraphArReadLimits::new(2, 1),
            },
            bytes,
        )
        .await
        .unwrap();
        std::fs::remove_dir_all(&source).unwrap();
        assert_eq!(captured.get().facts(&query).unwrap(), &[fact()]);
        assert_eq!(backend.status().active_resources, 1);
        let clone = captured.clone();
        let closing = backend.clone();
        let shutdown = tokio::spawn(async move { closing.shutdown().await });
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while backend.status().lifecycle != Lifecycle::Draining {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(captured);
        assert_eq!(backend.status().active_resources, 1);
        drop(clone);
        tokio::time::timeout(std::time::Duration::from_secs(3), shutdown)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(backend.status().resource_bytes, 0);
    }
}

#[test]
fn capture_refuses_an_unbound_or_different_catalog_projection_before_open() {
    let (_parent, _source, projection, inventory) = fixture();
    let limits = GraphInventoryLimits::default();
    let query = query(&inventory, "generation");
    let binding =
        GraphDatasetBinding::admit(&query, projection.relation_id(), &inventory, limits).unwrap();
    let unbound = BinaryEntityProjection::admit(&schema()).unwrap();
    assert!(matches!(
        capture_graphar_snapshot(
            std::path::Path::new("missing-source"),
            &query,
            binding.clone(),
            &inventory,
            &unbound,
            limits,
            GraphArReadLimits::new(2, 1)
        ),
        Err(GraphArCaptureError::SemanticScope)
    ));
    let altered = RelationSchema::new(
        schema().id(),
        "different-predicate",
        schema().fields().to_vec(),
        vec![],
    )
    .unwrap();
    let catalog = RelationCatalog::admit(vec![altered]).unwrap();
    let unrelated = BinaryEntityProjection::admit_catalog(&catalog, schema().id()).unwrap();
    assert!(matches!(
        capture_graphar_snapshot(
            std::path::Path::new("missing-source"),
            &query,
            binding,
            &inventory,
            &unrelated,
            limits,
            GraphArReadLimits::new(2, 1)
        ),
        Err(GraphArCaptureError::SemanticScope)
    ));
}
