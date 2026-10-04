//! Public immutable capture qualification, independent of query engine SDKs.
use super::binary_entity::{fact, query_for_count, schema};
use crate::{
    BinaryEntityProjection, GraphArCaptureError, GraphArReadLimits, capture_graphar_snapshot,
    inventory_graphar_directory, write_graphar_dataset,
};
use meta_relational_reasoning::{RelationCatalog, RelationSchema};
use mrr_data_core::{
    GraphDatasetBinding, GraphDatasetInventory, GraphFileKind, GraphInventoryLimits,
};

pub(super) fn query(
    inventory: &GraphDatasetInventory,
    generation: &str,
) -> mrr_data_core::BoundDataQuery {
    query_for_count(inventory, generation, 1)
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
pub(super) mod backend_qualification {
    use super::{
        GraphArReadLimits, GraphDatasetBinding, GraphInventoryLimits, fact, fixture, query,
    };
    use mrr_data_backend::{
        AuthorityCapability, Backend, BackendConfig, BackendError, Lifecycle, MetadataProvider,
        ProviderCapabilities, StoredOutcome, StoredRevision, StoredWrite,
        providers::ProviderResult,
    };
    use mrr_data_content::{ContentRevision, PublishReceipt};
    use mrr_data_core::GraphFile;
    pub(crate) struct MetadataStub;
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
