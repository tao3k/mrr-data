//! Real native files, cold publication recovery, leased Arrow and MRR admission.
#[path = "../../../mrr-data-content/tests/support/graph_publication_fixture.rs"]
mod fixture;
use fixture::{Fixture, Remote, limits, schema};
use meta_relational_reasoning::{
    EntityId, EvidenceCompleteness, Fact, FactId, FactProvenance, FactValidity, GenerationId,
    QueryResultLimits, RelationAuthority, RelationContext, Value, admit_query_result_candidate,
};
use mrr_data_arrow::{facts_to_ipc, facts_to_record_batch};
use mrr_data_backend::{ArrowQueryError, ArrowQueryLimits, Backend, BackendConfig};
use mrr_data_content::{
    ConditionalContentCommitPort, ConditionalContentWrite, ContentBlock, ContentCodec,
    ContentStore, MemoryContentStore, publish_graph_dataset, restore_graph_dataset,
};
use mrr_data_core::{GraphInventoryLimits, bind_data_query, project_data_query_output};
use mrr_data_datafusion::{datafusion_engine_profile, execute_binary_entity_query};
use mrr_data_graphar::{
    BinaryEntityProjection, GraphArReadLimits, capture_graphar_snapshot, write_graphar_dataset,
};
use std::{num::NonZeroUsize, sync::Arc};
#[cfg(feature = "backend-turso")]
fn native(path: &std::path::Path) -> mrr_data_backend::providers::TursoProvider {
    mrr_data_backend::providers::TursoProvider::new(path.into(), tokio::runtime::Handle::current())
}
#[cfg(all(not(feature = "backend-turso"), feature = "backend-duckdb"))]
fn native(path: &std::path::Path) -> mrr_data_backend::providers::DuckDbProvider {
    mrr_data_backend::providers::DuckDbProvider::new(path.into())
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
#[tokio::test]
async fn native_graph_publication_restoration_and_admission() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("metadata.db");
    let backend = Backend::open(
        BackendConfig::default(),
        native(&database),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let source = root.path().join("graph");
    let worker_source = source.clone();
    let fixture = backend
        .prepare_resource(1_000_000, move || {
            let projection = BinaryEntityProjection::admit_catalog(
                &meta_relational_reasoning::RelationCatalog::admit(vec![schema()]).unwrap(),
                schema().id(),
            )
            .unwrap();
            let receipt = write_graphar_dataset(
                &worker_source,
                &projection,
                &[projection.project(&fact()).unwrap()],
            )
            .unwrap();
            let local = MemoryContentStore::default();
            for file in receipt.inventory().files() {
                let bytes = std::fs::read(worker_source.join(file.path())).unwrap();
                assert_eq!(
                    *file.cid(),
                    local
                        .put(ContentBlock::new(ContentCodec::Raw, &bytes))
                        .unwrap()
                );
            }
            let ipc = facts_to_ipc(&schema(), &[fact()]).unwrap();
            Ok(Fixture::with_dataset(
                receipt.inventory().clone(),
                local,
                &ipc,
            ))
        })
        .await
        .unwrap();
    let owned = fixture.clone();
    let prepared = backend
        .prepare_resource(1_000_000, move || Ok(owned.get().prepare()))
        .await
        .unwrap();
    let remote = Remote::default();
    let publication = publish_graph_dataset(
        prepared.get(),
        &fixture.get().local,
        &remote,
        &remote,
        || async { Ok(()) },
    )
    .await
    .unwrap();
    let write = ConditionalContentWrite {
        scope: "dataset",
        operation_id: "publish",
        expected: None,
        replacement: *prepared.get().root(),
    };
    let port = backend.profile("graph.production.v1", "tenant").unwrap();
    port.commit_graph_publication(write, Some(&publication), |_| Ok::<_, ()>(()))
        .await
        .unwrap();
    let expected_query = fixture.get().query.clone();
    let binding = fixture.get().binding.clone();
    let inventory = fixture.get().inventory.clone();
    let snapshot = fixture.get().snapshot.clone();
    let relations = fixture.get().relations.clone();
    let entities = fixture.get().entities.clone();
    drop(prepared);
    drop(fixture);
    backend.shutdown().await.unwrap();
    std::fs::remove_dir_all(source).unwrap();
    let reopened = Backend::open(
        BackendConfig::default(),
        native(&database),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .profile("graph.production.v1", "tenant")
            .unwrap()
            .recover(write)
            .await
            .unwrap()
            .unwrap()
            .committed
            .root,
        write.replacement
    );
    let cold = Arc::new(MemoryContentStore::default());
    let restored = restore_graph_dataset(
        cold.as_ref(),
        &remote,
        &write.replacement,
        &expected_query,
        &relations,
        &entities,
        (limits(), GraphInventoryLimits::default()),
    )
    .await
    .unwrap();
    assert_eq!(restored.root(), &write.replacement);
    let restored = reopened
        .prepare_resource(1_000_000, move || Ok(restored))
        .await
        .unwrap();
    let worker_query = expected_query.clone();
    let native_capture = reopened
        .prepare_resource(1_000_000, move || {
            let directory = tempfile::tempdir().unwrap();
            for file in inventory.files() {
                let path = directory.path().join(file.path());
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, cold.get(file.cid()).unwrap()).unwrap();
            }
            let projection =
                BinaryEntityProjection::admit_catalog(&relations, schema().id()).unwrap();
            Ok(capture_graphar_snapshot(
                directory.path(),
                &worker_query,
                binding,
                &inventory,
                &projection,
                GraphInventoryLimits::default(),
                GraphArReadLimits::new(2, 1),
            )
            .unwrap())
        })
        .await
        .unwrap();
    assert_eq!(
        native_capture.get().facts(&expected_query).unwrap(),
        &[fact()]
    );
    let schema_ref = Arc::new(mrr_data_arrow::project_fact_schema(&schema()).unwrap());
    let expected = expected_query.clone();
    let retained = native_capture.clone();
    let mut query = reopened
        .query_arrow(
            schema_ref,
            ArrowQueryLimits {
                max_rows: 1,
                max_batches: 1,
                max_batch_bytes: 100_000,
                max_retained_bytes: 100_000,
                channel_capacity: 1,
            },
            100_000,
            move |out| {
                out.emit(|| {
                    facts_to_record_batch(
                        &schema(),
                        retained
                            .get()
                            .facts(&expected)
                            .map_err(|_| ArrowQueryError::Driver)?,
                    )
                    .map_err(|_| ArrowQueryError::Driver)
                })
            },
        )
        .unwrap();
    let batch = query.next_batch().await.unwrap().unwrap();
    assert!(query.next_batch().await.unwrap().is_none());
    assert_eq!(query.summary().unwrap().rows, 1);
    let profile = datafusion_engine_profile().unwrap();
    let bound = bind_data_query(expected_query.query(), &snapshot, &profile).unwrap();
    let output =
        execute_binary_entity_query(expected_query.query(), &schema(), batch.batch().clone())
            .await
            .unwrap();
    let candidate = project_data_query_output(&bound, &profile, output).unwrap();
    let admitted = admit_query_result_candidate(
        expected_query.query(),
        &candidate,
        QueryResultLimits::new(
            NonZeroUsize::new(16).unwrap(),
            NonZeroUsize::new(32).unwrap(),
        ),
    )
    .unwrap();
    assert_eq!(admitted.row_count(), 1);
    drop(batch);
    drop(query);
    drop(native_capture);
    drop(restored);
    reopened.shutdown().await.unwrap();
}
