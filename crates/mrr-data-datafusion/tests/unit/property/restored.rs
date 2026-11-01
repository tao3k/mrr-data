use super::{DataFusionQueryError, Fixture, execute_property_path_query, fixture, limits, mrr};
use crate::{RestoredPropertyQuery, execute_restored_property_path_query};
use arrow_array::RecordBatch;
use arrow_ipc::{
    CompressionType,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use mrr_data_content::{
    ContentBlock, ContentCodec, ContentSource, ContentStore, MemoryContentStore,
    RemoteContentStore, RemoteError, RemoteFuture, RestoredSnapshot, SnapshotTransferLimits,
    publish_snapshot, restore_snapshot,
};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, EntityDescriptor, RelationDescriptor,
    SnapshotBlock, SnapshotManifest, SnapshotManifestRequest, raw_cid,
};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

mod search;
#[cfg(feature = "source-handoff")]
mod source_handoff;
mod transformation;
#[tokio::test]
async fn mrr_dispatches_to_data_backend_and_admits_original_physical_candidate() {
    let f = fixture();
    let (cold, _, relations, entities) = restored(&f, EntityChildMode::Valid).await;
    println!("Dispatch fixture immutable physical root restored");
    let query = f.query.clone();
    println!("Original MRR query supplied to backend");
    let backend = crate::RestoredPropertyBackend {
        restored: &cold,
        relation_catalog: &relations,
        entity_catalog: &entities,
        limits: limits(),
    };
    let result_limits = mrr::QueryResultLimits::new(
        NonZeroUsize::new(100).unwrap(),
        NonZeroUsize::new(300).unwrap(),
    );
    let execution = query
        .execute_with(&backend, result_limits)
        .await
        .expect("MRR dispatch and result admission");
    println!("Data backend execution returned to MRR admission");
    assert_eq!(execution.receipt().row_count(), 3);
    assert_eq!(execution.physical_evidence().query(), &query);
    assert_eq!(
        execution.physical_evidence().snapshot_root(),
        cold.snapshot().cid()
    );
    assert_eq!(execution.candidate().rows().len(), 3);
    let cap = NonZeroUsize::new(1_048_576).unwrap();
    let handoff =
        mrr_data_core::DataQueryResultHandoff::export_execution(&execution, result_limits, cap)
            .unwrap();
    let received = handoff
        .verify(execution.physical_evidence(), result_limits, cap)
        .unwrap();
    assert_eq!(received.candidate(), execution.candidate());
    assert_eq!(received.receipt(), execution.receipt());
    println!("Original dispatch candidate preserved through Scheme v2 handoff");
}

#[tokio::test]
async fn mrr_dispatch_preserves_corrupt_backend_failure_instead_of_empty_success() {
    let f = fixture();
    let (cold, _, relations, entities) = restored(&f, EntityChildMode::InvalidIpc).await;
    println!("Corrupt dispatch fixture immutable root restored");
    let query = f.query.clone();
    println!("Original MRR query admitted before physical decode");
    let backend = crate::RestoredPropertyBackend {
        restored: &cold,
        relation_catalog: &relations,
        entity_catalog: &entities,
        limits: limits(),
    };
    let result_limits = mrr::QueryResultLimits::new(
        NonZeroUsize::new(100).unwrap(),
        NonZeroUsize::new(300).unwrap(),
    );
    assert!(matches!(
        query.execute_with(&backend, result_limits).await,
        Err(mrr::PropertyExecutionError::Backend(
            DataFusionQueryError::RestoredSnapshot("truncated IPC metadata")
        ))
    ));
}

#[tokio::test]
async fn mrr_dispatch_rejects_a_new_generation_bound_to_an_old_physical_root() {
    let f = fixture();
    let (cold, _, relations, entities) = restored(&f, EntityChildMode::Valid).await;
    println!("Old-generation physical root restored");
    let generation = mrr::GenerationId::from_canonical_bytes("dispatch-new-generation").unwrap();
    let semantic = mrr::SemanticSnapshot::admit(
        generation,
        vec![
            mrr::RevisionBinding::admit(
                mrr::ExternalRevisionIdentity::new("test", "source", "new-revision").unwrap(),
                generation,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let bundle = mrr::ReasoningBundle::admit(mrr::ReasoningBundleDeclaration {
        entities: f
            .entities
            .iter()
            .map(|table| table.schema.clone())
            .collect(),
        relations: f
            .relations
            .iter()
            .map(|table| table.schema.clone())
            .collect(),
        query_templates: vec![mrr::QueryTemplate::new(f.query.query().clone(), vec![])],
        ..Default::default()
    })
    .unwrap();
    let query = mrr::bind_query_to_catalog(&bundle, f.query.query().id(), &semantic).unwrap();
    assert_ne!(query.digest(), f.query.digest());
    assert_eq!(
        crate::property_transformation_endpoint(&query),
        crate::property_transformation_endpoint(&f.query),
        "generation changes instance binding, not query semantics"
    );
    println!("New-generation query admitted by MRR");
    let backend = crate::RestoredPropertyBackend {
        restored: &cold,
        relation_catalog: &relations,
        entity_catalog: &entities,
        limits: limits(),
    };
    let result_limits = mrr::QueryResultLimits::new(
        NonZeroUsize::new(100).unwrap(),
        NonZeroUsize::new(300).unwrap(),
    );
    assert!(matches!(
        query.execute_with(&backend, result_limits).await,
        Err(mrr::PropertyExecutionError::Backend(
            DataFusionQueryError::PhysicalBinding(
                mrr_data_core::DataQueryBindingError::GenerationMismatch { .. }
            )
        ))
    ));
}

#[tokio::test]
async fn backend_success_still_requires_mrr_result_admission() {
    let f = fixture();
    let (cold, _, relations, entities) = restored(&f, EntityChildMode::Valid).await;
    println!("Physical root restored before MRR admission limit gate");
    let backend = crate::RestoredPropertyBackend {
        restored: &cold,
        relation_catalog: &relations,
        entity_catalog: &entities,
        limits: limits(),
    };
    let result_limits =
        mrr::QueryResultLimits::new(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(3).unwrap());
    assert!(matches!(
        f.query.execute_with(&backend, result_limits).await,
        Err(mrr::PropertyExecutionError::Admission(_))
    ));
}

#[derive(Default)]
struct Remote(Mutex<BTreeMap<String, Vec<u8>>>);
impl RemoteContentStore for Remote {
    fn get<'a>(&'a self, cid: &'a cid::Cid, limit: usize) -> RemoteFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            let value = self.0.lock().unwrap().get(&cid.to_string()).cloned();
            if value.as_ref().is_some_and(|bytes| bytes.len() > limit) {
                return Err(RemoteError::TooLarge);
            }
            Ok(value)
        })
    }
    fn put<'a>(&'a self, block: ContentBlock<'a>) -> RemoteFuture<'a, ()> {
        Box::pin(async move {
            self.0
                .lock()
                .unwrap()
                .insert(block.cid().to_string(), block.bytes().to_vec());
            Ok(())
        })
    }
}

fn ipc(batch: &RecordBatch, schema: &SchemaRef, compressed: bool) -> Vec<u8> {
    let mut bytes = Vec::new();
    let batch = RecordBatch::try_new(schema.clone(), batch.columns().to_vec()).unwrap();
    let options = IpcWriteOptions::default()
        .try_with_compression(compressed.then_some(CompressionType::LZ4_FRAME))
        .unwrap();
    let mut writer = StreamWriter::try_new_with_options(&mut bytes, schema, options).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    bytes
}

#[derive(Clone, Copy)]
enum EntityChildMode {
    Valid,
    InvalidIpc,
    WrongRows,
    NullableDrift,
    Compressed,
}

fn test_property_type(schema: &mrr::ValueSchema) -> DataType {
    match schema {
        mrr::ValueSchema::String => DataType::Utf8,
        mrr::ValueSchema::Integer => DataType::Int64,
        _ => panic!("unsupported test property"),
    }
}

fn snapshot(
    f: &Fixture,
    mode: EntityChildMode,
) -> (
    SnapshotBlock,
    Vec<Vec<u8>>,
    mrr::RelationCatalog,
    mrr::EntityCatalog,
) {
    let relations = mrr::RelationCatalog::admit(
        f.relations
            .iter()
            .map(|table| table.schema.clone())
            .collect(),
    )
    .unwrap();
    let entities = mrr::EntityCatalog::admit(
        f.entities
            .iter()
            .map(|table| table.schema.clone())
            .collect(),
    )
    .unwrap();
    let mut children = Vec::new();
    let entity_descriptors = f
        .entities
        .iter()
        .enumerate()
        .map(|(index, table)| {
            let bytes = if matches!(mode, EntityChildMode::InvalidIpc) && index == 0 {
                b"not Arrow IPC".to_vec()
            } else {
                let mut fields = vec![Field::new(
                    "entity_id",
                    DataType::Utf8,
                    matches!(mode, EntityChildMode::NullableDrift) && index == 0,
                )];
                fields.extend(table.schema.properties().iter().map(|property| {
                    Field::new(
                        property.name(),
                        test_property_type(property.schema()),
                        property.nullable(),
                    )
                }));
                ipc(
                    &table.batch,
                    &Arc::new(Schema::new(fields)),
                    matches!(mode, EntityChildMode::Compressed) && index == 0,
                )
            };
            let declared_rows = if matches!(mode, EntityChildMode::WrongRows) && index == 0 {
                table.batch.num_rows() as u64 - 1
            } else {
                table.batch.num_rows() as u64
            };
            let child =
                BatchDescriptor::new(raw_cid(&bytes), declared_rows, bytes.len() as u64).unwrap();
            children.push(bytes);
            EntityDescriptor::new(table.schema.clone(), declared_rows, vec![child]).unwrap()
        })
        .collect();
    let relation_descriptors = f
        .relations
        .iter()
        .map(|table| {
            let schema = Arc::new(Schema::new(
                table
                    .schema
                    .fields()
                    .iter()
                    .map(|field| Field::new(field.name(), DataType::Utf8, field.nullable()))
                    .collect::<Vec<_>>(),
            ));
            let bytes = ipc(&table.batch, &schema, false);
            let child = BatchDescriptor::new(
                raw_cid(&bytes),
                table.batch.num_rows() as u64,
                bytes.len() as u64,
            )
            .unwrap();
            children.push(bytes);
            RelationDescriptor::new(
                table.schema.id(),
                table.batch.num_rows() as u64,
                vec![child],
            )
            .unwrap()
        })
        .collect();
    let coverage = b"complete Healthcare entity and relation evidence".to_vec();
    let declaration = CoverageDescriptor::new(CoverageKind::Complete, raw_cid(&coverage)).unwrap();
    children.push(coverage);
    let manifest = SnapshotManifest::admit(
        SnapshotManifestRequest::new(
            f.semantic.clone(),
            &relations,
            &entities,
            relation_descriptors,
            declaration,
        )
        .with_entities(entity_descriptors),
    )
    .unwrap();
    (
        SnapshotBlock::encode(manifest).unwrap(),
        children,
        relations,
        entities,
    )
}

async fn restored(
    f: &Fixture,
    mode: EntityChildMode,
) -> (
    RestoredSnapshot,
    RestoredSnapshot,
    mrr::RelationCatalog,
    mrr::EntityCatalog,
) {
    let (snapshot, children, relations, entities) = snapshot(f, mode);
    let local = MemoryContentStore::default();
    for bytes in &children {
        local
            .put(ContentBlock::new(ContentCodec::Raw, bytes))
            .unwrap();
    }
    let remote = Remote::default();
    let transfer_limits = SnapshotTransferLimits::new(100_000, 10, 100_000, 1_000_000);
    publish_snapshot(
        &local,
        &remote,
        &snapshot,
        &relations,
        &entities,
        transfer_limits,
    )
    .await
    .unwrap();
    let target = MemoryContentStore::default();
    let cold = restore_snapshot(
        &target,
        &remote,
        snapshot.cid(),
        &relations,
        &entities,
        transfer_limits,
    )
    .await
    .unwrap();
    let warm = restore_snapshot(
        &target,
        &remote,
        snapshot.cid(),
        &relations,
        &entities,
        transfer_limits,
    )
    .await
    .unwrap();
    (cold, warm, relations, entities)
}

#[tokio::test]
async fn verified_cold_and_warm_property_snapshots_reach_the_existing_mrr_admission() {
    let f = super::integer_fixture();
    let (cold, warm, relations, entities) = restored(&f, EntityChildMode::Valid).await;
    println!("Cold and warm immutable property snapshots restored");
    assert!(
        warm.sources()
            .values()
            .all(|source| *source == ContentSource::Local)
    );
    let direct = execute_property_path_query(&f.query, &f.entities, &f.relations, limits())
        .await
        .unwrap();
    println!("Direct property query completed");
    let result_limits = mrr::QueryResultLimits::new(
        NonZeroUsize::new(100).unwrap(),
        NonZeroUsize::new(300).unwrap(),
    );
    let cap = NonZeroUsize::new(1024 * 1024).unwrap();
    for snapshot in [&cold, &warm] {
        let handoff = crate::execute_restored_property_query_handoff(
            RestoredPropertyQuery {
                query: &f.query,
                restored: snapshot,
                relation_catalog: &relations,
                entity_catalog: &entities,
                limits: limits(),
            },
            result_limits,
            cap,
        )
        .await
        .unwrap();
        let profile = crate::datafusion_engine_profile().unwrap();
        let bound =
            mrr_data_core::bind_data_query(&f.query, snapshot.snapshot(), &profile).unwrap();
        let candidate =
            mrr_data_core::project_data_query_output(&bound, &profile, direct.clone()).unwrap();
        let receipt =
            mrr::admit_query_result_candidate(&f.query, &candidate, result_limits).unwrap();
        let bytes = handoff.result_bytes();
        assert!(bytes.starts_with(b"(object "));
        let received = handoff.verify(&bound, result_limits, cap).unwrap();
        assert_eq!(received.candidate(), &candidate);
        assert_eq!(received.receipt(), &receipt);
        assert_eq!(bound.snapshot_root(), snapshot.snapshot().cid());
        assert!(
            mrr::verify_query_result_transport(
                &f.query,
                &bytes[..bytes.len() - 1],
                result_limits,
                cap,
            )
            .is_err()
        );
        println!("RESTORED-PROPERTY -> ORIGINAL-MRR-SCHEME-V2 verified");
    }
}

#[tokio::test]
async fn verified_content_with_non_ipc_entity_child_is_not_query_evidence() {
    let f = fixture();
    let (cold, _, relations, entities) = restored(&f, EntityChildMode::InvalidIpc).await;
    let error = execute_restored_property_path_query(RestoredPropertyQuery {
        query: &f.query,
        restored: &cold,
        relation_catalog: &relations,
        entity_catalog: &entities,
        limits: limits(),
    })
    .await
    .err()
    .unwrap();
    assert!(matches!(
        error,
        DataFusionQueryError::RestoredSnapshot("truncated IPC metadata")
    ));
}

#[tokio::test]
async fn verified_ipc_with_nullable_schema_drift_is_not_query_evidence() {
    let f = fixture();
    let (cold, _, relations, entities) = restored(&f, EntityChildMode::NullableDrift).await;
    let error = execute_restored_property_path_query(RestoredPropertyQuery {
        query: &f.query,
        restored: &cold,
        relation_catalog: &relations,
        entity_catalog: &entities,
        limits: limits(),
    })
    .await
    .err()
    .unwrap();
    assert!(matches!(
        error,
        DataFusionQueryError::InvalidArrowBatch("catalog column shape")
    ));
}

#[tokio::test]
async fn compressed_ipc_is_rejected_before_reader_allocation() {
    let f = fixture();
    let (cold, _, relations, entities) = restored(&f, EntityChildMode::Compressed).await;
    let error = execute_restored_property_path_query(RestoredPropertyQuery {
        query: &f.query,
        restored: &cold,
        relation_catalog: &relations,
        entity_catalog: &entities,
        limits: limits(),
    })
    .await
    .err()
    .unwrap();
    assert!(matches!(
        error,
        DataFusionQueryError::RestoredSnapshot("compressed IPC is not supported by bounded decode")
    ));
}

#[tokio::test]
async fn verified_content_with_false_declared_rows_is_not_query_evidence() {
    let f = fixture();
    let (cold, _, relations, entities) = restored(&f, EntityChildMode::WrongRows).await;
    let error = execute_restored_property_path_query(RestoredPropertyQuery {
        query: &f.query,
        restored: &cold,
        relation_catalog: &relations,
        entity_catalog: &entities,
        limits: limits(),
    })
    .await
    .err()
    .unwrap();
    assert!(matches!(
        error,
        DataFusionQueryError::RestoredSnapshot("child row count mismatch")
    ));
}

#[tokio::test]
async fn declared_input_budget_rejects_before_arrow_decoding() {
    let f = fixture();
    let (cold, _, relations, entities) = restored(&f, EntityChildMode::InvalidIpc).await;
    let mut tight = limits();
    tight.max_input_bytes = 1;
    let error = execute_restored_property_path_query(RestoredPropertyQuery {
        query: &f.query,
        restored: &cold,
        relation_catalog: &relations,
        entity_catalog: &entities,
        limits: tight,
    })
    .await
    .err()
    .unwrap();
    assert!(matches!(
        error,
        DataFusionQueryError::ResourceLimit("declared input rows or bytes")
    ));
}
