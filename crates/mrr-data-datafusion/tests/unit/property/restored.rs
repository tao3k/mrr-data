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
                    Field::new(property.name(), DataType::Utf8, property.nullable())
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
    let f = fixture();
    let (cold, warm, relations, entities) = restored(&f, EntityChildMode::Valid).await;
    assert!(
        warm.sources()
            .values()
            .all(|source| *source == ContentSource::Local)
    );
    let direct = execute_property_path_query(&f.query, &f.entities, &f.relations, limits())
        .await
        .unwrap();
    for snapshot in [&cold, &warm] {
        let output = execute_restored_property_path_query(RestoredPropertyQuery {
            query: &f.query,
            restored: snapshot,
            relation_catalog: &relations,
            entity_catalog: &entities,
            limits: limits(),
        })
        .await
        .unwrap();
        assert_eq!(output.rows(), direct.rows());
        let candidate = mrr::CandidateQueryResult::new(
            mrr::QueryResultBinding::for_query(&f.query),
            output.columns().to_vec(),
            output.rows().to_vec(),
        );
        mrr::admit_query_result_candidate(
            &f.query,
            &candidate,
            mrr::QueryResultLimits::new(
                NonZeroUsize::new(100).unwrap(),
                NonZeroUsize::new(300).unwrap(),
            ),
        )
        .unwrap();
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
