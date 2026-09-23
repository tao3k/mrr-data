use super::compile_original_source;
use crate::{
    PropertyEntityRow, PropertyRelationRow, PropertySnapshotInput, PropertySnapshotLimits,
    PropertySnapshotRows, PropertySourceWorkerQuery, execute_property_source_worker_query,
    materialize_property_snapshot,
};
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use meta_relational_reasoning as mrr;
use mrr_data_content::{
    ContentBlock, ContentCodec, ContentStore, MemoryContentStore, RemoteContentStore, RemoteError,
    RemoteFuture, RemoteTransferLimits, SnapshotTransferLimits, TransferSession, publish_snapshot,
};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, EntityDescriptor, RelationDescriptor,
    SnapshotBlock, SnapshotManifest, SnapshotManifestRequest, raw_cid,
};
use mrr_data_datafusion::PropertyQueryLimits;
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

const SOURCE: &str = include_str!("../fixtures/healthcare-case-profile-relations.gql");
const SOURCE_DIGEST: &str =
    "sha256:7a3a88a9ebd24cd738d426c0def633247d1a0fc13e9e37cca13bb23e90ba0c63";

#[test]
fn parser_owned_source_receipt_rejects_source_drift() {
    let expected = format!("sha256:{}", crate::protocol::digest(SOURCE.as_bytes()));
    assert_eq!(expected, SOURCE_DIGEST);
    let compiled = compile_original_source("healthcare-case-profile", SOURCE, &expected)
        .expect("original GQL source compiles through MRR");
    assert_eq!(compiled.receipt.source_name, "healthcare-case-profile");
    assert_eq!(compiled.receipt.source_digest, expected);
    assert!(
        compile_original_source("healthcare-case-profile", SOURCE, "sha256:stale")
            .unwrap_err()
            .to_string()
            .contains("source digest mismatch")
    );
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

fn entity(name: &str) -> mrr::EntityId {
    mrr::EntityId::from_canonical_bytes(name).unwrap()
}

fn ipc(fields: Vec<Field>, columns: Vec<Vec<String>>) -> Vec<u8> {
    let schema = Arc::new(Schema::new(fields));
    let arrays: Vec<ArrayRef> = columns
        .into_iter()
        .map(|column| Arc::new(StringArray::from(column)) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    bytes
}

fn source_type_ids() -> ([mrr::EntityId; 3], [mrr::RelationId; 2]) {
    // The frontend owns label-to-type identity; the physical fixture does not
    // restate its namespace or derive parallel identifiers from label text.
    let digest = format!("sha256:{}", crate::protocol::digest(SOURCE.as_bytes()));
    let compiled = compile_original_source("healthcare-case-profile", SOURCE, &digest).unwrap();
    let path = &compiled.query.graph().paths()[0];
    let entities = [
        path.start().types()[0],
        path.segments()[0].node().types()[0],
        path.segments()[1].node().types()[0],
    ];
    let relations = [
        path.segments()[0].relation().types()[0],
        path.segments()[1].relation().types()[0],
    ];
    (entities, relations)
}

fn fixture() -> (
    SnapshotBlock,
    Vec<Vec<u8>>,
    mrr::RelationCatalog,
    mrr::EntityCatalog,
) {
    let (entity_ids, relation_ids) = source_type_ids();
    let entity_schemas = [
        ("Scenario", "identity"),
        ("Case", "id"),
        ("Profile", "identity"),
    ]
    .into_iter()
    .zip(entity_ids)
    .map(|((name, key), id)| {
        mrr::EntitySchema::new(
            id,
            name,
            vec![mrr::RelationField::new(key, mrr::ValueSchema::String, true).unwrap()],
        )
        .unwrap()
    })
    .collect::<Vec<_>>();
    let relation_schemas = ["HAS_CASE", "HAS_EFFECTIVE_PROFILE"]
        .into_iter()
        .zip(relation_ids)
        .map(|(name, id)| {
            mrr::RelationSchema::new(
                id,
                name,
                vec![
                    mrr::RelationField::new("source", mrr::ValueSchema::Entity, false).unwrap(),
                    mrr::RelationField::new("target", mrr::ValueSchema::Entity, false).unwrap(),
                ],
                vec![],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let entities = mrr::EntityCatalog::admit(entity_schemas.clone()).unwrap();
    let relations = mrr::RelationCatalog::admit(relation_schemas.clone()).unwrap();
    let mut children = Vec::new();
    let entity_descriptors = entity_schemas
        .into_iter()
        .zip([("s1", "healthcare"), ("c1", "case-1"), ("p1", "au-fhir")])
        .map(|(schema, (id, value))| {
            let bytes = ipc(
                vec![
                    Field::new("entity_id", DataType::Utf8, false),
                    Field::new(schema.properties()[0].name(), DataType::Utf8, true),
                ],
                vec![vec![entity(id).to_string()], vec![value.to_owned()]],
            );
            let descriptor = BatchDescriptor::new(raw_cid(&bytes), 1, bytes.len() as u64).unwrap();
            children.push(bytes);
            EntityDescriptor::new(schema, 1, vec![descriptor]).unwrap()
        })
        .collect();
    let relation_descriptors = relation_schemas
        .into_iter()
        .zip([("s1", "c1"), ("c1", "p1")])
        .map(|(schema, (from, to))| {
            let bytes = ipc(
                vec![
                    Field::new("source", DataType::Utf8, false),
                    Field::new("target", DataType::Utf8, false),
                ],
                vec![vec![entity(from).to_string()], vec![entity(to).to_string()]],
            );
            let descriptor = BatchDescriptor::new(raw_cid(&bytes), 1, bytes.len() as u64).unwrap();
            children.push(bytes);
            RelationDescriptor::new(schema.id(), 1, vec![descriptor]).unwrap()
        })
        .collect();
    let generation = mrr::GenerationId::from_canonical_bytes("healthcare-test").unwrap();
    let semantic = mrr::SemanticSnapshot::admit(
        generation,
        vec![
            mrr::RevisionBinding::admit(
                mrr::ExternalRevisionIdentity::new("test", "healthcare", "revision").unwrap(),
                generation,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let coverage = b"complete healthcare fixture".to_vec();
    let manifest = SnapshotManifest::admit(
        SnapshotManifestRequest::new(
            semantic,
            &relations,
            &entities,
            relation_descriptors,
            CoverageDescriptor::new(CoverageKind::Complete, raw_cid(&coverage)).unwrap(),
        )
        .with_entities(entity_descriptors),
    )
    .unwrap();
    children.push(coverage);
    (
        SnapshotBlock::encode(manifest).unwrap(),
        children,
        relations,
        entities,
    )
}

#[tokio::test]
async fn worker_restores_cold_and_warm_then_admits_original_gql() {
    let (snapshot, children, relations, entities) = fixture();
    let source_store = MemoryContentStore::default();
    for bytes in &children {
        source_store
            .put(ContentBlock::new(ContentCodec::Raw, bytes))
            .unwrap();
    }
    let remote = Remote::default();
    let transfer_limits = SnapshotTransferLimits::new(100_000, 10, 100_000, 1_000_000);
    publish_snapshot(
        &source_store,
        &remote,
        &snapshot,
        &relations,
        &entities,
        transfer_limits,
    )
    .await
    .unwrap();
    let cache = MemoryContentStore::default();
    let expected = format!("sha256:{}", crate::protocol::digest(SOURCE.as_bytes()));
    assert_eq!(expected, SOURCE_DIGEST);
    let session = TransferSession::new(
        Duration::from_secs(20),
        RemoteTransferLimits {
            operations: 20,
            bytes: 2_000_000,
            attempts_per_operation: 1,
            retry_delay: Duration::ZERO,
        },
    )
    .unwrap();
    let mut digest = None;
    for _ in 0..2 {
        let result = execute_property_source_worker_query(
            PropertySourceWorkerQuery {
                root: snapshot.cid(),
                source_name: "healthcare-case-profile",
                source_text: SOURCE,
                expected_source_digest: &expected,
                relation_catalog: &relations,
                entity_catalog: &entities,
                transfer_limits,
                physical_limits: PropertyQueryLimits {
                    max_input_rows: 10,
                    max_input_bytes: 1_000_000,
                    max_join_rows: 10,
                    max_output_cells: 30,
                    execution_memory_bytes: 16 * 1024 * 1024,
                },
                result_limits: mrr::QueryResultLimits::new(
                    NonZeroUsize::new(10).unwrap(),
                    NonZeroUsize::new(30).unwrap(),
                ),
            },
            &cache,
            &remote,
            &session,
        )
        .await
        .unwrap();
        assert_eq!(result.root, snapshot.cid().to_string());
        assert_eq!(result.compilation.source_digest, expected);
        let scalar = |text: &str| mrr::QueryResultValue::Scalar {
            schema: mrr::ValueSchema::String,
            value: mrr::Value::String(text.into()),
        };
        assert_eq!(
            result.candidate.rows(),
            &[vec![
                scalar("healthcare"),
                scalar("case-1"),
                scalar("au-fhir")
            ]]
        );
        assert_eq!(result.admission.row_count(), 1);
        let current = result.admission.digest().to_vec();
        if let Some(previous) = &digest {
            assert_eq!(previous, &current);
        }
        digest = Some(current);
    }
}

fn healthcare_property_rows() -> PropertySnapshotRows {
    let (entity_ids, relation_ids) = source_type_ids();
    let mut rows = PropertySnapshotRows::default();
    for (type_id, identity, property, value) in [
        (entity_ids[0], "s1", "identity", "healthcare"),
        (entity_ids[1], "c1", "id", "case-1"),
        (entity_ids[2], "p1", "identity", "au-fhir"),
    ] {
        rows.entities.insert(
            type_id,
            vec![PropertyEntityRow {
                entity_id: entity(identity),
                properties: BTreeMap::from([(property.to_owned(), Some(value.to_owned()))]),
            }],
        );
    }
    for (type_id, source, target) in [(relation_ids[0], "s1", "c1"), (relation_ids[1], "c1", "p1")]
    {
        rows.relations.insert(
            type_id,
            vec![PropertyRelationRow {
                source: entity(source),
                target: entity(target),
            }],
        );
    }
    rows
}

#[tokio::test]
async fn produced_property_rows_publish_and_admit_original_healthcare_gql() {
    let (reference, _, relations, entities) = fixture();
    let rows = healthcare_property_rows();
    let evidence = b"complete healthcare fixture";
    let source_store = MemoryContentStore::default();
    let produced = materialize_property_snapshot(
        PropertySnapshotInput {
            semantic_snapshot: reference.manifest().semantic_snapshot().clone(),
            relation_catalog: &relations,
            entity_catalog: &entities,
            rows: &rows,
            coverage: CoverageDescriptor::new(CoverageKind::Complete, raw_cid(evidence)).unwrap(),
            coverage_bytes: evidence,
            limits: PropertySnapshotLimits {
                max_rows: 10,
                max_blocks: 10,
                max_block_bytes: 100_000,
                max_total_bytes: 1_000_000,
            },
        },
        &source_store,
    )
    .await
    .unwrap();
    assert_eq!(produced.row_count, 5);
    let remote = Remote::default();
    let transfer_limits = SnapshotTransferLimits::new(100_000, 10, 100_000, 1_000_000);
    publish_snapshot(
        &source_store,
        &remote,
        &produced.snapshot,
        &relations,
        &entities,
        transfer_limits,
    )
    .await
    .unwrap();
    let digest = format!("sha256:{}", crate::protocol::digest(SOURCE.as_bytes()));
    let session = TransferSession::new(
        Duration::from_secs(20),
        RemoteTransferLimits {
            operations: 20,
            bytes: 2_000_000,
            attempts_per_operation: 1,
            retry_delay: Duration::ZERO,
        },
    )
    .unwrap();
    let result = execute_property_source_worker_query(
        PropertySourceWorkerQuery {
            root: produced.snapshot.cid(),
            source_name: "healthcare-case-profile",
            source_text: SOURCE,
            expected_source_digest: &digest,
            relation_catalog: &relations,
            entity_catalog: &entities,
            transfer_limits,
            physical_limits: PropertyQueryLimits {
                max_input_rows: 10,
                max_input_bytes: 1_000_000,
                max_join_rows: 10,
                max_output_cells: 30,
                execution_memory_bytes: 16 * 1024 * 1024,
            },
            result_limits: mrr::QueryResultLimits::new(
                NonZeroUsize::new(10).unwrap(),
                NonZeroUsize::new(30).unwrap(),
            ),
        },
        &MemoryContentStore::default(),
        &remote,
        &session,
    )
    .await
    .unwrap();
    assert_eq!(result.admission.row_count(), 1);
    let scalar = |text: &str| mrr::QueryResultValue::Scalar {
        schema: mrr::ValueSchema::String,
        value: mrr::Value::String(text.into()),
    };
    assert_eq!(
        result.candidate.rows(),
        &[vec![
            scalar("healthcare"),
            scalar("case-1"),
            scalar("au-fhir")
        ]]
    );
}
