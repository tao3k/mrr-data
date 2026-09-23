use super::{
    PropertyEntityRow, PropertyRelationRow, PropertySnapshotInput, PropertySnapshotLimits,
    PropertySnapshotRows, materialize_property_snapshot,
};
use meta_relational_reasoning as mrr;
use mrr_data_content::{ContentStore, MemoryContentStore};
use mrr_data_core::{CoverageDescriptor, CoverageKind, raw_cid};
use std::collections::BTreeMap;

fn fixture() -> (
    mrr::SemanticSnapshot,
    mrr::EntityCatalog,
    mrr::RelationCatalog,
    PropertySnapshotRows,
) {
    let entity_type = mrr::EntityId::from_canonical_bytes("test-type").unwrap();
    let relation_type = mrr::RelationId::from_canonical_bytes("test-edge").unwrap();
    let entities = mrr::EntityCatalog::admit(vec![
        mrr::EntitySchema::new(
            entity_type,
            "Test",
            vec![mrr::RelationField::new("identity", mrr::ValueSchema::String, false).unwrap()],
        )
        .unwrap(),
    ])
    .unwrap();
    let relations = mrr::RelationCatalog::admit(vec![
        mrr::RelationSchema::new(
            relation_type,
            "LINKS",
            vec![
                mrr::RelationField::new("source", mrr::ValueSchema::Entity, false).unwrap(),
                mrr::RelationField::new("target", mrr::ValueSchema::Entity, false).unwrap(),
            ],
            vec![],
        )
        .unwrap(),
    ])
    .unwrap();
    let generation = mrr::GenerationId::from_canonical_bytes("property-source-test").unwrap();
    let semantic = mrr::SemanticSnapshot::admit(
        generation,
        vec![
            mrr::RevisionBinding::admit(
                mrr::ExternalRevisionIdentity::new("test", "source", "revision").unwrap(),
                generation,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let mut rows = PropertySnapshotRows::default();
    rows.entities.insert(
        entity_type,
        ["second", "first"]
            .into_iter()
            .map(|name| PropertyEntityRow {
                entity_id: mrr::EntityId::from_canonical_bytes(name).unwrap(),
                properties: BTreeMap::from([("identity".to_owned(), Some(name.to_owned()))]),
            })
            .collect(),
    );
    rows.relations.insert(
        relation_type,
        vec![PropertyRelationRow {
            source: mrr::EntityId::from_canonical_bytes("first").unwrap(),
            target: mrr::EntityId::from_canonical_bytes("second").unwrap(),
        }],
    );
    (semantic, entities, relations, rows)
}

fn limits() -> PropertySnapshotLimits {
    PropertySnapshotLimits {
        max_rows: 10,
        max_blocks: 5,
        max_block_bytes: 100_000,
        max_total_bytes: 500_000,
    }
}

#[tokio::test]
async fn materializes_deterministic_verified_property_closure() {
    let (semantic, entities, relations, mut rows) = fixture();
    let evidence = b"complete admitted source rows";
    let coverage = CoverageDescriptor::new(CoverageKind::Complete, raw_cid(evidence)).unwrap();
    let local = MemoryContentStore::default();
    let first = materialize_property_snapshot(
        PropertySnapshotInput {
            semantic_snapshot: semantic.clone(),
            relation_catalog: &relations,
            entity_catalog: &entities,
            rows: &rows,
            coverage: coverage.clone(),
            coverage_bytes: evidence,
            limits: limits(),
        },
        &local,
    )
    .await
    .unwrap();
    assert_eq!(first.row_count, 3);
    assert_eq!(first.snapshot.manifest().entities().len(), 1);
    assert_eq!(first.snapshot.manifest().relations().len(), 1);
    assert_eq!(
        ContentStore::get(&local, first.snapshot.cid()).unwrap(),
        first.snapshot.bytes()
    );
    rows.entities.values_mut().next().unwrap().reverse();
    let second = materialize_property_snapshot(
        PropertySnapshotInput {
            semantic_snapshot: semantic,
            relation_catalog: &relations,
            entity_catalog: &entities,
            rows: &rows,
            coverage,
            coverage_bytes: evidence,
            limits: limits(),
        },
        &MemoryContentStore::default(),
    )
    .await
    .unwrap();
    assert_eq!(first.snapshot.cid(), second.snapshot.cid());
}

#[tokio::test]
async fn rejects_missing_tables_dangling_edges_and_coverage_drift() {
    let (semantic, entities, relations, mut rows) = fixture();
    let evidence = b"complete admitted source rows";
    let coverage = CoverageDescriptor::new(CoverageKind::Complete, raw_cid(evidence)).unwrap();
    let local = MemoryContentStore::default();
    rows.relations.clear();
    let result = materialize_property_snapshot(
        PropertySnapshotInput {
            semantic_snapshot: semantic.clone(),
            relation_catalog: &relations,
            entity_catalog: &entities,
            rows: &rows,
            coverage: coverage.clone(),
            coverage_bytes: evidence,
            limits: limits(),
        },
        &local,
    )
    .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("relation table set")
    );
    rows.relations.insert(
        relations.relations()[0].id(),
        vec![PropertyRelationRow {
            source: mrr::EntityId::from_canonical_bytes("absent").unwrap(),
            target: mrr::EntityId::from_canonical_bytes("second").unwrap(),
        }],
    );
    let result = materialize_property_snapshot(
        PropertySnapshotInput {
            semantic_snapshot: semantic.clone(),
            relation_catalog: &relations,
            entity_catalog: &entities,
            rows: &rows,
            coverage: coverage.clone(),
            coverage_bytes: evidence,
            limits: limits(),
        },
        &local,
    )
    .await;
    assert!(result.unwrap_err().to_string().contains("dangling"));
    rows.relations.values_mut().next().unwrap()[0].source =
        mrr::EntityId::from_canonical_bytes("first").unwrap();
    let result = materialize_property_snapshot(
        PropertySnapshotInput {
            semantic_snapshot: semantic,
            relation_catalog: &relations,
            entity_catalog: &entities,
            rows: &rows,
            coverage,
            coverage_bytes: b"changed evidence",
            limits: limits(),
        },
        &local,
    )
    .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("coverage evidence CID mismatch")
    );
}
