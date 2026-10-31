//! Native property mapping and bounded physical two-hop acceptance, not GQL parsing.
use super::fixture;
use crate::{
    GraphArChunkLayout, GraphArEntityPropertyError as Error, GraphArEntityPropertyLimits,
    GraphArEntityPropertyProjection, GraphArEntityPropertyTable, capture_graphar_entity_properties,
    write_graphar_entity_properties,
};
use meta_relational_reasoning as mrr;
use mrr_data_core::GraphInventoryLimits;
use std::num::NonZeroUsize;

pub(super) fn limits() -> GraphArEntityPropertyLimits {
    GraphArEntityPropertyLimits {
        max_rows: fixture::workload_rows(),
        max_types: 8,
        max_properties: 16,
        max_value_bytes: 1 << 20,
        inventory: GraphInventoryLimits::default(),
    }
}
pub(super) fn inputs(
    f: &fixture::Fixture,
) -> (
    GraphArEntityPropertyProjection,
    Vec<GraphArEntityPropertyTable>,
) {
    let catalog =
        mrr::EntityCatalog::admit(f.entities.iter().map(|t| t.schema.clone()).collect()).unwrap();
    (
        GraphArEntityPropertyProjection::admit(&catalog).unwrap(),
        f.entities
            .iter()
            .map(|t| GraphArEntityPropertyTable {
                schema: t.schema.clone(),
                batch: t.batch.clone(),
            })
            .collect(),
    )
}
pub(super) fn expected() -> Vec<Vec<mrr::QueryResultValue>> {
    let scalar = |s: &str| mrr::QueryResultValue::Scalar {
        schema: mrr::ValueSchema::String,
        value: mrr::Value::String(s.into()),
    };
    vec![
        vec![scalar("healthcare"), scalar("one"), scalar("shared")],
        vec![
            scalar("healthcare"),
            scalar("one"),
            mrr::QueryResultValue::Null,
        ],
        vec![scalar("healthcare"), scalar("two"), scalar("shared")],
    ]
}
#[tokio::test]
async fn native_property_tables_feed_two_hop_executor_and_mrr_admission() {
    let f = fixture::fixture();
    let (projection, tables) = inputs(&f);
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let receipt = write_graphar_entity_properties(
        &source,
        &projection,
        &f.semantic,
        &tables,
        GraphArChunkLayout::new(2, 4).unwrap(),
        limits(),
    )
    .unwrap();
    assert_eq!(receipt.row_count(), 8);
    let captured =
        capture_graphar_entity_properties(&source, &f.query, &projection, &receipt, limits())
            .unwrap();
    std::fs::remove_dir_all(&source).unwrap();
    let restored = captured
        .tables(&f.query)
        .unwrap()
        .iter()
        .map(|t| mrr_data_datafusion::EntityPropertyTable {
            schema: t.schema.clone(),
            batch: t.batch.clone(),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        restored.iter().map(|t| t.batch.num_rows()).sum::<usize>(),
        8
    );
    let relations = fixture::native_relations(&f, directory.path());
    std::fs::remove_dir_all(directory.path()).unwrap();
    let candidate = mrr_data_datafusion::execute_property_path_query(
        &f.query,
        &restored,
        &relations,
        fixture::limits(),
    )
    .await
    .unwrap();
    let mut actual = candidate.rows().to_vec();
    let mut wanted = expected();
    actual.sort_by_key(|r| format!("{r:?}"));
    wanted.sort_by_key(|r| format!("{r:?}"));
    assert_eq!(actual, wanted);
    let admitted_candidate = mrr::CandidateQueryResult::new(
        mrr::QueryResultBinding::for_query(&f.query),
        candidate.columns().to_vec(),
        candidate.rows().to_vec(),
    );
    mrr::admit_query_result_candidate(
        &f.query,
        &admitted_candidate,
        mrr::QueryResultLimits::new(
            NonZeroUsize::new(100).unwrap(),
            NonZeroUsize::new(300).unwrap(),
        ),
    )
    .unwrap();
}
#[test]
fn budgets_and_shape_refuse_before_publication() {
    let f = fixture::fixture();
    let (projection, mut tables) = inputs(&f);
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    for budget in [
        GraphArEntityPropertyLimits {
            max_rows: 1,
            ..limits()
        },
        GraphArEntityPropertyLimits {
            max_value_bytes: 1,
            ..limits()
        },
        GraphArEntityPropertyLimits {
            max_types: 1,
            ..limits()
        },
    ] {
        assert!(matches!(
            write_graphar_entity_properties(
                &source,
                &projection,
                &f.semantic,
                &tables,
                GraphArChunkLayout::default(),
                budget
            ),
            Err(Error::Budget(_))
        ));
        assert!(!source.exists());
    }
    tables.remove(0);
    assert!(matches!(
        write_graphar_entity_properties(
            &source,
            &projection,
            &f.semantic,
            &tables,
            GraphArChunkLayout::default(),
            limits()
        ),
        Err(Error::Shape(_))
    ));
    assert!(!source.exists());
}
#[test]
fn mutation_and_generation_substitution_refuse_capture() {
    let f = fixture::fixture();
    let (projection, tables) = inputs(&f);
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let wrong = mrr::GenerationId::from_canonical_bytes("wrong-generation").unwrap();
    let receipt = write_graphar_entity_properties(
        &source,
        &projection,
        &fixture::semantic(wrong, "wrong-revision"),
        &tables,
        GraphArChunkLayout::default(),
        limits(),
    )
    .unwrap();
    assert!(matches!(
        capture_graphar_entity_properties(&source, &f.query, &projection, &receipt, limits()),
        Err(Error::Scope)
    ));
    let source = directory.path().join("matching");
    let receipt = write_graphar_entity_properties(
        &source,
        &projection,
        &f.semantic,
        &tables,
        GraphArChunkLayout::default(),
        limits(),
    )
    .unwrap();
    std::fs::write(
        source.join("mrr.graph.yaml"),
        b"prefix: /untrusted
",
    )
    .unwrap();
    assert!(matches!(
        capture_graphar_entity_properties(&source, &f.query, &projection, &receipt, limits()),
        Err(Error::Capture(_))
    ));
}
#[test]
fn unsupported_scalar_schema_is_never_stringified() {
    let schema = mrr::EntitySchema::new(
        fixture::entity("typed"),
        "typed",
        vec![mrr::RelationField::new("flag", mrr::ValueSchema::Boolean, false).unwrap()],
    )
    .unwrap();
    let catalog = mrr::EntityCatalog::admit(vec![schema]).unwrap();
    assert!(matches!(
        GraphArEntityPropertyProjection::admit(&catalog),
        Err(Error::UnsupportedSchema)
    ));
}

#[test]
fn empty_types_and_null_empty_unicode_remain_distinct() {
    use arrow_array::{Array, StringArray};
    let f = fixture::fixture();
    let (projection, mut tables) = inputs(&f);
    // An entire declared type with zero rows is a real native empty table.
    tables[0].batch = tables[0].batch.slice(0, 0);
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let receipt = write_graphar_entity_properties(
        &source,
        &projection,
        &f.semantic,
        &tables,
        GraphArChunkLayout::new(2, 4).unwrap(),
        limits(),
    )
    .unwrap();
    let captured =
        capture_graphar_entity_properties(&source, &f.query, &projection, &receipt, limits())
            .unwrap();
    let restored = captured.tables(&f.query).unwrap();
    let empty = restored
        .iter()
        .find(|t| t.schema.id() == tables[0].schema.id())
        .unwrap();
    assert_eq!(empty.batch.num_rows(), 0);
    for original in &tables {
        let actual = restored
            .iter()
            .find(|t| t.schema == original.schema)
            .unwrap();
        let rows = |table: &GraphArEntityPropertyTable| {
            let arrays = table
                .batch
                .columns()
                .iter()
                .map(|a| a.as_any().downcast_ref::<StringArray>().unwrap())
                .collect::<Vec<_>>();
            let mut rows = (0..table.batch.num_rows())
                .map(|r| {
                    arrays
                        .iter()
                        .map(|a| {
                            if a.is_null(r) {
                                None
                            } else {
                                Some(a.value(r).to_owned())
                            }
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            rows.sort();
            rows
        };
        assert_eq!(rows(original), rows(actual));
    }
}
#[cfg(feature = "backend")]
#[tokio::test]
async fn shared_backend_retains_property_lease_through_consuming_transfer() {
    const RESERVED: usize = 1 << 20;
    use mrr_data_backend::{Backend, BackendConfig, ResourceControl, ResourcePreparationError};
    let f = fixture::fixture();
    let (projection, tables) = inputs(&f);
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let receipt = write_graphar_entity_properties(
        &source,
        &projection,
        &f.semantic,
        &tables,
        GraphArChunkLayout::default(),
        limits(),
    )
    .unwrap();
    let request = || crate::GraphArEntityPropertiesRequest {
        source: source.clone(),
        query: f.query.clone(),
        projection: projection.clone(),
        receipt: receipt.clone(),
        limits: limits(),
    };
    let backend = Backend::open(
        BackendConfig {
            max_resource_bytes: RESERVED,
            ..BackendConfig::default()
        },
        crate::tests::snapshot::backend_qualification::MetadataStub,
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let canceled = ResourceControl::new(None);
    canceled.cancel();
    assert!(matches!(
        crate::prepare_graphar_entity_properties(&backend, request(), RESERVED, canceled).await,
        Err(ResourcePreparationError::Preparation(Error::Stop(
            mrr_data_backend::ResourceStop::Cancelled
        )))
    ));
    assert_eq!(backend.status().resource_bytes, 0);
    let captured = crate::prepare_graphar_entity_properties(
        &backend,
        request(),
        RESERVED,
        ResourceControl::new(None),
    )
    .await
    .unwrap();
    let pointer = captured.get().tables(&f.query).unwrap().as_ptr();
    let transferred = captured
        .try_transform(|s| s.into_tables(&f.query))
        .unwrap_or_else(|_| panic!("unique property handle transfers"));
    assert_eq!(transferred.get().as_ptr(), pointer);
    assert_eq!(backend.status().resource_bytes, RESERVED);
    let clone = transferred.clone();
    drop(transferred);
    assert_eq!(backend.status().resource_bytes, RESERVED);
    let mut shutdown = tokio::spawn({
        let backend = backend.clone();
        async move { backend.shutdown().await }
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut shutdown)
            .await
            .is_err()
    );
    drop(clone);
    tokio::time::timeout(std::time::Duration::from_secs(3), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(backend.status().resource_bytes, 0);
}

#[test]
fn snapshot_and_catalog_scope_refuse_capture_and_retained_reuse() {
    let mut f = fixture::fixture();
    let (projection, tables) = inputs(&f);
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let receipt = write_graphar_entity_properties(
        &source,
        &projection,
        &f.semantic,
        &tables,
        GraphArChunkLayout::default(),
        limits(),
    )
    .unwrap();
    let captured =
        capture_graphar_entity_properties(&source, &f.query, &projection, &receipt, limits())
            .unwrap();
    let changed = fixture::semantic(f.semantic.generation(), "changed-revision");
    let query = fixture::rebind(&f, &changed);
    assert!(matches!(
        capture_graphar_entity_properties(&source, &query, &projection, &receipt, limits()),
        Err(Error::Scope)
    ));
    assert!(matches!(captured.tables(&query), Err(Error::Scope)));
    let s = &f.entities[0].schema;
    f.entities[0].schema =
        mrr::EntitySchema::new(s.id(), "ChangedType", s.properties().to_vec()).unwrap();
    let query = fixture::rebind(&f, &f.semantic);
    assert!(matches!(
        capture_graphar_entity_properties(&source, &query, &projection, &receipt, limits()),
        Err(Error::Scope)
    ));
    assert!(matches!(captured.tables(&query), Err(Error::Scope)));
}

#[test]
fn cross_type_aliases_noncanonical_ids_and_column_substitution_never_publish() {
    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    let f = fixture::fixture();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("refused");
    for (identity, reason) in [
        (
            fixture::entity("s1").to_string(),
            "noncanonical or duplicate identity",
        ),
        ("Case/one".to_owned(), "canonical entity identity"),
    ] {
        let (projection, mut tables) = inputs(&f);
        let original = &tables[1].batch;
        tables[1].batch = RecordBatch::try_new(
            original.schema(),
            vec![
                Arc::new(StringArray::from(vec![
                    identity,
                    fixture::entity("c2").to_string(),
                    fixture::entity("c3").to_string(),
                ])),
                original.column(1).clone(),
            ],
        )
        .unwrap();
        assert!(
            matches!(write_graphar_entity_properties(&source, &projection, &f.semantic, &tables, GraphArChunkLayout::default(), limits()), Err(Error::Shape(actual)) if actual == reason)
        );
        assert!(!source.exists());
    }
    let (projection, mut tables) = inputs(&f);
    let original = &tables[1].batch;
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("substituted", DataType::Utf8, true),
    ]));
    tables[1].batch = RecordBatch::try_new(schema, original.columns().to_vec()).unwrap();
    assert!(matches!(
        write_graphar_entity_properties(
            &source,
            &projection,
            &f.semantic,
            &tables,
            GraphArChunkLayout::default(),
            limits()
        ),
        Err(Error::Shape("catalog or Arrow schema"))
    ));
    assert!(!source.exists());
}
