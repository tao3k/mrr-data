//! Distinct original query, node/relation types and property schema on one Backend.
use super::{RESERVED, authority, capture, executor, metadata, restore, source_fixture};
use crate::tests::entity_properties::{
    combined::{fixture::Fixture, remote::Remote},
    fixture as properties,
};
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use meta_relational_reasoning as mrr;
use mrr_data_backend::{Backend, BackendConfig, ResourceControl};
use mrr_data_content::MemoryContentStore;
use mrr_data_core::DataQueryResultHandoff;
use mrr_property_source::compile_property_source_query;
use std::sync::Arc;
const SOURCE: &str = include_str!("../../../../fixtures/research-dataset-publications.gql");
const DIGEST: &str = "sha256:6bf9f34d89e31865652e5290df611aca3fc7c0ee41565501a5b4f021b74e5714";
#[tokio::test]
async fn original_source_handoff_distinct_research_catalog_refuses_cross_case_reuse() {
    let (research, bound) = research_fixture();
    let healthcare = Fixture::with_original(source_fixture());
    assert_ne!(
        research.query.snapshot_root(),
        healthcare.query.snapshot_root()
    );
    let backend = Backend::open(
        BackendConfig {
            max_resource_bytes: 3 * RESERVED,
            ..BackendConfig::default()
        },
        metadata::SimulatedMetadata::default(),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let remote = Arc::new(Remote::default());
    let (home, policy, _) =
        authority::publish_for(&research, &backend, remote.as_ref(), "research").await;
    let restored = restore(
        &research,
        &backend,
        remote,
        Arc::new(MemoryContentStore::default()),
    )
    .await;
    let captured = capture(&research, &backend, restored).await;
    assert!(captured.get().tables(&healthcare.query).is_err());
    assert!(captured.get().relations(&healthcare.query).is_err());
    let physical = executor::CapturedBackend {
        binding: research.query.clone(),
        tables: captured
            .get()
            .tables(&research.query)
            .unwrap()
            .iter()
            .map(|t| mrr_data_datafusion::EntityPropertyTable {
                schema: t.schema.clone(),
                batch: t.batch.clone(),
            })
            .collect(),
        relations: crate::tests::entity_properties::combined::acceptance::relation_tables(
            &research,
            captured.get(),
        ),
        limits: properties::limits(),
    };
    assert_eq!(bound.compilation().source_digest, DIGEST);
    let output = backend
        .prepare_resource_async_controlled(
            RESERVED,
            ResourceControl::default(),
            move |control| async move {
                control.check()?;
                let execution = bound
                    .execute_with(&physical, super::result_limits())
                    .await
                    .unwrap();
                control.check()?;
                drop(captured);
                Ok::<_, mrr_data_backend::ResourceStop>(execution)
            },
        )
        .await
        .unwrap();
    assert_research_rows(output.get().candidate().rows());
    let handoff = DataQueryResultHandoff::export_execution(
        output.get().execution(),
        super::result_limits(),
        std::num::NonZeroUsize::new(1 << 20).unwrap(),
    )
    .unwrap();
    handoff
        .verify(
            &research.query,
            super::result_limits(),
            std::num::NonZeroUsize::new(1 << 20).unwrap(),
        )
        .unwrap();
    assert!(
        handoff
            .verify(
                &healthcare.query,
                super::result_limits(),
                std::num::NonZeroUsize::new(1 << 20).unwrap()
            )
            .is_err()
    );
    assert!(authority::disclose(&home, policy).await);
    drop(output);
    assert_eq!(backend.status().resource_bytes, 0);
    assert_healthcare_isolation(&healthcare, &research, &backend).await;
    assert!(authority::disclose(&home, policy).await);
    assert_eq!(backend.status().resource_bytes, 0);
    backend.shutdown().await.unwrap();
}

fn research_fixture() -> (Fixture, mrr_property_source::BoundPropertySourceQuery) {
    let compiled =
        compile_property_source_query("research-dataset-publications.gql", SOURCE, DIGEST).unwrap();
    let [path] = compiled.query().graph().paths() else {
        panic!("one declared research path")
    };
    let mut original = source_fixture();
    original.semantic = properties::semantic(
        mrr::GenerationId::from_canonical_bytes("research-generation").unwrap(),
        "research-revision",
    );
    research_entities(&mut original, path);
    for (table, segment) in original.relations.iter_mut().zip(path.segments()) {
        let [id] = segment.relation().types() else {
            panic!("one relation type")
        };
        table.schema = mrr::RelationSchema::new(
            *id,
            if table.schema.predicate() == "HAS_CASE" {
                "OWNS_DATASET"
            } else {
                "HAS_PUBLICATION"
            },
            table.schema.fields().to_vec(),
            vec![],
        )
        .unwrap();
    }
    let relations = mrr::RelationCatalog::admit(
        original
            .relations
            .iter()
            .map(|t| t.schema.clone())
            .collect(),
    )
    .unwrap();
    let entities =
        mrr::EntityCatalog::admit(original.entities.iter().map(|t| t.schema.clone()).collect())
            .unwrap();
    let bound = compiled
        .bind(&relations, &entities, &original.semantic)
        .unwrap();
    original.query = bound.query().clone();
    (Fixture::with_original(original), bound)
}

fn research_entities(original: &mut properties::Fixture, path: &mrr::PathPattern) {
    for (index, (table, node)) in original
        .entities
        .iter_mut()
        .zip(
            std::iter::once(path.start()).chain(path.segments().iter().map(mrr::PathSegment::node)),
        )
        .enumerate()
    {
        let [id] = node.types() else {
            panic!("one node type")
        };
        let (name, key) = [
            ("Organization", "name"),
            ("Dataset", "title"),
            ("Publication", "doi"),
        ][index];
        table.schema = mrr::EntitySchema::new(
            *id,
            name,
            vec![mrr::RelationField::new(key, mrr::ValueSchema::String, true).unwrap()],
        )
        .unwrap();
        let values = table
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| {
                value.map(|v| match v {
                    "healthcare" => "research".to_owned(),
                    "one" => "alpha-dataset".to_owned(),
                    "two" => "beta-dataset".to_owned(),
                    "shared" => "10.test/shared".to_owned(),
                    other => other.to_owned(),
                })
            })
            .collect::<Vec<_>>();
        table.batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("entity_id", DataType::Utf8, false),
                Field::new(key, DataType::Utf8, true),
            ])),
            vec![
                table.batch.column(0).clone(),
                Arc::new(StringArray::from(values)) as ArrayRef,
            ],
        )
        .unwrap();
    }
}

fn assert_research_rows(actual: &[Vec<mrr::QueryResultValue>]) {
    let scalar = |s: &str| mrr::QueryResultValue::Scalar {
        schema: mrr::ValueSchema::String,
        value: mrr::Value::String(s.into()),
    };
    let mut expected = vec![
        vec![
            scalar("research"),
            scalar("alpha-dataset"),
            scalar("10.test/shared"),
        ],
        vec![
            scalar("research"),
            scalar("alpha-dataset"),
            mrr::QueryResultValue::Null,
        ],
        vec![
            scalar("research"),
            scalar("beta-dataset"),
            scalar("10.test/shared"),
        ],
    ];
    expected.push(expected[0].clone());
    let mut rows = actual.to_vec();
    rows.sort_by_key(|r| format!("{r:?}"));
    expected.sort_by_key(|r| format!("{r:?}"));
    assert_eq!(rows, expected);
}

async fn assert_healthcare_isolation(healthcare: &Fixture, research: &Fixture, backend: &Backend) {
    let health_remote = Arc::new(Remote::default());
    let (health_home, health_policy, publication) =
        authority::publish(healthcare, backend, health_remote.as_ref()).await;
    let health_captured = capture(
        healthcare,
        backend,
        restore(
            healthcare,
            backend,
            health_remote.clone(),
            Arc::new(MemoryContentStore::default()),
        )
        .await,
    )
    .await;
    assert!(health_captured.get().tables(&research.query).is_err());
    drop(health_captured);
    let health_result = super::query_transport(
        healthcare,
        backend,
        health_remote,
        Arc::new(MemoryContentStore::default()),
        &health_home,
        health_policy,
    )
    .await;
    drop(health_result);
    authority::retire_and_recover(healthcare, &health_home, health_policy, &publication).await;
}
