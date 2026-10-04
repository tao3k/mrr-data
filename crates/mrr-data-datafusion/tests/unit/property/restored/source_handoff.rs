//! A simulated caller delegates source compilation and admission to MRR.
use super::{
    DataFusionQueryError, EntityChildMode, Fixture, NonZeroUsize, fixture, limits, mrr, restored,
};
use mrr_property_source::{CompiledPropertySourceQuery, compile_property_source_query};

const SOURCE: &str = include_str!("../../../fixtures/healthcare-case-profile-relations.gql");
const SOURCE_DIGEST: &str =
    "sha256:7a3a88a9ebd24cd738d426c0def633247d1a0fc13e9e37cca13bb23e90ba0c63";

#[tokio::test]
async fn original_source_handoff_refuses_semantic_drift_and_final_admission_budget() {
    let f = source_fixture();
    let (cold, _, relations, entities) = restored(&f, EntityChildMode::Valid).await;
    let executor = crate::RestoredPropertyBackend {
        restored: &cold,
        relation_catalog: &relations,
        entity_catalog: &entities,
        limits: limits(),
    };
    let bound = compile().bind(&relations, &entities, &f.semantic).unwrap();
    let error = bound
        .execute_with(
            &executor,
            mrr::QueryResultLimits::new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(3).unwrap(),
            ),
        )
        .await
        .err()
        .unwrap();
    assert!(matches!(error, mrr::PropertyExecutionError::Admission(_)));

    let generation = mrr::GenerationId::from_canonical_bytes("caller-updated-generation").unwrap();
    let semantic = mrr::SemanticSnapshot::admit(
        generation,
        vec![
            mrr::RevisionBinding::admit(
                mrr::ExternalRevisionIdentity::new("test", "source", "updated-revision").unwrap(),
                generation,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let bound = compile().bind(&relations, &entities, &semantic).unwrap();
    let error = bound
        .execute_with(
            &executor,
            mrr::QueryResultLimits::new(
                NonZeroUsize::new(100).unwrap(),
                NonZeroUsize::new(300).unwrap(),
            ),
        )
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error,
        mrr::PropertyExecutionError::Backend(DataFusionQueryError::PhysicalBinding(
            mrr_data_core::DataQueryBindingError::GenerationMismatch { query, snapshot }
        )) if query == generation && snapshot == f.semantic.generation()
    ));
}
fn compile() -> CompiledPropertySourceQuery {
    compile_property_source_query("case-profile-relations.gql", SOURCE, SOURCE_DIGEST).unwrap()
}

fn source_fixture() -> Fixture {
    let compiled = compile();
    let [path] = compiled.query().graph().paths() else {
        panic!("one original path")
    };
    let mut f = fixture();
    // The caller's catalogs use the type identities emitted by MRR's frontend.
    // MRR Data does not infer labels or implement another identity convention.
    for (table, node) in f.entities.iter_mut().zip(
        std::iter::once(path.start()).chain(path.segments().iter().map(|segment| segment.node())),
    ) {
        let [id] = node.types() else {
            panic!("one declared node type")
        };
        table.schema =
            mrr::EntitySchema::new(*id, table.schema.name(), table.schema.properties().to_vec())
                .unwrap();
    }
    for (table, segment) in f.relations.iter_mut().zip(path.segments()) {
        let [id] = segment.relation().types() else {
            panic!("one declared relation type")
        };
        table.schema = mrr::RelationSchema::new(
            *id,
            table.schema.predicate(),
            table.schema.fields().to_vec(),
            table.schema.constraints().to_vec(),
        )
        .unwrap();
    }
    // Two distinct physical edges project to the same row under RETURN ALL.
    let table = &mut f.relations[1];
    let arrays = table
        .batch
        .columns()
        .iter()
        .map(|column| {
            let strings = column
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            let values = strings
                .iter()
                .chain(strings.iter().take(1))
                .collect::<Vec<_>>();
            std::sync::Arc::new(arrow_array::StringArray::from(values)) as arrow_array::ArrayRef
        })
        .collect();
    table.batch = arrow_array::RecordBatch::try_new(table.batch.schema(), arrays).unwrap();
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
    f.query = compiled
        .bind(&relations, &entities, &f.semantic)
        .unwrap()
        .query()
        .clone();
    f
}

#[tokio::test]
async fn original_healthcare_source_reaches_mrr_admission_over_cold_and_warm_restoration() {
    let f = source_fixture();
    let (cold, warm, relations, entities) = restored(&f, EntityChildMode::Valid).await;
    let mut previous = None;
    for snapshot in [&cold, &warm] {
        let executor = crate::RestoredPropertyBackend {
            restored: snapshot,
            relation_catalog: &relations,
            entity_catalog: &entities,
            limits: limits(),
        };
        let bound = compile().bind(&relations, &entities, &f.semantic).unwrap();
        let admitted = bound
            .execute_with(
                &executor,
                mrr::QueryResultLimits::new(
                    NonZeroUsize::new(100).unwrap(),
                    NonZeroUsize::new(300).unwrap(),
                ),
            )
            .await
            .unwrap();
        let candidate = admitted.candidate();
        assert_eq!(admitted.receipt().row_count(), 4);
        assert_eq!(admitted.physical_evidence().query(), bound.query());
        assert_eq!(bound.compilation().source_digest, SOURCE_DIGEST);
        let scalar = |text: &str| mrr::QueryResultValue::Scalar {
            schema: mrr::ValueSchema::String,
            value: mrr::Value::String(text.into()),
        };
        let mut expected = vec![
            vec![scalar("healthcare"), scalar("one"), scalar("shared")],
            vec![
                scalar("healthcare"),
                scalar("one"),
                mrr::QueryResultValue::Null,
            ],
            vec![scalar("healthcare"), scalar("two"), scalar("shared")],
        ];
        expected.push(vec![scalar("healthcare"), scalar("one"), scalar("shared")]);
        let mut actual = candidate.rows().to_vec();
        expected.sort_by_key(|row| format!("{row:?}"));
        actual.sort_by_key(|row| format!("{row:?}"));
        assert_eq!(actual, expected);
        let rows = candidate.rows().to_vec();
        if let Some(expected) = previous {
            assert_eq!(rows, expected);
        }
        previous = Some(rows);
    }
    assert!(
        compile_property_source_query(
            "case-profile-relations.gql",
            &SOURCE.replace("healthcare", "other"),
            SOURCE_DIGEST
        )
        .is_err()
    );
}
