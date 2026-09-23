//! Physical-query tests use MRR IR directly; these do not qualify GQL parsing.
use crate::{
    BinaryRelationTable, DataFusionQueryError, EntityPropertyTable, PropertyQueryLimits,
    execute_property_path_query,
};
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use meta_relational_reasoning as mrr;
use std::{num::NonZeroUsize, sync::Arc};

struct Fixture {
    query: mrr::CatalogBoundQuery,
    semantic: mrr::SemanticSnapshot,
    entities: Vec<EntityPropertyTable>,
    relations: Vec<BinaryRelationTable>,
}
fn entity(name: &str) -> mrr::EntityId {
    mrr::EntityId::from_canonical_bytes(name).unwrap()
}
fn binding(name: &str) -> mrr::Binding {
    mrr::Binding::new(name).unwrap()
}
fn op(name: &str) -> mrr::QueryOperatorId {
    mrr::QueryOperatorId::from_canonical_bytes(name).unwrap()
}
fn property(name: &str, key: &str) -> mrr::Expression {
    mrr::Expression::Property {
        binding: binding(name),
        key: mrr::PropertyKey::new(key).unwrap(),
    }
}
fn batch(names: &[&str], values: Vec<Vec<Option<String>>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|n| Field::new(*n, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<ArrayRef> = values
        .into_iter()
        .map(|v| Arc::new(StringArray::from(v)) as ArrayRef)
        .collect();
    RecordBatch::try_new(schema, arrays).unwrap()
}
fn values(input: &[&str]) -> Vec<Option<String>> {
    input.iter().map(|s| Some((*s).to_owned())).collect()
}
fn ids(input: &[&str]) -> Vec<Option<String>> {
    input.iter().map(|s| Some(entity(s).to_string())).collect()
}
fn limits() -> PropertyQueryLimits {
    PropertyQueryLimits {
        max_input_rows: 100,
        max_input_bytes: 1024 * 1024,
        max_join_rows: 100,
        max_output_cells: 300,
        execution_memory_bytes: 16 * 1024 * 1024,
    }
}
fn fixture() -> Fixture {
    let schemas = [
        ("Scenario", "identity"),
        ("Case", "id"),
        ("Profile", "identity"),
    ]
    .map(|(name, key)| {
        mrr::EntitySchema::new(
            entity(name),
            name,
            vec![mrr::RelationField::new(key, mrr::ValueSchema::String, true).unwrap()],
        )
        .unwrap()
    });
    let relations = ["HAS_CASE", "HAS_EFFECTIVE_PROFILE"].map(|name| {
        mrr::RelationSchema::new(
            mrr::RelationId::from_canonical_bytes(name).unwrap(),
            name,
            vec![
                mrr::RelationField::new("source", mrr::ValueSchema::Entity, false).unwrap(),
                mrr::RelationField::new("target", mrr::ValueSchema::Entity, false).unwrap(),
            ],
            vec![],
        )
        .unwrap()
    });
    let query = query_ir(&schemas, &relations);
    let query_id = query.id();
    let bundle = mrr::ReasoningBundle::admit(mrr::ReasoningBundleDeclaration {
        entities: schemas.to_vec(),
        relations: relations.to_vec(),
        query_templates: vec![mrr::QueryTemplate::new(query, vec![])],
        ..Default::default()
    })
    .unwrap();
    let generation = mrr::GenerationId::from_canonical_bytes("test-generation").unwrap();
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
    Fixture {
        query: mrr::bind_query_to_catalog(&bundle, query_id, &semantic).unwrap(),
        semantic,
        entities: vec![
            EntityPropertyTable {
                schema: schemas[0].clone(),
                batch: batch(
                    &["entity_id", "identity"],
                    vec![ids(&["s1", "s2"]), values(&["healthcare", "other"])],
                ),
            },
            EntityPropertyTable {
                schema: schemas[1].clone(),
                batch: batch(
                    &["entity_id", "id"],
                    vec![
                        ids(&["c1", "c2", "c3"]),
                        values(&["one", "two", "other-case"]),
                    ],
                ),
            },
            EntityPropertyTable {
                schema: schemas[2].clone(),
                batch: batch(
                    &["entity_id", "identity"],
                    vec![ids(&["p1", "p2"]), vec![Some("shared".into()), None]],
                ),
            },
        ],
        relations: vec![
            BinaryRelationTable {
                schema: relations[0].clone(),
                batch: batch(
                    &["source", "target"],
                    vec![ids(&["s1", "s1", "s2"]), ids(&["c1", "c2", "c3"])],
                ),
            },
            BinaryRelationTable {
                schema: relations[1].clone(),
                batch: batch(
                    &["source", "target"],
                    vec![
                        ids(&["c1", "c1", "c2", "c3"]),
                        ids(&["p1", "p2", "p1", "p1"]),
                    ],
                ),
            },
        ],
    }
}

mod restored;
fn query_ir(schemas: &[mrr::EntitySchema], relations: &[mrr::RelationSchema]) -> mrr::MetaQueryIr {
    mrr::MetaQueryIr::new(
        mrr::QueryId::from_canonical_bytes("property-test").unwrap(),
        mrr::GraphPattern::new(
            op("graph"),
            vec![mrr::PathPattern::new(
                mrr::NodePattern::new(binding("s"), vec![schemas[0].id()]),
                vec![
                    mrr::PathSegment::new(
                        mrr::RelationPattern::new(
                            None,
                            vec![relations[0].id()],
                            mrr::Direction::Outgoing,
                            1,
                            Some(1),
                        )
                        .unwrap(),
                        mrr::NodePattern::new(binding("c"), vec![schemas[1].id()]),
                    ),
                    mrr::PathSegment::new(
                        mrr::RelationPattern::new(
                            None,
                            vec![relations[1].id()],
                            mrr::Direction::Outgoing,
                            1,
                            Some(1),
                        )
                        .unwrap(),
                        mrr::NodePattern::new(binding("p"), vec![schemas[2].id()]),
                    ),
                ],
            )],
        )
        .unwrap(),
        vec![mrr::Filter::new(
            op("filter"),
            mrr::Expression::Binary {
                left: Box::new(property("s", "identity")),
                operator: mrr::BinaryOperator::Equal,
                right: Box::new(mrr::Expression::Literal(mrr::Value::String(
                    "healthcare".into(),
                ))),
            },
        )],
        mrr::QueryResult::returning(mrr::SetQuantifier::All).with_projections(vec![
            mrr::Projection::new(op("s"), property("s", "identity"), binding("scenario")),
            mrr::Projection::new(op("c"), property("c", "id"), binding("case")),
            mrr::Projection::new(op("p"), property("p", "identity"), binding("profile")),
        ]),
    )
    .unwrap()
}
#[tokio::test]
async fn two_hop_filter_preserves_shared_profiles_nulls_and_mrr_admission() {
    let f = fixture();
    let output = execute_property_path_query(&f.query, &f.entities, &f.relations, limits())
        .await
        .unwrap();
    let scalar = |text: &str| mrr::QueryResultValue::Scalar {
        schema: mrr::ValueSchema::String,
        value: mrr::Value::String(text.into()),
    };
    let mut actual = output.rows().to_vec();
    let mut expected = vec![
        vec![scalar("healthcare"), scalar("one"), scalar("shared")],
        vec![
            scalar("healthcare"),
            scalar("one"),
            mrr::QueryResultValue::Null,
        ],
        vec![scalar("healthcare"), scalar("two"), scalar("shared")],
    ];
    actual.sort_by_key(|r| format!("{r:?}"));
    expected.sort_by_key(|r| format!("{r:?}"));
    assert_eq!(actual, expected);
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
#[tokio::test]
async fn join_budget_is_enforced_before_execution() {
    let f = fixture();
    let mut budget = limits();
    budget.max_join_rows = 11;
    assert!(matches!(
        execute_property_path_query(&f.query, &f.entities, &f.relations, budget).await,
        Err(DataFusionQueryError::ResourceLimit("join rows"))
    ));
}
#[tokio::test]
async fn dangling_and_duplicate_entities_fail_closed() {
    let mut f = fixture();
    f.entities[0].batch = batch(
        &["entity_id", "identity"],
        vec![ids(&["s1", "s1"]), values(&["healthcare", "other"])],
    );
    assert!(matches!(
        execute_property_path_query(&f.query, &f.entities, &f.relations, limits()).await,
        Err(DataFusionQueryError::InvalidArrowBatch(
            "duplicate entity ID"
        ))
    ));
    let mut f = fixture();
    f.relations[0].batch = batch(&["source", "target"], vec![ids(&["s1"]), ids(&["missing"])]);
    assert!(matches!(
        execute_property_path_query(&f.query, &f.entities, &f.relations, limits()).await,
        Err(DataFusionQueryError::InvalidArrowBatch(
            "dangling edge endpoint"
        ))
    ));
}
#[tokio::test]
async fn catalog_substitution_is_rejected() {
    let mut f = fixture();
    f.entities[0].schema = mrr::EntitySchema::new(
        entity("Scenario"),
        "Different",
        f.entities[0].schema.properties().to_vec(),
    )
    .unwrap();
    assert!(matches!(
        execute_property_path_query(&f.query, &f.entities, &f.relations, limits()).await,
        Err(DataFusionQueryError::CatalogMismatch)
    ));
}

#[tokio::test]
async fn absent_and_null_filter_properties_produce_no_matches() {
    for identity in [Some("unrelated".into()), None] {
        let mut f = fixture();
        f.entities[0].batch = batch(
            &["entity_id", "identity"],
            vec![ids(&["s1", "s2"]), vec![identity.clone(), identity]],
        );
        let result = execute_property_path_query(&f.query, &f.entities, &f.relations, limits())
            .await
            .unwrap();
        assert!(result.rows().is_empty());
        assert_eq!(result.columns().len(), 3);
    }
}

#[tokio::test]
async fn return_all_preserves_edge_multiplicity() {
    let mut f = fixture();
    f.relations[0].batch = batch(
        &["source", "target"],
        vec![ids(&["s1", "s1"]), ids(&["c2", "c2"])],
    );
    let result = execute_property_path_query(&f.query, &f.entities, &f.relations, limits())
        .await
        .unwrap();
    assert_eq!(result.rows().len(), 2);
    assert_eq!(result.rows()[0], result.rows()[1]);
}

#[tokio::test]
async fn input_and_output_budgets_reject_without_truncation() {
    let f = fixture();
    for budget in [
        PropertyQueryLimits {
            max_input_rows: 1,
            ..limits()
        },
        PropertyQueryLimits {
            max_input_bytes: 1,
            ..limits()
        },
        PropertyQueryLimits {
            max_output_cells: 1,
            ..limits()
        },
        PropertyQueryLimits {
            execution_memory_bytes: 0,
            ..limits()
        },
    ] {
        assert!(matches!(
            execute_property_path_query(&f.query, &f.entities, &f.relations, budget).await,
            Err(DataFusionQueryError::ResourceLimit(_))
        ));
    }
}

#[tokio::test]
async fn physical_property_column_substitution_is_rejected() {
    let mut f = fixture();
    f.entities[0].batch = batch(
        &["entity_id", "spoofed"],
        vec![ids(&["s1", "s2"]), values(&["healthcare", "other"])],
    );
    assert!(matches!(
        execute_property_path_query(&f.query, &f.entities, &f.relations, limits()).await,
        Err(DataFusionQueryError::InvalidArrowBatch(
            "property column mismatch"
        ))
    ));
}

#[tokio::test]
async fn reordered_edge_input_preserves_exact_output() {
    let mut f = fixture();
    let original = execute_property_path_query(&f.query, &f.entities, &f.relations, limits())
        .await
        .unwrap();
    f.relations[1].batch = batch(
        &["source", "target"],
        vec![
            ids(&["c3", "c2", "c1", "c1"]),
            ids(&["p1", "p1", "p2", "p1"]),
        ],
    );
    let reordered = execute_property_path_query(&f.query, &f.entities, &f.relations, limits())
        .await
        .unwrap();
    assert_eq!(original, reordered);
}

#[tokio::test]
async fn datafusion_memory_budget_is_enforced() {
    let f = fixture();
    let budget = PropertyQueryLimits {
        execution_memory_bytes: 1,
        ..limits()
    };
    assert!(matches!(
        execute_property_path_query(&f.query, &f.entities, &f.relations, budget).await,
        Err(DataFusionQueryError::Engine(_))
    ));
}
