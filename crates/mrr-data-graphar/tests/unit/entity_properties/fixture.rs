//! Physical-query tests use MRR IR directly; these do not qualify GQL parsing.
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use meta_relational_reasoning as mrr;
use mrr_data_datafusion::{BinaryRelationTable, EntityPropertyTable, PropertyQueryLimits};
use std::sync::Arc;

pub(super) struct Fixture {
    pub query: mrr::CatalogBoundQuery,
    pub semantic: mrr::SemanticSnapshot,
    pub entities: Vec<EntityPropertyTable>,
    pub relations: Vec<BinaryRelationTable>,
}
pub(super) fn entity(name: &str) -> mrr::EntityId {
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
            .map(|n| Field::new(*n, DataType::Utf8, *n != "entity_id"))
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
pub(super) fn limits() -> PropertyQueryLimits {
    PropertyQueryLimits {
        max_input_rows: 100,
        max_input_bytes: 1024 * 1024,
        max_join_rows: 100,
        max_output_cells: 300,
        execution_memory_bytes: 16 * 1024 * 1024,
    }
}
pub(super) fn fixture() -> Fixture {
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
    let semantic = semantic(generation, "revision");
    Fixture {
        query: mrr::bind_query_to_catalog(&bundle, query_id, &semantic).unwrap(),
        semantic,
        entities: entity_tables(&schemas),
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

fn entity_tables(schemas: &[mrr::EntitySchema]) -> Vec<EntityPropertyTable> {
    vec![
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
                    values(&["one", "two", "其他案例😀"]),
                ],
            ),
        },
        EntityPropertyTable {
            schema: schemas[2].clone(),
            batch: batch(
                &["entity_id", "identity"],
                vec![
                    ids(&["p1", "p2", "isolated"]),
                    vec![Some("shared".into()), None, Some(String::new())],
                ],
            ),
        },
    ]
}

pub(super) fn semantic(generation: mrr::GenerationId, revision: &str) -> mrr::SemanticSnapshot {
    mrr::SemanticSnapshot::admit(
        generation,
        vec![
            mrr::RevisionBinding::admit(
                mrr::ExternalRevisionIdentity::new("test", "source", revision).unwrap(),
                generation,
            )
            .unwrap(),
        ],
    )
    .unwrap()
}
pub(super) fn rebind(f: &Fixture, semantic: &mrr::SemanticSnapshot) -> mrr::CatalogBoundQuery {
    let query = f.query.query().clone();
    let id = query.id();
    let bundle = mrr::ReasoningBundle::admit(mrr::ReasoningBundleDeclaration {
        entities: f.entities.iter().map(|t| t.schema.clone()).collect(),
        relations: f.relations.iter().map(|t| t.schema.clone()).collect(),
        query_templates: vec![mrr::QueryTemplate::new(query, vec![])],
        ..Default::default()
    })
    .unwrap();
    mrr::bind_query_to_catalog(&bundle, id, semantic).unwrap()
}

/// Both path relations round-trip through native `GraphAr` before physical execution.
pub(super) fn native_relations(f: &Fixture, root: &std::path::Path) -> Vec<BinaryRelationTable> {
    use crate::{
        BinaryEntityProjection, GraphArReadLimits, read_graphar_dataset, verify_graphar_directory,
        write_graphar_dataset,
    };
    use arrow_array::Array;
    use std::str::FromStr;
    let catalog =
        mrr::RelationCatalog::admit(f.relations.iter().map(|t| t.schema.clone()).collect())
            .unwrap();
    let owner = entity("source-authority");
    let context = mrr::RelationContext::new(
        f.semantic.generation(),
        mrr::RelationAuthority::Entity(owner),
        mrr::FactProvenance::Source(owner),
        mrr::EvidenceCompleteness::Complete,
        mrr::FactValidity::Valid,
    )
    .unwrap();
    f.relations
        .iter()
        .enumerate()
        .map(|(index, input)| {
            let projection =
                BinaryEntityProjection::admit_catalog(&catalog, input.schema.id()).unwrap();
            let strings = input
                .batch
                .columns()
                .iter()
                .map(|a| a.as_any().downcast_ref::<StringArray>().unwrap())
                .collect::<Vec<_>>();
            let edges = (0..input.batch.num_rows())
                .map(|row| {
                    assert!(!strings[0].is_null(row) && !strings[1].is_null(row));
                    projection
                        .project(&mrr::Fact::new(
                            mrr::FactId::from_canonical_bytes(format!(
                                "relation-{index}-row-{row}"
                            ))
                            .unwrap(),
                            input.schema.id(),
                            vec![
                                mrr::Value::Entity(
                                    mrr::EntityId::from_str(strings[0].value(row)).unwrap(),
                                ),
                                mrr::Value::Entity(
                                    mrr::EntityId::from_str(strings[1].value(row)).unwrap(),
                                ),
                            ],
                            context,
                        ))
                        .unwrap()
                })
                .collect::<Vec<_>>();
            let source = root.join(format!("relation-{index}"));
            let receipt = write_graphar_dataset(&source, &projection, &edges).unwrap();
            verify_graphar_directory(
                &source,
                receipt.inventory(),
                mrr_data_core::GraphInventoryLimits::default(),
            )
            .unwrap();
            let restored =
                read_graphar_dataset(&source, &projection, GraphArReadLimits::new(100, 100))
                    .unwrap();
            let mut values = [Vec::new(), Vec::new()];
            for fact in restored.facts() {
                assert_eq!(fact.context().generation(), f.query.generation());
                for (column, value) in fact.values().iter().enumerate() {
                    let mrr::Value::Entity(id) = value else {
                        panic!("admitted binary-Entity value")
                    };
                    values[column].push(Some(id.to_string()));
                }
            }
            BinaryRelationTable {
                schema: input.schema.clone(),
                batch: batch(&["source", "target"], values.into()),
            }
        })
        .collect()
}
