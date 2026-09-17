use graphar_rs::{
    info::{AdjListType, GraphInfo},
    reader::{read_edge_strings, read_vertex_strings},
};
use meta_relational_reasoning::{
    DerivationId, EntityId, EvidenceCompleteness, Fact, FactId, FactProvenance, FactValidity,
    GenerationId, RelationAuthority, RelationContext, RelationField, RelationId, RelationSchema,
    RuleId, Value, ValueSchema,
};

use crate::{
    BinaryEntityProjection, GraphArReadError, GraphArReadLimits, GraphArWriteError,
    read_graphar_dataset, write_graphar_dataset,
};

fn id<T: CanonicalId>(name: &str) -> T {
    T::from_name(name)
}

trait CanonicalId {
    fn from_name(name: &str) -> Self;
}

macro_rules! canonical_id {
    ($type:ty) => {
        impl CanonicalId for $type {
            fn from_name(name: &str) -> Self {
                Self::from_canonical_bytes(name).expect("valid identity")
            }
        }
    };
}

canonical_id!(EntityId);
canonical_id!(FactId);
canonical_id!(GenerationId);
canonical_id!(RelationId);
canonical_id!(DerivationId);
canonical_id!(RuleId);

fn projection_named(name: &str) -> BinaryEntityProjection {
    let relation = RelationSchema::new(
        id(name),
        name,
        vec![
            RelationField::new("source", ValueSchema::Entity, false).unwrap(),
            RelationField::new("destination", ValueSchema::Entity, false).unwrap(),
        ],
        vec![],
    )
    .unwrap();
    BinaryEntityProjection::admit(&relation).unwrap()
}

fn projection() -> BinaryEntityProjection {
    projection_named("knows")
}

fn fact(name: &str, source: &str, destination: &str) -> Fact {
    let owner = id::<EntityId>("source-owner");
    Fact::new(
        id(name),
        id("knows"),
        vec![Value::Entity(id(source)), Value::Entity(id(destination))],
        RelationContext::new(
            id("generation"),
            RelationAuthority::Entity(owner),
            FactProvenance::Source(owner),
            EvidenceCompleteness::Complete,
            FactValidity::Valid,
        )
        .unwrap(),
    )
}

#[test]
fn maintained_graphar_round_trips_vertices_edges_and_metadata() {
    let projection = projection();
    let edges = [
        projection.project(&fact("edge-1", "alice", "bob")).unwrap(),
        projection.project(&fact("edge-2", "bob", "carol")).unwrap(),
    ];
    let parent = tempfile::tempdir().unwrap();
    let output = parent.path().join("dataset");
    let receipt = write_graphar_dataset(&output, &projection, &edges).unwrap();

    assert_eq!(receipt.root(), output);
    assert_eq!(receipt.vertex_count(), 3);
    assert_eq!(receipt.edge_count(), 2);
    assert!(receipt.graph_info_path().is_file());
    let graph_info = GraphInfo::load(receipt.graph_info_path()).unwrap();
    assert_eq!(graph_info.vertex_info_num(), 1);
    assert_eq!(graph_info.edge_info_num(), 1);
    assert_eq!(graph_info.prefix(), format!("{}/", output.display()));
    assert!(output.join("entity.vertex.yaml").is_file());
    assert!(
        output
            .join("entity_mrr_relation_entity.edge.yaml")
            .is_file()
    );
    assert!(output.join("vertex/entity/vertex_count").is_file());
    assert!(output.join("edge/entity_mrr_relation_entity").is_dir());

    let vertex_properties = vec!["entity_id".to_string()];
    let vertices = read_vertex_strings(&graph_info, "entity", &vertex_properties, 3).unwrap();
    assert_eq!(vertices.len(), 3);
    let entity_ids = vertices
        .iter()
        .map(|vertex| vertex.values()[0].clone().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    let expected_entity_ids = ["alice", "bob", "carol"]
        .map(|name| id::<EntityId>(name).to_string())
        .into_iter()
        .collect();
    assert_eq!(entity_ids, expected_entity_ids);

    let edge_properties = vec![
        "fact_id".to_string(),
        "relation_id".to_string(),
        "predicate".to_string(),
    ];
    let read_edges = read_edge_strings(
        &graph_info,
        "entity",
        "mrr_relation",
        "entity",
        AdjListType::UnorderedBySource,
        &edge_properties,
        2,
    )
    .unwrap();
    assert_eq!(read_edges.len(), 2);
    let relation_id = projection.relation_id().to_string();
    assert!(read_edges.iter().all(|edge| {
        edge.values()[1].as_deref() == Some(relation_id.as_str())
            && edge.values()[2].as_deref() == Some("knows")
    }));
    let fact_ids = read_edges
        .iter()
        .map(|edge| edge.values()[0].clone().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    let expected_fact_ids = ["edge-1", "edge-2"]
        .map(|name| id::<FactId>(name).to_string())
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(fact_ids, expected_fact_ids);

    let budget_error = read_edge_strings(
        &graph_info,
        "entity",
        "mrr_relation",
        "entity",
        AdjListType::UnorderedBySource,
        &edge_properties,
        1,
    )
    .unwrap_err();
    assert!(budget_error.to_string().contains("exceeding max_rows=1"));
}

#[test]
fn maintained_graphar_reconstructs_and_re_admits_complete_mrr_facts() {
    let projection = projection();
    let source = fact("edge-1", "alice", "bob");
    let derived = Fact::new(
        id("edge-2"),
        id("knows"),
        vec![Value::Entity(id("bob")), Value::Entity(id("carol"))],
        RelationContext::new(
            id("generation"),
            RelationAuthority::Rule(id("rule")),
            FactProvenance::Derivation(id("derivation")),
            EvidenceCompleteness::Partial,
            FactValidity::InvalidatedBy(id("superseding-fact")),
        )
        .unwrap(),
    );
    let expected = [source, derived];
    let edges = expected
        .iter()
        .map(|fact| projection.project(fact).unwrap())
        .collect::<Vec<_>>();
    let parent = tempfile::tempdir().unwrap();
    let output = parent.path().join("semantic-dataset");
    write_graphar_dataset(&output, &projection, &edges).unwrap();

    let imported = read_graphar_dataset(
        &output,
        &projection,
        GraphArReadLimits::new(3, expected.len()),
    )
    .unwrap();

    assert_eq!(imported.root(), output);
    assert_eq!(imported.vertex_count(), 3);
    assert_eq!(imported.facts(), expected.as_slice());
}

#[test]
fn semantic_reader_enforces_native_row_budgets() {
    let projection = projection();
    let facts = [
        fact("edge-1", "alice", "bob"),
        fact("edge-2", "bob", "carol"),
    ];
    let edges = facts
        .iter()
        .map(|fact| projection.project(fact).unwrap())
        .collect::<Vec<_>>();
    let parent = tempfile::tempdir().unwrap();
    let output = parent.path().join("budgeted-dataset");
    write_graphar_dataset(&output, &projection, &edges).unwrap();

    let error =
        read_graphar_dataset(&output, &projection, GraphArReadLimits::new(3, 1)).unwrap_err();

    assert!(matches!(error, GraphArReadError::Native(_)));
    assert!(error.to_string().contains("exceeding max_rows=1"));
}

#[test]
fn semantic_reader_rejects_a_foreign_projection() {
    let projection = projection();
    let edge = projection.project(&fact("edge-1", "alice", "bob")).unwrap();
    let parent = tempfile::tempdir().unwrap();
    let output = parent.path().join("foreign-projection-dataset");
    write_graphar_dataset(&output, &projection, &[edge]).unwrap();

    let error = read_graphar_dataset(
        &output,
        &projection_named("follows"),
        GraphArReadLimits::new(2, 1),
    )
    .unwrap_err();

    assert!(matches!(error, GraphArReadError::PredicateMismatch { .. }));
}

#[test]
fn writer_refuses_to_replace_an_existing_destination() {
    let projection = projection();
    let parent = tempfile::tempdir().unwrap();
    let output = parent.path().join("dataset");
    std::fs::create_dir(&output).unwrap();

    assert!(matches!(
        write_graphar_dataset(&output, &projection, &[]),
        Err(GraphArWriteError::OutputExists(path)) if path == output
    ));
}

#[test]
fn writer_rejects_foreign_relations_before_creating_output() {
    let knows = projection();
    let follows = projection_named("follows");
    let foreign_fact = Fact::new(
        id("edge-1"),
        id("follows"),
        vec![Value::Entity(id("alice")), Value::Entity(id("bob"))],
        *fact("context", "alice", "bob").context(),
    );
    let edge = follows.project(&foreign_fact).unwrap();
    let parent = tempfile::tempdir().unwrap();
    let output = parent.path().join("dataset");

    assert!(matches!(
        write_graphar_dataset(&output, &knows, &[edge]),
        Err(GraphArWriteError::RelationMismatch { .. })
    ));
    assert!(!output.exists());
}

#[test]
fn writer_rejects_duplicate_fact_ids_before_creating_output() {
    let projection = projection();
    let edge = projection.project(&fact("edge-1", "alice", "bob")).unwrap();
    let parent = tempfile::tempdir().unwrap();
    let output = parent.path().join("dataset");

    assert!(matches!(
        write_graphar_dataset(&output, &projection, &[edge, edge]),
        Err(GraphArWriteError::DuplicateFact(_))
    ));
    assert!(!output.exists());
}
