use graphar_rs::info::GraphInfo;
use meta_relational_reasoning::{
    EntityId, EvidenceCompleteness, Fact, FactId, FactProvenance, FactValidity, GenerationId,
    RelationAuthority, RelationContext, RelationField, RelationId, RelationSchema, Value,
    ValueSchema,
};

use crate::{BinaryEntityProjection, GraphArWriteError, write_graphar_dataset};

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
fn upstream_writer_emits_vertices_edges_and_metadata() {
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
    assert!(output.join("entity.vertex.yaml").is_file());
    assert!(
        output
            .join("entity_mrr_relation_entity.edge.yaml")
            .is_file()
    );
    assert!(output.join("vertex/entity/vertex_count").is_file());
    assert!(output.join("edge/entity_mrr_relation_entity").is_dir());
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
