use meta_relational_reasoning::{
    EntityId, EvidenceCompleteness, Fact, FactId, FactProvenance, FactValidity, GenerationId,
    RelationAuthority, RelationContext, RelationField, RelationId, RelationSchema, Value,
    ValueSchema,
};

use crate::{BinaryEntityProjection, GraphProjectionError, PhysicalVertexIndex};

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

fn field(name: &str, schema: ValueSchema, nullable: bool) -> RelationField {
    RelationField::new(name, schema, nullable).expect("valid field")
}

fn relation(fields: Vec<RelationField>) -> RelationSchema {
    RelationSchema::new(id("knows"), "knows", fields, vec![]).expect("valid relation")
}

fn binary_relation() -> RelationSchema {
    relation(vec![
        field("source", ValueSchema::Entity, false),
        field("destination", ValueSchema::Entity, false),
    ])
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
        .expect("valid context"),
    )
}

#[test]
fn admits_ordered_non_null_entity_endpoints() {
    let projection = BinaryEntityProjection::admit(&binary_relation()).unwrap();
    assert_eq!(projection.source_field(), "source");
    assert_eq!(projection.destination_field(), "destination");
    assert_eq!(projection.predicate(), "knows");
}

#[test]
fn rejects_n_ary_relations_instead_of_reifying_them() {
    let schema = relation(vec![
        field("source", ValueSchema::Entity, false),
        field("destination", ValueSchema::Entity, false),
        field("weight", ValueSchema::Integer, false),
    ]);
    assert_eq!(
        BinaryEntityProjection::admit(&schema),
        Err(GraphProjectionError::UnsupportedArity { actual: 3 })
    );
}

#[test]
fn rejects_nullable_or_non_entity_endpoints() {
    let nullable = relation(vec![
        field("source", ValueSchema::Entity, true),
        field("destination", ValueSchema::Entity, false),
    ]);
    assert_eq!(
        BinaryEntityProjection::admit(&nullable),
        Err(GraphProjectionError::NullableEndpoint {
            field: "source".into()
        })
    );

    let scalar = relation(vec![
        field("source", ValueSchema::String, false),
        field("destination", ValueSchema::Entity, false),
    ]);
    assert_eq!(
        BinaryEntityProjection::admit(&scalar),
        Err(GraphProjectionError::UnsupportedEndpoint {
            field: "source".into(),
            schema: ValueSchema::String,
        })
    );
}

#[test]
fn projection_preserves_semantic_identity_and_context() {
    let projection = BinaryEntityProjection::admit(&binary_relation()).unwrap();
    let source = id("alice");
    let destination = id("bob");
    let projected = projection
        .project(&fact("alice-knows-bob", "alice", "bob"))
        .unwrap();
    assert_eq!(projected.source(), source);
    assert_eq!(projected.destination(), destination);
    assert_eq!(projected.fact_id(), id("alice-knows-bob"));
    assert_eq!(projected.relation_id(), id("knows"));
    assert_eq!(projected.generation_id(), id("generation"));
    assert_eq!(projected.completeness(), EvidenceCompleteness::Complete);
    assert_eq!(projected.validity(), FactValidity::Valid);
}

#[test]
fn physical_repartition_does_not_change_projected_records() {
    let projection = BinaryEntityProjection::admit(&binary_relation()).unwrap();
    let facts = [
        fact("edge-1", "alice", "bob"),
        fact("edge-2", "bob", "carol"),
        fact("edge-3", "carol", "alice"),
    ];
    let contiguous = facts
        .iter()
        .map(|item| projection.project(item).unwrap())
        .collect::<Vec<_>>();
    let repartitioned = facts
        .chunks(1)
        .flat_map(|chunk| chunk.iter())
        .map(|item| projection.project(item).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(repartitioned, contiguous);
}

#[test]
fn invalid_facts_fail_before_projection() {
    let projection = BinaryEntityProjection::admit(&binary_relation()).unwrap();
    let wrong_relation = Fact::new(
        id("wrong-fact"),
        id("other-relation"),
        vec![Value::Entity(id("alice")), Value::Entity(id("bob"))],
        *fact("context", "alice", "bob").context(),
    );
    assert!(matches!(
        projection.project(&wrong_relation),
        Err(GraphProjectionError::InvalidFact { .. })
    ));
}

#[test]
fn physical_vertex_ids_are_dense_and_bidirectional() {
    let projection = BinaryEntityProjection::admit(&binary_relation()).unwrap();
    let edges = [
        projection.project(&fact("edge-1", "alice", "bob")).unwrap(),
        projection.project(&fact("edge-2", "bob", "carol")).unwrap(),
    ];
    let index = PhysicalVertexIndex::from_edges(&edges);

    assert_eq!(index.len(), 3);
    assert!(!index.is_empty());
    for physical in 0..3 {
        let entity = index.entity_id(physical).expect("dense physical ID");
        assert_eq!(index.internal_id(entity), Some(physical));
    }
    assert_eq!(index.entity_id(-1), None);

    let indexed = index.index_edge(edges[0]).unwrap();
    assert_eq!(index.entity_id(indexed.source()), Some(edges[0].source()));
    assert_eq!(
        index.entity_id(indexed.destination()),
        Some(edges[0].destination())
    );
    assert_eq!(indexed.semantic(), edges[0]);
}

#[test]
fn physical_vertex_index_is_independent_of_edge_order() {
    let projection = BinaryEntityProjection::admit(&binary_relation()).unwrap();
    let mut edges = vec![
        projection.project(&fact("edge-1", "alice", "bob")).unwrap(),
        projection.project(&fact("edge-2", "bob", "carol")).unwrap(),
        projection
            .project(&fact("edge-3", "carol", "alice"))
            .unwrap(),
    ];
    let forward = PhysicalVertexIndex::from_edges(&edges);
    edges.reverse();
    let reversed = PhysicalVertexIndex::from_edges(&edges);
    assert_eq!(reversed, forward);
}

#[test]
fn indexing_rejects_edges_outside_the_admitted_endpoint_set() {
    let projection = BinaryEntityProjection::admit(&binary_relation()).unwrap();
    let admitted = projection.project(&fact("edge-1", "alice", "bob")).unwrap();
    let foreign = projection.project(&fact("edge-2", "carol", "bob")).unwrap();
    let index = PhysicalVertexIndex::from_edges(&[admitted]);

    assert_eq!(
        index.index_edge(foreign),
        Err(GraphProjectionError::UnknownEntity(id("carol")))
    );
}
