//! Synthetic catalog, facts and binding used by native physical fixtures.
use meta_relational_reasoning::{
    Binding, Direction, EntityCatalog, EntityId, EntitySchema, EvidenceCompleteness, Expression,
    ExternalRevisionIdentity, Fact, FactId, FactProvenance, FactValidity, GenerationId,
    GraphPattern, MetaQueryIr, NodePattern, PathPattern, PathSegment, Projection, QueryId,
    QueryOperatorId, QueryResult, QueryTemplate, ReasoningBundle, ReasoningBundleDeclaration,
    RelationAuthority, RelationCatalog, RelationContext, RelationField, RelationId,
    RelationPattern, RelationSchema, RevisionBinding, SemanticSnapshot, SetQuantifier, Value,
    ValueSchema, bind_query_to_catalog,
};
use mrr_data_core::{
    BatchDescriptor, BoundDataQuery, CoverageDescriptor, CoverageKind, DataEngineProfile,
    GraphDatasetInventory, GraphProjectionDescriptor, RelationDescriptor, SnapshotBlock,
    SnapshotManifest, SnapshotManifestRequest, bind_data_query, raw_cid,
};

pub(crate) fn schema() -> RelationSchema {
    RelationSchema::new(
        RelationId::from_canonical_bytes("knows").unwrap(),
        "knows",
        vec![
            RelationField::new("source", ValueSchema::Entity, false).unwrap(),
            RelationField::new("destination", ValueSchema::Entity, false).unwrap(),
        ],
        vec![],
    )
    .unwrap()
}
pub(crate) fn fact() -> Fact {
    let entity = |name: &str| EntityId::from_canonical_bytes(name).unwrap();
    Fact::new(
        FactId::from_canonical_bytes("edge").unwrap(),
        schema().id(),
        vec![Value::Entity(entity("alice")), Value::Entity(entity("bob"))],
        RelationContext::new(
            GenerationId::from_canonical_bytes("generation").unwrap(),
            RelationAuthority::Entity(entity("owner")),
            FactProvenance::Source(entity("owner")),
            EvidenceCompleteness::Complete,
            FactValidity::Valid,
        )
        .unwrap(),
    )
}
pub(crate) fn query_for_count(
    inventory: &GraphDatasetInventory,
    generation: &str,
    rows: u64,
) -> BoundDataQuery {
    let generation = GenerationId::from_canonical_bytes(generation).unwrap();
    let snapshot = SemanticSnapshot::admit(
        generation,
        vec![
            RevisionBinding::admit(
                ExternalRevisionIdentity::new("git", "fixture", "revision").unwrap(),
                generation,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let node = EntityId::from_canonical_bytes("node").unwrap();
    let binding = |name: &str| Binding::new(name).unwrap();
    let op = |name: &str| QueryOperatorId::from_canonical_bytes(name).unwrap();
    let query_id = QueryId::from_canonical_bytes("query").unwrap();
    let query = MetaQueryIr::new(
        query_id,
        GraphPattern::new(
            op("graph"),
            vec![PathPattern::new(
                NodePattern::new(binding("source"), vec![node]),
                vec![PathSegment::new(
                    RelationPattern::new(
                        None,
                        vec![schema().id()],
                        Direction::Outgoing,
                        1,
                        Some(1),
                    )
                    .unwrap(),
                    NodePattern::new(binding("target"), vec![node]),
                )],
            )],
        )
        .unwrap(),
        vec![],
        QueryResult::returning(SetQuantifier::All).with_projections(vec![Projection::new(
            op("return"),
            Expression::Binding(binding("source")),
            binding("entity"),
        )]),
    )
    .unwrap();
    let bundle = ReasoningBundle::admit(ReasoningBundleDeclaration {
        relations: vec![schema()],
        entities: vec![EntitySchema::new(node, "Node", vec![]).unwrap()],
        query_templates: vec![QueryTemplate::new(query, vec![])],
        ..ReasoningBundleDeclaration::default()
    })
    .unwrap();
    let bound = bind_query_to_catalog(&bundle, query_id, &snapshot).unwrap();
    let relations = RelationCatalog::admit(vec![schema()]).unwrap();
    let entities =
        EntityCatalog::admit(vec![EntitySchema::new(node, "Node", vec![]).unwrap()]).unwrap();
    let metadata = inventory
        .files()
        .iter()
        .find(|f| f.path() == inventory.entry())
        .unwrap()
        .cid();
    let manifest = SnapshotManifest::admit(
        SnapshotManifestRequest::new(
            snapshot,
            &relations,
            &entities,
            vec![
                RelationDescriptor::new(
                    schema().id(),
                    rows,
                    vec![BatchDescriptor::new(raw_cid(b"ipc"), rows, 3).unwrap()],
                )
                .unwrap(),
            ],
            CoverageDescriptor::new(CoverageKind::Complete, raw_cid(b"coverage")).unwrap(),
        )
        .with_graph_projection(GraphProjectionDescriptor::new("0.12.0", *metadata).unwrap()),
    )
    .unwrap();
    bind_data_query(
        &bound,
        &SnapshotBlock::encode(manifest).unwrap(),
        &DataEngineProfile::new("graphar-native", true, []).unwrap(),
    )
    .unwrap()
}
