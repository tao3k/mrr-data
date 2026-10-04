//! Shared authenticated-source and admitted-query fixture for physical engines.
use meta_relational_reasoning as mrr;
use mrr_data_core as core;
use mrr_data_graphar as graphar;
pub(crate) fn relation() -> mrr::RelationSchema {
    mrr::RelationSchema::new(
        mrr::RelationId::from_canonical_bytes("knows").unwrap(),
        "knows",
        vec![
            mrr::RelationField::new("source", mrr::ValueSchema::Entity, false).unwrap(),
            mrr::RelationField::new("destination", mrr::ValueSchema::Entity, false).unwrap(),
        ],
        vec![],
    )
    .unwrap()
}
pub(crate) fn entity(name: &str) -> mrr::EntityId {
    mrr::EntityId::from_canonical_bytes(name).unwrap()
}
pub(crate) fn fact(id: &str) -> mrr::Fact {
    fact_with_endpoints(id, "alice", "bob")
}
pub(crate) fn fact_with_endpoints(id: &str, source: &str, target: &str) -> mrr::Fact {
    mrr::Fact::new(
        mrr::FactId::from_canonical_bytes(id).unwrap(),
        relation().id(),
        vec![
            mrr::Value::Entity(entity(source)),
            mrr::Value::Entity(entity(target)),
        ],
        mrr::RelationContext::new(
            mrr::GenerationId::from_canonical_bytes("generation").unwrap(),
            mrr::RelationAuthority::Entity(entity("owner")),
            mrr::FactProvenance::Source(entity("owner")),
            mrr::EvidenceCompleteness::Complete,
            mrr::FactValidity::Valid,
        )
        .unwrap(),
    )
}
pub(crate) fn bound_query(
    inventory: &core::GraphDatasetInventory,
    generation: &str,
    engine: &core::DataEngineProfile,
) -> core::BoundDataQuery {
    bound_query_with_target(inventory, generation, "target", engine)
}
pub(crate) fn bound_query_with_target(
    inventory: &core::GraphDatasetInventory,
    generation: &str,
    target_binding: &str,
    engine: &core::DataEngineProfile,
) -> core::BoundDataQuery {
    bound_query_shape(inventory, generation, target_binding, None, engine).unwrap()
}
#[derive(Clone, Copy)]
pub(crate) enum RefusedShape {
    Filter,
    Distinct,
    Paging,
    Incoming,
    VariableLength,
    EdgeBinding,
}
pub(crate) fn bound_query_shape(
    inventory: &core::GraphDatasetInventory,
    generation: &str,
    target_binding: &str,
    shape: Option<RefusedShape>,
    engine: &core::DataEngineProfile,
) -> Result<core::BoundDataQuery, core::DataQueryBindingError> {
    bound_query_shape_with_rows(inventory, generation, target_binding, shape, engine, 2)
}
pub(crate) fn bound_query_shape_with_rows(
    inventory: &core::GraphDatasetInventory,
    generation: &str,
    target_binding: &str,
    shape: Option<RefusedShape>,
    engine: &core::DataEngineProfile,
    rows: usize,
) -> Result<core::BoundDataQuery, core::DataQueryBindingError> {
    let rows = u64::try_from(rows).unwrap();
    let generation = mrr::GenerationId::from_canonical_bytes(generation).unwrap();
    let snapshot = admitted_snapshot(generation);
    let binding = |name: &str| mrr::Binding::new(name).unwrap();
    let op = |name: &str| mrr::QueryOperatorId::from_canonical_bytes(name).unwrap();
    let query_id = mrr::QueryId::from_canonical_bytes("query").unwrap();
    let node_type = entity("node");
    let query = mrr::MetaQueryIr::new(
        query_id,
        mrr::GraphPattern::new(
            op("graph"),
            vec![mrr::PathPattern::new(
                mrr::NodePattern::new(binding("source"), vec![node_type]),
                vec![mrr::PathSegment::new(
                    mrr::RelationPattern::new(
                        matches!(shape, Some(RefusedShape::EdgeBinding)).then(|| binding("edge")),
                        vec![relation().id()],
                        if matches!(shape, Some(RefusedShape::Incoming)) {
                            mrr::Direction::Incoming
                        } else {
                            mrr::Direction::Outgoing
                        },
                        1,
                        Some(if matches!(shape, Some(RefusedShape::VariableLength)) {
                            2
                        } else {
                            1
                        }),
                    )
                    .unwrap(),
                    mrr::NodePattern::new(binding(target_binding), vec![node_type]),
                )],
            )],
        )
        .unwrap(),
        if matches!(shape, Some(RefusedShape::Filter)) {
            vec![mrr::Filter::new(
                op("filter"),
                mrr::Expression::Literal(mrr::Value::Boolean(true)),
            )]
        } else {
            vec![]
        },
        query_result(target_binding, shape),
    )
    .unwrap();
    let entity_schema = mrr::EntitySchema::new(node_type, "Node", vec![]).unwrap();
    let bundle = mrr::ReasoningBundle::admit(mrr::ReasoningBundleDeclaration {
        relations: vec![relation()],
        entities: vec![entity_schema.clone()],
        query_templates: vec![mrr::QueryTemplate::new(query, vec![])],
        ..mrr::ReasoningBundleDeclaration::default()
    })
    .unwrap();
    let catalog_query = mrr::bind_query_to_catalog(&bundle, query_id, &snapshot).unwrap();
    let relations = mrr::RelationCatalog::admit(vec![relation()]).unwrap();
    let entities = mrr::EntityCatalog::admit(vec![entity_schema]).unwrap();
    let metadata = inventory
        .files()
        .iter()
        .find(|f| f.path() == inventory.entry())
        .unwrap()
        .cid();
    let manifest = core::SnapshotManifest::admit(
        core::SnapshotManifestRequest::new(
            snapshot,
            &relations,
            &entities,
            vec![
                core::RelationDescriptor::new(
                    relation().id(),
                    rows,
                    vec![core::BatchDescriptor::new(core::raw_cid(b"ipc"), rows, 3).unwrap()],
                )
                .unwrap(),
            ],
            core::CoverageDescriptor::new(core::CoverageKind::Complete, core::raw_cid(b"coverage"))
                .unwrap(),
        )
        .with_graph_projection(core::GraphProjectionDescriptor::new("0.12.0", *metadata).unwrap()),
    )
    .unwrap();
    core::bind_data_query(
        &catalog_query,
        &core::SnapshotBlock::encode(manifest).unwrap(),
        engine,
    )
}
pub(crate) fn query_result(target_binding: &str, shape: Option<RefusedShape>) -> mrr::QueryResult {
    let binding = |name: &str| mrr::Binding::new(name).unwrap();
    let op = |name: &str| mrr::QueryOperatorId::from_canonical_bytes(name).unwrap();
    mrr::QueryResult::returning(if matches!(shape, Some(RefusedShape::Distinct)) {
        mrr::SetQuantifier::Distinct
    } else {
        mrr::SetQuantifier::All
    })
    .with_projections(vec![
        mrr::Projection::new(
            op("return-source"),
            mrr::Expression::Binding(binding("source")),
            binding("from"),
        ),
        mrr::Projection::new(
            op("return-target"),
            mrr::Expression::Binding(binding(target_binding)),
            binding("to"),
        ),
    ])
    .with_limit(matches!(shape, Some(RefusedShape::Paging)).then_some(mrr::PageValue::Literal(1)))
}
pub(crate) fn admitted_snapshot(generation: mrr::GenerationId) -> mrr::SemanticSnapshot {
    mrr::SemanticSnapshot::admit(
        generation,
        vec![
            mrr::RevisionBinding::admit(
                mrr::ExternalRevisionIdentity::new("git", "fixture", "revision").unwrap(),
                generation,
            )
            .unwrap(),
        ],
    )
    .unwrap()
}
pub(crate) fn captured(
    engine: &core::DataEngineProfile,
) -> (
    core::BoundDataQuery,
    graphar::BinaryEntityProjection,
    graphar::CapturedGraphArSnapshot,
    core::GraphDatasetInventory,
) {
    captured_facts(&[fact("edge-a"), fact("edge-b")], engine)
}
pub(crate) fn captured_facts(
    facts: &[mrr::Fact],
    engine: &core::DataEngineProfile,
) -> (
    core::BoundDataQuery,
    graphar::BinaryEntityProjection,
    graphar::CapturedGraphArSnapshot,
    core::GraphDatasetInventory,
) {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("graph");
    let projection = graphar::BinaryEntityProjection::admit_catalog(
        &mrr::RelationCatalog::admit(vec![relation()]).unwrap(),
        relation().id(),
    )
    .unwrap();
    let receipt = graphar::write_graphar_dataset(
        &source,
        &projection,
        &facts
            .iter()
            .map(|fact| projection.project(fact).unwrap())
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let inventory = receipt.inventory().clone();
    capture_published(&source, inventory, facts.len(), engine)
}
pub(crate) fn capture_published(
    source: &std::path::Path,
    inventory: core::GraphDatasetInventory,
    rows: usize,
    engine: &core::DataEngineProfile,
) -> (
    core::BoundDataQuery,
    graphar::BinaryEntityProjection,
    graphar::CapturedGraphArSnapshot,
    core::GraphDatasetInventory,
) {
    let projection = graphar::BinaryEntityProjection::admit_catalog(
        &mrr::RelationCatalog::admit(vec![relation()]).unwrap(),
        relation().id(),
    )
    .unwrap();
    let query = bound_query_shape_with_rows(&inventory, "generation", "target", None, engine, rows)
        .unwrap();
    let limits = core::GraphInventoryLimits::default();
    let binding =
        core::GraphDatasetBinding::admit(&query, relation().id(), &inventory, limits).unwrap();
    let captured = graphar::capture_graphar_snapshot(
        source,
        &query,
        binding,
        &inventory,
        &projection,
        limits,
        graphar::GraphArReadLimits::new(rows.checked_mul(2).unwrap(), rows),
    )
    .unwrap();
    (query, projection, captured, inventory)
}
