use std::num::NonZeroUsize;

use meta_relational_reasoning::{
    Binding, CandidateQueryResult, CatalogBoundQuery, Direction, EntityCatalog, EntityId,
    EntitySchema, EvidenceCompleteness, Expression, ExternalRevisionIdentity, Fact, FactId,
    FactProvenance, FactValidity, GenerationId, GraphPattern, NodePattern, PathPattern,
    PathSegment, Projection, QueryId, QueryOperatorId, QueryResult, QueryResultLimits,
    QueryResultValue, QueryTemplate, ReasoningBundle, ReasoningBundleDeclaration,
    RelationAuthority, RelationCatalog, RelationContext, RelationField, RelationId,
    RelationPattern, RelationSchema, RevisionBinding, SemanticSnapshot, SetQuantifier, Value,
    ValueSchema, admit_query_result_candidate, bind_query_to_catalog,
};
use mrr_data_arrow::{facts_to_record_batch, record_batch_to_facts};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, DataEngineProfile,
    GraphProjectionDescriptor, PhysicalQueryOutput, RelationDescriptor, SnapshotBlock,
    SnapshotManifest, SnapshotManifestRequest, bind_data_query, project_data_query_output, raw_cid,
};

use crate::{
    BinaryEntityProjection, GraphArReadLimits, read_graphar_dataset, write_graphar_dataset,
};

fn id<T>(name: &str, constructor: impl FnOnce(&str) -> Option<T>) -> T {
    constructor(name).expect("fixture identity")
}

fn entity_id(name: &str) -> EntityId {
    id(name, |value| EntityId::from_canonical_bytes(value).ok())
}

fn relation_id(name: &str) -> RelationId {
    id(name, |value| RelationId::from_canonical_bytes(value).ok())
}

fn fact_id(name: &str) -> FactId {
    id(name, |value| FactId::from_canonical_bytes(value).ok())
}

fn generation_id(name: &str) -> GenerationId {
    id(name, |value| GenerationId::from_canonical_bytes(value).ok())
}

fn query_id(name: &str) -> QueryId {
    id(name, |value| QueryId::from_canonical_bytes(value).ok())
}

fn operator_id(name: &str) -> QueryOperatorId {
    id(name, |value| {
        QueryOperatorId::from_canonical_bytes(value).ok()
    })
}

fn relation() -> RelationSchema {
    RelationSchema::new(
        relation_id("relation:knows"),
        "knows",
        vec![
            RelationField::new("source", ValueSchema::Entity, false).unwrap(),
            RelationField::new("target", ValueSchema::Entity, false).unwrap(),
        ],
        vec![],
    )
    .unwrap()
}

fn entity_type() -> EntityId {
    entity_id("entity-type:person")
}

fn semantic_snapshot() -> SemanticSnapshot {
    let generation = generation_id("generation:query-parity");
    SemanticSnapshot::admit(
        generation,
        vec![
            RevisionBinding::admit(
                ExternalRevisionIdentity::new("fixture", "query-parity", "revision:query-parity")
                    .unwrap(),
                generation,
            )
            .unwrap(),
        ],
    )
    .unwrap()
}

fn facts(relation: &RelationSchema) -> Vec<Fact> {
    let generation = semantic_snapshot().generation();
    let owner = entity_id("entity:source-owner");
    let context = RelationContext::new(
        generation,
        RelationAuthority::Entity(owner),
        FactProvenance::Source(owner),
        EvidenceCompleteness::Complete,
        FactValidity::Valid,
    )
    .unwrap();
    let mut facts = [
        ("fact:carol-dora", "entity:carol", "entity:dora"),
        ("fact:alice-bob", "entity:alice", "entity:bob"),
        ("fact:bob-carol", "entity:bob", "entity:carol"),
    ]
    .into_iter()
    .map(|(fact, source, target)| {
        Fact::new(
            fact_id(fact),
            relation.id(),
            vec![
                Value::Entity(entity_id(source)),
                Value::Entity(entity_id(target)),
            ],
            context,
        )
    })
    .collect::<Vec<_>>();
    facts.sort_unstable_by_key(Fact::id);
    facts
}

fn bound_query(relation: &RelationSchema) -> CatalogBoundQuery {
    let source = Binding::new("source").unwrap();
    let target = Binding::new("target").unwrap();
    let edge = Binding::new("edge").unwrap();
    let source_result = Binding::new("source_entity").unwrap();
    let target_result = Binding::new("target_entity").unwrap();
    let query_identity = query_id("query:knows-endpoints");
    let query = meta_relational_reasoning::MetaQueryIr::new(
        query_identity,
        GraphPattern::new(
            operator_id("operator:graph"),
            vec![PathPattern::new(
                NodePattern::new(source.clone(), vec![entity_type()]),
                vec![PathSegment::new(
                    RelationPattern::new(
                        Some(edge),
                        vec![relation.id()],
                        Direction::Outgoing,
                        1,
                        Some(1),
                    )
                    .unwrap(),
                    NodePattern::new(target.clone(), vec![entity_type()]),
                )],
            )],
        )
        .unwrap(),
        vec![],
        QueryResult::returning(SetQuantifier::All).with_projections(vec![
            Projection::new(
                operator_id("operator:source"),
                Expression::Binding(source.clone()),
                source_result,
            ),
            Projection::new(
                operator_id("operator:target"),
                Expression::Binding(target.clone()),
                target_result,
            ),
        ]),
    )
    .unwrap();
    let bundle = ReasoningBundle::admit(ReasoningBundleDeclaration {
        relations: vec![relation.clone()],
        entities: vec![EntitySchema::new(entity_type(), "Person", vec![]).unwrap()],
        query_templates: vec![QueryTemplate::new(query, vec![])],
        ..ReasoningBundleDeclaration::default()
    })
    .unwrap();
    bind_query_to_catalog(&bundle, query_identity, &semantic_snapshot()).unwrap()
}

fn snapshot(relation: &RelationSchema, row_count: usize) -> SnapshotBlock {
    let relations = RelationCatalog::admit(vec![relation.clone()]).unwrap();
    let entities = EntityCatalog::admit(vec![
        EntitySchema::new(entity_type(), "Person", vec![]).unwrap(),
    ])
    .unwrap();
    let descriptor = RelationDescriptor::new(
        relation.id(),
        row_count as u64,
        vec![
            BatchDescriptor::new(raw_cid(b"query-parity-arrow-ipc"), row_count as u64, 1).unwrap(),
        ],
    )
    .unwrap();
    let manifest = SnapshotManifest::admit(
        SnapshotManifestRequest::new(
            semantic_snapshot(),
            &relations,
            &entities,
            vec![descriptor],
            CoverageDescriptor::new(CoverageKind::Complete, raw_cid(b"query-parity-coverage"))
                .unwrap(),
        )
        .with_graph_projection(
            GraphProjectionDescriptor::new("0.12.0", raw_cid(b"query-parity-graphar")).unwrap(),
        ),
    )
    .unwrap();
    SnapshotBlock::encode(manifest).unwrap()
}

fn output_from_facts(facts: &[Fact]) -> PhysicalQueryOutput {
    let rows = facts
        .iter()
        .map(|fact| {
            let [Value::Entity(source), Value::Entity(target)] = fact.values() else {
                panic!("binary Entity relation was admitted before execution")
            };
            vec![
                QueryResultValue::node(*source, entity_type()),
                QueryResultValue::node(*target, entity_type()),
            ]
        })
        .collect();
    PhysicalQueryOutput::new(
        vec![
            Binding::new("source_entity").unwrap(),
            Binding::new("target_entity").unwrap(),
        ],
        rows,
    )
}

fn execute(
    query: &CatalogBoundQuery,
    snapshot: &SnapshotBlock,
    profile: &DataEngineProfile,
    physical_facts: &[Fact],
) -> CandidateQueryResult {
    let bound = bind_data_query(query, snapshot, profile).unwrap();
    project_data_query_output(&bound, profile, output_from_facts(physical_facts)).unwrap()
}

#[test]
fn arrow_and_maintained_graphar_project_one_identical_mrr_candidate() {
    let relation = relation();
    let facts = facts(&relation);
    let query = bound_query(&relation);
    let snapshot = snapshot(&relation, facts.len());

    let arrow_batch = facts_to_record_batch(&relation, &facts).unwrap();
    let arrow_facts = record_batch_to_facts(&relation, &arrow_batch).unwrap();

    let projection = BinaryEntityProjection::admit(&relation).unwrap();
    let edges = facts
        .iter()
        .map(|fact| projection.project(fact).unwrap())
        .collect::<Vec<_>>();
    let directory = tempfile::tempdir().unwrap();
    let graphar_root = directory.path().join("query-parity");
    write_graphar_dataset(&graphar_root, &projection, &edges).unwrap();
    let graphar =
        read_graphar_dataset(&graphar_root, &projection, GraphArReadLimits::new(16, 16)).unwrap();

    let arrow_candidate = execute(
        &query,
        &snapshot,
        &DataEngineProfile::new("arrow-round-trip", false, []).unwrap(),
        &arrow_facts,
    );
    let graphar_candidate = execute(
        &query,
        &snapshot,
        &DataEngineProfile::new("graphar-native", true, []).unwrap(),
        graphar.facts(),
    );
    assert_eq!(arrow_candidate, graphar_candidate);

    let limits = QueryResultLimits::new(
        NonZeroUsize::new(16).unwrap(),
        NonZeroUsize::new(32).unwrap(),
    );
    let arrow_receipt = admit_query_result_candidate(&query, &arrow_candidate, limits).unwrap();
    let graphar_receipt = admit_query_result_candidate(&query, &graphar_candidate, limits).unwrap();
    assert_eq!(arrow_receipt, graphar_receipt);
    assert_eq!(arrow_receipt.row_count(), facts.len());
}
