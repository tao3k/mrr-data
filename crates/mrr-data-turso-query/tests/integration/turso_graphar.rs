#![cfg(feature = "turso-graphar")]
use std::num::NonZeroUsize;
#[cfg(feature = "backend-worker")]
use std::sync::Arc;

use meta_relational_reasoning as mrr;
use mrr_data_core as core;
use mrr_data_graphar as graphar;
use mrr_data_turso_query::{
    SqlQueryError, SqlQueryLimits, TursoSingleHopSql, execute_turso_graphar_single_hop,
    turso_graphar_engine_profile,
};
#[cfg(feature = "backend-worker")]
use mrr_data_turso_query::{TursoBackendQuery, execute_turso_graphar_on_backend};

fn relation() -> mrr::RelationSchema {
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
fn entity(name: &str) -> mrr::EntityId {
    mrr::EntityId::from_canonical_bytes(name).unwrap()
}
fn fact(id: &str) -> mrr::Fact {
    mrr::Fact::new(
        mrr::FactId::from_canonical_bytes(id).unwrap(),
        relation().id(),
        vec![
            mrr::Value::Entity(entity("alice")),
            mrr::Value::Entity(entity("bob")),
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
fn bound_query(inventory: &core::GraphDatasetInventory, generation: &str) -> core::BoundDataQuery {
    let generation = mrr::GenerationId::from_canonical_bytes(generation).unwrap();
    let snapshot = mrr::SemanticSnapshot::admit(
        generation,
        vec![
            mrr::RevisionBinding::admit(
                mrr::ExternalRevisionIdentity::new("git", "fixture", "revision").unwrap(),
                generation,
            )
            .unwrap(),
        ],
    )
    .unwrap();
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
                        None,
                        vec![relation().id()],
                        mrr::Direction::Outgoing,
                        1,
                        Some(1),
                    )
                    .unwrap(),
                    mrr::NodePattern::new(binding("target"), vec![node_type]),
                )],
            )],
        )
        .unwrap(),
        vec![],
        mrr::QueryResult::returning(mrr::SetQuantifier::All).with_projections(vec![
            mrr::Projection::new(
                op("return-source"),
                mrr::Expression::Binding(binding("source")),
                binding("from"),
            ),
            mrr::Projection::new(
                op("return-target"),
                mrr::Expression::Binding(binding("target")),
                binding("to"),
            ),
        ]),
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
                    2,
                    vec![core::BatchDescriptor::new(core::raw_cid(b"ipc"), 2, 3).unwrap()],
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
        &turso_graphar_engine_profile().unwrap(),
    )
    .unwrap()
}
fn captured() -> (
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
        &[
            projection.project(&fact("edge-a")).unwrap(),
            projection.project(&fact("edge-b")).unwrap(),
        ],
    )
    .unwrap();
    let inventory = receipt.inventory().clone();
    let query = bound_query(&inventory, "generation");
    let limits = core::GraphInventoryLimits::default();
    let binding =
        core::GraphDatasetBinding::admit(&query, relation().id(), &inventory, limits).unwrap();
    let captured = graphar::capture_graphar_snapshot(
        &source,
        &query,
        binding,
        &inventory,
        &projection,
        limits,
        graphar::GraphArReadLimits::new(4, 2),
    )
    .unwrap();
    (query, projection, captured, inventory)
}
fn limits() -> SqlQueryLimits {
    SqlQueryLimits {
        max_input_rows: 2,
        max_input_bytes: 1024,
        max_output_rows: 2,
        max_output_cells: 4,
    }
}
#[tokio::test]
async fn turso_single_hop_preserves_duplicate_edges_and_mrr_admission() {
    let (query, projection, source, inventory) = captured();
    let plan = TursoSingleHopSql::compile(&query, &projection).unwrap();
    assert!(plan.statement().contains("?1"));
    assert!(plan.statement().contains("?2"));
    assert!(!plan.statement().contains("knows"));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("query.db");
    let database = turso::Builder::new_local(path.to_str().unwrap())
        .build()
        .await
        .unwrap();
    let output =
        execute_turso_graphar_single_hop(&database, &query, &source, &projection, limits())
            .await
            .unwrap();
    let node = |id| mrr::QueryResultValue::node(entity(id), entity("node"));
    assert_eq!(
        output.columns(),
        &[
            mrr::Binding::new("from").unwrap(),
            mrr::Binding::new("to").unwrap()
        ]
    );
    assert_eq!(
        output.rows(),
        &[
            vec![node("alice"), node("bob")],
            vec![node("alice"), node("bob")]
        ]
    );
    let candidate = core::project_data_query_output(&query, query.engine(), output).unwrap();
    mrr::admit_query_result_candidate(
        query.query(),
        &candidate,
        mrr::QueryResultLimits::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(4).unwrap()),
    )
    .unwrap();
    let drifted = bound_query(&inventory, "other-generation");
    assert!(matches!(
        execute_turso_graphar_single_hop(&database, &drifted, &source, &projection, limits()).await,
        Err(SqlQueryError::SourceMismatch)
    ));
    let mut small = limits();
    small.max_input_rows = 1;
    assert!(matches!(
        execute_turso_graphar_single_hop(&database, &query, &source, &projection, small).await,
        Err(SqlQueryError::Limit("input rows"))
    ));
}

#[cfg(feature = "backend-worker")]
#[tokio::test]
async fn turso_single_hop_uses_backend_resource_worker() {
    use mrr_data_backend::{Backend, BackendConfig, BackendError, providers::TursoProvider};
    let (query, projection, source, _) = captured();
    let directory = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Handle::current();
    let backend = Backend::open(
        BackendConfig {
            max_resources: 1,
            max_resource_bytes: 2048,
            ..BackendConfig::default()
        },
        TursoProvider::new(directory.path().join("metadata.db"), runtime.clone()),
        runtime.clone(),
    )
    .await
    .unwrap();
    let database = Arc::new(
        turso::Builder::new_local(directory.path().join("query.db").to_str().unwrap())
            .build()
            .await
            .unwrap(),
    );
    let source = Arc::new(source);
    assert!(matches!(
        execute_turso_graphar_on_backend(
            &backend,
            runtime.clone(),
            TursoBackendQuery {
                database: database.clone(),
                query: query.clone(),
                source: source.clone(),
                projection: projection.clone(),
                limits: limits(),
                reserved_bytes: 2049,
            },
        )
        .await,
        Err(SqlQueryError::Backend(BackendError::Saturated))
    ));
    let output = execute_turso_graphar_on_backend(
        &backend,
        runtime,
        TursoBackendQuery {
            database,
            query: query.clone(),
            source,
            projection,
            limits: limits(),
            reserved_bytes: 2048,
        },
    )
    .await
    .unwrap();
    assert_eq!(output.rows().len(), 2);
    assert_eq!(backend.status().active_resources, 0);
    assert_eq!(backend.status().resource_bytes, 0);
    let candidate = core::project_data_query_output(&query, query.engine(), output).unwrap();
    mrr::admit_query_result_candidate(
        query.query(),
        &candidate,
        mrr::QueryResultLimits::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(4).unwrap()),
    )
    .unwrap();
    backend.shutdown().await.unwrap();
}
