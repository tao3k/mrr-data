#![cfg(feature = "turso-graphar")]
#[cfg(feature = "backend-worker")]
mod backend_lifecycle;
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
use mrr_data_turso_query::{
    TursoBackendQuery, execute_turso_graphar_on_backend, execute_turso_graphar_retained_on_backend,
};

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
    fact_with_endpoints(id, "alice", "bob")
}
fn fact_with_endpoints(id: &str, source: &str, target: &str) -> mrr::Fact {
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
fn bound_query(inventory: &core::GraphDatasetInventory, generation: &str) -> core::BoundDataQuery {
    bound_query_with_target(inventory, generation, "target")
}
fn bound_query_with_target(
    inventory: &core::GraphDatasetInventory,
    generation: &str,
    target_binding: &str,
) -> core::BoundDataQuery {
    bound_query_shape(inventory, generation, target_binding, None).unwrap()
}
#[derive(Clone, Copy)]
enum RefusedShape {
    Filter,
    Distinct,
    Paging,
    Incoming,
    VariableLength,
    EdgeBinding,
}
fn bound_query_shape(
    inventory: &core::GraphDatasetInventory,
    generation: &str,
    target_binding: &str,
    shape: Option<RefusedShape>,
) -> Result<core::BoundDataQuery, core::DataQueryBindingError> {
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
}
fn query_result(target_binding: &str, shape: Option<RefusedShape>) -> mrr::QueryResult {
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
fn admitted_snapshot(generation: mrr::GenerationId) -> mrr::SemanticSnapshot {
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
fn captured() -> (
    core::BoundDataQuery,
    graphar::BinaryEntityProjection,
    graphar::CapturedGraphArSnapshot,
    core::GraphDatasetInventory,
) {
    captured_facts(&[fact("edge-a"), fact("edge-b")])
}
fn captured_facts(
    facts: &[mrr::Fact; 2],
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
#[tokio::test]
async fn canonical_fact_order_and_all_budgets_are_qualified() {
    // Input order and endpoint lexical order both disagree with FactId order.
    let mut facts = [
        fact_with_endpoints("edge-b", "alice", "bob"),
        fact_with_endpoints("edge-a", "zoe", "yan"),
    ];
    let first = facts.iter().min_by_key(|fact| fact.id()).unwrap();
    let expected_first = first.values().to_vec();
    let (query, projection, source, _) = captured_facts(&facts);
    let dir = tempfile::tempdir().unwrap();
    let database = turso::Builder::new_local(dir.path().join("ordering.db").to_str().unwrap())
        .build()
        .await
        .unwrap();
    let output =
        execute_turso_graphar_single_hop(&database, &query, &source, &projection, limits())
            .await
            .unwrap();
    let expected_node = |value: &mrr::Value| match value {
        mrr::Value::Entity(id) => mrr::QueryResultValue::node(*id, entity("node")),
        _ => panic!("fixture has only Entity endpoints"),
    };
    assert_eq!(
        output.rows()[0],
        expected_first.iter().map(expected_node).collect::<Vec<_>>()
    );
    facts.sort_by_key(mrr::Fact::id);
    let expected = facts
        .iter()
        .map(|fact| fact.values().iter().map(expected_node).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(output.rows(), expected);
    let candidate = core::project_data_query_output(&query, query.engine(), output).unwrap();
    mrr::admit_query_result_candidate(
        query.query(),
        &candidate,
        mrr::QueryResultLimits::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(4).unwrap()),
    )
    .unwrap();
    for (budget, reason) in [
        (
            SqlQueryLimits {
                max_input_rows: 1,
                ..limits()
            },
            "input rows",
        ),
        (
            SqlQueryLimits {
                max_input_bytes: 1,
                ..limits()
            },
            "input bytes",
        ),
        (
            SqlQueryLimits {
                max_output_rows: 1,
                ..limits()
            },
            "output rows or cells",
        ),
        (
            SqlQueryLimits {
                max_output_cells: 3,
                ..limits()
            },
            "output rows or cells",
        ),
        (
            SqlQueryLimits {
                max_output_cells: 0,
                ..limits()
            },
            "zero query budget",
        ),
    ] {
        assert!(
            matches!(execute_turso_graphar_single_hop(&database, &query, &source, &projection, budget).await, Err(SqlQueryError::Limit(actual)) if actual == reason)
        );
    }
    assert_eq!(
        execute_turso_graphar_single_hop(&database, &query, &source, &projection, limits())
            .await
            .unwrap()
            .rows(),
        expected
    );
}
fn limits() -> SqlQueryLimits {
    SqlQueryLimits {
        max_input_rows: 2,
        max_input_bytes: 1024,
        max_output_rows: 2,
        max_output_cells: 4,
    }
}
#[test]
fn unsupported_admitted_shapes_are_refused_without_weaker_queries() {
    let (_, projection, _, inventory) = captured();
    for shape in [
        RefusedShape::Filter,
        RefusedShape::Distinct,
        RefusedShape::Paging,
        RefusedShape::Incoming,
        RefusedShape::VariableLength,
        RefusedShape::EdgeBinding,
    ] {
        let query = bound_query_shape(&inventory, "generation", "target", Some(shape));
        if matches!(shape, RefusedShape::VariableLength) {
            assert!(matches!(
                query,
                Err(core::DataQueryBindingError::UnsupportedFeature(_))
            ));
            continue;
        }
        let query = query.unwrap();
        assert!(matches!(
            TursoSingleHopSql::compile(&query, &projection),
            Err(SqlQueryError::UnsupportedShape(_))
        ));
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
    let repeated = bound_query_with_target(&inventory, "generation", "source");
    assert!(matches!(
        TursoSingleHopSql::compile(&repeated, &projection),
        Err(SqlQueryError::UnsupportedShape(
            "distinct endpoint bindings required"
        ))
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
        runtime.clone(),
        TursoBackendQuery {
            database: database.clone(),
            query: query.clone(),
            source: source.clone(),
            projection: projection.clone(),
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
    qualify_retained_output(
        &backend,
        runtime,
        TursoBackendQuery {
            database,
            query,
            source,
            projection,
            limits: limits(),
            reserved_bytes: 2048,
        },
    )
    .await;
}

#[cfg(feature = "backend-worker")]
async fn qualify_retained_output(
    backend: &mrr_data_backend::Backend,
    runtime: tokio::runtime::Handle,
    request: TursoBackendQuery,
) {
    use mrr_data_backend::BackendError;
    let TursoBackendQuery {
        database,
        query,
        source,
        projection,
        ..
    } = request;
    let refused = execute_turso_graphar_retained_on_backend(
        backend,
        runtime.clone(),
        TursoBackendQuery {
            database: database.clone(),
            query: query.clone(),
            source: source.clone(),
            projection: projection.clone(),
            limits: SqlQueryLimits {
                max_input_rows: 1,
                ..limits()
            },
            reserved_bytes: 2048,
        },
    )
    .await;
    assert!(matches!(refused, Err(SqlQueryError::Limit("input rows"))));
    assert_eq!(backend.status().active_resources, 0);
    assert_eq!(backend.status().resource_bytes, 0);
    let retained = execute_turso_graphar_retained_on_backend(
        backend,
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
    let last = retained.clone();
    assert_eq!(backend.status().active_resources, 1);
    assert_eq!(backend.status().resource_bytes, 2048);
    assert_eq!(backend.status().blocking_resources, 0);
    assert!(matches!(
        backend.prepare_resource(1, || Ok(())).await,
        Err(BackendError::Saturated)
    ));
    // Consume physical output into the MRR candidate under the same lease,
    // rather than creating an unaccounted output clone.
    drop(last);
    let Ok(retained) = retained
        .try_transform(|output| core::project_data_query_output(&query, query.engine(), output))
    else {
        panic!("retained candidate conversion failed");
    };
    mrr::admit_query_result_candidate(
        query.query(),
        retained.get(),
        mrr::QueryResultLimits::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(4).unwrap()),
    )
    .unwrap();
    let last = retained.clone();
    let closing = backend.clone();
    let shutdown = tokio::spawn(async move { closing.shutdown().await });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while backend.status().lifecycle != mrr_data_backend::Lifecycle::Draining {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(retained);
    assert_eq!(backend.status().active_resources, 1);
    assert!(!shutdown.is_finished());
    assert_eq!(backend.status().resource_bytes, 2048);
    drop(last);
    tokio::time::timeout(std::time::Duration::from_secs(3), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(backend.status().resource_bytes, 0);
}

#[cfg(feature = "backend-worker")]
#[tokio::test]
async fn controlled_turso_stops_publish_no_output_and_release_budget() {
    use mrr_data_backend::{Backend, BackendConfig, ResourceControl, providers::TursoProvider};
    use mrr_data_turso_query::execute_turso_graphar_controlled_on_backend;
    let (query, projection, source, _) = captured();
    let dir = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Handle::current();
    let backend = Backend::open(
        BackendConfig::default(),
        TursoProvider::new(dir.path().join("control-metadata.db"), runtime.clone()),
        runtime.clone(),
    )
    .await
    .unwrap();
    let database = Arc::new(
        turso::Builder::new_local(dir.path().join("control-query.db").to_str().unwrap())
            .build()
            .await
            .unwrap(),
    );
    let source = Arc::new(source);
    let cancelled = ResourceControl::default();
    cancelled.cancel();
    let expired = ResourceControl::new(Some(std::time::Instant::now()));
    for (control, expected) in [
        (cancelled, SqlQueryError::Cancelled),
        (expired, SqlQueryError::Deadline),
    ] {
        let result = execute_turso_graphar_controlled_on_backend(
            &backend,
            runtime.clone(),
            TursoBackendQuery {
                database: database.clone(),
                query: query.clone(),
                source: source.clone(),
                projection: projection.clone(),
                limits: limits(),
                reserved_bytes: 2048,
            },
            control,
        )
        .await;
        assert!(matches!(result, Err(error) if error == expected));
        assert_eq!(backend.status().active_resources, 0);
        assert_eq!(backend.status().resource_bytes, 0);
    }
    backend.shutdown().await.unwrap();
}
