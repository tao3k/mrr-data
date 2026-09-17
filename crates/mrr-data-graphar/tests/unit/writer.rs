use std::{
    cell::Cell,
    time::{Duration, Instant},
};

use asp_rust_build_support::{
    AspRustScenario, AspRustScenarioMeasurement, AspRustScenarioObservation, asp_rust_scenario,
    measure_asp_rust_scenario, render_asp_rust_scenario_benchmark_toml,
};
use graphar_rs::{
    info::{AdjListType, GraphInfo},
    reader::{read_edge_strings, read_vertex_strings},
};
use meta_relational_reasoning::{
    DerivationId, EntityId, EvidenceCompleteness, Fact, FactId, FactProvenance, FactValidity,
    GenerationId, RelationAuthority, RelationContext, RelationField, RelationId, RelationSchema,
    RuleId, Value, ValueSchema,
};

use crate::reader::scan_graphar_edge_chunks;
use crate::{
    BinaryEntityProjection, GraphArReadError, GraphArReadLimits, GraphArWriteError,
    prepare_graphar_source, read_graphar_dataset, read_graphar_dataset_observed,
    write_graphar_dataset,
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
    let mut expected = vec![source, derived];
    expected.sort_unstable_by_key(Fact::id);
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
fn prepared_graphar_source_reuses_semantic_facts_and_remains_fail_closed() {
    let projection = projection();
    let mut expected = vec![
        fact("edge-1", "alice", "bob"),
        fact("edge-2", "bob", "carol"),
    ];
    expected.sort_unstable_by_key(Fact::id);
    let edges = expected
        .iter()
        .map(|fact| projection.project(fact).unwrap())
        .collect::<Vec<_>>();
    let parent = tempfile::tempdir().unwrap();
    let output = parent.path().join("prepared-dataset");
    write_graphar_dataset(&output, &projection, &edges).unwrap();

    let prepared =
        prepare_graphar_source(&output, GraphArReadLimits::new(3, expected.len())).unwrap();
    assert_eq!(prepared.root(), output);
    assert_eq!(prepared.vertex_count(), 3);
    assert_eq!(prepared.edge_count(), 2);

    let first = prepared.admit(&projection).unwrap();
    let second = prepared.admit(&projection).unwrap();
    assert_eq!(first.facts(), expected.as_slice());
    assert_eq!(second, first);

    let error = prepared.admit(&projection_named("follows")).unwrap_err();
    assert!(matches!(error, GraphArReadError::PredicateMismatch { .. }));
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
#[ignore = "run as an isolated ASP Rust performance Scenario"]
fn scenario_semantically_reads_ten_thousand_graphar_edges() {
    const EDGE_COUNT: usize = 10_000;

    let projection = projection();
    let facts = graphar_scenario_facts(&projection, EDGE_COUNT);
    let mut expected_facts = facts.clone();
    expected_facts.sort_unstable_by_key(Fact::id);
    let edges = facts
        .iter()
        .map(|fact| projection.project(fact).unwrap())
        .collect::<Vec<_>>();
    let parent = tempfile::tempdir().unwrap();
    let output = parent.path().join("ten-thousand-edge-dataset");
    let write_started = Instant::now();
    let receipt = write_graphar_dataset(&output, &projection, &edges).unwrap();
    let write_elapsed = write_started.elapsed();
    assert_eq!(receipt.vertex_count(), EDGE_COUNT);
    assert_eq!(receipt.edge_count(), EDGE_COUNT);

    let scenario = graphar_semantic_read_scenario();
    let parity_scenario = graphar_arrow_bridge_parity_scenario();
    let prepared_scenario = graphar_prepared_admission_scenario();
    let limits = GraphArReadLimits::new(EDGE_COUNT, EDGE_COUNT);
    let prepared = prepare_graphar_source(&output, limits).expect("prepare GraphAr semantic facts");
    let parity_iteration = Cell::new(0_usize);
    let parity_measurement = measure_asp_rust_scenario(&parity_scenario, || {
        let iteration = parity_iteration.get();
        parity_iteration.set(iteration + 1);
        let (edge_count, official_elapsed, imported, timings) = if iteration.is_multiple_of(2) {
            let (edge_count, official_elapsed) = scan_graphar_edge_chunks(&output, limits)
                .expect("scan official GraphAr Arrow chunks");
            let (imported, timings) = read_graphar_dataset_observed(&output, &projection, limits)
                .expect("read GraphAr through the Rust Arrow bridge");
            (edge_count, official_elapsed, imported, timings)
        } else {
            let (imported, timings) = read_graphar_dataset_observed(&output, &projection, limits)
                .expect("read GraphAr through the Rust Arrow bridge");
            let (edge_count, official_elapsed) = scan_graphar_edge_chunks(&output, limits)
                .expect("scan official GraphAr Arrow chunks");
            (edge_count, official_elapsed, imported, timings)
        };
        assert_eq!(imported.facts(), expected_facts);
        let rust_graphar_edge_read = timings.edge_storage_read() + timings.arrow_c_stream_import();
        AspRustScenarioObservation::default()
            .with_timing("official_arrow_edge_scan", official_elapsed)
            .with_timing("rust_graphar_edge_read", rust_graphar_edge_read)
            .with_timing("graphar_storage_read", timings.edge_storage_read())
            .with_timing("arrow_c_stream_import", timings.arrow_c_stream_import())
            .with_metric("edge_count", edge_count as u64)
    })
    .expect("measure official GraphAr/Rust Arrow bridge parity Scenario");
    let measurement = measure_asp_rust_scenario(&scenario, || {
        let semantic_read_started = Instant::now();
        let (imported, timings) = read_graphar_dataset_observed(&output, &projection, limits)
            .expect("read semantic GraphAr dataset");
        let semantic_read_elapsed = semantic_read_started.elapsed();
        assert_eq!(imported.vertex_count(), EDGE_COUNT);
        assert_eq!(imported.facts(), expected_facts);
        AspRustScenarioObservation::default()
            .with_timing("graph_info", timings.graph_info())
            .with_timing("native_vertex_read", timings.native_vertex_read())
            .with_timing("vertex_admission", timings.vertex_admission())
            .with_timing("edge_storage_read", timings.edge_storage_read())
            .with_timing("arrow_c_stream_import", timings.arrow_c_stream_import())
            .with_timing("fact_preparation", timings.fact_preparation())
            .with_timing("fact_admission", timings.fact_admission())
            .with_timing("semantic_read_admission", semantic_read_elapsed)
            .with_metric("vertex_count", EDGE_COUNT as u64)
            .with_metric("fact_count", imported.facts().len() as u64)
    })
    .expect("measure GraphAr semantic read Scenario");
    let prepared_measurement = measure_asp_rust_scenario(&prepared_scenario, || {
        let (imported, fact_admission) = prepared
            .admit_observed(&projection)
            .expect("admit prepared GraphAr semantic facts");
        assert_eq!(imported.vertex_count(), EDGE_COUNT);
        assert_eq!(imported.facts(), expected_facts);
        AspRustScenarioObservation::default()
            .with_timing("prepared_fact_admission", fact_admission)
            .with_metric("fact_count", imported.facts().len() as u64)
    })
    .expect("measure prepared GraphAr admission Scenario");

    let rendered = render_asp_rust_scenario_benchmark_toml(&scenario, &measurement)
        .expect("render measured GraphAr Scenario benchmark");
    let parity_rendered =
        render_asp_rust_scenario_benchmark_toml(&parity_scenario, &parity_measurement)
            .expect("render measured GraphAr Arrow bridge parity benchmark");
    let prepared_rendered =
        render_asp_rust_scenario_benchmark_toml(&prepared_scenario, &prepared_measurement)
            .expect("render measured prepared GraphAr admission benchmark");
    eprintln!(
        "mrr-data-graphar-scenario edges={EDGE_COUNT} write_us={}\n{parity_rendered}\n{rendered}\n{prepared_rendered}",
        write_elapsed.as_micros(),
    );
    assert_graphar_performance_budgets(&parity_measurement, &measurement, &prepared_measurement);
}

fn assert_graphar_performance_budgets(
    parity: &AspRustScenarioMeasurement,
    semantic_read: &AspRustScenarioMeasurement,
    prepared_admission: &AspRustScenarioMeasurement,
) {
    let official_p95 = parity.observed_timings["official_arrow_edge_scan"];
    let c_stream_import_p95 = parity.observed_timings["arrow_c_stream_import"];
    let import_budget = (official_p95 / 10).min(Duration::from_millis(1));
    assert!(
        c_stream_import_p95 <= import_budget,
        "zero-copy Arrow C Stream import of 10,000 edges exceeded its min(1ms, 10% of official read) budget: official={official_p95:?} import={c_stream_import_p95:?} budget={import_budget:?}"
    );
    assert!(
        semantic_read.total_p50 <= Duration::from_millis(300),
        "10,000-edge semantic GraphAr read P50 exceeded 300ms: {:?}",
        semantic_read.total_p50
    );
    assert!(
        semantic_read.total_p95 <= Duration::from_millis(500),
        "10,000-edge semantic GraphAr read P95 exceeded 500ms: {:?}",
        semantic_read.total_p95
    );
    assert!(
        prepared_admission.total_p50 <= Duration::from_millis(25),
        "10,000-fact prepared admission P50 exceeded 25ms: {:?}",
        prepared_admission.total_p50
    );
    assert!(
        prepared_admission.total_p95 <= Duration::from_millis(50),
        "10,000-fact prepared admission P95 exceeded 50ms: {:?}",
        prepared_admission.total_p95
    );
    assert!(
        prepared_admission.observed_timings["prepared_fact_admission"] <= Duration::from_millis(20),
        "10,000-fact prepared semantic admission exceeded 20ms: {:?}",
        prepared_admission.observed_timings["prepared_fact_admission"]
    );
}

fn graphar_scenario_facts(projection: &BinaryEntityProjection, edge_count: usize) -> Vec<Fact> {
    let owner = id::<EntityId>("graphar-scenario-owner");
    let generation = id::<GenerationId>("graphar-scenario-generation");
    (0..edge_count)
        .map(|index| {
            Fact::new(
                id(&format!("graphar-scenario-fact-{index}")),
                projection.relation_id(),
                vec![
                    Value::Entity(id(&format!("graphar-scenario-entity-{index}"))),
                    Value::Entity(id(&format!(
                        "graphar-scenario-entity-{}",
                        (index + 1) % edge_count
                    ))),
                ],
                RelationContext::new(
                    generation,
                    RelationAuthority::Entity(owner),
                    FactProvenance::Source(owner),
                    EvidenceCompleteness::Complete,
                    FactValidity::Valid,
                )
                .unwrap(),
            )
        })
        .collect()
}

fn graphar_semantic_read_scenario() -> AspRustScenario {
    asp_rust_scenario! {
        name: "graphar-semantic-read-10k",
        package: "mrr-data-graphar",
        description: "GraphAr Arrow chunks reconstruct and re-admit 10,000 canonical MRR facts",
        fixture_root: "tests/unit/scenarios/graphar_semantic_read_10k",
        tags: ["graphar", "arrow", "semantic-admission", "performance"],
        commands: [
            { label: "focused", argv: ["cargo", "test", "-p", "mrr-data-graphar", "--features", "native-graphar", "scenario_semantically_reads_ten_thousand_graphar_edges", "--", "--ignored", "--nocapture"] }
        ],
        benchmark: {
            harness: "libtest",
            test: "scenario_semantically_reads_ten_thousand_graphar_edges",
            snapshot: "graphar_semantic_read_10k",
            target_total: "300ms",
            max_total: "500ms",
            regression_budget: "100ms",
            memory_budget_bytes: 268_435_456,
            target_rationale: "The native path reads GraphAr Parquet through upstream Arrow chunk readers before typed MRR identity parsing and admission.",
            warmup_iterations: 2,
            measure_iterations: 21,
            metrics: [
                { name: "vertex_count", unit: "count", kind: Exact, target: 10_000 },
                { name: "fact_count", unit: "count", kind: Exact, target: 10_000 }
            ]
        }
    }
}

fn graphar_arrow_bridge_parity_scenario() -> AspRustScenario {
    asp_rust_scenario! {
        name: "graphar-arrow-bridge-parity-10k",
        package: "mrr-data-graphar",
        description: "The zero-copy Arrow C Stream import remains below one millisecond and ten percent of the official GraphAr read",
        fixture_root: "tests/unit/scenarios/graphar_arrow_bridge_parity_10k",
        tags: ["graphar", "arrow", "rust-bridge", "performance"],
        commands: [
            { label: "focused", argv: ["cargo", "test", "-p", "mrr-data-graphar", "--features", "native-graphar", "scenario_semantically_reads_ten_thousand_graphar_edges", "--", "--ignored", "--nocapture"] }
        ],
        benchmark: {
            harness: "libtest",
            test: "scenario_semantically_reads_ten_thousand_graphar_edges",
            snapshot: "graphar_arrow_bridge_parity_10k",
            target_total: "300ms",
            max_total: "500ms",
            regression_budget: "100ms",
            memory_budget_bytes: 268_435_456,
            target_rationale: "The reference phase alternates execution order on the same 10,000-edge fixture and separates GraphAr Parquet/chunk loading from zero-copy Arrow C Stream import into Rust.",
            warmup_iterations: 2,
            measure_iterations: 21,
            metrics: [
                { name: "edge_count", unit: "count", kind: Exact, target: 10_000 }
            ]
        }
    }
}

fn graphar_prepared_admission_scenario() -> AspRustScenario {
    asp_rust_scenario! {
        name: "graphar-prepared-admission-10k",
        package: "mrr-data-graphar",
        description: "Prepared immutable semantic facts admit 10,000 canonical MRR facts without re-entering GraphAr storage",
        fixture_root: "tests/unit/scenarios/graphar_prepared_admission_10k",
        tags: ["graphar", "arrow", "prepared-source", "semantic-admission", "performance"],
        commands: [
            { label: "focused", argv: ["cargo", "test", "-p", "mrr-data-graphar", "--features", "native-graphar", "scenario_semantically_reads_ten_thousand_graphar_edges", "--", "--ignored", "--nocapture"] }
        ],
        benchmark: {
            harness: "libtest",
            test: "scenario_semantically_reads_ten_thousand_graphar_edges",
            snapshot: "graphar_prepared_admission_10k",
            target_total: "25ms",
            max_total: "50ms",
            regression_budget: "10ms",
            memory_budget_bytes: 268_435_456,
            target_rationale: "GraphAr storage decode, Arrow C Stream import, and projection-independent identity parsing finish before measurement; repeated consumption reuses immutable typed MRR facts and measures only projection-owned re-admission.",
            warmup_iterations: 2,
            measure_iterations: 21,
            metrics: [
                { name: "fact_count", unit: "count", kind: Exact, target: 10_000 }
            ]
        }
    }
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
