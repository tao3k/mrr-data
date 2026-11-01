//! One process per engine/size, with real OS peak RSS and MRR admission timing.
use super::{artifact, comparison, limits, query_fixture};
use mrr_data_core::{BoundDataQuery, PhysicalQueryOutput};
use mrr_data_duckgql_query::{DuckGqlArtifact, DuckGqlLimits};
use mrr_data_graphar::{BinaryEntityProjection, CapturedGraphArSnapshot};
use nix::sys::resource::{UsageWho, getrusage};
use std::time::Instant;
mod matrix;
mod phases;

fn resource_schema() -> serde_json::Value {
    serde_json::from_str(include_str!("../../query-resources-schema.json")).unwrap()
}

fn source_directory() -> std::path::PathBuf {
    std::env::var_os("MRR_DATA_QUERY_SOURCE_DIR")
        .unwrap()
        .into()
}
fn selected_rows() -> usize {
    let rows: usize = std::env::var("MRR_DATA_QUERY_ROWS")
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        resource_schema()["properties"]["row_counts"]["const"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value.as_u64() == Some(u64::try_from(rows).unwrap()))
    );
    rows
}

fn publish_source_fixture(source: &std::path::Path, rows: usize) -> String {
    println!("resource comparison: publishing {rows}-row source");
    let (facts, _) = comparison::comparison_facts(rows);
    let projection = BinaryEntityProjection::admit_catalog(
        &meta_relational_reasoning::RelationCatalog::admit(vec![query_fixture::relation()])
            .unwrap(),
        query_fixture::relation().id(),
    )
    .unwrap();
    let receipt = mrr_data_graphar::write_graphar_dataset(
        source,
        &projection,
        &facts
            .iter()
            .map(|fact| projection.project(fact).unwrap())
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let inventory = receipt
        .inventory()
        .canonical_bytes(mrr_data_core::GraphInventoryLimits::default())
        .unwrap();
    std::fs::write(source.with_extension("inventory"), &inventory).unwrap();
    mrr_data_core::dag_cbor_cid(&inventory).to_string()
}

fn capture_shared_source(
    rows: usize,
    profile: &mrr_data_core::DataEngineProfile,
) -> (
    BoundDataQuery,
    BinaryEntityProjection,
    CapturedGraphArSnapshot,
) {
    let source = source_directory();
    let root = std::env::var("MRR_DATA_QUERY_INVENTORY_ROOT")
        .unwrap()
        .parse()
        .unwrap();
    let bytes = std::fs::read(source.with_extension("inventory")).unwrap();
    let inventory = mrr_data_core::GraphDatasetInventory::decode_checked(
        &bytes,
        &root,
        mrr_data_core::GraphInventoryLimits::default(),
    )
    .unwrap();
    let (query, projection, capture, _) =
        query_fixture::capture_published(&source, inventory, rows, profile);
    (query, projection, capture)
}

fn peak_rss_bytes() -> u64 {
    let bytes = u64::try_from(getrusage(UsageWho::RUSAGE_SELF).unwrap().max_rss()).unwrap();
    #[cfg(target_os = "macos")]
    return bytes;
    #[cfg(target_os = "linux")]
    bytes.checked_mul(1024).unwrap()
}

struct Case {
    engine: String,
    rows: usize,
    query: BoundDataQuery,
    projection: BinaryEntityProjection,
    source: CapturedGraphArSnapshot,
    artifact: Option<DuckGqlArtifact>,
    database: Option<turso::Database>,
}
impl Case {
    async fn execute(&self) -> (PhysicalQueryOutput, serde_json::Value) {
        match self.engine.as_str() {
            "duckgql" => {
                let (output, timings) =
                    mrr_data_duckgql_query::execute_duckgql_graphar_single_hop_observed(
                        self.artifact.as_ref().unwrap(),
                        &self.query,
                        &self.source,
                        &self.projection,
                        &DuckGqlLimits {
                            max_input_rows: self.rows,
                            max_input_bytes: self.rows * 512,
                            max_output_rows: self.rows,
                            max_output_cells: self.rows * 2,
                            ..limits()
                        },
                    )
                    .unwrap();
                (output, phases::duckgql(timings))
            }
            "turso" => {
                let (output, timings) =
                    mrr_data_turso_query::execute_turso_graphar_single_hop_observed(
                        self.database.as_ref().unwrap(),
                        &self.query,
                        &self.source,
                        &self.projection,
                        mrr_data_turso_query::SqlQueryLimits {
                            max_input_rows: self.rows,
                            max_input_bytes: self.rows * 512,
                            max_output_rows: self.rows,
                            max_output_cells: self.rows * 2,
                        },
                    )
                    .await
                    .unwrap();
                (output, phases::turso(timings))
            }
            "datafusion" => {
                let batch = mrr_data_arrow::facts_to_record_batch(
                    &query_fixture::relation(),
                    self.source.facts(&self.query).unwrap(),
                )
                .unwrap();
                let (output, timings) = mrr_data_datafusion::execute_binary_entity_query_observed(
                    self.query.query(),
                    &query_fixture::relation(),
                    batch,
                )
                .await
                .unwrap();
                (output, phases::datafusion(timings))
            }
            _ => panic!("unsupported comparison engine"),
        }
    }
}

fn selected_profile(engine: &str) -> mrr_data_core::DataEngineProfile {
    match engine {
        "duckgql" => mrr_data_duckgql_query::duckgql_graphar_engine_profile().unwrap(),
        "turso" => mrr_data_turso_query::turso_graphar_engine_profile().unwrap(),
        "datafusion" => mrr_data_datafusion::datafusion_engine_profile().unwrap(),
        _ => panic!("MRR_DATA_QUERY_ENGINE must be duckgql, turso or datafusion"),
    }
}

#[tokio::test]
#[ignore = "worker of the Rust isolated-process comparison matrix"]
async fn isolated_engine_scale_receipt() {
    let engine = std::env::var("MRR_DATA_QUERY_ENGINE").unwrap();
    let rows = selected_rows();
    println!("resource comparison: {engine}, {rows} rows, capturing source");
    let profile = selected_profile(&engine);
    let start = Instant::now();
    let (_, expected) = comparison::comparison_facts(rows);
    let (query, projection, source) = capture_shared_source(rows, &profile);
    let capture_ms = start.elapsed().as_secs_f64() * 1000.0;
    let fixture_peak_rss_bytes = peak_rss_bytes();
    let directory = tempfile::tempdir().unwrap();
    let start = Instant::now();
    let artifact = (engine == "duckgql").then(artifact);
    let database = if engine == "turso" {
        Some(comparison::comparison_database(directory.path()).await)
    } else {
        None
    };
    let engine_prepare_ms = start.elapsed().as_secs_f64() * 1000.0;
    let case = Case {
        engine,
        rows,
        query,
        projection,
        source,
        artifact,
        database,
    };
    let mut request_ms = Vec::new();
    let mut admission_ms = Vec::new();
    let mut native_phases_ms = Vec::new();
    for iteration in 0..resource_schema()["properties"]["samples"]["const"]
        .as_u64()
        .unwrap()
    {
        println!(
            "resource comparison: {}, {rows} rows, request {iteration}",
            case.engine
        );
        let start = Instant::now();
        let (output, timings) = case.execute().await;
        request_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        native_phases_ms.push(timings);
        assert_eq!(output.rows(), expected);
        let start = Instant::now();
        comparison::admit(&case.query, output, rows);
        admission_ms.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    let request_peak_rss_bytes = peak_rss_bytes();
    println!(
        "QUERY-RESOURCE {}",
        serde_json::json!({
            "engine":case.engine,"rows":rows,
            "snapshot_root":case.query.snapshot_root().to_string(),
            "inventory_root":case.source.binding().inventory_root().to_string(),
            "capture_ms":capture_ms,"engine_prepare_ms":engine_prepare_ms,
            "whole_request_ms":request_ms,"projection_and_admission_ms":admission_ms,
            "native_phases_ms":native_phases_ms,
            "first_result_scope":"safe SDK cursor row or DataFusion Arrow batch; execution may already have buffered rows before the first application-visible result",
            "fixture_peak_rss_bytes":fixture_peak_rss_bytes,
            "request_peak_rss_bytes":request_peak_rss_bytes,
            "peak_growth_bytes":request_peak_rss_bytes.saturating_sub(fixture_peak_rss_bytes),
            "parity":true,"all_results_admitted":true,
            "os":std::env::consts::OS,"arch":std::env::consts::ARCH
        })
    );
}
