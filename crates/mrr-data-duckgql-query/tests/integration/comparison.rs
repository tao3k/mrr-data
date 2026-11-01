//! Whole-request comparison on one capture; no standalone native query claim.
use super::{artifact, entity, limits, query_fixture};
use meta_relational_reasoning as mrr;
use mrr_data_duckgql_query::{
    DuckGqlLimits, duckgql_graphar_engine_profile, execute_duckgql_graphar_single_hop,
};
use std::{num::NonZeroUsize, time::Instant};

pub(super) fn admit(
    query: &mrr_data_core::BoundDataQuery,
    output: mrr_data_core::PhysicalQueryOutput,
    rows: usize,
) {
    let candidate =
        mrr_data_core::project_data_query_output(query, query.engine(), output).unwrap();
    mrr::admit_query_result_candidate(
        query.query(),
        &candidate,
        mrr::QueryResultLimits::new(
            NonZeroUsize::new(rows).unwrap(),
            NonZeroUsize::new(rows * 2).unwrap(),
        ),
    )
    .unwrap();
}
pub(super) fn comparison_facts(rows: usize) -> (Vec<mrr::Fact>, Vec<Vec<mrr::QueryResultValue>>) {
    let mut facts: Vec<_> = (0..rows)
        .map(|index| {
            query_fixture::fact_with_endpoints(
                &format!("edge-{index:08}"),
                if index % 2 == 0 { "alice" } else { "zoe" },
                if index % 2 == 0 { "bob" } else { "yan" },
            )
        })
        .collect();
    facts.sort_by_key(mrr::Fact::id);
    let expected: Vec<_> = facts
        .iter()
        .map(|fact| {
            fact.values()
                .iter()
                .map(|value| {
                    let mrr::Value::Entity(id) = value else {
                        panic!("fixture entity")
                    };
                    mrr::QueryResultValue::node(*id, entity("node"))
                })
                .collect::<Vec<_>>()
        })
        .collect();
    (facts, expected)
}
pub(super) async fn comparison_database(directory: &std::path::Path) -> turso::Database {
    turso::Builder::new_local(directory.join("comparison.db").to_str().unwrap())
        .build()
        .await
        .unwrap()
}
#[tokio::test]
#[ignore = "bounded native whole-request comparison; run explicitly"]
async fn same_source_scale_receipts() {
    let start = Instant::now();
    let artifact = artifact();
    let artifact_prepare_ms = start.elapsed().as_secs_f64() * 1000.0;
    for rows in [2, 128, 1024] {
        println!("comparison: preparing authenticated {rows}-row source");
        let (facts, expected) = comparison_facts(rows);
        let start = Instant::now();
        let engine = duckgql_graphar_engine_profile().unwrap();
        let (query, projection, source, inventory) = query_fixture::captured_facts(&facts, &engine);
        let fixture_ms = start.elapsed().as_secs_f64() * 1000.0;
        let turso_engine = mrr_data_turso_query::turso_graphar_engine_profile().unwrap();
        let turso_query = query_fixture::bound_query_shape_with_rows(
            &inventory,
            "generation",
            "target",
            None,
            &turso_engine,
            rows,
        )
        .unwrap();
        let datafusion_engine = mrr_data_datafusion::datafusion_engine_profile().unwrap();
        let datafusion_query = query_fixture::bound_query_shape_with_rows(
            &inventory,
            "generation",
            "target",
            None,
            &datafusion_engine,
            rows,
        )
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let database = comparison_database(directory.path()).await;
        let duckgql_limits = DuckGqlLimits {
            max_input_rows: rows,
            max_input_bytes: rows * 512,
            max_output_rows: rows,
            max_output_cells: rows * 2,
            ..limits()
        };
        let mut duckgql_ms = Vec::new();
        let mut turso_ms = Vec::new();
        let mut datafusion_ms = Vec::new();
        for iteration in 0..3 {
            println!("comparison: {rows} rows, iteration {iteration}, DuckGQL");
            let start = Instant::now();
            let output = execute_duckgql_graphar_single_hop(
                &artifact,
                &query,
                &source,
                &projection,
                &duckgql_limits,
            )
            .unwrap();
            duckgql_ms.push(start.elapsed().as_secs_f64() * 1000.0);
            assert_eq!(output.rows(), expected);
            admit(&query, output.clone(), rows);
            println!("comparison: {rows} rows, iteration {iteration}, Turso");
            let start = Instant::now();
            let turso_output = mrr_data_turso_query::execute_turso_graphar_single_hop(
                &database,
                &turso_query,
                &source,
                &projection,
                mrr_data_turso_query::SqlQueryLimits {
                    max_input_rows: rows,
                    max_input_bytes: rows * 512,
                    max_output_rows: rows,
                    max_output_cells: rows * 2,
                },
            )
            .await
            .unwrap();
            turso_ms.push(start.elapsed().as_secs_f64() * 1000.0);
            assert_eq!(turso_output, output);
            admit(&turso_query, turso_output, rows);
            println!("comparison: {rows} rows, iteration {iteration}, DataFusion");
            let start = Instant::now();
            let batch = mrr_data_arrow::facts_to_record_batch(
                &query_fixture::relation(),
                source.facts(&datafusion_query).unwrap(),
            )
            .unwrap();
            let arrow_output = mrr_data_datafusion::execute_binary_entity_query(
                datafusion_query.query(),
                &query_fixture::relation(),
                batch,
            )
            .await
            .unwrap();
            datafusion_ms.push(start.elapsed().as_secs_f64() * 1000.0);
            assert_eq!(arrow_output, output);
            admit(&datafusion_query, arrow_output, rows);
        }
        println!(
            "QUERY-COMPARISON {}",
            serde_json::json!({"rows":rows,"snapshot_root":query.snapshot_root().to_string(),"inventory_root":source.binding().inventory_root().to_string(),"cold_fixture_ms":fixture_ms,"shared_artifact_prepare_ms":artifact_prepare_ms,"artifact_bytes":artifact.byte_len(),"duckgql_whole_request_ms":duckgql_ms,"turso_whole_request_ms":turso_ms,"datafusion_whole_request_ms":datafusion_ms,"parity":true,"all_results_admitted":true,"os":std::env::consts::OS,"arch":std::env::consts::ARCH})
        );
    }
}
