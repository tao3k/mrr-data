//! Native children own query admission; the Rust parent verifies paired receipts.
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    process::{Command, Stdio},
};
const CASE_TEST: &str = "tests::entity_properties::combined::source_handoff::backend::selective::resources::original_source_resource_case";
const FIXTURE_TEST: &str = "tests::entity_properties::combined::source_handoff::backend::selective::resources::fixture::original_source_resource_fixture";

fn verify(receipt: &Value, shape: &str, mode: &str, snapshot: Option<&str>) -> bool {
    let samples = receipt["samples"].as_array();
    receipt["scale"] == crate::tests::entity_properties::fixture::workload_scale()
        && receipt["shape"] == shape
        && receipt["mode"] == mode
        && receipt["source_digest"] == super::super::super::super::SOURCE_DIGEST
        && receipt["snapshot_root"].as_str().is_some_and(|root| {
            root.parse::<cid::Cid>().is_ok() && snapshot.is_none_or(|expected| expected == root)
        })
        && receipt["all_results_admitted"] == true
        && receipt["expected_rows"] == 4
        && receipt["cleanup_bytes"] == 0
        && samples.is_some_and(|samples| {
            samples.len()
                == usize::try_from(
                    super::schema()["properties"]["samples"]["const"]
                        .as_u64()
                        .unwrap(),
                )
                .unwrap()
                && samples.iter().all(|sample| {
                    [
                        "total_ns",
                        "cpu_ns",
                        "physical_backend_ns",
                        "engine_first_nonempty_batch_ns",
                        "output_batches",
                        "process_peak_rss_bytes",
                        "relation_materialized_rows",
                        "relation_selected_edges",
                        "relation_read_bytes",
                    ]
                    .iter()
                    .all(|field| sample[field].as_u64().is_some_and(|value| value > 0))
                        && sample["decoded_utf8_copy_bytes"] == 70
                        && sample["observed_spill_bytes"].is_null()
                            == (sample["spill_reporting_operators"] == 0)
                        && sample["remote_read_bytes"].as_u64().is_some_and(|bytes| {
                            if mode == "cold-full" {
                                bytes > 0
                            } else {
                                bytes == 0
                            }
                        })
                })
        })
}
fn child_output(test: &str, shape: &str, mode: &str, fixture: &std::path::Path) -> Vec<String> {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            test,
            "--exact",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("MRR_DATA_SOURCE_SHAPE", shape)
        .env("MRR_DATA_SOURCE_MODE", mode)
        .env("MRR_DATA_SOURCE_FIXTURE", fixture)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut output = Vec::new();
    for line in BufReader::new(child.stdout.take().unwrap()).lines() {
        let line = line.unwrap();
        println!("{line}");
        output.push(line);
    }
    assert!(
        child.wait().unwrap().success(),
        "native original-source child refused"
    );
    output
}
fn execute(shape: &str, mode: &str, fixture: &std::path::Path) -> Value {
    let receipts = child_output(CASE_TEST, shape, mode, fixture)
        .into_iter()
        .filter_map(|line| {
            line.strip_prefix("SOURCE-RESOURCE ")
                .map(|receipt| serde_json::from_str::<Value>(receipt).unwrap())
        })
        .collect::<Vec<_>>();
    assert_eq!(receipts.len(), 1, "one checked native receipt required");
    receipts.into_iter().next().unwrap()
}

#[test]
#[ignore = "native original-source process matrix; use the glue progress supervisor"]
fn original_source_resource_matrix() {
    let schema = super::schema();
    let contract = &schema["properties"];
    let mut receipts = Vec::new();
    for shape in contract["shapes"]["const"].as_array().unwrap() {
        let shape = shape.as_str().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("fixture.json");
        // Keep the parent free of native runtime initialization and its process
        // signal ownership; it must retain every child's actual wait status.
        child_output(FIXTURE_TEST, shape, "fixture", &fixture);
        assert!(fixture.is_file(), "checked immutable fixture required");
        let mut snapshot = None;
        let mut full_rows = None;
        for mode in contract["modes"]["const"].as_array().unwrap() {
            let mode = mode.as_str().unwrap();
            let receipt = execute(shape, mode, &fixture);
            assert!(
                verify(&receipt, shape, mode, snapshot.as_deref()),
                "source/admission/resource receipt refused"
            );
            snapshot = Some(receipt["snapshot_root"].as_str().unwrap().to_owned());
            let rows = receipt["samples"][0]["relation_selected_edges"]
                .as_u64()
                .unwrap();
            if mode.ends_with("full") {
                assert!(full_rows.is_none_or(|expected| expected == rows));
                full_rows = Some(rows);
            } else {
                assert!(rows < full_rows.unwrap());
            }
            receipts.push(receipt);
        }
    }
    println!(
        "SOURCE-RESOURCE-MATRIX {}",
        json!({"schema_namespace":contract["schema_namespace"]["const"],"schema_version":contract["schema_version"]["const"],"cases":receipts})
    );
}

#[test]
fn source_resource_matrix_refuses_unadmitted_and_foreign_source_receipts() {
    let snapshot = mrr_data_core::raw_cid(b"resource-source").to_string();
    let sample = json!({"total_ns":1,"cpu_ns":1,"physical_backend_ns":1,"engine_first_nonempty_batch_ns":1,"output_batches":1,"decoded_utf8_copy_bytes":70,"observed_spill_bytes":null,"spill_reporting_operators":0,"process_peak_rss_bytes":1,"relation_materialized_rows":1,"relation_selected_edges":1,"relation_read_bytes":1,"remote_read_bytes":0});
    let valid = json!({"scale":crate::tests::entity_properties::fixture::workload_scale(),"shape":"uniform","mode":"warm-full","source_digest":super::super::super::super::SOURCE_DIGEST,"snapshot_root":snapshot,"all_results_admitted":true,"expected_rows":4,"cleanup_bytes":0,"samples":[sample.clone(),sample.clone(),sample]});
    assert!(verify(&valid, "uniform", "warm-full", Some(&snapshot)));
    assert!(!verify(&valid, "uniform", "warm-full", Some("foreign")));
    assert!(!verify(&valid, "skewed", "warm-full", Some(&snapshot)));
    let mut invalid = valid.clone();
    invalid["all_results_admitted"] = json!(false);
    assert!(!verify(&invalid, "uniform", "warm-full", Some(&snapshot)));
    invalid = valid.clone();
    invalid["cleanup_bytes"] = json!(1);
    assert!(!verify(&invalid, "uniform", "warm-full", Some(&snapshot)));
    invalid = valid;
    invalid["samples"][0]["remote_read_bytes"] = json!(1);
    assert!(!verify(&invalid, "uniform", "warm-full", Some(&snapshot)));
}
