//! Rust owns case dispatch, immutable-source parity and result acceptance.
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    path::Path,
    process::{Command, Stdio},
};

#[derive(Debug, Eq, PartialEq)]
enum ReceiptError {
    Identity,
    Source,
    Admission,
    Resources,
}
fn verify_case(
    receipt: &Value,
    engine: &str,
    rows: u64,
    inventory: &str,
    snapshot: Option<&str>,
) -> Result<(), ReceiptError> {
    if receipt["engine"] != engine || receipt["rows"] != rows {
        return Err(ReceiptError::Identity);
    }
    if receipt["inventory_root"] != inventory
        || receipt["snapshot_root"]
            .as_str()
            .is_none_or(|root| root.is_empty() || snapshot.is_some_and(|expected| root != expected))
    {
        return Err(ReceiptError::Source);
    }
    if receipt["parity"] != true || receipt["all_results_admitted"] != true {
        return Err(ReceiptError::Admission);
    }
    let samples = super::resource_schema()["properties"]["samples"]["const"]
        .as_u64()
        .unwrap();
    if receipt["native_phases_ms"].as_array().is_none_or(|phases| {
        u64::try_from(phases.len()).ok() != Some(samples)
            || phases.iter().any(|phase| {
                phase.as_object().is_none_or(|values| {
                    values
                        .get("total")
                        .and_then(Value::as_f64)
                        .is_none_or(|total| !total.is_finite() || total <= 0.0)
                        || values.values().any(|value| {
                            value
                                .as_f64()
                                .is_none_or(|number| !number.is_finite() || number < 0.0)
                        })
                })
            })
    }) {
        return Err(ReceiptError::Resources);
    }
    Ok(())
}

fn execute_case(source: &Path, inventory: &str, engine: &str, rows: u64) -> Value {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "resources::isolated_engine_scale_receipt",
            "--exact",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("MRR_DATA_QUERY_SOURCE_DIR", source)
        .env("MRR_DATA_QUERY_INVENTORY_ROOT", inventory)
        .env("MRR_DATA_QUERY_ENGINE", engine)
        .env("MRR_DATA_QUERY_ROWS", rows.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut receipts = Vec::new();
    for line in BufReader::new(child.stdout.take().unwrap()).lines() {
        let line = line.unwrap();
        println!("{line}");
        if let Some(receipt) = line.strip_prefix("QUERY-RESOURCE ") {
            receipts.push(receipt.to_owned());
        }
    }
    assert!(
        child.wait().unwrap().success(),
        "isolated engine case failed"
    );
    assert_eq!(receipts.len(), 1, "one isolated native receipt required");
    serde_json::from_str(&receipts[0]).unwrap()
}

#[test]
#[ignore = "native resource matrix; invoke through the glue progress supervisor"]
fn isolated_engine_scale_matrix() {
    let schema = super::resource_schema();
    let properties = &schema["properties"];
    let directory = tempfile::tempdir().unwrap();
    let mut receipts = Vec::new();
    for count in properties["row_counts"]["const"].as_array().unwrap() {
        let rows = count.as_u64().unwrap();
        let source = directory.path().join(format!("graph-{rows}"));
        let inventory = super::publish_source_fixture(&source, usize::try_from(rows).unwrap());
        let mut snapshot = None;
        for name in properties["engines"]["const"].as_array().unwrap() {
            let engine = name.as_str().unwrap();
            let receipt = execute_case(&source, &inventory, engine, rows);
            verify_case(&receipt, engine, rows, &inventory, snapshot.as_deref()).unwrap();
            snapshot = Some(receipt["snapshot_root"].as_str().unwrap().to_owned());
            receipts.push(receipt);
        }
    }
    println!(
        "QUERY-RESOURCE-MATRIX {}",
        json!({
            "schema_namespace":properties["schema_namespace"]["const"],
            "schema_version":properties["schema_version"]["const"],
            "cases":receipts
        })
    );
}

#[test]
fn resource_matrix_refuses_wrong_source_identity_and_unadmitted_cases() {
    let valid = json!({"engine":"duckgql","rows":2,"inventory_root":"inventory","snapshot_root":"snapshot","parity":true,"all_results_admitted":true,"native_phases_ms":[{"total":1.0},{"total":1.0},{"total":1.0}]});
    assert_eq!(
        verify_case(&valid, "duckgql", 2, "inventory", Some("snapshot")),
        Ok(())
    );
    assert_eq!(
        verify_case(&valid, "turso", 2, "inventory", None),
        Err(ReceiptError::Identity)
    );
    assert_eq!(
        verify_case(&valid, "duckgql", 2, "other-inventory", None),
        Err(ReceiptError::Source)
    );
    assert_eq!(
        verify_case(&valid, "duckgql", 2, "inventory", Some("other-snapshot")),
        Err(ReceiptError::Source)
    );
    let mut missing_measurement = valid.clone();
    missing_measurement["native_phases_ms"] = json!(null);
    assert_eq!(
        verify_case(&missing_measurement, "duckgql", 2, "inventory", None),
        Err(ReceiptError::Resources)
    );
    let mut unadmitted = valid;
    unadmitted["all_results_admitted"] = json!(false);
    assert_eq!(
        verify_case(&unadmitted, "duckgql", 2, "inventory", None),
        Err(ReceiptError::Admission)
    );
}
