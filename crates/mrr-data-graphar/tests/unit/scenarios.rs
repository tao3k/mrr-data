use asp_rust::{RustScenarioBenchmarkStatus, validate_rust_scenario_benchmark};

#[test]
fn graphar_performance_scenario_contracts_are_admitted_by_asp_rust() {
    for scenario in [
        "graphar_arrow_bridge_parity_10k",
        "graphar_semantic_read_10k",
    ] {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/unit/scenarios")
            .join(scenario);
        let receipt = validate_rust_scenario_benchmark(root)
            .unwrap_or_else(|error| panic!("validate GraphAr Scenario {scenario}: {error}"));
        assert_eq!(
            receipt.status,
            RustScenarioBenchmarkStatus::Pass,
            "GraphAr Scenario {scenario} was not admitted: {receipt:#?}"
        );
    }
}
