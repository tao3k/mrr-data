#![cfg(feature = "duckgql-graphar")]
#[cfg(feature = "backend-worker")]
mod backend_lifecycle;
mod comparison;
#[path = "../../../mrr-data-graphar/tests/support/query_fixture.rs"]
mod query_fixture;
use meta_relational_reasoning as mrr;
use mrr_data_core as core;
use mrr_data_duckgql_query::{
    DuckGqlArtifact, DuckGqlError, DuckGqlLimits, DuckGqlSingleHopProgram,
    duckgql_graphar_engine_profile, execute_duckgql_graphar_single_hop,
};
use query_fixture::{RefusedShape, entity, fact_with_endpoints};
use std::num::NonZeroUsize;

fn artifact() -> DuckGqlArtifact {
    // Native tests require a Host-installed exact artifact and fail if missing.
    // No test downloads an extension or silently treats absence as a pass.
    let path = std::env::var_os("MRR_DUCKGQL_EXTENSION")
        .expect("set MRR_DUCKGQL_EXTENSION to the qualified SDK artifact");
    let digest = std::env::var("MRR_DUCKGQL_SHA256")
        .expect("set MRR_DUCKGQL_SHA256 to its authenticated digest");
    let bytes: Vec<_> = digest
        .as_bytes()
        .chunks_exact(2)
        .map(|part| u8::from_str_radix(std::str::from_utf8(part).unwrap(), 16).unwrap())
        .collect();
    DuckGqlArtifact::capture(
        std::path::PathBuf::from(path),
        bytes.try_into().unwrap(),
        128 * 1024 * 1024,
        std::env::var("MRR_DUCKGQL_ALLOW_UNSIGNED").as_deref() == Ok("1"),
    )
    .unwrap()
}
fn limits() -> DuckGqlLimits {
    DuckGqlLimits {
        max_input_rows: 2,
        max_input_bytes: 4096,
        max_output_rows: 2,
        max_output_cells: 4,
        native_memory_limit: "128MB".into(),
        native_threads: 1,
    }
}
#[tokio::test]
async fn native_mrr_turso_duckgql_parity_and_final_admission() {
    let engine = duckgql_graphar_engine_profile().unwrap();
    let mut facts = [
        fact_with_endpoints("edge-z", "alice", "bob"),
        fact_with_endpoints("edge-a", "zoe", "yan"),
    ];
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
    facts.reverse();
    let (query, projection, source, inventory) = query_fixture::captured_facts(&facts, &engine);
    let output =
        execute_duckgql_graphar_single_hop(&artifact(), &query, &source, &projection, &limits())
            .unwrap();
    println!("native typed query materialized expected rows");
    assert_eq!(output.rows(), expected);
    let candidate = core::project_data_query_output(&query, &engine, output.clone()).unwrap();
    mrr::admit_query_result_candidate(
        query.query(),
        &candidate,
        mrr::QueryResultLimits::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(4).unwrap()),
    )
    .unwrap();
    let turso_engine = mrr_data_turso_query::turso_graphar_engine_profile().unwrap();
    let turso_query = query_fixture::bound_query(&inventory, "generation", &turso_engine);
    let directory = tempfile::tempdir().unwrap();
    let database = turso::Builder::new_local(directory.path().join("parity.db").to_str().unwrap())
        .build()
        .await
        .unwrap();
    let turso_output = mrr_data_turso_query::execute_turso_graphar_single_hop(
        &database,
        &turso_query,
        &source,
        &projection,
        mrr_data_turso_query::SqlQueryLimits {
            max_input_rows: 2,
            max_input_bytes: 4096,
            max_output_rows: 2,
            max_output_cells: 4,
        },
    )
    .await
    .unwrap();
    assert_eq!(turso_output, output);
}
#[test]
fn native_duplicate_edges_and_all_limits_refuse_partial_output() {
    let engine = duckgql_graphar_engine_profile().unwrap();
    let (query, projection, source, inventory) = query_fixture::captured(&engine);
    let artifact = artifact();
    let output =
        execute_duckgql_graphar_single_hop(&artifact, &query, &source, &projection, &limits())
            .unwrap();
    println!("native duplicate edges materialized");
    assert_eq!(output.rows().len(), 2);
    assert_eq!(output.rows()[0], output.rows()[1]);
    for (name, limited) in [
        (
            "input rows",
            DuckGqlLimits {
                max_input_rows: 1,
                ..limits()
            },
        ),
        (
            "input bytes",
            DuckGqlLimits {
                max_input_bytes: 1,
                ..limits()
            },
        ),
        (
            "output rows",
            DuckGqlLimits {
                max_output_rows: 1,
                ..limits()
            },
        ),
        (
            "output cells",
            DuckGqlLimits {
                max_output_cells: 3,
                ..limits()
            },
        ),
    ] {
        assert!(
            matches!(execute_duckgql_graphar_single_hop(&artifact, &query, &source, &projection, &limited), Err(DuckGqlError::Limit(reason)) if reason == name)
        );
        println!("native budget refused: {name}");
    }
    let drifted = query_fixture::bound_query(&inventory, "other-generation", &engine);
    assert!(matches!(
        execute_duckgql_graphar_single_hop(&artifact, &drifted, &source, &projection, &limits()),
        Err(DuckGqlError::SourceMismatch)
    ));
    let mut invalid_digest = artifact.sha256();
    invalid_digest[0] ^= 1;
    assert!(matches!(
        DuckGqlArtifact::capture(
            std::path::PathBuf::from(std::env::var_os("MRR_DUCKGQL_EXTENSION").unwrap()),
            invalid_digest,
            128 * 1024 * 1024,
            true,
        ),
        Err(DuckGqlError::Artifact)
    ));
    let repeated =
        query_fixture::bound_query_with_target(&inventory, "generation", "source", &engine);
    assert!(DuckGqlSingleHopProgram::compile(&repeated, &projection).is_err());
    for shape in [
        RefusedShape::Filter,
        RefusedShape::Distinct,
        RefusedShape::Paging,
        RefusedShape::Incoming,
        RefusedShape::EdgeBinding,
    ] {
        let query = query_fixture::bound_query_shape(
            &inventory,
            "generation",
            "target",
            Some(shape),
            &engine,
        )
        .unwrap();
        assert!(matches!(
            DuckGqlSingleHopProgram::compile(&query, &projection),
            Err(DuckGqlError::Shape(_))
        ));
    }
    assert!(
        query_fixture::bound_query_shape(
            &inventory,
            "generation",
            "target",
            Some(RefusedShape::VariableLength),
            &engine
        )
        .is_err()
    );
}
