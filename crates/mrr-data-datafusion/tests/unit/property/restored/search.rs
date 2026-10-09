use super::{EntityChildMode, fixture, limits, mrr, restored};
use crate::{
    DataSearchExecutionError, RestoredPropertyBackend, execute_restored_property_search_stage,
};
use mrr_data_core::{DataSearchBindingError, DataSearchStageBinding, bind_data_query};
use std::num::NonZeroUsize;

#[tokio::test]
async fn actual_restored_property_rows_become_source_bound_search_acquisition() {
    let f = fixture();
    let (cold, _, relations, entities) = restored(&f, EntityChildMode::Valid).await;
    println!("Search immutable Arrow property source restored");
    let profile = crate::datafusion_engine_profile().unwrap();
    let binding = DataSearchStageBinding::new(
        bind_data_query(&f.query, cold.snapshot(), &profile).unwrap(),
        mrr::SearchFactor::from_canonical_input(
            "mrr.search.factor.v1:property:acquisition",
            mrr::SearchFactorRole::Acquisition,
        )
        .unwrap(),
    )
    .unwrap();
    let backend = RestoredPropertyBackend {
        restored: &cold,
        relation_catalog: &relations,
        entity_catalog: &entities,
        limits: limits(),
    };
    let result_limits = mrr::QueryResultLimits::new(
        NonZeroUsize::new(100).unwrap(),
        NonZeroUsize::new(300).unwrap(),
    );
    let cap = NonZeroUsize::new(1_048_576).unwrap();
    let receipt = execute_restored_property_search_stage(
        &binding,
        f.query.generation(),
        &backend,
        0,
        result_limits,
        cap,
    )
    .await
    .unwrap();
    let transport = receipt
        .handoff()
        .verify(binding.query(), result_limits, cap)
        .unwrap();
    assert_eq!(transport.candidate().rows().len(), 3);
    assert_eq!(
        receipt.observations().len(),
        transport.candidate().rows().len()
    );
    assert!(
        receipt
            .observations()
            .iter()
            .all(|row| row.generation() == f.query.generation()
                && row.factor() == binding.factor().id()
                && row.causal_parents().is_empty())
    );
    let again = execute_restored_property_search_stage(
        &binding,
        f.query.generation(),
        &backend,
        0,
        result_limits,
        cap,
    )
    .await
    .unwrap();
    assert_eq!(receipt.observations(), again.observations());
    println!(
        "DATA-PROPERTY-SEARCH-OK original_rows={} observations={}",
        transport.candidate().rows().len(),
        receipt.observations().len()
    );
    let stale = mrr::GenerationId::from_canonical_bytes("stale-runtime").unwrap();
    assert!(matches!(
        execute_restored_property_search_stage(&binding, stale, &backend, 0, result_limits, cap)
            .await,
        Err(DataSearchExecutionError::Binding(
            DataSearchBindingError::GenerationMismatch { .. }
        ))
    ));
    let foreign =
        mrr_data_core::DataEngineProfile::new("foreign-source-engine", false, []).unwrap();
    let substituted = DataSearchStageBinding::new(
        bind_data_query(&f.query, cold.snapshot(), &foreign).unwrap(),
        binding.factor(),
    )
    .unwrap();
    assert!(matches!(
        execute_restored_property_search_stage(
            &substituted,
            f.query.generation(),
            &backend,
            0,
            result_limits,
            cap
        )
        .await,
        Err(DataSearchExecutionError::Binding(
            DataSearchBindingError::PhysicalBindingMismatch
        ))
    ));
}
