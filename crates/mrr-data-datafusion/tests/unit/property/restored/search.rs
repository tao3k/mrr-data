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

#[cfg(feature = "search-dispatch")]
#[tokio::test]
async fn original_poo_root_dispatches_real_query_and_rejects_retired_results() {
    let f = fixture();
    let (cold, _, relations, entities) = restored(&f, EntityChildMode::Valid).await;
    let backend = RestoredPropertyBackend {
        restored: &cold,
        relation_catalog: &relations,
        entity_catalog: &entities,
        limits: limits(),
    };
    let plan = mrr::PooSearchPlan::Stage {
        name: "property".into(),
        role: mrr::PooSearchRole::Acquisition,
        input_domain: "workspace".into(),
        output_domain: "rows".into(),
    };
    let projection =
        mrr::compile_poo_search_plan("data-dispatch", f.query.generation(), &plan).unwrap();
    let factor = projection.factor_by_name("property").unwrap();
    let binding = DataSearchStageBinding::new(
        bind_data_query(
            &f.query,
            cold.snapshot(),
            &crate::datafusion_engine_profile().unwrap(),
        )
        .unwrap(),
        factor,
    )
    .unwrap();
    let budget = mrr::SearchDispatchResources {
        memory_bytes: 4096,
        input_bytes: 1024,
        output_bytes: 1_048_576,
        results: 3,
    };
    let dispatch =
        mrr::SearchDispatch::new(projection.clone(), NonZeroUsize::new(1).unwrap(), budget);
    let rows = mrr::QueryResultLimits::new(
        NonZeroUsize::new(3).unwrap(),
        NonZeroUsize::new(300).unwrap(),
    );
    let request = || crate::DataSearchDispatchRequest {
        binding: &binding,
        logical_position: 0,
        result_limits: rows,
        reservation: budget,
    };
    let result = crate::dispatch_restored_property_search_stage(&dispatch, request(), &backend)
        .await
        .unwrap();
    assert_eq!(result.stage.observations().len(), 3);
    assert_eq!(result.dispatch.factor, factor);
    assert_eq!(result.dispatch.generation, f.query.generation());
    assert_eq!(
        result.dispatch.output_bytes,
        result.stage.handoff().result_bytes().len()
    );
    assert_eq!(dispatch.snapshot().unwrap().in_flight, 0);
    // Retirement is checked before backend execution; admitted output does not change.
    dispatch.retire().unwrap();
    let before = dispatch.snapshot().unwrap();
    assert!(matches!(
        crate::dispatch_restored_property_search_stage(&dispatch, request(), &backend).await,
        Err(crate::DataSearchDispatchError::Dispatch(
            mrr::SearchDispatchError::Retired
        ))
    ));
    assert_eq!(dispatch.snapshot().unwrap(), before);
    // The actual Data execution can finish after retirement, but loses admission.
    let late = mrr::SearchDispatch::new(projection, NonZeroUsize::new(1).unwrap(), budget);
    let lease = late.reserve(f.query.generation(), factor, budget).unwrap();
    let actual = execute_restored_property_search_stage(
        &binding,
        f.query.generation(),
        &backend,
        1,
        rows,
        NonZeroUsize::new(budget.output_bytes).unwrap(),
    )
    .await
    .unwrap();
    late.retire().unwrap();
    assert_eq!(
        lease.admit(
            actual.handoff().result_bytes().len(),
            actual.observations().len()
        ),
        Err(mrr::SearchDispatchError::Retired)
    );
    assert_eq!(late.snapshot().unwrap().consumed.results, 0);
    assert_eq!(late.snapshot().unwrap().in_flight, 0);
    println!("DATA-DISPATCH-OK original-rows=3 late-result=rejected retired-before-query=rejected");
}
