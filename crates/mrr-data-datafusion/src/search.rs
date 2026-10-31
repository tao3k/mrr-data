//! Actual restored property acquisition for consumer-owned Search stages.
use crate::{DataFusionQueryError, RestoredPropertyBackend};
use meta_relational_reasoning::{GenerationId, PropertyExecutionError, QueryResultLimits};
use mrr_data_core::{DataSearchBindingError, DataSearchStageBinding, DataSearchStageReceipt};
use std::{fmt, num::NonZeroUsize};

#[derive(Debug)]
pub enum DataSearchExecutionError {
    Binding(DataSearchBindingError),
    Execution(PropertyExecutionError<DataFusionQueryError>),
}
impl fmt::Display for DataSearchExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for DataSearchExecutionError {}

/// Execute the original query over verified restored Arrow data, admit its result
/// in MRR, and project acquisition observations bound to the physical receipt.
/// # Errors
/// Propagates generation/source drift, physical failures, admission and transport bounds.
pub async fn execute_restored_property_search_stage(
    binding: &DataSearchStageBinding,
    expected_generation: GenerationId,
    backend: &RestoredPropertyBackend<'_>,
    logical_position: u64,
    result_limits: QueryResultLimits,
    max_bytes: NonZeroUsize,
) -> Result<DataSearchStageReceipt, DataSearchExecutionError> {
    binding
        .verify_generation(expected_generation)
        .map_err(DataSearchExecutionError::Binding)?;
    let actual = mrr_data_core::bind_data_query(
        binding.query().query(),
        backend.restored.snapshot(),
        &crate::datafusion_engine_profile().map_err(|error| {
            DataSearchExecutionError::Execution(PropertyExecutionError::Backend(error))
        })?,
    )
    .map_err(|error| {
        DataSearchExecutionError::Execution(PropertyExecutionError::Backend(
            DataFusionQueryError::PhysicalBinding(error),
        ))
    })?;
    if &actual != binding.query() {
        return Err(DataSearchExecutionError::Binding(
            DataSearchBindingError::PhysicalBindingMismatch,
        ));
    }
    let execution = binding
        .query()
        .query()
        .execute_with(backend, result_limits)
        .await
        .map_err(DataSearchExecutionError::Execution)?;
    binding
        .project_execution(&execution, logical_position, result_limits, max_bytes)
        .map_err(DataSearchExecutionError::Binding)
}

/// Original physical request plus conservative reservation supplied by the consumer.
pub struct DataSearchDispatchRequest<'a> {
    pub binding: &'a DataSearchStageBinding,
    pub logical_position: u64,
    pub result_limits: QueryResultLimits,
    pub reservation: meta_relational_reasoning::SearchDispatchResources,
}
#[derive(Debug)]
pub enum DataSearchDispatchError {
    Dispatch(meta_relational_reasoning::SearchDispatchError),
    Execution(DataSearchExecutionError),
}
impl fmt::Display for DataSearchDispatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for DataSearchDispatchError {}
/// Retains the original Data source receipt and separate dispatch admission metadata.
#[derive(Debug)]
pub struct DataSearchDispatchReceipt {
    pub stage: DataSearchStageReceipt,
    pub dispatch: meta_relational_reasoning::SearchDispatchReceipt,
}
/// Execute an original POO acquisition root through MRR's bounded Rust lease.
/// Dropping this future releases reserved capacity. Retiring the shared dispatch
/// stops new work and rejects late results; it does not kill a backend thread.
/// # Errors
/// Rejects foreign plans, insufficient reservations, retirement and Data source drift.
pub async fn dispatch_restored_property_search_stage(
    dispatch: &meta_relational_reasoning::SearchDispatch,
    request: DataSearchDispatchRequest<'_>,
    backend: &RestoredPropertyBackend<'_>,
) -> Result<DataSearchDispatchReceipt, DataSearchDispatchError> {
    use meta_relational_reasoning::SearchDispatchError;
    let cap = NonZeroUsize::new(request.reservation.output_bytes).ok_or(
        DataSearchDispatchError::Dispatch(SearchDispatchError::ReservationExceeded),
    )?;
    if request.result_limits.max_rows().get() > request.reservation.results {
        return Err(DataSearchDispatchError::Dispatch(
            SearchDispatchError::ReservationExceeded,
        ));
    }
    let lease = dispatch
        .reserve(
            dispatch.generation(),
            request.binding.factor(),
            request.reservation,
        )
        .map_err(DataSearchDispatchError::Dispatch)?;
    let stage = execute_restored_property_search_stage(
        request.binding,
        dispatch.generation(),
        backend,
        request.logical_position,
        request.result_limits,
        cap,
    )
    .await
    .map_err(DataSearchDispatchError::Execution)?;
    let admitted = lease
        .admit(
            stage.handoff().result_bytes().len(),
            stage.observations().len(),
        )
        .map_err(DataSearchDispatchError::Dispatch)?;
    Ok(DataSearchDispatchReceipt {
        stage,
        dispatch: admitted,
    })
}
