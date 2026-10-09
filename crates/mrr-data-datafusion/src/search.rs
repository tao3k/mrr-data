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
