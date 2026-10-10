//! Source-bound candidate execution through the original MRR inference port.
use crate::{
    DataPooSearchCandidateBranch, DataSearchCandidateComposition, DataSearchCandidateError,
    DataSearchCandidateReceipt, DataSearchSourceBinding, compose_poo_data_search_candidates,
};
use meta_relational_reasoning::{
    PooSearchProjection, SearchFrameworkError, SearchFrameworkLimits, SearchFrameworkReceipt,
    SearchFrameworkStatus, SearchObservation,
};
use std::num::NonZeroUsize;

#[derive(Debug)]
pub enum DataSearchExecutionError {
    Candidates(DataSearchCandidateError),
    InfluenceBudgetOverflow,
    Inference(SearchFrameworkError),
    IncompleteInference,
}
impl std::fmt::Display for DataSearchExecutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl std::error::Error for DataSearchExecutionError {}

/// Data evidence and the admitted MRR inference over exactly that evidence.
#[derive(Debug)]
pub struct DataSearchExecution {
    pub candidates: DataSearchCandidateReceipt,
    pub reasoning: SearchFrameworkReceipt,
}

/// Compose source-bound sets, admit candidate identities, bound inference and
/// collect its result. The caller supplies the existing MRR execution port;
/// Data does not choose a Scheme runtime or introduce another inference engine.
///
/// # Errors
/// Rejects foreign plans/bindings, incomplete truth inputs, resource overflow,
/// failed inference and results outside the original generation or completion.
pub fn execute_poo_data_search_candidates<F>(
    binding: &DataSearchSourceBinding,
    mode: DataSearchCandidateComposition,
    projection: &PooSearchProjection,
    merge_name: &str,
    branches: &[DataPooSearchCandidateBranch],
    max_observations: NonZeroUsize,
    infer: F,
) -> Result<DataSearchExecution, DataSearchExecutionError>
where
    F: FnOnce(
        &PooSearchProjection,
        &[SearchObservation],
        SearchFrameworkLimits,
    ) -> Result<SearchFrameworkReceipt, SearchFrameworkError>,
{
    let candidates = compose_poo_data_search_candidates(
        binding,
        mode,
        projection,
        merge_name,
        branches,
        max_observations,
    )
    .map_err(DataSearchExecutionError::Candidates)?;
    for (owner, candidate) in candidates.candidate_owners() {
        candidates
            .verify_candidate(binding, owner, *candidate)
            .map_err(DataSearchExecutionError::Candidates)?;
    }
    let influence_bound = candidates
        .observations()
        .len()
        .checked_mul(candidates.factors().len())
        .ok_or(DataSearchExecutionError::InfluenceBudgetOverflow)?;
    let positive = |count: usize| NonZeroUsize::new(count).unwrap_or(NonZeroUsize::MIN);
    let limits = SearchFrameworkLimits::new(
        positive(candidates.factors().len()),
        positive(candidates.edges().len()),
        max_observations,
        positive(influence_bound),
        positive(influence_bound),
    );
    let reasoning = infer(projection, candidates.observations(), limits)
        .map_err(DataSearchExecutionError::Inference)?;
    if reasoning.generation() != binding.generation()
        || reasoning.status() != SearchFrameworkStatus::Complete
    {
        return Err(DataSearchExecutionError::IncompleteInference);
    }
    Ok(DataSearchExecution {
        candidates,
        reasoning,
    })
}
