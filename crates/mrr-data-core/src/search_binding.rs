//! Bind physical acquisition to an admitted MRR query and immutable source.
use crate::{BoundDataQuery, DataQueryHandoffError, DataQueryResultHandoff};
use meta_relational_reasoning::{
    AdmittedPropertyExecution, FactId, GenerationId, QueryResultLimits, SearchFactor,
    SearchFactorRole, SearchObservation,
};
use sha2::{Digest, Sha256};
use std::{fmt, num::NonZeroUsize};

/// Physical acquisition input supplied by the consumer's Search composition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataSearchStageBinding {
    query: BoundDataQuery,
    factor: SearchFactor,
}

/// Original admitted query transport and its row occurrence observations.
/// Candidate identities include the immutable source and canonical whole-result
/// digest plus row ordinal. They identify execution occurrences, not entities.
#[derive(Clone, Debug)]
pub struct DataSearchStageReceipt {
    binding: DataSearchStageBinding,
    handoff: DataQueryResultHandoff,
    observations: Vec<SearchObservation>,
}

#[derive(Debug)]
pub enum DataSearchBindingError {
    AcquisitionFactorRequired,
    GenerationMismatch {
        expected: GenerationId,
        actual: GenerationId,
    },
    PhysicalBindingMismatch,
    Handoff(DataQueryHandoffError),
    Identity(String),
}
impl fmt::Display for DataSearchBindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for DataSearchBindingError {}

impl DataSearchStageBinding {
    /// Bind an acquisition factor to an independently selected physical query.
    /// # Errors
    /// Rejects factors whose role cannot produce acquisition roots.
    pub fn new(
        query: BoundDataQuery,
        factor: SearchFactor,
    ) -> Result<Self, DataSearchBindingError> {
        if factor.role() != SearchFactorRole::Acquisition {
            return Err(DataSearchBindingError::AcquisitionFactorRequired);
        }
        Ok(Self { query, factor })
    }
    #[must_use]
    pub const fn query(&self) -> &BoundDataQuery {
        &self.query
    }
    #[must_use]
    pub const fn factor(&self) -> SearchFactor {
        self.factor
    }
    /// Check the runtime generation before starting physical acquisition.
    /// # Errors
    /// Rejects a stale or foreign runtime generation.
    pub fn verify_generation(&self, expected: GenerationId) -> Result<(), DataSearchBindingError> {
        let actual = self.query.query().generation();
        if expected != actual {
            return Err(DataSearchBindingError::GenerationMismatch { expected, actual });
        }
        Ok(())
    }
    /// Publish observations only from MRR's original admitted physical execution.
    /// # Errors
    /// Rejects source, query, engine or generation substitution and transport overflow.
    pub fn project_execution(
        &self,
        execution: &AdmittedPropertyExecution<BoundDataQuery>,
        logical_position: u64,
        limits: QueryResultLimits,
        max_bytes: NonZeroUsize,
    ) -> Result<DataSearchStageReceipt, DataSearchBindingError> {
        if execution.physical_evidence() != &self.query {
            return Err(DataSearchBindingError::PhysicalBindingMismatch);
        }
        let handoff = DataQueryResultHandoff::export_execution(execution, limits, max_bytes)
            .map_err(DataSearchBindingError::Handoff)?;
        let result_digest = Sha256::digest(handoff.result_bytes());
        let digest = format!("{result_digest:x}");
        let prefix = format!(
            "mrr.data.search.row.v1:{}:{digest}",
            self.query.snapshot_root()
        );
        let observations = execution
            .candidate()
            .rows()
            .iter()
            .enumerate()
            .map(|(ordinal, _)| {
                let candidate = FactId::from_canonical_bytes(format!("{prefix}:{ordinal}"))
                    .map_err(|error| DataSearchBindingError::Identity(error.to_string()))?;
                let id = FactId::from_canonical_bytes(format!(
                    "mrr.data.search.observation.v1:{candidate}:{}:{logical_position}",
                    self.factor.id()
                ))
                .map_err(|error| DataSearchBindingError::Identity(error.to_string()))?;
                Ok(SearchObservation::new(
                    id,
                    candidate,
                    self.factor.id(),
                    self.query.query().generation(),
                    logical_position,
                    vec![],
                ))
            })
            .collect::<Result<_, DataSearchBindingError>>()?;
        Ok(DataSearchStageReceipt {
            binding: self.clone(),
            handoff,
            observations,
        })
    }
}
impl DataSearchStageReceipt {
    #[must_use]
    pub const fn binding(&self) -> &DataSearchStageBinding {
        &self.binding
    }
    #[must_use]
    pub const fn handoff(&self) -> &DataQueryResultHandoff {
        &self.handoff
    }
    #[must_use]
    pub fn observations(&self) -> &[SearchObservation] {
        &self.observations
    }
}
