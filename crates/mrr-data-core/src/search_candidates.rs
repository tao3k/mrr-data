//! Consumer-owned merging of actual, complete physical Search candidate sets.
use meta_relational_reasoning::{
    FactId, GenerationId, SearchFactor, SearchFactorEdge, SearchFactorRole, SearchObservation,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroUsize,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataSearchSourceBinding {
    scope: String,
    source_digest: String,
    resident_view_digest: String,
    composition_abi: String,
    generation: GenerationId,
}
impl DataSearchSourceBinding {
    /// Bind caller-owned immutable source and resident view identities.
    /// These identities are compared, not independently authenticated by Data.
    /// # Errors
    /// Rejects empty or surrounding-whitespace identity components.
    pub fn new(
        scope: String,
        source_digest: String,
        resident_view_digest: String,
        composition_abi: String,
        generation: GenerationId,
    ) -> Result<Self, DataSearchCandidateError> {
        for value in [
            &scope,
            &source_digest,
            &resident_view_digest,
            &composition_abi,
        ] {
            if value.is_empty() || value.trim() != value {
                return Err(DataSearchCandidateError::InvalidBinding);
            }
        }
        Ok(Self {
            scope,
            source_digest,
            resident_view_digest,
            composition_abi,
            generation,
        })
    }
    #[must_use]
    pub const fn generation(&self) -> GenerationId {
        self.generation
    }
    #[must_use]
    pub fn scope(&self) -> &str {
        &self.scope
    }
    #[must_use]
    pub fn source_digest(&self) -> &str {
        &self.source_digest
    }
    #[must_use]
    pub fn resident_view_digest(&self) -> &str {
        &self.resident_view_digest
    }
    #[must_use]
    pub fn composition_abi(&self) -> &str {
        &self.composition_abi
    }
    /// Derive the existing Version 1 candidate identity from exact owner bytes.
    /// No path normalization or physical source authentication is performed.
    /// # Errors
    /// Rejects empty or surrounding-whitespace owners and identity failures.
    pub fn candidate_identity(&self, owner: &str) -> Result<FactId, DataSearchCandidateError> {
        if owner.is_empty() || owner.trim() != owner {
            return Err(DataSearchCandidateError::InvalidCandidate);
        }
        FactId::from_canonical_bytes(canonical(&[
            "mrr.data.search.owner.v1",
            &self.canonical(),
            owner,
        ]))
        .map_err(|error| DataSearchCandidateError::Identity(error.to_string()))
    }
    fn canonical(&self) -> String {
        canonical(&[
            &self.scope,
            &self.source_digest,
            &self.resident_view_digest,
            &self.composition_abi,
            &self.generation.to_string(),
        ])
    }
}
fn canonical(parts: &[&str]) -> String {
    let mut encoded = String::new();
    for value in parts {
        encoded.push_str(&value.len().to_string());
        encoded.push(':');
        encoded.push_str(value);
    }
    encoded
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataSearchCandidateComposition {
    Single,
    RankJoin,
    Intersect,
}

#[derive(Clone, Debug)]
pub struct DataSearchCandidateBranch {
    binding: DataSearchSourceBinding,
    factor: SearchFactor,
    candidates: BTreeSet<String>,
    complete: bool,
    truncated: bool,
}
impl DataSearchCandidateBranch {
    #[must_use]
    pub const fn new(
        binding: DataSearchSourceBinding,
        factor: SearchFactor,
        candidates: BTreeSet<String>,
        complete: bool,
        truncated: bool,
    ) -> Self {
        Self {
            binding,
            factor,
            candidates,
            complete,
            truncated,
        }
    }
}

/// Actual merged owner set plus the exact MRR factor graph used to reason about it.
#[derive(Clone, Debug)]
pub struct DataSearchCandidateReceipt {
    binding: DataSearchSourceBinding,
    mode: DataSearchCandidateComposition,
    merged: BTreeSet<String>,
    factors: Vec<SearchFactor>,
    edges: Vec<SearchFactorEdge>,
    observations: Vec<SearchObservation>,
    branch_completeness: Vec<(bool, bool)>,
    composition_id: FactId,
    candidate_owners: BTreeMap<String, FactId>,
}
impl DataSearchCandidateReceipt {
    /// Recheck a stored receipt against the receiver's current source binding.
    /// # Errors
    /// Rejects receipts whose source, resident view, generation or ABI was revoked.
    pub fn verify_binding(
        &self,
        expected: &DataSearchSourceBinding,
    ) -> Result<(), DataSearchCandidateError> {
        if &self.binding != expected {
            return Err(DataSearchCandidateError::BindingMismatch);
        }
        Ok(())
    }
    /// Exact owner-to-candidate inventory across all admitted physical branches.
    /// Includes auxiliary and excluded owners, not just the merged truth set.
    #[must_use]
    pub fn candidate_owners(&self) -> &BTreeMap<String, FactId> {
        &self.candidate_owners
    }
    /// Admit an owner/candidate pair only under the receiver's current binding.
    /// # Errors
    /// Rejects revoked bindings, unobserved owners and forged candidate identities.
    pub fn verify_candidate(
        &self,
        expected: &DataSearchSourceBinding,
        owner: &str,
        candidate: FactId,
    ) -> Result<(), DataSearchCandidateError> {
        self.verify_binding(expected)?;
        if self.candidate_owners.get(owner) != Some(&candidate)
            || expected.candidate_identity(owner)? != candidate
        {
            return Err(DataSearchCandidateError::CandidateIdentityMismatch);
        }
        Ok(())
    }
    #[must_use]
    pub const fn binding(&self) -> &DataSearchSourceBinding {
        &self.binding
    }
    #[must_use]
    pub const fn mode(&self) -> DataSearchCandidateComposition {
        self.mode
    }
    #[must_use]
    pub const fn merged_candidates(&self) -> &BTreeSet<String> {
        &self.merged
    }
    #[must_use]
    pub fn factors(&self) -> &[SearchFactor] {
        &self.factors
    }
    #[must_use]
    pub fn edges(&self) -> &[SearchFactorEdge] {
        &self.edges
    }
    #[must_use]
    pub fn observations(&self) -> &[SearchObservation] {
        &self.observations
    }
    /// Each input branch's `(complete, truncated)` flags in composition order.
    #[must_use]
    pub fn branch_completeness(&self) -> &[(bool, bool)] {
        &self.branch_completeness
    }
    /// Binds mode, source, ordered factors, completeness flags and actual owner sets.
    #[must_use]
    pub const fn composition_id(&self) -> FactId {
        self.composition_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataSearchCandidateError {
    InvalidBinding,
    BindingMismatch,
    IncompleteBranch,
    TruncatedBranch,
    InvalidComposition,
    InvalidFactor,
    DuplicateFactor,
    PooPlanMismatch,
    InvalidCandidate,
    CandidateLimit,
    CandidateIdentityMismatch,
    CandidateIdentityCollision,
    Identity(String),
}
impl fmt::Display for DataSearchCandidateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for DataSearchCandidateError {}

/// A physical leaf identified by its original POO stage name, not a new factor.
pub struct DataPooSearchCandidateBranch {
    pub stage_name: String,
    pub binding: DataSearchSourceBinding,
    pub candidates: BTreeSet<String>,
    pub complete: bool,
    pub truncated: bool,
}
/// Consume the actual POO graph. Flat aggregation supports acquisition leaves
/// feeding one refinement stage; other graphs are refused, never reconstructed.
/// # Errors
/// Rejects a different generation, missing stage, extra factor/edge or invalid
/// physical candidate input. Existing completeness and resource gates still apply.
pub fn compose_poo_data_search_candidates(
    expected: &DataSearchSourceBinding,
    mode: DataSearchCandidateComposition,
    projection: &meta_relational_reasoning::PooSearchProjection,
    merge_name: &str,
    branches: &[DataPooSearchCandidateBranch],
    max_candidates: NonZeroUsize,
) -> Result<DataSearchCandidateReceipt, DataSearchCandidateError> {
    if projection.generation() != expected.generation() {
        return Err(DataSearchCandidateError::BindingMismatch);
    }
    let merge = projection
        .factor_by_name(merge_name)
        .ok_or(DataSearchCandidateError::PooPlanMismatch)?;
    let resolved = branches
        .iter()
        .map(|branch| {
            let factor = projection
                .factor_by_name(&branch.stage_name)
                .ok_or(DataSearchCandidateError::PooPlanMismatch)?;
            Ok(DataSearchCandidateBranch::new(
                branch.binding.clone(),
                factor,
                branch.candidates.clone(),
                branch.complete,
                branch.truncated,
            ))
        })
        .collect::<Result<Vec<_>, DataSearchCandidateError>>()?;
    let mut data =
        compose_data_search_candidates(expected, mode, merge, &resolved, max_candidates)?;
    let factors = data.factors.iter().copied().collect::<BTreeSet<_>>();
    let projected_factors = projection
        .factors()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let edges = data.edges.iter().copied().collect::<BTreeSet<_>>();
    let projected_edges = projection.edges().iter().copied().collect::<BTreeSet<_>>();
    if factors != projected_factors
        || edges != projected_edges
        || projected_factors.len() != projection.factors().len()
        || projected_edges.len() != projection.edges().len()
    {
        return Err(DataSearchCandidateError::PooPlanMismatch);
    }
    data.factors = projection.factors().to_vec();
    data.edges = projection.edges().to_vec();
    Ok(data)
}

/// Merge physical owner sets before creating immutable MRR observations.
/// `RankJoin` retains exactly the first branch's truth set; ranking belongs to the
/// consumer. Intersect admits an owner only when every complete branch has it.
/// # Errors
/// Rejects stale source/view/generation/ABI, incomplete intersection or primary truth leaves,
/// duplicate/wrong-role factors, malformed owners and bounded candidate overflow.
pub fn compose_data_search_candidates(
    expected: &DataSearchSourceBinding,
    mode: DataSearchCandidateComposition,
    merge_factor: SearchFactor,
    branches: &[DataSearchCandidateBranch],
    max_candidates: NonZeroUsize,
) -> Result<DataSearchCandidateReceipt, DataSearchCandidateError> {
    let count = validate_branches(expected, mode, merge_factor, branches, max_candidates)?;
    let mut merged = branches[0].candidates.clone();
    if mode == DataSearchCandidateComposition::Intersect {
        for branch in &branches[1..] {
            merged.retain(|candidate| branch.candidates.contains(candidate));
        }
    }
    if count
        .checked_add(merged.len())
        .is_none_or(|total| total > max_candidates.get())
    {
        return Err(DataSearchCandidateError::CandidateLimit);
    }
    let composition_id = composition_identity(expected, mode, merge_factor, branches)?;
    let mut candidate_owners = BTreeMap::new();
    for branch in branches {
        for owner in &branch.candidates {
            candidate_owners.insert(owner.clone(), expected.candidate_identity(owner)?);
        }
    }
    if candidate_owners
        .values()
        .copied()
        .collect::<BTreeSet<_>>()
        .len()
        != candidate_owners.len()
    {
        return Err(DataSearchCandidateError::CandidateIdentityCollision);
    }
    let candidate_id = |owner: &str| expected.candidate_identity(owner);
    let observation_id = |candidate: FactId, factor: SearchFactor| {
        FactId::from_canonical_bytes(canonical(&[
            "mrr.data.search.owner.observation.v1",
            &composition_id.to_string(),
            &candidate.to_string(),
            &factor.id().to_string(),
        ]))
        .map_err(|error| DataSearchCandidateError::Identity(error.to_string()))
    };
    let mut observations = Vec::with_capacity(count + merged.len());
    for branch in branches {
        for owner in &branch.candidates {
            let candidate = candidate_id(owner)?;
            observations.push(SearchObservation::new(
                observation_id(candidate, branch.factor)?,
                candidate,
                branch.factor.id(),
                expected.generation,
                0,
                vec![],
            ));
        }
    }
    for owner in &merged {
        let candidate = candidate_id(owner)?;
        let parents = branches
            .iter()
            .filter(|branch| branch.candidates.contains(owner))
            .map(|branch| observation_id(candidate, branch.factor))
            .collect::<Result<Vec<_>, _>>()?;
        observations.push(SearchObservation::new(
            observation_id(candidate, merge_factor)?,
            candidate,
            merge_factor.id(),
            expected.generation,
            1,
            parents,
        ));
    }
    let mut factors = branches
        .iter()
        .map(|branch| branch.factor)
        .collect::<Vec<_>>();
    factors.push(merge_factor);
    let edges = branches
        .iter()
        .map(|branch| SearchFactorEdge::new(branch.factor.id(), merge_factor.id()))
        .collect();
    let branch_completeness = branches
        .iter()
        .map(|branch| (branch.complete, branch.truncated))
        .collect();
    Ok(DataSearchCandidateReceipt {
        binding: expected.clone(),
        candidate_owners,
        mode,
        merged,
        factors,
        edges,
        observations,
        branch_completeness,
        composition_id,
    })
}

fn validate_branches(
    expected: &DataSearchSourceBinding,
    mode: DataSearchCandidateComposition,
    merge_factor: SearchFactor,
    branches: &[DataSearchCandidateBranch],
    max_candidates: NonZeroUsize,
) -> Result<usize, DataSearchCandidateError> {
    if branches.is_empty()
        || (mode == DataSearchCandidateComposition::Single && branches.len() != 1)
    {
        return Err(DataSearchCandidateError::InvalidComposition);
    }
    if merge_factor.role() != SearchFactorRole::Refinement {
        return Err(DataSearchCandidateError::InvalidFactor);
    }
    let mut factor_ids = BTreeSet::from([merge_factor.id()]);
    let mut count = 0usize;
    for (index, branch) in branches.iter().enumerate() {
        if &branch.binding != expected {
            return Err(DataSearchCandidateError::BindingMismatch);
        }
        let requires_complete = mode == DataSearchCandidateComposition::Intersect
            || (mode == DataSearchCandidateComposition::RankJoin && index == 0);
        if requires_complete && !branch.complete {
            return Err(DataSearchCandidateError::IncompleteBranch);
        }
        if requires_complete && branch.truncated {
            return Err(DataSearchCandidateError::TruncatedBranch);
        }
        if branch.factor.role() != SearchFactorRole::Acquisition {
            return Err(DataSearchCandidateError::InvalidFactor);
        }
        if !factor_ids.insert(branch.factor.id()) {
            return Err(DataSearchCandidateError::DuplicateFactor);
        }
        count = count
            .checked_add(branch.candidates.len())
            .ok_or(DataSearchCandidateError::CandidateLimit)?;
        if count > max_candidates.get() {
            return Err(DataSearchCandidateError::CandidateLimit);
        }
        if branch
            .candidates
            .iter()
            .any(|owner| owner.is_empty() || owner.trim() != owner)
        {
            return Err(DataSearchCandidateError::InvalidCandidate);
        }
    }
    Ok(count)
}

fn composition_identity(
    expected: &DataSearchSourceBinding,
    mode: DataSearchCandidateComposition,
    merge_factor: SearchFactor,
    branches: &[DataSearchCandidateBranch],
) -> Result<FactId, DataSearchCandidateError> {
    let source = expected.canonical();
    let mut composition = canonical(&[
        "mrr.data.search.composition.v1",
        &source,
        match mode {
            DataSearchCandidateComposition::Single => "single",
            DataSearchCandidateComposition::RankJoin => "rank-join",
            DataSearchCandidateComposition::Intersect => "intersect",
        },
        &merge_factor.id().to_string(),
    ]);
    for branch in branches {
        composition.push_str(&canonical(&[
            &branch.factor.id().to_string(),
            if branch.complete {
                "complete"
            } else {
                "partial"
            },
            if branch.truncated {
                "truncated"
            } else {
                "untruncated"
            },
            &branch.candidates.len().to_string(),
        ]));
        for owner in &branch.candidates {
            composition.push_str(&canonical(&[owner]));
        }
    }
    let composition_id = FactId::from_canonical_bytes(composition)
        .map_err(|error| DataSearchCandidateError::Identity(error.to_string()))?;
    Ok(composition_id)
}

/// Native MRR inference consumes exactly the receipt's admitted owner observations.
/// # Errors
/// Propagates native factor validation, resource bounds and inference errors.
#[cfg(feature = "native-search")]
pub fn evaluate_data_search_candidates(
    receipt: &DataSearchCandidateReceipt,
    limits: meta_relational_reasoning::SearchFrameworkLimits,
) -> Result<
    meta_relational_reasoning::SearchFrameworkReceipt,
    meta_relational_reasoning::SearchFrameworkError,
> {
    meta_relational_reasoning::evaluate_search_factors(
        receipt.binding.generation,
        &receipt.factors,
        &receipt.edges,
        &receipt.observations,
        limits,
    )
}
