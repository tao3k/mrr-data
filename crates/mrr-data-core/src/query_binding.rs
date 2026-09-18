//! Physical snapshot and engine binding for an already admitted MRR query.

use std::collections::BTreeSet;
use std::fmt;

use cid::Cid;
use meta_relational_reasoning::{
    Binding, CandidateQueryResult, CatalogBoundQuery, Direction, GenerationId, PageValue,
    QueryResultBinding, QueryResultValue,
};

use crate::SnapshotBlock;

/// Optional execution features that a physical query engine may support.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DataQueryFeature {
    BoundedVariableLengthPath,
    UnboundedPath,
    UndirectedPath,
    Aggregation,
    Ordering,
    Offset,
    ParameterizedPagination,
}

/// Named physical engine contract used after MRR semantic admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataEngineProfile {
    name: String,
    requires_graph_projection: bool,
    features: BTreeSet<DataQueryFeature>,
}

/// One MRR query bound to an immutable physical snapshot and engine profile.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundDataQuery {
    query: CatalogBoundQuery,
    snapshot_root: Cid,
    graph_projection_manifest: Option<Cid>,
    engine: DataEngineProfile,
}

/// Storage-neutral rows produced by one physical engine invocation.
///
/// This value intentionally carries no semantic identity. The identity is
/// injected from [`BoundDataQuery`] only after the executor profile is checked.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalQueryOutput {
    columns: Vec<Binding>,
    rows: Vec<Vec<QueryResultValue>>,
}

/// A physical executor for one already-bound query.
///
/// Implementations may read Arrow, `GraphAr`, or another admitted physical form,
/// but they cannot manufacture the MRR result binding returned to the caller.
pub trait DataQueryExecutor {
    type Error;

    fn profile(&self) -> &DataEngineProfile;

    /// Produces storage-neutral rows for the exact physical binding.
    ///
    /// # Errors
    ///
    /// Returns the implementation's typed read, planning, or execution failure.
    fn execute(&self, query: &BoundDataQuery) -> Result<PhysicalQueryOutput, Self::Error>;
}

/// Physical execution failures before MRR result admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataQueryExecutionError<E> {
    EngineProfileMismatch { bound: String, executor: String },
    Executor(E),
}

/// Physical reasons an admitted MRR query cannot execute against a snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataQueryBindingError {
    InvalidEngineProfile,
    GenerationMismatch {
        query: GenerationId,
        snapshot: GenerationId,
    },
    RelationCatalogMismatch,
    EntityCatalogMismatch,
    SemanticSnapshotMismatch,
    GraphProjectionRequired,
    UnsupportedFeature(DataQueryFeature),
}

impl fmt::Display for DataQueryBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for DataQueryBindingError {}

impl<E: fmt::Display> fmt::Display for DataQueryExecutionError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EngineProfileMismatch { bound, executor } => write!(
                formatter,
                "query is bound to engine `{bound}`, not executor `{executor}`"
            ),
            Self::Executor(error) => error.fmt(formatter),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for DataQueryExecutionError<E> {}

impl DataEngineProfile {
    /// Admits one named engine profile and its optional execution features.
    ///
    /// # Errors
    ///
    /// Returns [`DataQueryBindingError::InvalidEngineProfile`] when the name is
    /// empty or has surrounding whitespace.
    pub fn new(
        name: impl Into<String>,
        requires_graph_projection: bool,
        features: impl IntoIterator<Item = DataQueryFeature>,
    ) -> Result<Self, DataQueryBindingError> {
        let name = name.into();
        if name.is_empty() || name.trim() != name {
            return Err(DataQueryBindingError::InvalidEngineProfile);
        }
        Ok(Self {
            name,
            requires_graph_projection,
            features: features.into_iter().collect(),
        })
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn requires_graph_projection(&self) -> bool {
        self.requires_graph_projection
    }

    #[must_use]
    pub fn supports(&self, feature: DataQueryFeature) -> bool {
        self.features.contains(&feature)
    }
}

impl BoundDataQuery {
    #[must_use]
    pub const fn query(&self) -> &CatalogBoundQuery {
        &self.query
    }

    #[must_use]
    pub const fn snapshot_root(&self) -> &Cid {
        &self.snapshot_root
    }

    #[must_use]
    pub const fn graph_projection_manifest(&self) -> Option<&Cid> {
        self.graph_projection_manifest.as_ref()
    }

    #[must_use]
    pub const fn engine(&self) -> &DataEngineProfile {
        &self.engine
    }
}

impl PhysicalQueryOutput {
    #[must_use]
    pub const fn new(columns: Vec<Binding>, rows: Vec<Vec<QueryResultValue>>) -> Self {
        Self { columns, rows }
    }

    #[must_use]
    pub fn columns(&self) -> &[Binding] {
        &self.columns
    }

    #[must_use]
    pub fn rows(&self) -> &[Vec<QueryResultValue>] {
        &self.rows
    }
}

/// Executes a physically bound query and projects its rows into an MRR-owned
/// result candidate.
///
/// This function performs no result admission. It only checks that the selected
/// executor exactly matches the profile used during physical binding, executes
/// it, and injects the immutable semantic identity from the admitted query.
/// Callers must pass the returned candidate to MRR's
/// `admit_query_result_candidate` boundary.
///
/// # Errors
///
/// Returns [`DataQueryExecutionError::EngineProfileMismatch`] before execution
/// when the executor differs from the bound profile, or preserves the
/// executor's own typed failure.
pub fn execute_data_query<E: DataQueryExecutor>(
    query: &BoundDataQuery,
    executor: &E,
) -> Result<CandidateQueryResult, DataQueryExecutionError<E::Error>> {
    if executor.profile() != query.engine() {
        return Err(DataQueryExecutionError::EngineProfileMismatch {
            bound: query.engine().name().to_owned(),
            executor: executor.profile().name().to_owned(),
        });
    }
    let output = executor
        .execute(query)
        .map_err(DataQueryExecutionError::Executor)?;
    Ok(CandidateQueryResult::new(
        QueryResultBinding::for_query(query.query()),
        output.columns,
        output.rows,
    ))
}

/// Binds an admitted semantic query to one verified physical snapshot.
///
/// This boundary deliberately does not repeat MRR label, property, expression,
/// or result admission. It rejects only stale physical identity, unavailable
/// projection data, and unsupported engine features.
///
/// # Errors
///
/// Returns [`DataQueryBindingError`] when the query's admitted semantic
/// identity does not match the snapshot or the engine cannot execute it.
pub fn bind_data_query(
    query: &CatalogBoundQuery,
    snapshot: &SnapshotBlock,
    engine: &DataEngineProfile,
) -> Result<BoundDataQuery, DataQueryBindingError> {
    let manifest = snapshot.manifest();
    let semantic = manifest.semantic_snapshot();
    if query.generation() != semantic.generation() {
        return Err(DataQueryBindingError::GenerationMismatch {
            query: query.generation(),
            snapshot: semantic.generation(),
        });
    }
    if query.catalog_digest().as_bytes() != manifest.relation_catalog_digest() {
        return Err(DataQueryBindingError::RelationCatalogMismatch);
    }
    if query.entity_catalog_digest().as_bytes() != manifest.entity_catalog_digest() {
        return Err(DataQueryBindingError::EntityCatalogMismatch);
    }
    if query.snapshot_digest() != semantic.digest() {
        return Err(DataQueryBindingError::SemanticSnapshotMismatch);
    }

    let graph_projection_manifest = manifest
        .graph_projection()
        .map(|projection| *projection.manifest_cid());
    if engine.requires_graph_projection && graph_projection_manifest.is_none() {
        return Err(DataQueryBindingError::GraphProjectionRequired);
    }
    for feature in required_features(query) {
        if !engine.supports(feature) {
            return Err(DataQueryBindingError::UnsupportedFeature(feature));
        }
    }

    Ok(BoundDataQuery {
        query: query.clone(),
        snapshot_root: *snapshot.cid(),
        graph_projection_manifest,
        engine: engine.clone(),
    })
}

fn required_features(query: &CatalogBoundQuery) -> BTreeSet<DataQueryFeature> {
    let query = query.query();
    let mut required = BTreeSet::new();
    for relation in query
        .graph()
        .paths()
        .iter()
        .flat_map(meta_relational_reasoning::PathPattern::segments)
        .map(meta_relational_reasoning::PathSegment::relation)
    {
        match relation.max_hops() {
            None => {
                required.insert(DataQueryFeature::UnboundedPath);
            }
            Some(max_hops) if max_hops != relation.min_hops() => {
                required.insert(DataQueryFeature::BoundedVariableLengthPath);
            }
            Some(_) => {}
        }
        if relation.direction() == Direction::Undirected {
            required.insert(DataQueryFeature::UndirectedPath);
        }
    }
    if !query.aggregations().is_empty() {
        required.insert(DataQueryFeature::Aggregation);
    }
    if !query.ordering().is_empty() {
        required.insert(DataQueryFeature::Ordering);
    }
    if query.offset().is_some() {
        required.insert(DataQueryFeature::Offset);
    }
    if matches!(query.offset(), Some(PageValue::Parameter(_)))
        || matches!(query.limit(), Some(PageValue::Parameter(_)))
    {
        required.insert(DataQueryFeature::ParameterizedPagination);
    }
    required
}
