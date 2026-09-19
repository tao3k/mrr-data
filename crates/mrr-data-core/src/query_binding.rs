//! Physical snapshot and engine binding for an already admitted MRR query.

use std::collections::BTreeSet;
use std::fmt;

#[cfg(feature = "ipfs")]
use crate::SnapshotBlock;
#[cfg(feature = "ipfs")]
use cid::Cid;
use meta_relational_reasoning::{Binding, GenerationId, QueryResultValue};
#[cfg(feature = "ipfs")]
use meta_relational_reasoning::{
    CandidateQueryResult, CatalogBoundQuery, Direction, PageValue, QueryResultBinding, RelationId,
};

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
#[cfg(feature = "ipfs")]
pub struct BoundDataQuery {
    query: CatalogBoundQuery,
    snapshot_root: Cid,
    graph_projection_manifest: Option<Cid>,
    engine: DataEngineProfile,
}

/// Storage-neutral rows produced by one physical engine invocation.
///
/// This value intentionally carries no semantic identity. The identity is
/// injected from `BoundDataQuery` (with the `ipfs` feature) only after the producer profile is checked.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalQueryOutput {
    columns: Vec<Binding>,
    rows: Vec<Vec<QueryResultValue>>,
}

/// Physical output projection failures before MRR result admission.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "ipfs")]
pub enum DataQueryOutputError {
    EngineProfileMismatch { bound: String, actual: String },
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

/// Physical identity failures when selecting a graph source for a bound query.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "ipfs")]
pub enum DataGraphSourceBindingError {
    GraphProjectionRequired,
    SourceRelationUnavailable(RelationId),
    GraphProjectionManifestMismatch {
        expected: Box<Cid>,
        actual: Box<Cid>,
    },
}

impl fmt::Display for DataQueryBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for DataQueryBindingError {}

#[cfg(feature = "ipfs")]
impl fmt::Display for DataGraphSourceBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

#[cfg(feature = "ipfs")]
impl std::error::Error for DataGraphSourceBindingError {}

#[cfg(feature = "ipfs")]
impl fmt::Display for DataQueryOutputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EngineProfileMismatch { bound, actual } => write!(
                formatter,
                "query is bound to engine `{bound}`, not output producer `{actual}`"
            ),
        }
    }
}

#[cfg(feature = "ipfs")]
impl std::error::Error for DataQueryOutputError {}

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

#[cfg(feature = "ipfs")]
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

/// Projects physical engine output into an MRR-owned result candidate.
///
/// This function owns no engine lifecycle and performs no result admission. An
/// engine executes through its native synchronous or asynchronous API, then
/// supplies storage-neutral output together with its selected profile. This
/// boundary checks the profile against the physical binding and injects the
/// immutable semantic identity from the admitted query. Callers must pass the
/// returned candidate to MRR's `admit_query_result_candidate` boundary.
///
/// # Errors
///
/// Returns [`DataQueryOutputError::EngineProfileMismatch`] when the engine that
/// produced `output` differs from the bound profile.
#[cfg(feature = "ipfs")]
pub fn project_data_query_output(
    query: &BoundDataQuery,
    engine: &DataEngineProfile,
    output: PhysicalQueryOutput,
) -> Result<CandidateQueryResult, DataQueryOutputError> {
    if engine != query.engine() {
        return Err(DataQueryOutputError::EngineProfileMismatch {
            bound: query.engine().name().to_owned(),
            actual: engine.name().to_owned(),
        });
    }
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
#[cfg(feature = "ipfs")]
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

/// Admits one relation-specific graph source against an immutable query binding.
///
/// This boundary checks physical identity only. It neither opens storage nor
/// owns an engine lifecycle; storage adapters project their source metadata to
/// `relation` and `projection_manifest`, then retain their native execution API.
///
/// # Errors
///
/// Returns [`DataGraphSourceBindingError`] when the bound snapshot has no graph
/// projection, the query does not reference `relation`, or the supplied
/// manifest differs from the projection selected during query binding.
#[cfg(feature = "ipfs")]
pub fn admit_graph_projection_source(
    query: &BoundDataQuery,
    relation: RelationId,
    projection_manifest: &Cid,
) -> Result<(), DataGraphSourceBindingError> {
    let expected_manifest = query
        .graph_projection_manifest()
        .ok_or(DataGraphSourceBindingError::GraphProjectionRequired)?;
    let relation_is_referenced = query.query().query().graph().paths().iter().any(|path| {
        path.segments()
            .iter()
            .any(|segment| segment.relation().types().contains(&relation))
    });
    if !relation_is_referenced {
        return Err(DataGraphSourceBindingError::SourceRelationUnavailable(
            relation,
        ));
    }
    if projection_manifest != expected_manifest {
        return Err(
            DataGraphSourceBindingError::GraphProjectionManifestMismatch {
                expected: Box::new(*expected_manifest),
                actual: Box::new(*projection_manifest),
            },
        );
    }
    Ok(())
}

#[cfg(feature = "ipfs")]
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
