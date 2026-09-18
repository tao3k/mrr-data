//! Physical snapshot and engine binding for an already admitted MRR query.

use std::collections::BTreeSet;
use std::fmt;

use cid::Cid;
use meta_relational_reasoning::{CatalogBoundQuery, Direction, GenerationId, PageValue};

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
