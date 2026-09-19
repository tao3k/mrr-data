//! Database-neutral registration contract for one immutable `GraphAr` source.

use std::path::{Path, PathBuf};

use meta_relational_reasoning::RelationId;

#[cfg(feature = "native-graphar")]
use crate::BinaryEntityProjection;

pub(crate) const GRAPH_INFO_FILE: &str = "mrr.graph.yaml";
pub(crate) const ENTITY_TYPE: &str = "entity";
pub(crate) const EDGE_TYPE: &str = "mrr_relation";
pub(crate) const ENTITY_ID_PROPERTY: &str = "entity_id";
pub(crate) const FACT_ID_PROPERTY: &str = "fact_id";
pub(crate) const GENERATION_PROPERTY: &str = "generation_id";

/// Immutable metadata a downstream query adapter needs to register `GraphAr`.
///
/// The value deliberately contains no SQL, database connection, execution
/// trait, or mutable catalog state. A downstream crate may combine it with
/// `DuckDB`, `DataFusion`, or another engine while this crate continues to own
/// the physical graph projection contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphArQuerySource {
    root: PathBuf,
    relation_id: RelationId,
    predicate: String,
    source_field: String,
    destination_field: String,
    vertex_count: usize,
    edge_count: usize,
}

impl GraphArQuerySource {
    #[cfg(feature = "native-graphar")]
    pub(crate) fn new(
        root: PathBuf,
        projection: &BinaryEntityProjection,
        vertex_count: usize,
        edge_count: usize,
    ) -> Self {
        Self {
            root,
            relation_id: projection.relation_id(),
            predicate: projection.predicate().to_owned(),
            source_field: projection.source_field().to_owned(),
            destination_field: projection.destination_field().to_owned(),
            vertex_count,
            edge_count,
        }
    }

    /// Root containing the `GraphAr` metadata and physical chunks.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Canonical metadata entry point; consumers discover physical chunks from it.
    #[must_use]
    pub fn graph_info_path(&self) -> PathBuf {
        self.root.join(GRAPH_INFO_FILE)
    }

    /// MRR relation represented by every edge in this source.
    #[must_use]
    pub const fn relation_id(&self) -> RelationId {
        self.relation_id
    }

    /// Stable edge predicate declared by the admitted MRR relation.
    #[must_use]
    pub fn predicate(&self) -> &str {
        &self.predicate
    }

    /// Semantic source endpoint field; no physical column name is inferred.
    #[must_use]
    pub fn source_field(&self) -> &str {
        &self.source_field
    }

    /// Semantic destination endpoint field; no physical column name is inferred.
    #[must_use]
    pub fn destination_field(&self) -> &str {
        &self.destination_field
    }

    #[must_use]
    pub const fn vertex_count(&self) -> usize {
        self.vertex_count
    }

    #[must_use]
    pub const fn edge_count(&self) -> usize {
        self.edge_count
    }

    /// Physical vertex label written by this exact projection profile.
    #[must_use]
    pub const fn vertex_label(&self) -> &'static str {
        ENTITY_TYPE
    }

    /// Physical edge label written by this exact projection profile.
    #[must_use]
    pub const fn edge_label(&self) -> &'static str {
        EDGE_TYPE
    }

    /// Stable semantic identity property retained beside snapshot-local IDs.
    #[must_use]
    pub const fn entity_identity_property(&self) -> &'static str {
        ENTITY_ID_PROPERTY
    }

    /// Stable semantic edge identity property retained beside physical rows.
    #[must_use]
    pub const fn fact_identity_property(&self) -> &'static str {
        FACT_ID_PROPERTY
    }

    /// Semantic generation property required for downstream snapshot pinning.
    #[must_use]
    pub const fn generation_property(&self) -> &'static str {
        GENERATION_PROPERTY
    }
}
