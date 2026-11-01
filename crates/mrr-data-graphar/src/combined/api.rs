use crate::{
    CapturedGraphArEntityProperties, GraphArEntityPropertyError as Error,
    GraphArEntityPropertyLimits, GraphArEntityPropertyTable, GraphArReadLimits,
};
use cid::Cid;
use meta_relational_reasoning::{Fact, RelationId};
use mrr_data_core::{BoundDataQuery, GraphDatasetLimits, GraphProjectionKind};
use std::sync::Arc;

/// Limits cover the whole closure; topology counts sum native rows across artifacts.
/// Native decompression and metadata scratch are not a hard process RSS ceiling.
#[derive(Clone, Copy, Debug)]
pub struct CombinedGraphArLimits {
    pub dataset: GraphDatasetLimits,
    pub properties: GraphArEntityPropertyLimits,
    pub topology: GraphArReadLimits,
}
/// One declared relation and its immutable, context-preserving MRR facts.
pub struct CapturedGraphArRelation {
    pub relation: RelationId,
    pub facts: Arc<[Fact]>,
}
/// Immutable Arrow properties and fully re-admitted contextual relation facts.
/// No native handles or paths escape. Retain the shared Backend lease during use.
pub struct CapturedCombinedGraphAr {
    pub(super) snapshot: Cid,
    pub(super) descriptor: Cid,
    pub(super) properties: CapturedGraphArEntityProperties,
    pub(super) relations: Vec<CapturedGraphArRelation>,
}
/// Consuming Arrow/fact handoff preserves allocations under `ResourceHandle::try_transform`.
pub struct CombinedGraphArParts {
    pub tables: Vec<GraphArEntityPropertyTable>,
    pub relations: Vec<CapturedGraphArRelation>,
}
impl CapturedCombinedGraphAr {
    /// # Errors
    /// Refuses scope drift before moving existing table and relation allocations.
    pub fn into_parts(self, query: &BoundDataQuery) -> Result<CombinedGraphArParts, Error> {
        self.check(query)?;
        Ok(CombinedGraphArParts {
            tables: self.properties.into_tables(query.query())?,
            relations: self.relations,
        })
    }

    pub(super) fn check(&self, query: &BoundDataQuery) -> Result<(), Error> {
        if query.snapshot_root() != &self.snapshot
            || query.graph_projection_manifest() != Some(&self.descriptor)
            || query.graph_projection_kind() != Some(GraphProjectionKind::Dataset)
        {
            return Err(Error::Scope);
        }
        self.properties.tables(query.query())?;
        Ok(())
    }
    /// # Errors
    /// Refuses physical root, catalog, generation or semantic snapshot substitution.
    pub fn tables(&self, query: &BoundDataQuery) -> Result<&[GraphArEntityPropertyTable], Error> {
        self.check(query)?;
        self.properties.tables(query.query())
    }
    /// # Errors
    /// Applies the same full scope admission before returning contextual facts.
    pub fn relations(&self, query: &BoundDataQuery) -> Result<&[CapturedGraphArRelation], Error> {
        self.check(query)?;
        Ok(&self.relations)
    }
}
