//! Complete Dataset admission precedes reusable ordered relation reads.
use super::{CapturedCombinedGraphAr, CombinedGraphArLimits};
use crate::{
    BinaryEntityProjection, GraphArChunkLayout, GraphArEntityPropertyError as Error,
    GraphArEntityPropertyProjection, GraphArEntityPropertyTable, GraphArSelection,
    GraphArSelectionMetrics, GraphArSelectiveCaptureOptions, GraphArSelectivePreparationMetrics,
    selective::{SelectivePhysicalSnapshot, capture_verified_physical},
};
use meta_relational_reasoning::{EntityId, RelationCatalog, RelationId};
use mrr_data_content::PreparedCombinedGraph;
use mrr_data_core::BoundDataQuery;

struct Member {
    projection: BinaryEntityProjection,
    physical: SelectivePhysicalSnapshot,
    validation: GraphArSelectionMetrics,
}
/// Full-source validation metrics remain separate from subsequent range reads.
#[derive(Clone, Copy, Debug)]
pub struct CombinedGraphArSelectiveMetrics {
    pub relation: RelationId,
    pub preparation: GraphArSelectivePreparationMetrics,
    pub validation: GraphArSelectionMetrics,
}
/// A validated combined Dataset with immutable Arrow properties and private
/// ordered relation storage. Whole-source facts are discarded after admission.
/// Initial capture copies/verifies and scans every relation; it is not a cold
/// selective-validation shortcut. Retain the common Backend lease during use.
pub struct CapturedCombinedGraphArSelective {
    // This private scope retains only the properties; no full relation facts.
    scope: CapturedCombinedGraphAr,
    members: Vec<Member>,
}
impl CapturedCombinedGraphArSelective {
    /// # Errors
    /// Refuses snapshot, Dataset, catalog, generation and semantic drift.
    pub fn tables(&self, query: &BoundDataQuery) -> Result<&[GraphArEntityPropertyTable], Error> {
        self.scope.tables(query)
    }
    #[must_use]
    pub fn preparation_metrics(&self) -> Vec<CombinedGraphArSelectiveMetrics> {
        self.members
            .iter()
            .map(|m| CombinedGraphArSelectiveMetrics {
                relation: m.projection.relation_id(),
                preparation: m.physical.preparation_metrics(),
                validation: m.validation,
            })
            .collect()
    }
    fn member(&self, query: &BoundDataQuery, relation: RelationId) -> Result<&Member, Error> {
        self.scope.check(query)?;
        self.members
            .iter()
            .find(|m| m.projection.relation_id() == relation)
            .ok_or(Error::Scope)
    }
    /// Read a declared member's outgoing neighborhood after full Dataset checks.
    /// # Errors
    /// Refuses foreign scope/member, malformed selected rows and edge limits.
    pub fn outgoing(
        &self,
        query: &BoundDataQuery,
        relation: RelationId,
        source: EntityId,
        max_edges: usize,
    ) -> Result<GraphArSelection, Error> {
        let member = self.member(query, relation)?;
        Ok(member
            .physical
            .outgoing_checked(&member.projection, source, max_edges, || Ok(()))?)
    }
    /// Reference scan over the same storage and indexes used by outgoing reads.
    /// # Errors
    /// Refuses foreign scope/member, malformed rows and aggregate edge limits.
    pub fn scan_all(
        &self,
        query: &BoundDataQuery,
        relation: RelationId,
        max_edges: usize,
    ) -> Result<GraphArSelection, Error> {
        let member = self.member(query, relation)?;
        Ok(member.physical.scan_all(&member.projection, max_edges)?)
    }
}
/// Capture controlled ordered-by-source members of a complete combined closure.
/// Full semantic validation is charged to preparation before any slice escapes.
/// # Errors
/// Refuses root/catalog drift, unsupported layout, corruption and aggregate limits.
pub fn capture_combined_graphar_selective(
    closure: &PreparedCombinedGraph,
    query: &BoundDataQuery,
    relations: &RelationCatalog,
    properties: &GraphArEntityPropertyProjection,
    limits: CombinedGraphArLimits,
    layout: GraphArChunkLayout,
) -> Result<CapturedCombinedGraphArSelective, Error> {
    capture_checked(
        closure,
        query,
        relations,
        properties,
        limits,
        layout,
        || Ok(()),
    )
}
pub(super) fn capture_checked(
    closure: &PreparedCombinedGraph,
    query: &BoundDataQuery,
    relations: &RelationCatalog,
    properties: &GraphArEntityPropertyProjection,
    limits: CombinedGraphArLimits,
    layout: GraphArChunkLayout,
    mut check: impl FnMut() -> Result<(), Error>,
) -> Result<CapturedCombinedGraphArSelective, Error> {
    check()?;
    if closure.root() != query.snapshot_root()
        || relations.digest() != query.query().catalog_digest()
    {
        return Err(Error::Scope);
    }
    closure
        .dataset()
        .admit_query(query, limits.dataset)
        .map_err(|_| Error::Scope)?;
    let properties =
        super::capture::capture_properties(closure, query, properties, limits, &mut check)?;
    let directory = tempfile::tempdir()?;
    let mut members = Vec::new();
    let mut remaining_vertices = limits.topology.max_vertices();
    let mut remaining_edges = limits.topology.max_edges();
    for (index, member) in closure.dataset().relations().iter().enumerate() {
        check()?;
        let projection = BinaryEntityProjection::admit_catalog(relations, member.relation)
            .map_err(|_| Error::Scope)?;
        let path = directory.path().join(format!("relation-{index}"));
        super::capture::materialize(closure, &member.inventory, &path, &mut check)?;
        let physical = capture_verified_physical(
            &path,
            &member.inventory,
            GraphArSelectiveCaptureOptions {
                inventory_limits: limits.dataset.inventory,
                max_vertices: remaining_vertices,
                layout,
            },
            query.query().generation(),
            || selective_checkpoint(&mut check),
        )?;
        check()?;
        // This scan includes facts outside every later neighborhood. A malformed
        // unselected edge must refuse preparation, never inherit complete coverage.
        let full = physical.scan_all_checked(&projection, remaining_edges, || {
            selective_checkpoint(&mut check)
        })?;
        check()?;
        remaining_vertices = remaining_vertices
            .checked_sub(physical.vertex_count())
            .ok_or(Error::Budget("aggregate topology vertices"))?;
        remaining_edges = remaining_edges
            .checked_sub(full.facts().len())
            .ok_or(Error::Budget("aggregate topology facts"))?;
        let validation = full.metrics();
        drop(full);
        members.push(Member {
            projection,
            physical,
            validation,
        });
        std::fs::remove_dir_all(path)?;
    }
    check()?;
    directory.close()?;
    Ok(CapturedCombinedGraphArSelective {
        scope: CapturedCombinedGraphAr {
            snapshot: *query.snapshot_root(),
            descriptor: *query.graph_projection_manifest().ok_or(Error::Scope)?,
            properties,
            relations: Vec::new(),
        },
        members,
    })
}
fn selective_checkpoint(
    check: &mut impl FnMut() -> Result<(), Error>,
) -> Result<(), crate::GraphArSelectiveError> {
    check().map_err(|error| {
        #[cfg(feature = "backend")]
        if let Error::Stop(stop) = error {
            return match stop {
                mrr_data_backend::ResourceStop::Cancelled => {
                    crate::GraphArSelectiveError::Cancelled
                }
                mrr_data_backend::ResourceStop::Deadline => crate::GraphArSelectiveError::Deadline,
            };
        }
        let _ = error;
        crate::GraphArSelectiveError::Scope
    })
}
