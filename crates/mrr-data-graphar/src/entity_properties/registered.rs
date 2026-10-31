//! Snapshot-root admission for portable native property artifacts.
use super::{
    CapturedGraphArEntityProperties, GraphArEntityPropertyBlock,
    GraphArEntityPropertyError as Error, GraphArEntityPropertyLimits,
    GraphArEntityPropertyProjection, GraphArEntityPropertyReceipt, GraphArEntityPropertyTable,
};
use cid::Cid;
use mrr_data_core::{BoundDataQuery, GraphProjectionDescriptor};
use std::path::Path;

impl GraphArEntityPropertyBlock {
    /// Register this exact descriptor in a `SnapshotManifest` request.
    /// Native format version and property profile identity remain explicit.
    /// # Errors
    /// Refuses invalid native format versions or CID profiles.
    pub fn snapshot_projection(
        &self,
        graphar_version: impl Into<String>,
    ) -> Result<GraphProjectionDescriptor, Error> {
        GraphProjectionDescriptor::entity_properties(graphar_version, *self.cid())
            .map_err(|_| Error::Shape("snapshot property projection"))
    }
}
/// Captured property tables retain both physical root and descriptor identity.
/// A caller authenticates the `SnapshotManifest` root before binding its query.
pub struct RegisteredGraphArEntityProperties {
    captured: CapturedGraphArEntityProperties,
    snapshot: Cid,
    descriptor: Cid,
}
impl RegisteredGraphArEntityProperties {
    fn check(&self, query: &BoundDataQuery) -> Result<(), Error> {
        if query.snapshot_root() != &self.snapshot
            || query.graph_projection_manifest() != Some(&self.descriptor)
        {
            return Err(Error::Scope);
        }
        Ok(())
    }
    /// # Errors
    /// Refuses physical root, property descriptor or MRR semantic scope drift.
    pub fn tables(&self, query: &BoundDataQuery) -> Result<&[GraphArEntityPropertyTable], Error> {
        self.check(query)?;
        self.captured.tables(query.query())
    }
    /// Preserve the existing vector allocation; Backend callers retain its lease
    /// with `ResourceHandle::try_transform`.
    /// # Errors
    /// Refuses the same substitutions as `tables()`.
    pub fn into_tables(
        self,
        query: &BoundDataQuery,
    ) -> Result<Vec<GraphArEntityPropertyTable>, Error> {
        self.check(query)?;
        self.captured.into_tables(query.query())
    }
}
/// Resolve descriptor identity from an MRR query bound to `SnapshotManifest`,
/// authenticate its canonical bytes, then capture every verified native child.
/// Native files and YAML never choose the physical root or logical catalog.
/// # Errors
/// Refuses absent/foreign descriptor, semantic drift and native capture failures.
pub fn capture_registered_graphar_entity_properties(
    source: &Path,
    query: &BoundDataQuery,
    projection: &GraphArEntityPropertyProjection,
    descriptor_bytes: &[u8],
    limits: GraphArEntityPropertyLimits,
) -> Result<RegisteredGraphArEntityProperties, Error> {
    capture_checked(source, query, projection, descriptor_bytes, limits, || {
        Ok(())
    })
}
pub(super) fn capture_checked(
    source: &Path,
    query: &BoundDataQuery,
    projection: &GraphArEntityPropertyProjection,
    descriptor_bytes: &[u8],
    limits: GraphArEntityPropertyLimits,
    mut check: impl FnMut() -> Result<(), Error>,
) -> Result<RegisteredGraphArEntityProperties, Error> {
    check()?;
    if query.graph_projection_kind() != Some(mrr_data_core::GraphProjectionKind::EntityProperties) {
        return Err(Error::Scope);
    }
    let descriptor = query.graph_projection_manifest().ok_or(Error::Scope)?;
    let receipt = GraphArEntityPropertyReceipt::decode_descriptor_checked(
        source.to_path_buf(),
        descriptor,
        descriptor_bytes,
        projection,
        limits,
    )?;
    let captured =
        super::read::capture_checked(source, query.query(), projection, &receipt, limits, check)?;
    Ok(RegisteredGraphArEntityProperties {
        captured,
        snapshot: *query.snapshot_root(),
        descriptor: *descriptor,
    })
}
