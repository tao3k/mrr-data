//! Ordered snapshot preparation and range reads on the shared Backend.
use crate::BinaryEntityProjection;
use crate::{
    GraphArChunkLayout, GraphArSelection, GraphArSelectiveCaptureOptions, GraphArSelectiveError,
    GraphArSelectiveSnapshot, capture_graphar_selective_snapshot_checked,
};
use meta_relational_reasoning::EntityId;
use mrr_data_backend::{Backend, BackendError, ResourceHandle};
use mrr_data_backend::{ResourceControl, ResourcePreparationError};
use mrr_data_core::{
    BoundDataQuery, GraphDatasetBinding, GraphDatasetInventory, GraphInventoryLimits,
};
use std::path::PathBuf;

/// Host-owned source request for the controlled ordered physical profile.
pub struct GraphArSelectiveSnapshotRequest {
    pub source: PathBuf,
    pub query: BoundDataQuery,
    pub binding: GraphDatasetBinding,
    pub inventory: GraphDatasetInventory,
    pub projection: BinaryEntityProjection,
    pub inventory_limits: GraphInventoryLimits,
    pub max_vertices: usize,
    pub layout: GraphArChunkLayout,
}
/// Physical outgoing access selected by an MRR-owned execution plan. This
/// adapter never parses GQL or chooses semantic result admission.
pub struct GraphArOutgoingRequest {
    pub query: BoundDataQuery,
    pub projection: BinaryEntityProjection,
    pub source: EntityId,
    pub max_edges: usize,
}
/// Prepare a private file snapshot under shared Backend admission. The Host
/// reserves physical bytes plus vertex/offset indexing and native scratch;
/// this contractual reservation is not a native RSS ceiling.
/// # Errors
/// Separates admission/stop failures from physical/semantic source refusals.
pub async fn prepare_graphar_selective_snapshot(
    backend: &Backend,
    request: GraphArSelectiveSnapshotRequest,
    reserved_bytes: usize,
    control: ResourceControl,
) -> Result<ResourceHandle<GraphArSelectiveSnapshot>, ResourcePreparationError<GraphArSelectiveError>>
{
    let physical_bytes = request
        .inventory
        .files()
        .iter()
        .try_fold(0usize, |total, file| {
            let size = usize::try_from(file.byte_length()).map_err(|_| BackendError::Limit)?;
            total.checked_add(size).ok_or(BackendError::Limit)
        })
        .map_err(ResourcePreparationError::Backend)?;
    if reserved_bytes < physical_bytes {
        return Err(ResourcePreparationError::Backend(BackendError::Limit));
    }
    backend
        .prepare_resource_controlled(reserved_bytes, control, move |control| {
            capture_graphar_selective_snapshot_checked(
                crate::GraphArSelectiveCaptureRequest {
                    source: &request.source,
                    query: &request.query,
                    binding: request.binding,
                    inventory: &request.inventory,
                    projection: &request.projection,
                    options: GraphArSelectiveCaptureOptions {
                        inventory_limits: request.inventory_limits,
                        max_vertices: request.max_vertices,
                        layout: request.layout,
                    },
                },
                || control.check().map_err(Into::into),
            )
        })
        .await
}
/// Run range reads on the same worker pool; source and output keep distinct
/// retained leases. A dropped/failed waiter releases work only after cleanup.
/// # Errors
/// Refuses scope, output budget, malformed ranges and cooperative stops.
pub async fn prepare_graphar_outgoing(
    backend: &Backend,
    snapshot: ResourceHandle<GraphArSelectiveSnapshot>,
    request: GraphArOutgoingRequest,
    reserved_bytes: usize,
    control: ResourceControl,
) -> Result<ResourceHandle<GraphArSelection>, ResourcePreparationError<GraphArSelectiveError>> {
    backend
        .prepare_resource_controlled(reserved_bytes, control, move |control| {
            snapshot.get().outgoing_checked(
                &request.query,
                &request.projection,
                request.source,
                request.max_edges,
                || control.check().map_err(Into::into),
            )
        })
        .await
}
