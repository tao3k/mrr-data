//! Optional composition with the Host's single shared Backend.
use crate::{
    BinaryEntityProjection, CapturedGraphArSnapshot, GraphArReadLimits, capture_graphar_snapshot,
};
use mrr_data_backend::{Backend, BackendError, ResourceHandle};
use mrr_data_core::{
    BoundDataQuery, GraphDatasetBinding, GraphDatasetInventory, GraphInventoryLimits,
};
use std::path::PathBuf;
/// Owned preparation request. The Host authenticates the binding root before
/// constructing this request; this adapter does not select publication authority.
pub struct GraphArSnapshotRequest {
    pub source: PathBuf,
    pub query: BoundDataQuery,
    pub binding: GraphDatasetBinding,
    pub inventory: GraphDatasetInventory,
    pub projection: BinaryEntityProjection,
    pub inventory_limits: GraphInventoryLimits,
    pub read_limits: GraphArReadLimits,
}
/// Capture using shared admission, blocking slots, reservations and shutdown.
/// The reservation must cover at least declared physical bytes. The Host also
/// budgets fact materialization/native decompression; reservation is not RSS.
/// # Errors
/// Refuses lifecycle/budget limits or invalid semantic/physical content.
pub async fn prepare_graphar_snapshot(
    backend: &Backend,
    request: GraphArSnapshotRequest,
    reserved_bytes: usize,
) -> Result<ResourceHandle<CapturedGraphArSnapshot>, BackendError> {
    let physical_bytes = request
        .inventory
        .files()
        .iter()
        .try_fold(0usize, |total, file| {
            let size = usize::try_from(file.byte_length()).map_err(|_| BackendError::Limit)?;
            total.checked_add(size).ok_or(BackendError::Limit)
        })?;
    if reserved_bytes < physical_bytes {
        return Err(BackendError::Limit);
    }
    request
        .binding
        .admit_query(&request.query, &request.inventory, request.inventory_limits)
        .map_err(|_| BackendError::Corrupt)?;
    backend
        .prepare_resource(reserved_bytes, move || {
            capture_graphar_snapshot(
                &request.source,
                &request.query,
                request.binding,
                &request.inventory,
                &request.projection,
                request.inventory_limits,
                request.read_limits,
            )
            .map_err(|_| BackendError::Corrupt)
        })
        .await
}

#[cfg(feature = "selective-graphar")]
#[path = "backend_selective.rs"]
mod selective;
#[cfg(feature = "selective-graphar")]
pub use selective::{
    GraphArOutgoingRequest, GraphArSelectiveSnapshotRequest, prepare_graphar_outgoing,
    prepare_graphar_selective_snapshot,
};
