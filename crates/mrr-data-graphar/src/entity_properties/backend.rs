//! Optional composition with the Host's single shared Backend scheduler.
use super::{
    CapturedGraphArEntityProperties, GraphArEntityPropertyError, GraphArEntityPropertyLimits,
    GraphArEntityPropertyProjection, GraphArEntityPropertyReceipt,
};
use meta_relational_reasoning::CatalogBoundQuery;
use mrr_data_backend::{
    Backend, BackendError, ResourceControl, ResourceHandle, ResourcePreparationError,
};
use std::path::PathBuf;

/// Host-owned query scope and authenticated local property source.
pub struct GraphArEntityPropertiesRequest {
    pub source: PathBuf,
    pub query: CatalogBoundQuery,
    pub projection: GraphArEntityPropertyProjection,
    pub receipt: GraphArEntityPropertyReceipt,
    pub limits: GraphArEntityPropertyLimits,
}
/// Capture on the shared pool with retained buffer ownership and cooperative
/// stops between files/native type reads. Native calls remain nonpreemptible.
/// Host reservations cover physical bytes plus decoded/native scratch; they
/// are contractual ownership accounting, not a hard RSS limit.
/// # Errors
/// Preserves typed physical refusals and shared admission/cancel/deadline failures.
pub async fn prepare_graphar_entity_properties(
    backend: &Backend,
    request: GraphArEntityPropertiesRequest,
    reserved_bytes: usize,
    control: ResourceControl,
) -> Result<
    ResourceHandle<CapturedGraphArEntityProperties>,
    ResourcePreparationError<GraphArEntityPropertyError>,
> {
    let physical = request
        .receipt
        .inventory()
        .files()
        .iter()
        .try_fold(0usize, |total, file| {
            let bytes = usize::try_from(file.byte_length()).map_err(|_| BackendError::Limit)?;
            total.checked_add(bytes).ok_or(BackendError::Limit)
        })
        .map_err(ResourcePreparationError::Backend)?;
    if reserved_bytes < physical {
        return Err(ResourcePreparationError::Backend(BackendError::Limit));
    }
    backend
        .prepare_resource_controlled(reserved_bytes, control, move |control| {
            super::read::capture_checked(
                &request.source,
                &request.query,
                &request.projection,
                &request.receipt,
                request.limits,
                || control.check().map_err(GraphArEntityPropertyError::Stop),
            )
        })
        .await
}
