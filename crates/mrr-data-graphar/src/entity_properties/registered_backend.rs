//! Physical root admission uses the same shared scheduler and reservation lease.
use super::{
    GraphArEntityPropertyError as Error, GraphArEntityPropertyLimits,
    GraphArEntityPropertyProjection, GraphArEntityPropertyReceipt,
    RegisteredGraphArEntityProperties,
};
use mrr_data_backend::{
    Backend, BackendError, ResourceControl, ResourceHandle, ResourcePreparationError,
};
use mrr_data_core::BoundDataQuery;
use std::path::PathBuf;

/// Root-bound request with explicit, bounded canonical descriptor bytes.
pub struct RegisteredGraphArEntityPropertiesRequest {
    pub source: PathBuf,
    pub query: BoundDataQuery,
    pub projection: GraphArEntityPropertyProjection,
    pub descriptor_bytes: Vec<u8>,
    pub limits: GraphArEntityPropertyLimits,
}
/// Admit descriptor before scheduling native capture on the existing Backend.
/// Reserved bytes must cover the physical inventory; decoded/native scratch
/// remains part of the Host reservation contract, not a hard RSS ceiling.
/// # Errors
/// Preserves descriptor, scheduler, cancellation and native capture refusals.
pub async fn prepare_registered_graphar_entity_properties(
    backend: &Backend,
    request: RegisteredGraphArEntityPropertiesRequest,
    reserved_bytes: usize,
    control: ResourceControl,
) -> Result<ResourceHandle<RegisteredGraphArEntityProperties>, ResourcePreparationError<Error>> {
    let descriptor = request
        .query
        .graph_projection_manifest()
        .ok_or(ResourcePreparationError::Preparation(Error::Scope))?;
    let receipt = GraphArEntityPropertyReceipt::decode_descriptor_checked(
        request.source.clone(),
        descriptor,
        &request.descriptor_bytes,
        &request.projection,
        request.limits,
    )
    .map_err(ResourcePreparationError::Preparation)?;
    let physical = receipt
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
            super::registered::capture_checked(
                &request.source,
                &request.query,
                &request.projection,
                &request.descriptor_bytes,
                request.limits,
                || control.check().map_err(Error::Stop),
            )
        })
        .await
}
