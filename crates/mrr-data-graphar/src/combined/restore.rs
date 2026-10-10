//! Async cold restoration shares Backend admission with every physical profile.
use crate::GraphArEntityPropertyError as Error;
use meta_relational_reasoning::{EntityCatalog, RelationCatalog};
use mrr_data_backend::{Backend, ResourceControl, ResourceHandle, ResourcePreparationError};
use mrr_data_content::{
    AsyncContentStore, GraphTransferLimits, PreparedCombinedGraph, RemoteContentStore,
    restore_combined_graph_checked,
};
use mrr_data_core::{BoundDataQuery, GraphDatasetLimits};
use std::sync::Arc;

/// Host-authenticated query and externally configured async content providers.
#[derive(Clone)]
pub struct CombinedGraphArRestoreRequest {
    pub local: Arc<dyn AsyncContentStore + Send + Sync>,
    pub remote: Arc<dyn RemoteContentStore + Send + Sync>,
    pub query: BoundDataQuery,
    pub relations: RelationCatalog,
    pub entities: EntityCatalog,
    pub dataset: GraphDatasetLimits,
    pub transfer: GraphTransferLimits,
}
/// Reserve before any cache/remote reads and retain admission with the restored closure.
/// Payload is capped by the smaller of transfer budget and reservation. Provider
/// copies, cache storage and decoded metadata require additional Host headroom.
/// Read-throughs are nonpreemptible; abandoned requests keep their drain barrier
/// until the current read-through finishes and observes its stop checkpoint.
/// # Errors
/// Refuses scheduler limits, typed stops, corrupt scope/children and transfer budgets.
pub async fn restore_combined_graph_content(
    backend: &Backend,
    request: CombinedGraphArRestoreRequest,
    reserved_bytes: usize,
    control: ResourceControl,
) -> Result<ResourceHandle<PreparedCombinedGraph>, ResourcePreparationError<Error>> {
    backend
        .prepare_resource_async_controlled(reserved_bytes, control, move |control| async move {
            restore_combined_graph_checked(
                request.local.as_ref(),
                request.remote.as_ref(),
                &request.query,
                (&request.relations, &request.entities),
                (
                    GraphTransferLimits {
                        max_total_bytes: request.transfer.max_total_bytes.min(reserved_bytes),
                        ..request.transfer
                    },
                    request.dataset,
                ),
                || control.check().map_err(Error::Stop),
            )
            .await
        })
        .await
}
