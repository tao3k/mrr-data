//! Acquire the shared reservation before content reads and native preparation.
use super::{CapturedCombinedGraphAr, CombinedGraphArLimits};
use crate::{GraphArEntityPropertyError as Error, GraphArEntityPropertyProjection};
use meta_relational_reasoning::{EntityCatalog, RelationCatalog};
use mrr_data_backend::{Backend, ResourceControl, ResourceHandle, ResourcePreparationError};
use mrr_data_content::{
    CombinedGraphInputs, ContentStore, GraphTransferLimits, PreparedCombinedGraph,
    prepare_combined_graph_checked,
};
use mrr_data_core::{BoundDataQuery, SnapshotBlock};
use std::sync::Arc;

/// A caller-authenticated snapshot and bounded local content provider.
/// Provider handles are supplied by the Host; this owner creates no runtime.
#[derive(Clone)]
pub struct CombinedGraphArContentRequest {
    pub local: Arc<dyn ContentStore + Send + Sync>,
    pub query: BoundDataQuery,
    pub snapshot: SnapshotBlock,
    pub relations: RelationCatalog,
    pub entities: EntityCatalog,
    pub properties: GraphArEntityPropertyProjection,
    pub limits: CombinedGraphArLimits,
    pub transfer: GraphTransferLimits,
}
impl CombinedGraphArContentRequest {
    fn prepare(
        &self,
        reserved_bytes: usize,
        control: &ResourceControl,
    ) -> Result<PreparedCombinedGraph, Error> {
        prepare_combined_graph_checked(
            self.local.as_ref(),
            CombinedGraphInputs {
                query: &self.query,
                snapshot: &self.snapshot,
                relations: &self.relations,
                entities: &self.entities,
                dataset_limits: self.limits.dataset,
                limits: GraphTransferLimits {
                    max_total_bytes: self.transfer.max_total_bytes.min(reserved_bytes),
                    ..self.transfer
                },
            },
            || control.check().map_err(Error::Stop),
        )
    }
}
/// Reserve before reading any content; retain the closure lease across async publication.
/// Effective payload bytes cannot exceed either transfer or reservation limits.
/// Decoded metadata/provider scratch remain part of the Host's headroom contract.
/// # Errors
/// Returns typed scheduler/stop/content refusals without exporting a partial closure.
pub async fn prepare_combined_graph_content(
    backend: &Backend,
    request: CombinedGraphArContentRequest,
    reserved_bytes: usize,
    control: ResourceControl,
) -> Result<ResourceHandle<PreparedCombinedGraph>, ResourcePreparationError<Error>> {
    backend
        .prepare_resource_controlled(reserved_bytes, control, move |control| {
            request.prepare(reserved_bytes, control)
        })
        .await
}
/// Read content and capture native tables/facts under one reservation on one worker.
/// Content bytes are freed before returning the captured output. Cancellation
/// checkpoints surround each content read and native phase; calls are nonpreemptible.
/// # Errors
/// Preserves scheduler, cancellation, payload-budget, integrity and native refusals.
pub async fn prepare_combined_graphar_from_content(
    backend: &Backend,
    request: CombinedGraphArContentRequest,
    reserved_bytes: usize,
    control: ResourceControl,
) -> Result<ResourceHandle<CapturedCombinedGraphAr>, ResourcePreparationError<Error>> {
    backend
        .prepare_resource_controlled(reserved_bytes, control, move |control| {
            let closure = request.prepare(reserved_bytes, control)?;
            super::capture::capture_checked(
                &closure,
                &request.query,
                &request.relations,
                &request.properties,
                request.limits,
                || control.check().map_err(Error::Stop),
            )
        })
        .await
}
