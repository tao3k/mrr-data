use super::{CapturedCombinedGraphAr, CombinedGraphArLimits};
use crate::{GraphArEntityPropertyError as Error, GraphArEntityPropertyProjection};
use meta_relational_reasoning::RelationCatalog;
use mrr_data_backend::{
    Backend, BackendError, ResourceControl, ResourceHandle, ResourcePreparationError,
};
use mrr_data_content::PreparedCombinedGraph;
use mrr_data_core::BoundDataQuery;
/// One complete closure, one existing Backend reservation and lifecycle.
pub struct CombinedGraphArRequest {
    pub closure: PreparedCombinedGraph,
    pub query: BoundDataQuery,
    pub relations: RelationCatalog,
    pub properties: GraphArEntityPropertyProjection,
    pub limits: CombinedGraphArLimits,
}
/// Run native work on the shared bounded scheduler with cooperative control.
/// Native calls are nonpreemptible; controls are checked between phases/files.
/// Reserve physical payload plus Host-chosen headroom for decoded/native scratch.
/// # Errors
/// Refuses insufficient reservation, scheduling/control, scope or native capture failures.
pub async fn prepare_combined_graphar(
    backend: &Backend,
    request: CombinedGraphArRequest,
    reserved_bytes: usize,
    control: ResourceControl,
) -> Result<ResourceHandle<CapturedCombinedGraphAr>, ResourcePreparationError<Error>> {
    if reserved_bytes < request.closure.total_bytes() {
        return Err(ResourcePreparationError::Backend(BackendError::Limit));
    }
    backend
        .prepare_resource_controlled(reserved_bytes, control, move |control| {
            super::capture::capture_checked(
                &request.closure,
                &request.query,
                &request.relations,
                &request.properties,
                request.limits,
                || control.check().map_err(Error::Stop),
            )
        })
        .await
}
