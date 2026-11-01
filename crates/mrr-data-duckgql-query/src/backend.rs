use crate::{DuckGqlArtifact, DuckGqlError, DuckGqlLimits};
use mrr_data_backend::{
    Backend, ResourceControl, ResourceHandle, ResourcePreparationError, ResourceStop,
};
use mrr_data_core::{BoundDataQuery, PhysicalQueryOutput};
use mrr_data_graphar::{BinaryEntityProjection, CapturedGraphArSnapshot};
use std::sync::Arc;
#[cfg(test)]
#[path = "../tests/unit/interrupt.rs"]
mod interrupt_tests;

/// The Host reservation includes captured input, conversion scratch, plugin
/// state and bounded output. It is an estimate, not a native RSS measurement.
pub struct DuckGqlBackendQuery {
    pub artifact: DuckGqlArtifact,
    pub query: BoundDataQuery,
    pub source: Arc<CapturedGraphArSnapshot>,
    pub projection: BinaryEntityProjection,
    pub limits: DuckGqlLimits,
    pub reserved_bytes: usize,
}
impl From<ResourceStop> for DuckGqlError {
    fn from(stop: ResourceStop) -> Self {
        match stop {
            ResourceStop::Cancelled => Self::Cancelled,
            ResourceStop::Deadline => Self::Deadline,
        }
    }
}
struct InterruptMonitor(tokio::task::JoinHandle<()>);
impl InterruptMonitor {
    fn start(
        runtime: &tokio::runtime::Handle,
        control: ResourceControl,
        connection: &duckdb::Connection,
    ) -> Self {
        let interrupt = connection.interrupt_handle();
        Self(runtime.spawn(async move {
            control.stopped().await;
            interrupt.interrupt();
        }))
    }
}
impl Drop for InterruptMonitor {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Shared admission and retained output, with native `DuckDB` interrupt signaling.
/// The Host runtime drives the monitor; the common worker owns every native call
/// through connection teardown. Interrupt signaling is not a hard time bound on
/// plugin code that does not check `DuckDB`'s interrupt flag.
/// # Errors
/// Refuses stops, admission/source/artifact drift, unsupported shapes and drivers.
pub async fn execute_duckgql_graphar_controlled_on_backend(
    backend: &Backend,
    runtime: tokio::runtime::Handle,
    request: DuckGqlBackendQuery,
    control: ResourceControl,
) -> Result<ResourceHandle<PhysicalQueryOutput>, DuckGqlError> {
    backend
        .prepare_resource_controlled(request.reserved_bytes, control, move |control| {
            crate::native::execute_checked(
                &request.artifact,
                &request.query,
                &request.source,
                &request.projection,
                &request.limits,
                &|| control.check().map_err(DuckGqlError::from),
                &|connection| {
                    Ok(InterruptMonitor::start(
                        &runtime,
                        control.clone(),
                        connection,
                    ))
                },
            )
        })
        .await
        .map_err(|error| match error {
            ResourcePreparationError::Backend(error) => DuckGqlError::Backend(error),
            ResourcePreparationError::Preparation(error) => error,
        })
}
