//! Wait asynchronously before occupying the Host's bounded blocking executor.
use crate::{
    BackendConfig, BackendError, ResourceHandle,
    scheduler::{Lease, ResourceLease},
};
use std::sync::Arc;
use tokio::{
    runtime::Handle,
    sync::{Semaphore, oneshot},
};

pub(crate) struct Dispatcher {
    writes: Arc<Semaphore>,
    recoveries: Arc<Semaphore>,
    resources: Arc<Semaphore>,
    runtime: Handle,
}
impl Dispatcher {
    pub(crate) fn new(config: BackendConfig, runtime: Handle) -> Self {
        Self {
            writes: Arc::new(Semaphore::new(config.max_write_workers)),
            recoveries: Arc::new(Semaphore::new(config.max_recovery_workers)),
            resources: Arc::new(Semaphore::new(config.max_resource_workers)),
            runtime,
        }
    }
    pub(crate) fn prepare<T: Send + Sync + 'static>(
        &self,
        lease: ResourceLease,
        run: impl FnOnce() -> Result<T, BackendError> + Send + 'static,
    ) -> oneshot::Receiver<Result<ResourceHandle<T>, BackendError>> {
        let (mut tx, rx) = oneshot::channel();
        let slots = self.resources.clone();
        let runtime = self.runtime.clone();
        self.runtime.spawn(async move {
            let permit = tokio::select! {
                biased;
                () = tx.closed() => return,
                permit = slots.acquire_owned() => permit.expect("resource slots never closed"),
            };
            let mut lease = lease.submitted();
            runtime.spawn_blocking(move || {
                if tx.is_closed() {
                    return;
                }
                let result = run();
                lease.finished();
                // A failed preparation releases its lease before waking a waiter.
                let result = match result {
                    Ok(value) => Ok(ResourceHandle::new(value, lease)),
                    Err(error) => {
                        drop(lease);
                        Err(error)
                    }
                };
                drop(permit);
                let _ = tx.send(result);
            });
        });
        rx
    }
    pub(crate) fn run<T: Send + 'static>(
        &self,
        recovery: bool,
        lease: Lease,
        run: impl FnOnce() -> T + Send + 'static,
    ) -> oneshot::Receiver<T> {
        let (tx, rx) = oneshot::channel();
        let slots = if recovery {
            &self.recoveries
        } else {
            &self.writes
        }
        .clone();
        let runtime = self.runtime.clone();
        // This task owns admission and the request before it begins waiting.
        // Dropping the caller cannot abandon administrative or validated work.
        self.runtime.spawn(async move {
            let permit = slots
                .acquire_owned()
                .await
                .expect("worker slots never closed");
            let lease = lease.submitted();
            runtime.spawn_blocking(move || {
                // Release accounting before a permit wakes the next job.
                let result = {
                    let _permit = permit;
                    let _lease = lease;
                    run()
                };
                // Success must not race the predecessor's admission release.
                let _ = tx.send(result);
            });
        });
        rx
    }
}
