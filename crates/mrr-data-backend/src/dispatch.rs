//! Wait asynchronously before occupying the Host's bounded blocking executor.
use crate::{BackendConfig, scheduler::Lease};
use std::sync::Arc;
use tokio::{runtime::Handle, sync::Semaphore};

pub(crate) struct Dispatcher {
    writes: Arc<Semaphore>,
    recoveries: Arc<Semaphore>,
    runtime: Handle,
}
impl Dispatcher {
    pub(crate) fn new(config: BackendConfig, runtime: Handle) -> Self {
        Self {
            writes: Arc::new(Semaphore::new(config.max_write_workers)),
            recoveries: Arc::new(Semaphore::new(config.max_recovery_workers)),
            runtime,
        }
    }
    pub(crate) fn run(&self, recovery: bool, lease: Lease, run: impl FnOnce() + Send + 'static) {
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
                let _permit = permit;
                let _lease = lease;
                run();
            });
        });
    }
}
