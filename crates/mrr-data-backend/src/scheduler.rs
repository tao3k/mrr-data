//! Atomic lifecycle admission and tracked blocking worker leases.
use crate::{BackendConfig, BackendError, BackendStatus, Lifecycle};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
pub(crate) struct Scheduler {
    pub(crate) config: BackendConfig,
    state: Mutex<BackendStatus>,
    pub(crate) changed: Notify,
}
impl Scheduler {
    pub(crate) fn new(config: BackendConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            state: Mutex::new(BackendStatus {
                lifecycle: Lifecycle::Ready,
                active_resources: 0,
                blocking_resources: 0,
                resource_bytes: 0,
                saturated_resources: 0,
                active_writes: 0,
                active_recoveries: 0,
                blocking_writes: 0,
                blocking_recoveries: 0,
                retained_bytes: 0,
                completed: 0,
                saturated_writes: 0,
                saturated_recoveries: 0,
            }),
            changed: Notify::new(),
        })
    }
    pub(crate) fn status(&self) -> BackendStatus {
        *self.state.lock().expect("scheduler lock")
    }
    pub(crate) fn admit(
        self: &Arc<Self>,
        recovery: bool,
        bytes: usize,
    ) -> Result<Lease, BackendError> {
        let mut s = self.state.lock().map_err(|_| BackendError::Unavailable)?;
        if s.lifecycle != Lifecycle::Ready {
            return Err(BackendError::NotReady);
        }
        let (active, limit) = if recovery {
            (s.active_recoveries, self.config.max_recoveries)
        } else {
            (s.active_writes, self.config.max_writes)
        };
        // Recovery has its own count AND byte reservation so saturated fresh work
        // cannot consume the resources needed to reconcile already accepted work.
        let lane_bytes = if recovery {
            self.config.max_recoveries.saturating_mul(65536)
        } else {
            self.config.max_retained_bytes
        };
        let used = if recovery {
            s.active_recoveries.saturating_mul(65536)
        } else {
            s.retained_bytes
        };
        if active >= limit || bytes > lane_bytes.saturating_sub(used) {
            let refused = if recovery {
                &mut s.saturated_recoveries
            } else {
                &mut s.saturated_writes
            };
            *refused = refused.saturating_add(1);
            return Err(BackendError::Saturated);
        }
        if recovery {
            s.active_recoveries += 1;
        } else {
            s.active_writes += 1;
            s.retained_bytes += bytes;
        }
        Ok(Lease {
            scheduler: self.clone(),
            recovery,
            bytes,
            submitted: false,
        })
    }
    pub(crate) fn admit_resource(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<ResourceLease, BackendError> {
        if bytes == 0 {
            return Err(BackendError::Limit);
        }
        let mut state = self.state.lock().map_err(|_| BackendError::Unavailable)?;
        if state.lifecycle != Lifecycle::Ready {
            return Err(BackendError::NotReady);
        }
        if state.active_resources >= self.config.max_resources
            || bytes
                > self
                    .config
                    .max_resource_bytes
                    .saturating_sub(state.resource_bytes)
        {
            state.saturated_resources = state.saturated_resources.saturating_add(1);
            return Err(BackendError::Saturated);
        }
        state.active_resources += 1;
        state.resource_bytes += bytes;
        Ok(ResourceLease {
            scheduler: self.clone(),
            bytes,
            submitted: false,
        })
    }
    pub(crate) fn drain(&self) {
        let mut s = self.state.lock().expect("scheduler lock");
        if s.lifecycle == Lifecycle::Ready {
            s.lifecycle = Lifecycle::Draining;
        }
    }
    pub(crate) fn lifecycle(&self, phase: Lifecycle) {
        self.state.lock().expect("scheduler lock").lifecycle = phase;
        self.changed.notify_waiters();
    }
}

#[cfg(test)]
#[path = "../tests/unit/scheduler.rs"]
mod tests;
pub(crate) struct Lease {
    scheduler: Arc<Scheduler>,
    recovery: bool,
    bytes: usize,
    submitted: bool,
}
impl Lease {
    pub(crate) fn submitted(mut self) -> Self {
        let mut state = self.scheduler.state.lock().expect("scheduler lock");
        if self.recovery {
            state.blocking_recoveries += 1;
        } else {
            state.blocking_writes += 1;
        }
        drop(state);
        self.submitted = true;
        self
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut s = self.scheduler.state.lock().expect("scheduler lock");
        if self.submitted {
            if self.recovery {
                s.blocking_recoveries -= 1;
            } else {
                s.blocking_writes -= 1;
            }
        }
        if self.recovery {
            s.active_recoveries -= 1;
        } else {
            s.active_writes -= 1;
            s.retained_bytes -= self.bytes;
        }
        s.completed = s.completed.saturating_add(1);
        drop(s);
        self.scheduler.changed.notify_waiters();
    }
}

/// Keeps the reservation alive through final resource destruction.
pub(crate) struct ResourceLease {
    scheduler: Arc<Scheduler>,
    bytes: usize,
    submitted: bool,
}
impl ResourceLease {
    pub(crate) fn submitted(mut self) -> Self {
        self.scheduler
            .state
            .lock()
            .expect("scheduler lock")
            .blocking_resources += 1;
        self.submitted = true;
        self
    }
    pub(crate) fn finished(&mut self) {
        if self.submitted {
            self.scheduler
                .state
                .lock()
                .expect("scheduler lock")
                .blocking_resources -= 1;
            self.submitted = false;
            self.scheduler.changed.notify_waiters();
        }
    }
}
impl Drop for ResourceLease {
    fn drop(&mut self) {
        let mut state = self.scheduler.state.lock().expect("scheduler lock");
        if self.submitted {
            state.blocking_resources -= 1;
        }
        state.active_resources -= 1;
        state.resource_bytes -= self.bytes;
        state.completed = state.completed.saturating_add(1);
        drop(state);
        self.scheduler.changed.notify_waiters();
    }
}
