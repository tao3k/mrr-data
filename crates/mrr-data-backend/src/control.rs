//! Cooperative stop requests; native interrupt remains a driver capability.
use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};
use std::time::Instant;

/// A sticky stop reason observed at a physical execution checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceStop {
    Cancelled,
    Deadline,
}
impl std::fmt::Display for ResourceStop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ResourceStop {}

/// Shared request control on the Host's monotonic clock. A deadline is checked
/// at driver checkpoints; it does not interrupt an in-flight native call.
#[derive(Clone, Default)]
pub struct ResourceControl(Arc<State>);
#[derive(Default)]
struct State {
    reason: AtomicU8,
    deadline: Option<Instant>,
    changed: tokio::sync::Notify,
}
impl ResourceControl {
    #[must_use]
    pub fn new(deadline: Option<Instant>) -> Self {
        Self(Arc::new(State {
            reason: AtomicU8::new(0),
            deadline,
            changed: tokio::sync::Notify::new(),
        }))
    }
    /// Request cancellation. The first observed stop reason wins.
    pub fn cancel(&self) {
        let reason = if self
            .0
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            2
        } else {
            1
        };
        let _ = self
            .0
            .reason
            .compare_exchange(0, reason, Ordering::AcqRel, Ordering::Acquire);
        self.0.changed.notify_waiters();
    }
    /// Check for cancellation/deadline without creating a runtime or timer.
    /// # Errors
    /// Returns the first observed stop reason, permanently for this request.
    pub fn check(&self) -> Result<(), ResourceStop> {
        if self
            .0
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            let _ = self
                .0
                .reason
                .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire);
        }
        match self.0.reason.load(Ordering::Acquire) {
            0 => Ok(()),
            1 => Err(ResourceStop::Cancelled),
            _ => Err(ResourceStop::Deadline),
        }
    }
    /// Wait for the sticky explicit stop or Host-clock deadline on the current
    /// runtime. Drivers may use this to signal native interrupt; the wait does
    /// not release admission or establish that native cleanup has completed.
    pub async fn stopped(&self) -> ResourceStop {
        loop {
            let notified = self.0.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Err(stop) = self.check() {
                return stop;
            }
            if let Some(deadline) = self.0.deadline {
                tokio::select! {
                    () = &mut notified => {},
                    () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {},
                }
            } else {
                notified.await;
            }
        }
    }
}
pub(crate) struct CancelOnDrop {
    control: ResourceControl,
    armed: bool,
}
impl CancelOnDrop {
    pub(crate) fn new(control: ResourceControl) -> Self {
        Self {
            control,
            armed: true,
        }
    }
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.control.cancel();
        }
    }
}
