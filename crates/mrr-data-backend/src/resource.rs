//! Retained preparation results share the engine's admission and drain barrier.
use crate::scheduler::ResourceLease;
use std::sync::Arc;
/// Borrow results while retaining their Backend reservation. Clones share a lease.
/// Resource implementations must keep their buffers private and avoid exporting
/// ownership that outlives this handle. Reservations are Host estimates, not RSS.
pub struct ResourceHandle<T>(Arc<Retained<T>>);
struct Retained<T> {
    // Drop the physical resource before returning the reservation to admission.
    value: T,
    _lease: ResourceLease,
}
impl<T> Clone for ResourceHandle<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T> ResourceHandle<T> {
    pub(crate) fn new(value: T, lease: ResourceLease) -> Self {
        Self(Arc::new(Retained {
            value,
            _lease: lease,
        }))
    }
    #[must_use]
    pub fn get(&self) -> &T {
        &self.0.value
    }
}

/// Distinguish shared Backend admission/worker failure from a typed driver refusal.
#[derive(Debug)]
pub enum ResourcePreparationError<E> {
    Backend(crate::BackendError),
    Preparation(E),
}

impl<E: std::fmt::Display> std::fmt::Display for ResourcePreparationError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend(error) => write!(f, "resource backend: {error}"),
            Self::Preparation(error) => write!(f, "resource preparation: {error}"),
        }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for ResourcePreparationError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Backend(error) => Some(error),
            Self::Preparation(error) => Some(error),
        }
    }
}
