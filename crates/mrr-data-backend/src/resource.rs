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
