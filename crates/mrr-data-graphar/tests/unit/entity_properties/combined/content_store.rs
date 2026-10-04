//! Observes actual provider reads under the shared reservation, and injects stops.
use cid::Cid;
use mrr_data_backend::{Backend, ResourceControl};
use mrr_data_content::{ContentBlock, ContentError, ContentStore, MemoryContentStore};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

pub(super) struct ObservedStore {
    pub inner: Arc<MemoryContentStore>,
    pub backend: Backend,
    pub control: ResourceControl,
    pub reserved: usize,
    pub stop_after: usize,
    pub gate: Option<Arc<ReadGate>>,
    pub reads: AtomicUsize,
    pub caps: Mutex<Vec<usize>>,
}
impl ContentStore for ObservedStore {
    fn put(&self, block: ContentBlock<'_>) -> Result<Cid, ContentError> {
        self.inner.put(block)
    }
    fn get_bounded(&self, cid: &Cid, max_bytes: usize) -> Result<Vec<u8>, ContentError> {
        assert_eq!(self.backend.status().resource_bytes, self.reserved);
        assert_eq!(self.backend.status().active_resources, 1);
        assert!(max_bytes <= self.reserved);
        self.caps.lock().unwrap().push(max_bytes);
        let index = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        let result = self.inner.get_bounded(cid, max_bytes);
        if index == 1
            && let Some(gate) = &self.gate
        {
            gate.pause();
        }
        if index == self.stop_after {
            self.control.cancel();
        }
        result
    }
}

/// A real in-flight provider read held until the caller permits cleanup.
pub(super) struct ReadGate {
    released: Mutex<bool>,
    changed: std::sync::Condvar,
    pub entered: tokio::sync::Notify,
}
impl ReadGate {
    pub fn new() -> Self {
        Self {
            released: Mutex::new(false),
            changed: std::sync::Condvar::new(),
            entered: tokio::sync::Notify::new(),
        }
    }
    fn pause(&self) {
        let released = self.released.lock().unwrap();
        println!("content provider: first verified block held in an in-flight read");
        self.entered.notify_one();
        let (_released, timeout) = self
            .changed
            .wait_timeout_while(released, std::time::Duration::from_secs(3), |released| {
                !*released
            })
            .unwrap();
        assert!(
            !timeout.timed_out(),
            "test did not release the real content read"
        );
    }
    pub fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
