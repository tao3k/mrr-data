//! Test-only native adapter that stalls an already-validated transaction.
use mrr_data_backend::{
    AuthorityChange, AuthorityKey, AuthorityState, BackendError, MetadataProvider,
    ProviderCapabilities, StoredOutcome, StoredRevision, StoredWrite, providers::ProviderResult,
};
use mrr_data_content::{ContentRevision, PublishReceipt};
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};

pub(crate) struct Gate(pub(crate) Arc<(Mutex<bool>, Condvar)>);
impl Gate {
    pub(crate) fn release(&self) {
        let (lock, changed) = &*self.0;
        *lock.lock().unwrap() = true;
        changed.notify_all();
    }
}
impl Drop for Gate {
    fn drop(&mut self) {
        self.release();
    }
}
pub(crate) struct Held<P> {
    pub(crate) native: P,
    pub(crate) gate: Arc<(Mutex<bool>, Condvar)>,
    pub(crate) entered: Arc<AtomicBool>,
}
impl<P: MetadataProvider> MetadataProvider for Held<P> {
    fn capabilities(&self) -> ProviderCapabilities {
        self.native.capabilities()
    }
    fn open(&self) -> Result<(), BackendError> {
        self.native.open()
    }
    fn close(&self) -> Result<(), BackendError> {
        self.native.close()
    }
    fn recover(&self, w: &StoredWrite) -> ProviderResult<Option<StoredRevision>> {
        self.native.recover(w)
    }
    fn authority(&self, key: &AuthorityKey) -> ProviderResult<Option<AuthorityState>> {
        self.native.authority(key)
    }
    fn advance_authority(&self, change: &AuthorityChange) -> ProviderResult<AuthorityState> {
        self.native.advance_authority(change)
    }
    fn commit(
        &self,
        w: &StoredWrite,
        p: Option<&PublishReceipt>,
        v: &mut dyn FnMut(Option<ContentRevision>) -> bool,
    ) -> ProviderResult<StoredOutcome> {
        self.native.commit(w, p, &mut |head| {
            let accepted = v(head);
            if accepted && w.operation_id == "held" {
                self.entered.store(true, Ordering::Release);
                let (lock, changed) = &*self.gate;
                let mut release = lock.lock().unwrap();
                while !*release {
                    release = changed.wait(release).unwrap();
                }
            }
            accepted
        })
    }
}
