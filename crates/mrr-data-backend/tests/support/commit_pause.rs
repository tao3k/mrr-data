//! Bounded test pauses around the real physical transaction; no replacement store.
use mrr_data_backend::{
    BackendError,
    providers::{MetadataTransaction, ProviderResult, TransactionProvider, TursoProvider},
};
use mrr_data_content::ConditionalCommitPortError as PortError;
use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum PausePoint {
    BeforeDecision,
    BeforeCommit,
    CommittedReplyLost,
}
#[derive(Default)]
pub(crate) struct Pause {
    armed: AtomicBool,
    entered: AtomicBool,
    release: Mutex<bool>,
    changed: Condvar,
}
impl Pause {
    pub(crate) fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }
    pub(crate) fn block(&self) -> bool {
        self.entered.store(true, Ordering::Release);
        let released = self.release.lock().unwrap();
        *self
            .changed
            .wait_timeout_while(released, Duration::from_secs(5), |v| !*v)
            .unwrap()
            .0
    }
    pub(crate) async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !self.entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fixed pause rendezvous bound");
    }
    pub(crate) fn release(&self) {
        *self.release.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
pub(crate) struct Release(pub(crate) Arc<Pause>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
pub(crate) struct PausedStorage {
    pub(crate) native: Arc<TursoProvider>,
    pub(crate) pause: Arc<Pause>,
    pub(crate) point: PausePoint,
    pub(crate) closes: Arc<AtomicUsize>,
}
impl TransactionProvider for PausedStorage {
    fn open_storage(&self) -> Result<(), BackendError> {
        self.native.open_storage()
    }
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        self.native.read(key)
    }
    fn close_storage(&self) -> Result<(), BackendError> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        self.native.close_storage()
    }
    fn transaction(
        &self,
        run: &mut dyn FnMut(&mut dyn MetadataTransaction) -> ProviderResult<()>,
    ) -> ProviderResult<()> {
        let selected = self.pause.armed.swap(false, Ordering::AcqRel);
        self.native.transaction(&mut |tx| {
            if selected && self.point == PausePoint::BeforeDecision && !self.pause.block() {
                return Err(PortError::BeforeCommit(BackendError::Cancelled));
            }
            run(tx)?;
            if selected && self.point == PausePoint::BeforeCommit && !self.pause.block() {
                return Err(PortError::BeforeCommit(BackendError::Cancelled));
            }
            Ok(())
        })?;
        if selected && self.point == PausePoint::CommittedReplyLost {
            self.pause.block();
            // Durable success was observed internally; reply loss is still Unknown to the caller.
            return Err(PortError::Unknown(BackendError::Unavailable));
        }
        Ok(())
    }
}
