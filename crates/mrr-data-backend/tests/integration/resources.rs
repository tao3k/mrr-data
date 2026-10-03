//! Shared resource admission, retained ownership and cancellation qualification.
use mrr_data_backend::{
    AuthorityCapability, Backend, BackendConfig, BackendError, Lifecycle, MetadataProvider,
    ProviderCapabilities, StoredOutcome, StoredRevision, StoredWrite, providers::ProviderResult,
};
use mrr_data_content::{ContentRevision, PublishReceipt};
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
struct TestProvider(Arc<AtomicUsize>);
impl MetadataProvider for TestProvider {
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            atomic_head_operation: true,
            durable_commit: true,
            historical_lookup: true,
            authority_versions: AuthorityCapability::Unsupported,
        }
    }
    fn open(&self) -> Result<(), BackendError> {
        Ok(())
    }
    fn close(&self) -> Result<(), BackendError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn commit(
        &self,
        _: &StoredWrite,
        _: Option<&PublishReceipt>,
        _: &mut dyn FnMut(Option<ContentRevision>) -> bool,
    ) -> ProviderResult<StoredOutcome> {
        panic!("resource tests do not exercise metadata")
    }
    fn recover(&self, _: &StoredWrite) -> ProviderResult<Option<StoredRevision>> {
        panic!("resource tests do not exercise metadata")
    }
}
async fn open(config: BackendConfig) -> (Backend, Arc<AtomicUsize>) {
    let closes = Arc::new(AtomicUsize::new(0));
    let backend = Backend::open(
        config,
        TestProvider(closes.clone()),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    (backend, closes)
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn final_clone_holds_shared_drain_and_resource_budget() {
    let (backend, closes) = open(BackendConfig {
        max_resources: 1,
        max_resource_bytes: 16,
        ..BackendConfig::default()
    })
    .await;
    let resource = backend
        .prepare_resource(16, || Ok(vec![1u8, 2]))
        .await
        .unwrap();
    let other = resource.clone();
    assert_eq!(other.get(), &[1, 2]);
    assert!(matches!(
        backend.prepare_resource(1, || Ok(())).await,
        Err(BackendError::Saturated)
    ));
    assert_eq!(backend.status().resource_bytes, 16);
    assert_eq!(backend.status().blocking_resources, 0);
    let closing = backend.clone();
    let task = tokio::spawn(async move { closing.shutdown().await });
    until(|| backend.status().lifecycle == Lifecycle::Draining).await;
    assert!(matches!(
        backend.prepare_resource(1, || Ok(())).await,
        Err(BackendError::NotReady)
    ));
    drop(resource);
    assert_eq!(backend.status().active_resources, 1);
    assert_eq!(closes.load(Ordering::SeqCst), 0);
    drop(other);
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(backend.status().resource_bytes, 0);
    assert_eq!(backend.status().lifecycle, Lifecycle::Closed);
    assert_eq!(closes.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn failure_and_panicking_workers_return_all_reservations() {
    let (backend, _) = open(BackendConfig::default()).await;
    assert!(matches!(
        backend
            .prepare_resource::<()>(8, || Err(BackendError::Corrupt))
            .await,
        Err(BackendError::Corrupt)
    ));
    assert_eq!(backend.status().active_resources, 0);
    assert!(matches!(
        backend
            .prepare_resource::<()>(8, || panic!("worker failure"))
            .await,
        Err(BackendError::WorkerLost)
    ));
    until(|| backend.status().active_resources == 0).await;
    assert_eq!(backend.status().blocking_resources, 0);
    assert_eq!(backend.status().resource_bytes, 0);
    backend.shutdown().await.unwrap();
}
#[tokio::test]
async fn cancelled_queue_skips_work_and_running_cancellation_keeps_worker_lease() {
    let (backend, _) = open(BackendConfig::default()).await;
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let running_backend = backend.clone();
    let worker_gate = gate.clone();
    let running = tokio::spawn(async move {
        running_backend
            .prepare_resource(8, move || {
                entered_tx.send(()).unwrap();
                let (lock, changed) = &*worker_gate;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = changed.wait(released).unwrap();
                }
                Ok(())
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), entered_rx)
        .await
        .unwrap()
        .unwrap();
    let queued_backend = backend.clone();
    let executed = Arc::new(AtomicUsize::new(0));
    let mark = executed.clone();
    let queued = tokio::spawn(async move {
        queued_backend
            .prepare_resource(8, move || {
                mark.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await
    });
    until(|| backend.status().active_resources == 2).await;
    queued.abort();
    let _ = queued.await;
    until(|| backend.status().active_resources == 1).await;
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    running.abort();
    let _ = running.await;
    assert_eq!(backend.status().active_resources, 1);
    assert_eq!(backend.status().blocking_resources, 1);
    let (lock, changed) = &*gate;
    *lock.lock().unwrap() = true;
    changed.notify_all();
    until(|| backend.status().active_resources == 0).await;
    assert_eq!(backend.status().resource_bytes, 0);
    backend.shutdown().await.unwrap();
}
