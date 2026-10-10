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
    let refusal = backend
        .prepare_resource_fallible::<(), _>(8, || Err("driver-refusal"))
        .await;
    assert!(matches!(
        refusal,
        Err(mrr_data_backend::ResourcePreparationError::Preparation(
            "driver-refusal"
        ))
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

#[tokio::test]
async fn controlled_stops_refuse_before_driver_and_keep_first_reason() {
    use mrr_data_backend::{ResourceControl, ResourcePreparationError, ResourceStop};
    let (backend, _) = open(BackendConfig::default()).await;
    let cancelled = ResourceControl::default();
    cancelled.cancel();
    let expired = ResourceControl::new(Some(std::time::Instant::now()));
    for (control, reason) in [
        (cancelled, ResourceStop::Cancelled),
        (expired, ResourceStop::Deadline),
    ] {
        let result = backend
            .prepare_resource_controlled::<(), ResourceStop>(8, control.clone(), |_| {
                panic!("stopped request reached physical driver")
            })
            .await;
        assert!(
            matches!(result, Err(ResourcePreparationError::Preparation(stop)) if stop == reason)
        );
        control.cancel();
        assert_eq!(control.check(), Err(reason));
        assert_eq!(backend.status().active_resources, 0);
    }
    backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn caller_abort_signals_running_control_but_cleanup_keeps_admission() {
    use mrr_data_backend::{ResourceControl, ResourceStop};
    let (backend, _) = open(BackendConfig::default()).await;
    let control = ResourceControl::default();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let worker_gate = gate.clone();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
    let running_backend = backend.clone();
    let worker_control = control.clone();
    let running = tokio::spawn(async move {
        running_backend
            .prepare_resource_controlled::<(), ResourceStop>(8, worker_control, move |control| {
                entered_tx.send(()).unwrap();
                // Simulate an in-flight native operation and its cleanup. The stop
                // cannot release worker admission until this operation completes.
                let (lock, changed) = &*worker_gate;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = changed.wait(released).unwrap();
                }
                let refusal = control.check();
                cleanup_tx.send(()).unwrap();
                refusal
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), entered_rx)
        .await
        .unwrap()
        .unwrap();
    running.abort();
    let _ = running.await;
    assert_eq!(control.check(), Err(ResourceStop::Cancelled));
    assert_eq!(backend.status().active_resources, 1);
    assert_eq!(backend.status().blocking_resources, 1);
    let (lock, changed) = &*gate;
    *lock.lock().unwrap() = true;
    changed.notify_all();
    tokio::time::timeout(Duration::from_secs(3), cleanup_rx)
        .await
        .unwrap()
        .unwrap();
    until(|| backend.status().active_resources == 0).await;
    assert_eq!(backend.status().resource_bytes, 0);
    backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_queued_stops_return_before_held_worker_releases() {
    use mrr_data_backend::{ResourceControl, ResourcePreparationError, ResourceStop};
    for resource_workers in [1, 2] {
        let (backend, _) = open(BackendConfig {
            max_resource_workers: resource_workers,
            max_shared_workers: 1,
            ..BackendConfig::default()
        })
        .await;
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let worker_gate = gate.clone();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let held_backend = backend.clone();
        let held = tokio::spawn(async move {
            held_backend
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
        for expired in [false, true] {
            let control = ResourceControl::new(
                expired.then(|| std::time::Instant::now() + Duration::from_millis(50)),
            );
            let waiting_backend = backend.clone();
            let queued_control = control.clone();
            let queued = tokio::spawn(async move {
                waiting_backend
                    .prepare_resource_controlled::<(), ResourceStop>(8, queued_control, |_| {
                        panic!("queued stop reached driver")
                    })
                    .await
            });
            until(|| backend.status().active_resources == 2).await;
            if !expired {
                control.cancel();
            }
            let result = tokio::time::timeout(Duration::from_secs(3), queued).await;
            // Release the held operation even when the assertion fails.
            if result.is_err() {
                let (lock, changed) = &*gate;
                *lock.lock().unwrap() = true;
                changed.notify_all();
            }
            let result = result.unwrap().unwrap();
            let expected = if expired {
                ResourceStop::Deadline
            } else {
                ResourceStop::Cancelled
            };
            assert!(
                matches!(result, Err(ResourcePreparationError::Preparation(stop)) if stop == expected)
            );
            assert_eq!(backend.status().active_resources, 1);
            assert_eq!(backend.status().resource_bytes, 8);
        }
        let (lock, changed) = &*gate;
        *lock.lock().unwrap() = true;
        changed.notify_all();
        drop(held.await.unwrap().unwrap());
        backend.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn consuming_conversion_transfers_lease_and_refuses_shared_input() {
    use mrr_data_backend::ResourceTransformError;
    let (backend, _) = open(BackendConfig {
        max_resources: 1,
        max_resource_bytes: 16,
        ..BackendConfig::default()
    })
    .await;
    let input = backend
        .prepare_resource(16, || Ok(vec![1u8, 2]))
        .await
        .unwrap();
    let clone = input.clone();
    let Err(ResourceTransformError::Shared(input)) =
        input.try_transform::<usize, ()>(|_| panic!("shared input was consumed"))
    else {
        panic!("shared conversion was not refused");
    };
    drop(clone);
    let Ok(converted) = input.try_transform::<_, ()>(|bytes| Ok(bytes.len())) else {
        panic!("unique conversion failed");
    };
    assert_eq!(*converted.get(), 2);
    assert_eq!(backend.status().resource_bytes, 16);
    assert_eq!(backend.status().active_resources, 1);
    assert!(matches!(
        backend.prepare_resource(1, || Ok(())).await,
        Err(BackendError::Saturated)
    ));
    assert!(matches!(
        converted.try_transform::<(), _>(|_| Err("conversion refused")),
        Err(ResourceTransformError::Conversion("conversion refused"))
    ));
    assert_eq!(backend.status().active_resources, 0);
    assert_eq!(backend.status().resource_bytes, 0);
    backend.shutdown().await.unwrap();
}

mod async_resources;
