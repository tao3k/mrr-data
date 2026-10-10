//! Two profile ports share admission, recovery and final-consumer drain.
//! Physical preparation is simulated; metadata uses the selected native provider.
use super::{ack, native, write};
use mrr_data_backend::{
    Backend, BackendConfig, BackendError, Lifecycle, ProfilePort, ResourceControl,
    ResourcePreparationError, ResourceStop,
};
use mrr_data_content::{
    ConditionalCommitPortError, ConditionalContentCommitPort, ContentBlock, ContentCodec,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

struct Consumer {
    backend: Backend,
    destroyed: Arc<AtomicUsize>,
}
impl Drop for Consumer {
    fn drop(&mut self) {
        // The resource destructor must run while its reservation still exists.
        assert_eq!(self.backend.status().resource_bytes, 16);
        assert_eq!(self.backend.status().active_resources, 1);
        assert_eq!(self.backend.status().lifecycle, Lifecycle::Draining);
        self.destroyed.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_profiles_recover_through_abandoned_io_and_drain_after_final_consumer() {
    let directory = tempfile::tempdir().unwrap();
    let backend = Backend::open(
        BackendConfig {
            max_resources: 4,
            max_resource_bytes: 32,
            max_resource_workers: 1,
            max_shared_workers: 1,
            ..BackendConfig::default()
        },
        native(directory.path().join("mixed-lifecycle.db")),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let healthcare = backend.profile("healthcare.v1", "tenant").unwrap();
    let commerce = backend.profile("commerce.v1", "tenant").unwrap();
    let healthcare_write = write("same-home", "same-operation");
    let mut commerce_write = healthcare_write;
    commerce_write.replacement = ContentBlock::new(ContentCodec::Raw, b"commerce").cid();
    for (port, request) in [(&healthcare, healthcare_write), (&commerce, commerce_write)] {
        port.commit(request, Some(&ack(request)), |_| Ok::<_, ()>(()))
            .await
            .unwrap();
    }
    println!("mixed lifecycle: distinct profile histories committed on one Backend");

    let (release, cleaned) =
        abandon_io_with_saturated_recovery(&backend, &healthcare, &commerce).await;
    drain_after_cleanup_and_final_consumer(&backend, &healthcare, release, cleaned).await;
}

async fn abandon_io_with_saturated_recovery(
    backend: &Backend,
    healthcare: &ProfilePort,
    commerce: &ProfilePort,
) -> (Arc<tokio::sync::Notify>, Arc<AtomicUsize>) {
    let healthcare_write = write("same-home", "same-operation");
    let mut commerce_write = healthcare_write;
    commerce_write.replacement = ContentBlock::new(ContentCodec::Raw, b"commerce").cid();

    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let cleaned = Arc::new(AtomicUsize::new(0));
    let control = ResourceControl::default();
    let io_backend = backend.clone();
    let io_control = control.clone();
    let io_entered = entered.clone();
    let io_release = release.clone();
    let io_cleaned = cleaned.clone();
    let io = tokio::spawn(async move {
        io_backend
            .prepare_resource_async_controlled(16, io_control, move |control| async move {
                io_entered.notify_one();
                tokio::time::timeout(Duration::from_secs(3), io_release.notified())
                    .await
                    .unwrap();
                io_cleaned.fetch_add(1, Ordering::SeqCst);
                control.check()
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();

    let queued_control = ResourceControl::default();
    let query_control = queued_control.clone();
    let query_backend = backend.clone();
    let cancelled_query = tokio::spawn(async move {
        query_backend
            .prepare_resource_controlled::<(), ResourceStop>(16, query_control, |_| {
                panic!("cancelled queued query reached its driver")
            })
            .await
    });
    until(|| backend.status().active_resources == 2).await;
    assert!(matches!(
        backend.prepare_resource(1, || Ok(())).await,
        Err(BackendError::Saturated)
    ));
    assert_eq!(backend.status().resource_bytes, 32);

    for (port, request) in [(healthcare, healthcare_write), (commerce, commerce_write)] {
        let revision = tokio::time::timeout(Duration::from_millis(500), port.recover(request))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(revision.committed.root, request.replacement);
    }
    println!("mixed lifecycle: recovery progresses with the shared resource bytes saturated");
    queued_control.cancel();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), cancelled_query)
            .await
            .unwrap()
            .unwrap(),
        Err(ResourcePreparationError::Preparation(
            ResourceStop::Cancelled
        ))
    ));
    io.abort();
    assert!(matches!(io.await, Err(error) if error.is_cancelled()));
    assert_eq!(control.check(), Err(ResourceStop::Cancelled));
    assert_eq!(backend.status().resource_bytes, 16);
    assert_eq!(cleaned.load(Ordering::SeqCst), 0);
    (release, cleaned)
}

async fn drain_after_cleanup_and_final_consumer(
    backend: &Backend,
    healthcare: &ProfilePort,
    release: Arc<tokio::sync::Notify>,
    cleaned: Arc<AtomicUsize>,
) {
    let destroyed = Arc::new(AtomicUsize::new(0));
    let consumer_destroyed = destroyed.clone();
    let query_backend = backend.clone();
    let value_backend = backend.clone();
    let query = tokio::spawn(async move {
        query_backend
            .prepare_resource(16, move || {
                Ok(Consumer {
                    backend: value_backend,
                    destroyed: consumer_destroyed,
                })
            })
            .await
    });
    until(|| backend.status().active_resources == 2).await;
    let closing_backend = backend.clone();
    let shutdown = tokio::spawn(async move { closing_backend.shutdown().await });
    until(|| backend.status().lifecycle == Lifecycle::Draining).await;
    assert!(!shutdown.is_finished());
    assert!(matches!(
        backend.prepare_resource(1, || Ok(())).await,
        Err(BackendError::NotReady)
    ));
    assert!(matches!(
        healthcare
            .recover(write("same-home", "same-operation"))
            .await,
        Err(ConditionalCommitPortError::BeforeCommit(
            BackendError::NotReady
        ))
    ));
    release.notify_one();
    let consumer = tokio::time::timeout(Duration::from_secs(3), query)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(backend.status().resource_bytes, 16);
    assert_eq!(backend.status().blocking_resources, 0);
    let last = consumer.clone();
    drop(consumer);
    assert_eq!(destroyed.load(Ordering::SeqCst), 0);
    assert!(!shutdown.is_finished());
    println!("mixed lifecycle: abandoned I/O cleaned; accepted query retains final-consumer drain");
    drop(last);
    tokio::time::timeout(Duration::from_secs(3), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(destroyed.load(Ordering::SeqCst), 1);
    assert_eq!(backend.status().resource_bytes, 0);
    assert_eq!(backend.status().active_resources, 0);
    assert_eq!(backend.status().active_writes, 0);
    assert_eq!(backend.status().active_recoveries, 0);
    assert_eq!(backend.status().lifecycle, Lifecycle::Closed);
}
