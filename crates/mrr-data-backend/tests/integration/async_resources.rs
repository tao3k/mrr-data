//! Async and blocking preparation use the same worker lane and retained budget.
use super::{open, until};
use mrr_data_backend::{BackendConfig, ResourceControl, ResourcePreparationError, ResourceStop};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

#[tokio::test]
async fn async_queued_cancel_and_deadline_skip_driver_under_shared_worker_limit() {
    let (backend, _) = open(BackendConfig {
        max_resource_workers: 1,
        max_resource_bytes: 32,
        ..BackendConfig::default()
    })
    .await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let first_backend = backend.clone();
    let first_entered = entered.clone();
    let first_release = release.clone();
    let first = tokio::spawn(async move {
        first_backend
            .prepare_resource_async_controlled(
                16,
                ResourceControl::default(),
                move |_| async move {
                    println!("async resource: admitted driver awaiting test release");
                    first_entered.notify_one();
                    tokio::time::timeout(Duration::from_secs(3), first_release.notified())
                        .await
                        .unwrap();
                    Ok::<_, ResourceStop>(())
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    assert_eq!(backend.status().blocking_resources, 0);
    let called = Arc::new(AtomicUsize::new(0));
    for deadline in [false, true] {
        let control =
            ResourceControl::new(deadline.then(|| Instant::now() + Duration::from_millis(30)));
        let waiter_control = control.clone();
        let engine = backend.clone();
        let invoked = called.clone();
        let queued = tokio::spawn(async move {
            engine
                .prepare_resource_async_controlled(16, waiter_control, move |_| async move {
                    invoked.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, ResourceStop>(())
                })
                .await
        });
        until(|| backend.status().active_resources == 2).await;
        if !deadline {
            control.cancel();
        }
        let result = tokio::time::timeout(Duration::from_secs(3), queued)
            .await
            .unwrap()
            .unwrap();
        let expected = if deadline {
            ResourceStop::Deadline
        } else {
            ResourceStop::Cancelled
        };
        assert!(
            matches!(result, Err(ResourcePreparationError::Preparation(reason)) if reason == expected)
        );
        assert_eq!(backend.status().resource_bytes, 16);
    }
    // A blocking preparation also queues behind the same async worker permit.
    let engine = backend.clone();
    let invoked = called.clone();
    let blocking = tokio::spawn(async move {
        engine
            .prepare_resource_controlled(16, ResourceControl::default(), move |_| {
                invoked.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ResourceStop>(())
            })
            .await
    });
    until(|| backend.status().active_resources == 2).await;
    assert_eq!(called.load(Ordering::SeqCst), 0);
    release.notify_one();
    let retained = first.await.unwrap().unwrap();
    let blocking_retained = blocking.await.unwrap().unwrap();
    assert_eq!(called.load(Ordering::SeqCst), 1);
    drop(retained);
    drop(blocking_retained);
    assert_eq!(backend.status().resource_bytes, 0);
    backend.shutdown().await.unwrap();
}
