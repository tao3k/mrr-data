//! Native interrupt on the same shared admission path, independent of plugins.
use super::{DuckGqlError, InterruptMonitor};
use mrr_data_backend::{Backend, BackendConfig, ResourceControl, ResourcePreparationError};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[path = "../support/provider.rs"]
mod provider;
use provider::Provider;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_native_cancel_and_deadline_close_before_releasing_worker() {
    for deadline in [false, true] {
        let runtime = tokio::runtime::Handle::current();
        let backend = Backend::open(
            BackendConfig {
                max_shared_workers: 1,
                ..BackendConfig::default()
            },
            Provider,
            runtime.clone(),
        )
        .await
        .unwrap();
        let control =
            ResourceControl::new(deadline.then(|| Instant::now() + Duration::from_millis(200)));
        let (started, entered) = tokio::sync::oneshot::channel();
        let worker_backend = backend.clone();
        let worker_control = control.clone();
        let worker = tokio::spawn(async move {
            worker_backend
                .prepare_resource_controlled(1024, worker_control, move |control| {
                    let connection = duckdb::Connection::open_in_memory().unwrap();
                    let monitor = InterruptMonitor::start(&runtime, control.clone(), &connection);
                    started.send(()).unwrap();
                    let result = connection.query_row::<u64, _, _>(
                        "SELECT count(*) FROM range(10000000) t1, range(1000000) t2",
                        [],
                        |row| row.get(0),
                    );
                    assert!(
                        result.is_err(),
                        "native call must be interrupted before completion"
                    );
                    drop(monitor);
                    connection.close().map_err(|_| DuckGqlError::Cleanup)?;
                    control.check().map_err(DuckGqlError::from)?;
                    result.map_err(|_| DuckGqlError::Native)
                })
                .await
        });
        entered.await.unwrap();
        println!("native driver entered; deadline={deadline}");
        assert_eq!(backend.status().blocking_resources, 1);
        assert_eq!(backend.status().active_resources, 1);
        if !deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
            control.cancel();
        }
        let refused = tokio::time::timeout(Duration::from_secs(3), worker)
            .await
            .unwrap()
            .unwrap();
        let expected = if deadline {
            DuckGqlError::Deadline
        } else {
            DuckGqlError::Cancelled
        };
        assert!(
            matches!(refused, Err(ResourcePreparationError::Preparation(error)) if error == expected)
        );
        assert_eq!(backend.status().blocking_resources, 0);
        assert_eq!(backend.status().active_resources, 0);
        assert_eq!(backend.status().resource_bytes, 0);
        let later = backend
            .prepare_resource(1, || Ok(Arc::new(1u8)))
            .await
            .unwrap();
        drop(later);
        backend.shutdown().await.unwrap();
        println!("native driver closed and admission released; deadline={deadline}");
    }
}
