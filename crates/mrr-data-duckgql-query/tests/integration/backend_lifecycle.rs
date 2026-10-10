#[path = "../support/provider.rs"]
mod provider;
use super::{artifact, limits, query_fixture};
use mrr_data_backend::{
    Backend, BackendConfig, BackendError, Lifecycle, ResourceControl, ResourceTransformError,
};
use mrr_data_duckgql_query::{
    DuckGqlBackendQuery, DuckGqlError, duckgql_graphar_engine_profile,
    execute_duckgql_graphar_controlled_on_backend,
};
use std::{
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

const RESERVATION: usize = 512 * 1024 * 1024;
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn open() -> Backend {
    Backend::open(
        BackendConfig {
            max_shared_workers: 1,
            max_resources: 2,
            max_resource_bytes: RESERVATION + 1,
            ..BackendConfig::default()
        },
        provider::Provider,
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_output_lease_survives_conversion_and_final_clone_drains_backend() {
    let engine = duckgql_graphar_engine_profile().unwrap();
    let (query, projection, source, _) = query_fixture::captured(&engine);
    let source = Arc::new(source);
    let backend = open().await;
    let resource = execute_duckgql_graphar_controlled_on_backend(
        &backend,
        tokio::runtime::Handle::current(),
        DuckGqlBackendQuery {
            artifact: artifact(),
            query: query.clone(),
            source: source.clone(),
            projection,
            limits: limits(),
            reserved_bytes: RESERVATION,
        },
        ResourceControl::default(),
    )
    .await
    .unwrap();
    println!("native output materialized and connection closed");
    assert_eq!(resource.get().rows().len(), 2);
    assert_eq!(Arc::strong_count(&source), 1);
    assert_eq!(backend.status().blocking_resources, 0);
    assert_eq!(backend.status().resource_bytes, RESERVATION);
    assert!(matches!(
        backend.prepare_resource(2, || Ok(())).await,
        Err(BackendError::Saturated)
    ));
    let worker = backend.prepare_resource(1, || Ok(())).await.unwrap();
    drop(worker);
    let sibling = resource.clone();
    let Err(ResourceTransformError::Shared(resource)) =
        resource.try_transform(|_| -> Result<(), ()> { panic!("shared conversion must not run") })
    else {
        panic!("shared resource must be refused");
    };
    drop(sibling);
    let Ok(candidate) = resource
        .try_transform(|output| mrr_data_core::project_data_query_output(&query, &engine, output))
    else {
        panic!("uniquely owned conversion must succeed");
    };
    meta_relational_reasoning::admit_query_result_candidate(
        query.query(),
        candidate.get(),
        meta_relational_reasoning::QueryResultLimits::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(4).unwrap(),
        ),
    )
    .unwrap();
    let final_clone = candidate.clone();
    let closing_backend = backend.clone();
    let closing = tokio::spawn(async move { closing_backend.shutdown().await });
    until(|| backend.status().lifecycle == Lifecycle::Draining).await;
    drop(candidate);
    assert!(!closing.is_finished());
    assert_eq!(backend.status().resource_bytes, RESERVATION);
    drop(final_clone);
    tokio::time::timeout(Duration::from_secs(3), closing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(backend.status().resource_bytes, 0);
    assert_eq!(backend.status().lifecycle, Lifecycle::Closed);
}

#[tokio::test]
async fn stopped_queued_native_requests_release_source_and_publish_no_output() {
    let engine = duckgql_graphar_engine_profile().unwrap();
    let (query, projection, source, _) = query_fixture::captured(&engine);
    let backend = open().await;
    let source = Arc::new(source);
    for deadline in [false, true] {
        let control = ResourceControl::new(deadline.then(|| {
            Instant::now()
                .checked_sub(Duration::from_millis(1))
                .unwrap()
        }));
        if !deadline {
            control.cancel();
        }
        let expected = if deadline {
            DuckGqlError::Deadline
        } else {
            DuckGqlError::Cancelled
        };
        let request = DuckGqlBackendQuery {
            artifact: artifact(),
            query: query.clone(),
            source: source.clone(),
            projection: projection.clone(),
            limits: limits(),
            reserved_bytes: RESERVATION,
        };
        assert!(
            matches!(execute_duckgql_graphar_controlled_on_backend(&backend, tokio::runtime::Handle::current(), request, control).await, Err(error) if error == expected)
        );
        assert_eq!(Arc::strong_count(&source), 1);
        assert_eq!(backend.status().active_resources, 0);
        assert_eq!(backend.status().blocking_resources, 0);
        assert_eq!(backend.status().resource_bytes, 0);
    }
    backend.shutdown().await.unwrap();
}
