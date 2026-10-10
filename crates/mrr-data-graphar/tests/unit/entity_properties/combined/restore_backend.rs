//! Cold async provider calls retain the same admission and drain barriers.
use super::{
    fixture::{Fixture, capture_limits, transfer_limits},
    remote::Remote,
};
use crate::{
    CombinedGraphArRestoreRequest, GraphArEntityPropertyError as Error,
    restore_combined_graph_content,
};
use mrr_data_backend::{
    Backend, BackendConfig, BackendError, Lifecycle, ResourceControl, ResourcePreparationError,
    ResourceStop,
};
use mrr_data_content::{
    ContentBlock, ContentError, ContentStore, MemoryContentStore, RemoteContentStore, RemoteFuture,
    publish_combined_graph,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
const RESERVED: usize = 8 << 20;
struct Cache {
    backend: Backend,
    reads: AtomicUsize,
    inner: MemoryContentStore,
}
impl ContentStore for Cache {
    fn put(&self, block: ContentBlock<'_>) -> Result<cid::Cid, ContentError> {
        self.inner.put(block)
    }
    fn get_bounded(&self, cid: &cid::Cid, cap: usize) -> Result<Vec<u8>, ContentError> {
        assert_eq!(self.backend.status().resource_bytes, RESERVED);
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get_bounded(cid, cap)
    }
}
struct Gate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
struct Provider {
    remote: Remote,
    backend: Backend,
    control: ResourceControl,
    reads: AtomicUsize,
    stop_after: usize,
    gate: Option<Arc<Gate>>,
}
impl RemoteContentStore for Provider {
    fn get<'a>(&'a self, cid: &'a cid::Cid, cap: usize) -> RemoteFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            assert_eq!(self.backend.status().resource_bytes, RESERVED);
            assert_eq!(self.backend.status().blocking_resources, 0);
            assert!(cap <= RESERVED);
            let index = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
            if index == 1
                && let Some(gate) = &self.gate
            {
                println!("cold provider: first real async read waiting for release");
                gate.entered.notify_one();
                tokio::time::timeout(Duration::from_secs(3), gate.release.notified())
                    .await
                    .unwrap();
            }
            let result = self.remote.get(cid, cap).await;
            if index == self.stop_after {
                self.control.cancel();
            }
            result
        })
    }
    fn put<'a>(&'a self, block: ContentBlock<'a>) -> RemoteFuture<'a, ()> {
        self.remote.put(block)
    }
}
async fn backend() -> Backend {
    Backend::open(
        BackendConfig {
            max_resource_bytes: RESERVED,
            max_resource_workers: 1,
            ..BackendConfig::default()
        },
        crate::tests::snapshot::backend_qualification::MetadataStub,
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap()
}
fn request(f: &Fixture, local: Arc<Cache>, remote: Arc<Provider>) -> CombinedGraphArRestoreRequest {
    CombinedGraphArRestoreRequest {
        local,
        remote,
        query: f.query.clone(),
        relations: f.relations.clone(),
        entities: f.entities.clone(),
        dataset: capture_limits().dataset,
        transfer: transfer_limits(),
    }
}
async fn provider(
    f: &Fixture,
    backend: &Backend,
    control: ResourceControl,
    stop_after: usize,
    gate: Option<Arc<Gate>>,
) -> Arc<Provider> {
    let remote = Remote::default();
    publish_combined_graph(&f.prepare(), &f.local, &remote, &remote, || async {
        Ok(())
    })
    .await
    .unwrap();
    Arc::new(Provider {
        remote,
        backend: backend.clone(),
        control,
        reads: AtomicUsize::new(0),
        stop_after,
        gate,
    })
}
fn cache(backend: &Backend) -> Arc<Cache> {
    Arc::new(Cache {
        backend: backend.clone(),
        reads: AtomicUsize::new(0),
        inner: MemoryContentStore::default(),
    })
}
#[tokio::test]
async fn cold_async_admission_precedes_reads_and_retained_closure_drains_last_clone() {
    let f = Fixture::new();
    let backend = backend().await;
    let local = cache(&backend);
    let remote = provider(&f, &backend, ResourceControl::default(), usize::MAX, None).await;
    for control in [
        {
            let c = ResourceControl::default();
            c.cancel();
            c
        },
        ResourceControl::new(Some(Instant::now())),
    ] {
        assert!(matches!(
            restore_combined_graph_content(
                &backend,
                request(&f, local.clone(), remote.clone()),
                RESERVED,
                control
            )
            .await,
            Err(ResourcePreparationError::Preparation(Error::Stop(_)))
        ));
    }
    assert_eq!(local.reads.load(Ordering::SeqCst), 0);
    let held = backend.prepare_resource(RESERVED, || Ok(())).await.unwrap();
    assert!(matches!(
        restore_combined_graph_content(
            &backend,
            request(&f, local.clone(), remote.clone()),
            RESERVED,
            ResourceControl::default()
        )
        .await,
        Err(ResourcePreparationError::Backend(BackendError::Saturated))
    ));
    assert_eq!(local.reads.load(Ordering::SeqCst), 0);
    drop(held);
    let restored = restore_combined_graph_content(
        &backend,
        request(&f, local.clone(), remote.clone()),
        RESERVED,
        ResourceControl::default(),
    )
    .await
    .unwrap();
    assert_eq!(restored.get().total_bytes(), f.prepare().total_bytes());
    let clone = restored.clone();
    let closing = backend.clone();
    let shutdown = tokio::spawn(async move { closing.shutdown().await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while backend.status().lifecycle != Lifecycle::Draining {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(restored);
    assert_eq!(backend.status().resource_bytes, RESERVED);
    drop(clone);
    tokio::time::timeout(Duration::from_secs(3), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(backend.status().resource_bytes, 0);
}
#[tokio::test]
async fn abandoned_cold_async_read_retains_admission_until_provider_cleanup() {
    let f = Fixture::new();
    let backend = backend().await;
    let local = cache(&backend);
    let control = ResourceControl::default();
    let gate = Arc::new(Gate {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let remote = provider(
        &f,
        &backend,
        control.clone(),
        usize::MAX,
        Some(gate.clone()),
    )
    .await;
    let input = request(&f, local, remote.clone());
    let engine = backend.clone();
    let worker_control = control.clone();
    let caller = tokio::spawn(async move {
        restore_combined_graph_content(&engine, input, RESERVED, worker_control).await
    });
    tokio::time::timeout(Duration::from_secs(3), gate.entered.notified())
        .await
        .unwrap();
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    assert_eq!(control.check(), Err(ResourceStop::Cancelled));
    assert_eq!(backend.status().resource_bytes, RESERVED);
    let closing = backend.clone();
    let shutdown = tokio::spawn(async move { closing.shutdown().await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while backend.status().lifecycle != Lifecycle::Draining {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!shutdown.is_finished());
    gate.release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(remote.reads.load(Ordering::SeqCst), 1);
    assert_eq!(backend.status().resource_bytes, 0);
}
#[tokio::test]
async fn cold_async_stop_and_payload_budget_refuse_without_retained_output() {
    let f = Fixture::new();
    let backend = backend().await;
    let control = ResourceControl::default();
    let remote = provider(&f, &backend, control.clone(), 2, None).await;
    assert!(matches!(
        restore_combined_graph_content(
            &backend,
            request(&f, cache(&backend), remote.clone()),
            RESERVED,
            control
        )
        .await,
        Err(ResourcePreparationError::Preparation(Error::Stop(
            ResourceStop::Cancelled
        )))
    ));
    assert_eq!(remote.reads.load(Ordering::SeqCst), 2);
    assert_eq!(backend.status().resource_bytes, 0);
    let remote = provider(&f, &backend, ResourceControl::default(), usize::MAX, None).await;
    let mut input = request(&f, cache(&backend), remote);
    input.transfer.max_total_bytes = f.prepare().total_bytes() - 1;
    assert!(
        restore_combined_graph_content(&backend, input, RESERVED, ResourceControl::default())
            .await
            .is_err()
    );
    assert_eq!(backend.status().resource_bytes, 0);
    let plain_remote = Arc::new(Remote::default());
    publish_combined_graph(
        &f.prepare(),
        &f.local,
        plain_remote.as_ref(),
        plain_remote.as_ref(),
        || async { Ok(()) },
    )
    .await
    .unwrap();
    let undersized = CombinedGraphArRestoreRequest {
        local: Arc::new(MemoryContentStore::default()),
        remote: plain_remote,
        query: f.query.clone(),
        relations: f.relations.clone(),
        entities: f.entities.clone(),
        dataset: capture_limits().dataset,
        transfer: transfer_limits(),
    };
    assert!(
        restore_combined_graph_content(&backend, undersized, 1, ResourceControl::default())
            .await
            .is_err()
    );
    assert_eq!(backend.status().resource_bytes, 0);
    backend.shutdown().await.unwrap();
}
