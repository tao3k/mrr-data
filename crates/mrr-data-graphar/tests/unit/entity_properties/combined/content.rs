use super::{
    content_store::ObservedStore,
    fixture::{Fixture, capture_limits, transfer_limits},
    remote::Remote,
};
use crate::{
    CombinedGraphArContentRequest, GraphArEntityPropertyError as Error,
    prepare_combined_graph_content, prepare_combined_graphar_from_content,
};
use mrr_data_backend::{
    Backend, BackendConfig, BackendError, ResourceControl, ResourcePreparationError, ResourceStop,
};
use mrr_data_content::{GraphTransferError, MemoryContentStore, publish_combined_graph};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
const RESERVED: usize = 8 << 20;
async fn backend() -> Backend {
    Backend::open(
        BackendConfig {
            max_resource_bytes: RESERVED,
            max_resources: 1,
            ..BackendConfig::default()
        },
        crate::tests::snapshot::backend_qualification::MetadataStub,
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap()
}
fn request(f: &Fixture, store: Arc<ObservedStore>) -> CombinedGraphArContentRequest {
    CombinedGraphArContentRequest {
        local: store,
        query: f.query.clone(),
        snapshot: f.snapshot.clone(),
        relations: f.relations.clone(),
        entities: f.entities.clone(),
        properties: f.projection.clone(),
        limits: capture_limits(),
        transfer: transfer_limits(),
    }
}
fn observed(
    f: &Fixture,
    backend: &Backend,
    control: ResourceControl,
    reserved: usize,
    stop_after: usize,
) -> Arc<ObservedStore> {
    // A fixture's content store has no clone API; copy its verified closure into a
    // reusable test provider. These bytes are test setup, not a Backend allocation.
    use mrr_data_content::{ContentBlock, ContentCodec, ContentStore};
    let prepared = f.prepare();
    let remote_cids = prepared
        .snapshot()
        .manifest()
        .referenced_cids()
        .into_iter()
        .chain(std::iter::once(
            *f.query.graph_projection_manifest().unwrap(),
        ));
    let native_cids = std::iter::once(f.dataset.properties().inventory())
        .chain(f.dataset.relations().iter().map(|m| &m.inventory))
        .flat_map(mrr_data_core::GraphDatasetInventory::files)
        .map(|file| *file.cid());
    let inner = MemoryContentStore::default();
    for cid in remote_cids.chain(native_cids) {
        inner
            .put(ContentBlock::new(
                ContentCodec::from_cid(&cid).unwrap(),
                prepared.block(&cid).unwrap(),
            ))
            .unwrap();
    }
    Arc::new(ObservedStore {
        inner: Arc::new(inner),
        backend: backend.clone(),
        control,
        reserved,
        stop_after,
        gate: None,
        reads: AtomicUsize::new(0),
        caps: Mutex::new(Vec::new()),
    })
}
#[tokio::test]
async fn shared_admission_and_stops_precede_any_content_read() {
    let f = Fixture::new();
    let backend = backend().await;
    let control = ResourceControl::new(None);
    control.cancel();
    let store = observed(&f, &backend, control.clone(), RESERVED, 0);
    assert!(matches!(
        prepare_combined_graph_content(&backend, request(&f, store.clone()), RESERVED, control)
            .await,
        Err(ResourcePreparationError::Preparation(Error::Stop(
            ResourceStop::Cancelled
        )))
    ));
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
    let deadline = ResourceControl::new(Some(Instant::now()));
    assert!(matches!(
        prepare_combined_graph_content(&backend, request(&f, store.clone()), RESERVED, deadline)
            .await,
        Err(ResourcePreparationError::Preparation(Error::Stop(
            ResourceStop::Deadline
        )))
    ));
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
    let closure = prepare_combined_graph_content(
        &backend,
        request(&f, store.clone()),
        RESERVED,
        ResourceControl::new(None),
    )
    .await
    .unwrap();
    let before = store.reads.load(Ordering::SeqCst);
    assert!(matches!(
        prepare_combined_graph_content(
            &backend,
            request(&f, store.clone()),
            RESERVED,
            ResourceControl::new(None)
        )
        .await,
        Err(ResourcePreparationError::Backend(
            BackendError::Saturated | BackendError::Limit
        ))
    ));
    assert_eq!(store.reads.load(Ordering::SeqCst), before);
    drop(closure);
    assert_eq!(backend.status().resource_bytes, 0);
    backend.shutdown().await.unwrap();
}
#[tokio::test]
async fn every_content_read_stop_discards_partial_buffers_and_releases_reservation() {
    let f = Fixture::new();
    let backend = backend().await;
    let reads = f.prepare().block_count() - 1;
    for stop_after in 1..=reads {
        let control = ResourceControl::new(None);
        let store = observed(&f, &backend, control.clone(), RESERVED, stop_after);
        assert!(matches!(
            prepare_combined_graphar_from_content(
                &backend,
                request(&f, store.clone()),
                RESERVED,
                control
            )
            .await,
            Err(ResourcePreparationError::Preparation(Error::Stop(
                ResourceStop::Cancelled
            )))
        ));
        assert_eq!(store.reads.load(Ordering::SeqCst), stop_after);
        assert_eq!(backend.status().resource_bytes, 0);
        assert_eq!(backend.status().active_resources, 0);
    }
    backend.shutdown().await.unwrap();
}
#[tokio::test]
async fn publication_and_native_capture_keep_reservation_across_content_acquisition() {
    let f = Fixture::new();
    let backend = backend().await;
    let store = observed(&f, &backend, ResourceControl::new(None), RESERVED, 0);
    let closure = prepare_combined_graph_content(
        &backend,
        request(&f, store.clone()),
        RESERVED,
        ResourceControl::new(None),
    )
    .await
    .unwrap();
    assert!(closure.get().total_bytes() < RESERVED);
    let remote = Remote::default();
    publish_combined_graph(
        closure.get(),
        store.inner.as_ref(),
        &remote,
        &remote,
        || async {
            assert_eq!(backend.status().resource_bytes, RESERVED);
            Ok(())
        },
    )
    .await
    .unwrap();
    drop(closure);
    assert_eq!(backend.status().resource_bytes, 0);
    let captured = prepare_combined_graphar_from_content(
        &backend,
        request(&f, store.clone()),
        RESERVED,
        ResourceControl::new(None),
    )
    .await
    .unwrap();
    assert_eq!(
        captured
            .get()
            .tables(&f.query)
            .unwrap()
            .iter()
            .map(|t| t.batch.num_rows())
            .sum::<usize>(),
        8
    );
    assert_eq!(captured.get().relations(&f.query).unwrap().len(), 2);
    assert_eq!(backend.status().resource_bytes, RESERVED);
    drop(captured);
    assert_eq!(backend.status().resource_bytes, 0);
    backend.shutdown().await.unwrap();
}
#[tokio::test]
async fn content_payload_is_bounded_by_reservation_and_failed_reads_release_it() {
    let f = Fixture::new();
    let backend = backend().await;
    let reserved = f.snapshot.bytes().len() + 1;
    let store = observed(&f, &backend, ResourceControl::new(None), reserved, 0);
    assert!(matches!(
        prepare_combined_graph_content(
            &backend,
            request(&f, store.clone()),
            reserved,
            ResourceControl::new(None)
        )
        .await,
        Err(ResourcePreparationError::Preparation(Error::Transfer(_)))
    ));
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
    assert_eq!(store.caps.lock().unwrap()[0], 1);
    assert_eq!(backend.status().resource_bytes, 0);
    let mut req = request(&f, store.clone());
    req.transfer.max_blocks = 1;
    assert!(matches!(
        prepare_combined_graph_content(&backend, req, reserved, ResourceControl::new(None)).await,
        Err(ResourcePreparationError::Preparation(Error::Transfer(
            GraphTransferError::Limit
        )))
    ));
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
    assert_eq!(backend.status().resource_bytes, 0);
    backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn abandoned_content_preparation_holds_reservation_until_read_cleanup() {
    let f = Fixture::new();
    let backend = backend().await;
    let control = ResourceControl::new(None);
    let mut store = observed(&f, &backend, control.clone(), RESERVED, 0);
    let gate = Arc::new(super::content_store::ReadGate::new());
    Arc::get_mut(&mut store).unwrap().gate = Some(gate.clone());
    let req = request(&f, store.clone());
    let owner = backend.clone();
    let worker_control = control.clone();
    let preparation = tokio::spawn(async move {
        prepare_combined_graphar_from_content(&owner, req, RESERVED, worker_control).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), gate.entered.notified())
        .await
        .unwrap();
    preparation.abort();
    assert!(matches!(preparation.await, Err(error) if error.is_cancelled()));
    assert_eq!(control.check(), Err(ResourceStop::Cancelled));
    let owner = backend.clone();
    let shutdown = tokio::spawn(async move { owner.shutdown().await });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while backend.status().lifecycle != mrr_data_backend::Lifecycle::Draining {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(backend.status().resource_bytes, RESERVED);
    assert!(!shutdown.is_finished());
    gate.release();
    tokio::time::timeout(std::time::Duration::from_secs(3), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
    assert_eq!(backend.status().resource_bytes, 0);
}
#[tokio::test]
async fn stop_observed_on_failed_read_preserves_the_typed_cancellation() {
    let f = Fixture::new();
    let backend = backend().await;
    let control = ResourceControl::new(None);
    let reserved = f.snapshot.bytes().len() + 1;
    let store = observed(&f, &backend, control.clone(), reserved, 1);
    assert!(matches!(
        prepare_combined_graph_content(&backend, request(&f, store.clone()), reserved, control)
            .await,
        Err(ResourcePreparationError::Preparation(Error::Stop(
            ResourceStop::Cancelled
        )))
    ));
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
    assert_eq!(backend.status().resource_bytes, 0);
    backend.shutdown().await.unwrap();
}
#[tokio::test]
async fn aggregate_content_budget_refuses_late_child_without_exporting_buffers() {
    let f = Fixture::new();
    let backend = backend().await;
    let reserved = f.prepare().total_bytes() - 1;
    let store = observed(&f, &backend, ResourceControl::new(None), reserved, 0);
    assert!(matches!(
        prepare_combined_graphar_from_content(
            &backend,
            request(&f, store.clone()),
            reserved,
            ResourceControl::new(None)
        )
        .await,
        Err(ResourcePreparationError::Preparation(Error::Transfer(
            GraphTransferError::Content(mrr_data_content::ContentError::BlockTooLarge { .. })
        )))
    ));
    assert!(store.reads.load(Ordering::SeqCst) > 1);
    assert!(
        store
            .caps
            .lock()
            .unwrap()
            .windows(2)
            .all(|pair| pair[0] >= pair[1])
    );
    assert_eq!(backend.status().resource_bytes, 0);
    assert_eq!(backend.status().active_resources, 0);
    backend.shutdown().await.unwrap();
}
