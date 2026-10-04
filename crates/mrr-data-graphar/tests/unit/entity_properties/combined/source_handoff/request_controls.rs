//! Original-source requests retain real captured data through cooperative cleanup.
use super::{
    Execution, RESERVED, SOURCE_DIGEST, authority, capture, compile, executor, metadata,
    relation_tables, restore, result_limits, source_fixture,
};
use crate::CapturedCombinedGraphAr;
use crate::tests::entity_properties::{
    combined::{fixture::Fixture, remote::Remote},
    fixture as properties,
};
use meta_relational_reasoning as mrr;
use mrr::PropertyQueryBackend;
use mrr_data_backend::{
    Backend, BackendConfig, Lifecycle, ResourceControl, ResourceHandle, ResourcePreparationError,
    ResourceStop,
};
use mrr_data_content::MemoryContentStore;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Notify;

#[derive(Default)]
struct Gate {
    pause: bool,
    entered: Notify,
    release: Notify,
    calls: AtomicUsize,
    admitted: AtomicUsize,
}
struct Physical {
    inner: executor::CapturedBackend,
    gate: Arc<Gate>,
}
impl PropertyQueryBackend for Physical {
    type PhysicalEvidence = mrr_data_core::BoundDataQuery;
    type Error = mrr_data_datafusion::DataFusionQueryError;

    async fn execute<'a>(
        &'a self,
        query: &'a mrr::CatalogBoundQuery,
    ) -> Result<mrr::PropertyExecutionCandidate<Self::PhysicalEvidence>, Self::Error> {
        self.gate.calls.fetch_add(1, Ordering::SeqCst);
        println!("original-source physical dispatch entered");
        self.gate.entered.notify_one();
        if self.gate.pause {
            wait(self.gate.release.notified()).await;
        }
        self.inner.execute(query).await
    }
}
struct Request {
    bound: Arc<mrr_property_source::BoundPropertySourceQuery>,
    source: ResourceHandle<CapturedCombinedGraphAr>,
    physical: executor::CapturedBackend,
}
impl Request {
    fn new(
        f: &Fixture,
        bound: Arc<mrr_property_source::BoundPropertySourceQuery>,
        source: ResourceHandle<CapturedCombinedGraphAr>,
    ) -> Self {
        let tables = source
            .get()
            .tables(&f.query)
            .unwrap()
            .iter()
            .map(|table| mrr_data_datafusion::EntityPropertyTable {
                schema: table.schema.clone(),
                batch: table.batch.clone(),
            })
            .collect();
        let physical = executor::CapturedBackend {
            binding: f.query.clone(),
            tables,
            relations: relation_tables(f, source.get()),
            limits: properties::limits(),
        };
        Self {
            bound,
            source,
            physical,
        }
    }
    async fn run(
        self,
        backend: Backend,
        control: ResourceControl,
        gate: Arc<Gate>,
    ) -> Result<ResourceHandle<Execution>, ResourcePreparationError<ResourceStop>> {
        let active = backend.clone();
        backend
            .prepare_resource_async_controlled(RESERVED, control, move |control| async move {
                control.check()?;
                let physical = Physical {
                    inner: self.physical,
                    gate: gate.clone(),
                };
                let admitted = self
                    .bound
                    .execute_with(&physical, result_limits())
                    .await
                    .unwrap();
                verify_rows(&admitted);
                gate.admitted.fetch_add(1, Ordering::SeqCst);
                let result = control.check().map(|()| admitted);
                drop(physical);
                drop(self.source);
                // Native/source buffers are gone while the output reservation
                // still protects the owned driver's cleanup boundary.
                assert!(active.status().resource_bytes >= RESERVED);
                println!("original-source physical cleanup completed");
                result
            })
            .await
    }
}
fn verify_rows(execution: &Execution) {
    let mut rows = execution.candidate().rows().to_vec();
    let mut expected = crate::tests::entity_properties::acceptance::expected();
    expected.push(expected[0].clone());
    rows.sort_by_key(|row| format!("{row:?}"));
    expected.sort_by_key(|row| format!("{row:?}"));
    assert_eq!(rows, expected);
}
async fn prepared(
    bytes: usize,
) -> (
    Fixture,
    Backend,
    ResourceHandle<CapturedCombinedGraphAr>,
    Arc<mrr_property_source::BoundPropertySourceQuery>,
) {
    let f = Fixture::with_original(source_fixture());
    let bound = compile()
        .bind(&f.relations, &f.entities, &f.original.semantic)
        .unwrap();
    assert_eq!(bound.compilation().source_digest, SOURCE_DIGEST);
    let backend = Backend::open(
        BackendConfig {
            max_resource_bytes: bytes,
            ..BackendConfig::default()
        },
        metadata::SimulatedMetadata::default(),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let remote = Arc::new(Remote::default());
    authority::publish(&f, &backend, &remote).await;
    let restored = restore(
        &f,
        &backend,
        remote,
        Arc::new(MemoryContentStore::default()),
    )
    .await;
    let source = capture(&f, &backend, restored).await;
    assert_eq!(backend.status().resource_bytes, RESERVED);
    (f, backend, source, Arc::new(bound))
}
async fn wait<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("original-source control checkpoint stalled")
}
async fn until(mut condition: impl FnMut() -> bool) {
    wait(async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}
fn control(deadline: bool) -> ResourceControl {
    ResourceControl::new(deadline.then(|| Instant::now() + Duration::from_millis(30)))
}
fn stopped(
    result: Result<ResourceHandle<Execution>, ResourcePreparationError<ResourceStop>>,
    deadline: bool,
) {
    let expected = if deadline {
        ResourceStop::Deadline
    } else {
        ResourceStop::Cancelled
    };
    assert!(
        matches!(result, Err(ResourcePreparationError::Preparation(reason)) if reason == expected)
    );
}

#[tokio::test]
async fn original_source_handoff_queued_cancel_and_deadline_release_real_capture() {
    let (f, backend, source, bound) = prepared(2 * RESERVED + 1).await;
    let blocker = Arc::new(Gate {
        pause: true,
        ..Gate::default()
    });
    let held_backend = backend.clone();
    let held_gate = blocker.clone();
    let held = tokio::spawn(async move {
        held_backend
            .prepare_resource_async_controlled(1, ResourceControl::default(), move |_| async move {
                println!("original-source worker lane held at acceptance checkpoint");
                held_gate.entered.notify_one();
                wait(held_gate.release.notified()).await;
                Ok::<_, ResourceStop>(())
            })
            .await
            .unwrap()
    });
    wait(blocker.entered.notified()).await;
    let gate = Arc::new(Gate::default());
    let mut last_source = Some(source);
    for deadline in [false, true] {
        let captured = if deadline {
            last_source.take().unwrap()
        } else {
            last_source.as_ref().unwrap().clone()
        };
        let request = Request::new(&f, bound.clone(), captured);
        let control = control(deadline);
        let queued = tokio::spawn(request.run(backend.clone(), control.clone(), gate.clone()));
        until(|| backend.status().resource_bytes == 2 * RESERVED + 1).await;
        assert_eq!(backend.status().blocking_resources, 0);
        if !deadline {
            control.cancel();
        }
        stopped(wait(queued).await.unwrap(), deadline);
        assert_eq!(
            backend.status().resource_bytes,
            if deadline { 1 } else { RESERVED + 1 }
        );
    }
    assert_eq!(gate.calls.load(Ordering::SeqCst), 0);
    assert_eq!(gate.admitted.load(Ordering::SeqCst), 0);
    blocker.release.notify_one();
    drop(wait(held).await.unwrap());
    assert_eq!(backend.status().resource_bytes, 0);
    backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn original_source_handoff_running_stop_refuses_consumer_after_physical_cleanup() {
    let (f, backend, source, bound) = prepared(2 * RESERVED).await;
    for deadline in [false, true] {
        let gate = Arc::new(Gate {
            pause: true,
            ..Gate::default()
        });
        let request = Request::new(&f, bound.clone(), source.clone());
        let control = control(deadline);
        let running = tokio::spawn(request.run(backend.clone(), control.clone(), gate.clone()));
        wait(gate.entered.notified()).await;
        if deadline {
            tokio::time::sleep(Duration::from_millis(40)).await;
        } else {
            control.cancel();
        }
        assert!(!running.is_finished());
        assert_eq!(backend.status().resource_bytes, 2 * RESERVED);
        gate.release.notify_one();
        stopped(wait(running).await.unwrap(), deadline);
        assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
        assert_eq!(gate.admitted.load(Ordering::SeqCst), 1);
        assert_eq!(backend.status().resource_bytes, RESERVED);
    }
    drop(source);
    assert_eq!(backend.status().resource_bytes, 0);
    backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn original_source_handoff_abandoned_waiter_retains_capture_until_owned_cleanup() {
    let (f, backend, source, bound) = prepared(2 * RESERVED).await;
    let gate = Arc::new(Gate {
        pause: true,
        ..Gate::default()
    });
    let control = ResourceControl::default();
    let request = Request::new(&f, bound, source);
    let running = tokio::spawn(request.run(backend.clone(), control.clone(), gate.clone()));
    wait(gate.entered.notified()).await;
    running.abort();
    assert!(matches!(wait(running).await, Err(error) if error.is_cancelled()));
    assert_eq!(control.check(), Err(ResourceStop::Cancelled));
    assert_eq!(backend.status().resource_bytes, 2 * RESERVED);
    let closing = backend.clone();
    let drain = tokio::spawn(async move { closing.shutdown().await });
    until(|| backend.status().lifecycle == Lifecycle::Draining).await;
    assert!(!drain.is_finished());
    assert_eq!(backend.status().resource_bytes, 2 * RESERVED);
    gate.release.notify_one();
    wait(drain).await.unwrap().unwrap();
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    assert_eq!(gate.admitted.load(Ordering::SeqCst), 1);
    assert_eq!(backend.status().resource_bytes, 0);
    assert_eq!(backend.status().lifecycle, Lifecycle::Closed);
}
