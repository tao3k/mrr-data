//! One simulated original-source caller retains each phase on the common Backend.
use super::{SOURCE_DIGEST, compile, source_fixture};
use crate::{
    CapturedCombinedGraphAr, CombinedGraphArRestoreRequest, capture_combined_graphar,
    restore_combined_graph_content,
    tests::entity_properties::{
        combined::{
            acceptance::{alternate_root, relation_tables},
            fixture::{Fixture, capture_limits},
            remote::Remote,
        },
        fixture as properties,
    },
};
use meta_relational_reasoning as mrr;
use mrr_data_backend::{Backend, BackendConfig, ResourceControl, ResourceHandle, ResourceStop};
use mrr_data_content::MemoryContentStore;
use mrr_data_core::DataQueryResultHandoff;
use std::{num::NonZeroUsize, sync::Arc};
#[path = "authority.rs"]
mod authority;
#[path = "executor.rs"]
mod executor;
#[path = "metadata.rs"]
mod metadata;
type Execution = mrr::AdmittedPropertyExecution<mrr_data_core::BoundDataQuery>;
const RESERVED: usize = 8 << 20;

async fn restore(
    f: &Fixture,
    backend: &Backend,
    remote: Arc<Remote>,
    cache: Arc<MemoryContentStore>,
) -> ResourceHandle<mrr_data_content::PreparedCombinedGraph> {
    restore_combined_graph_content(
        backend,
        CombinedGraphArRestoreRequest {
            local: cache,
            remote,
            query: f.query.clone(),
            relations: f.relations.clone(),
            entities: f.entities.clone(),
            dataset: f.capture_limits.dataset,
            transfer: f.transfer_limits,
        },
        RESERVED,
        ResourceControl::default(),
    )
    .await
    .unwrap()
}
async fn capture(
    f: &Fixture,
    backend: &Backend,
    restored: ResourceHandle<mrr_data_content::PreparedCombinedGraph>,
) -> ResourceHandle<CapturedCombinedGraphAr> {
    let query = f.query.clone();
    let relations = f.relations.clone();
    let projection = f.projection.clone();
    backend
        .prepare_resource_controlled(RESERVED, ResourceControl::default(), move |_| {
            capture_combined_graphar(
                restored.get(),
                &query,
                &relations,
                &projection,
                capture_limits(),
            )
        })
        .await
        .unwrap()
}
async fn execute(
    f: &Fixture,
    backend: &Backend,
    captured: ResourceHandle<CapturedCombinedGraphAr>,
) -> ResourceHandle<Execution> {
    let tables = captured
        .get()
        .tables(&f.query)
        .unwrap()
        .iter()
        .map(|t| mrr_data_datafusion::EntityPropertyTable {
            schema: t.schema.clone(),
            batch: t.batch.clone(),
        })
        .collect::<Vec<_>>();
    let relations = relation_tables(f, captured.get());
    let physical = executor::CapturedBackend {
        binding: f.query.clone(),
        tables,
        relations,
        limits: properties::limits(),
    };
    dispatch(f, backend, captured, physical).await
}
async fn dispatch<T: Send + Sync + 'static>(
    f: &Fixture,
    backend: &Backend,
    captured: ResourceHandle<T>,
    physical: executor::CapturedBackend,
) -> ResourceHandle<Execution> {
    let bound = compile()
        .bind(&f.relations, &f.entities, &f.original.semantic)
        .unwrap();
    assert_eq!(bound.compilation().source_digest, SOURCE_DIGEST);
    backend
        .prepare_resource_async_controlled(
            RESERVED,
            ResourceControl::default(),
            move |control| async move {
                control.check()?;
                let output = bound
                    .execute_with(&physical, result_limits())
                    .await
                    .unwrap();
                control.check()?;
                drop(captured);
                Ok::<_, ResourceStop>(output)
            },
        )
        .await
        .unwrap()
}
#[tokio::test]
async fn original_source_handoff_shared_backend_reaches_retained_consumer_transport() {
    let f = Fixture::with_original(source_fixture());
    let storage = metadata::SimulatedMetadata::default();
    let backend = Backend::open(
        BackendConfig {
            max_resource_bytes: 3 * RESERVED,
            ..BackendConfig::default()
        },
        storage.clone(),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let remote = Arc::new(Remote::default());
    let (policy_home, policy, publication) =
        authority::publish(&f, &backend, remote.as_ref()).await;
    let cache = Arc::new(MemoryContentStore::default());
    let first = query_transport(
        &f,
        &backend,
        remote.clone(),
        cache.clone(),
        &policy_home,
        policy,
    )
    .await;
    drop(first);
    assert_eq!(backend.status().resource_bytes, 0);
    remote.blocks.lock().unwrap().clear();
    let warm = query_transport(
        &f,
        &backend,
        remote.clone(),
        cache.clone(),
        &policy_home,
        policy,
    )
    .await;
    drop(warm);
    assert_eq!(backend.status().resource_bytes, 0);
    let restored = restore(&f, &backend, remote, cache).await;
    let captured = capture(&f, &backend, restored).await;
    let other = alternate_root(&f);
    assert!(captured.get().tables(&other).is_err());
    assert!(captured.get().relations(&other).is_err());
    for _ in 0..2 {
        let transport = captured_transport(&f, &backend, captured.clone()).await;
        assert!(authority::disclose(&policy_home, policy).await);
        assert_eq!(backend.status().resource_bytes, 2 * RESERVED);
        let insufficient = mrr::QueryResultLimits::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );
        assert!(
            transport
                .get()
                .verify(&f.query, insufficient, NonZeroUsize::new(1 << 20).unwrap())
                .is_err()
        );
        drop(transport);
        assert_eq!(backend.status().resource_bytes, RESERVED);
    }
    let transport = captured_transport(&f, &backend, captured.clone()).await;
    assert!(authority::disclose(&policy_home, policy).await);
    let limits = result_limits();
    let cap = NonZeroUsize::new(1 << 20).unwrap();
    authority::retire_and_recover(&f, &policy_home, policy, &publication).await;
    // Immutable physical source remains usable, but a previous disclosure does
    // not authorize another consumer after policy retirement.
    let after_retirement = captured_transport(&f, &backend, captured.clone()).await;
    assert!(!authority::disclose(&policy_home, policy).await);
    assert_eq!(backend.status().resource_bytes, 3 * RESERVED);
    drop(after_retirement);
    drop(captured);
    assert_eq!(backend.status().resource_bytes, RESERVED);
    drain(&backend, transport, &f.query, limits, cap).await;
    let reopened = Backend::open(
        BackendConfig::default(),
        storage,
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    authority::verify_reopened_history(&f, &reopened, policy).await;
    reopened.shutdown().await.unwrap();
}

fn result_limits() -> mrr::QueryResultLimits {
    mrr::QueryResultLimits::new(
        NonZeroUsize::new(100).unwrap(),
        NonZeroUsize::new(300).unwrap(),
    )
}
async fn query_transport(
    f: &Fixture,
    backend: &Backend,
    remote: Arc<Remote>,
    cache: Arc<MemoryContentStore>,
    policy_home: &mrr_data_backend::ProfilePort,
    policy: mrr_data_backend::AuthorityState,
) -> ResourceHandle<DataQueryResultHandoff> {
    let restored = restore(f, backend, remote, cache).await;
    assert_eq!(backend.status().resource_bytes, RESERVED);
    let captured = capture(f, backend, restored).await;
    assert_eq!(backend.status().resource_bytes, RESERVED);
    let transport = captured_transport(f, backend, captured).await;
    assert!(authority::disclose(policy_home, policy).await);
    assert_eq!(backend.status().resource_bytes, RESERVED);
    transport
}

async fn captured_transport(
    f: &Fixture,
    backend: &Backend,
    captured: ResourceHandle<CapturedCombinedGraphAr>,
) -> ResourceHandle<DataQueryResultHandoff> {
    let output = execute(f, backend, captured).await;
    execution_transport(f, output)
}
fn execution_transport(
    f: &Fixture,
    output: ResourceHandle<Execution>,
) -> ResourceHandle<DataQueryResultHandoff> {
    let mut rows = output.get().candidate().rows().to_vec();
    rows.sort_by_key(|row| format!("{row:?}"));
    let mut expected = crate::tests::entity_properties::acceptance::expected();
    expected.push(expected[0].clone());
    expected.sort_by_key(|row| format!("{row:?}"));
    assert_eq!(rows, expected);
    let limits = mrr::QueryResultLimits::new(
        NonZeroUsize::new(100).unwrap(),
        NonZeroUsize::new(300).unwrap(),
    );
    let cap = NonZeroUsize::new(1 << 20).unwrap();
    let insufficient =
        mrr::QueryResultLimits::new(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(1).unwrap());
    assert!(DataQueryResultHandoff::export_execution(output.get(), insufficient, cap).is_err());
    let transport = output
        .try_transform(|execution| {
            DataQueryResultHandoff::export_execution(&execution, limits, cap)
        })
        .unwrap_or_else(|_| panic!("unique candidate transport conversion"));
    transport.get().verify(&f.query, limits, cap).unwrap();
    transport
}

async fn drain(
    backend: &Backend,
    transport: ResourceHandle<DataQueryResultHandoff>,
    query: &mrr_data_core::BoundDataQuery,
    limits: mrr::QueryResultLimits,
    cap: NonZeroUsize,
) {
    let retained = transport.clone();
    drop(transport);
    let closing = backend.clone();
    let shutdown = tokio::spawn(async move { closing.shutdown().await });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while backend.status().lifecycle != mrr_data_backend::Lifecycle::Draining {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!shutdown.is_finished());
    retained.get().verify(query, limits, cap).unwrap();
    drop(retained);
    tokio::time::timeout(std::time::Duration::from_secs(3), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(backend.status().resource_bytes, 0);
}

#[path = "cross_profile.rs"]
mod cross_profile;
#[path = "request_controls.rs"]
mod request_controls;

#[cfg(feature = "selective-graphar")]
#[path = "selective.rs"]
mod selective;

#[cfg(all(
    feature = "selective-graphar",
    any(target_os = "linux", target_os = "macos")
))]
#[path = "process_recovery.rs"]
mod process_recovery;

#[path = "research.rs"]
mod research;
