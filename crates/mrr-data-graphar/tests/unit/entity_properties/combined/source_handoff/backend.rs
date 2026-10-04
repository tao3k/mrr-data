//! One simulated original-source caller retains each phase on the common Backend.
use super::*;
use crate::{
    CapturedCombinedGraphAr, CombinedGraphArRestoreRequest, restore_combined_graph_content,
};
use mrr_data_backend::{Backend, BackendConfig, ResourceControl, ResourceHandle, ResourceStop};
use mrr_data_core::{DataQueryResultHandoff, PhysicalQueryOutput};
use std::sync::Arc;
#[path = "authority.rs"]
mod authority;
#[path = "metadata.rs"]
mod metadata;
const RESERVED: usize = 8 << 20;

async fn restore(
    f: &Fixture,
    backend: &Backend,
    remote: Arc<Remote>,
) -> ResourceHandle<mrr_data_content::PreparedCombinedGraph> {
    restore_combined_graph_content(
        backend,
        CombinedGraphArRestoreRequest {
            local: Arc::new(MemoryContentStore::default()),
            remote,
            query: f.query.clone(),
            relations: f.relations.clone(),
            entities: f.entities.clone(),
            dataset: capture_limits().dataset,
            transfer: transfer_limits(),
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
) -> ResourceHandle<PhysicalQueryOutput> {
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
    let bound = compile()
        .bind(&f.relations, &f.entities, &f.original.semantic)
        .unwrap();
    assert_eq!(bound.compilation().source_digest, SOURCE_DIGEST);
    let query = bound.query().clone();
    backend
        .prepare_resource_async_controlled(
            RESERVED,
            ResourceControl::default(),
            move |control| async move {
                control.check()?;
                let output = mrr_data_datafusion::execute_property_path_query(
                    &query,
                    &tables,
                    &relations,
                    properties::limits(),
                )
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
    let restored = restore(&f, &backend, remote).await;
    assert_eq!(backend.status().resource_bytes, RESERVED);
    let captured = capture(&f, &backend, restored).await;
    assert_eq!(backend.status().resource_bytes, RESERVED);
    let output = execute(&f, &backend, captured).await;
    let mut rows = output.get().rows().to_vec();
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
    let transport = output
        .try_transform(|output| {
            DataQueryResultHandoff::export(
                &f.query,
                &mrr_data_datafusion::datafusion_engine_profile().unwrap(),
                output,
                limits,
                cap,
            )
        })
        .unwrap_or_else(|_| panic!("unique candidate transport conversion"));
    assert!(authority::disclose(&policy_home, policy).await);
    transport.get().verify(&f.query, limits, cap).unwrap();
    assert_eq!(backend.status().resource_bytes, RESERVED);
    authority::retire_and_recover(&f, &policy_home, policy, &publication).await;
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
