//! Real metadata writes and Turso queries share worker admission.
#[path = "../../../mrr-data-backend/tests/support/held_provider.rs"]
mod held_provider;

use super::{TursoBackendQuery, captured, limits};
use held_provider::{Gate, Held};
use mrr_data_backend::{Backend, BackendConfig, providers::TursoProvider};
use mrr_data_content::{
    CacheAdmission, ConditionalContentCommitPort, ConditionalContentWrite, ContentBlock,
    ContentCodec, PublishReceipt,
};
use mrr_data_turso_query::execute_turso_graphar_retained_on_backend;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn write(scope: &str) -> ConditionalContentWrite<'_> {
    ConditionalContentWrite {
        scope,
        operation_id: scope,
        expected: None,
        replacement: ContentBlock::new(ContentCodec::Raw, b"query-dispatch").cid(),
    }
}
fn ack(request: ConditionalContentWrite<'_>) -> PublishReceipt {
    PublishReceipt {
        cid: request.replacement,
        cache: CacheAdmission::Stored,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_metadata_write_queues_query_and_reserved_recovery_progresses() {
    let (query, projection, source, _) = captured();
    let dir = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Handle::current();
    let gate = Gate(Arc::new((Mutex::new(false), Condvar::new())));
    let entered = Arc::new(AtomicBool::new(false));
    let backend = Backend::open(
        BackendConfig {
            max_shared_workers: 1,
            ..BackendConfig::default()
        },
        Held {
            native: TursoProvider::new(dir.path().join("metadata.db"), runtime.clone()),
            gate: gate.0.clone(),
            entered: entered.clone(),
        },
        runtime.clone(),
    )
    .await
    .unwrap();
    let port = backend.profile("query-dispatch", "tenant").unwrap();
    let seed = write("seed");
    port.commit(seed, Some(&ack(seed)), |_| Ok::<_, ()>(()))
        .await
        .unwrap();
    let holding_port = port.clone();
    let holding = tokio::spawn(async move {
        let request = write("held");
        holding_port
            .commit(request, Some(&ack(request)), |_| Ok::<_, ()>(()))
            .await
            .unwrap();
    });
    until(|| entered.load(Ordering::Acquire)).await;
    let database = Arc::new(
        turso::Builder::new_local(dir.path().join("query.db").to_str().unwrap())
            .build()
            .await
            .unwrap(),
    );
    let source = Arc::new(source);
    let queued_backend = backend.clone();
    let queued_runtime = runtime.clone();
    let request = TursoBackendQuery {
        database: database.clone(),
        query: query.clone(),
        source: source.clone(),
        projection: projection.clone(),
        limits: limits(),
        reserved_bytes: 2048,
    };
    let queued = tokio::spawn(async move {
        execute_turso_graphar_retained_on_backend(&queued_backend, queued_runtime, request).await
    });
    until(|| backend.status().active_resources == 1).await;
    assert_eq!(backend.status().blocking_writes, 1);
    assert_eq!(backend.status().blocking_resources, 0);
    assert!(!queued.is_finished());
    let recovered = tokio::time::timeout(Duration::from_secs(3), port.recover(seed))
        .await
        .unwrap()
        .unwrap();
    assert!(recovered.is_some());
    // Aborting the waiter while the common permit is held discards the owned
    // request before the query driver can open its invocation connection.
    queued.abort();
    assert!(matches!(queued.await, Err(error) if error.is_cancelled()));
    until(|| backend.status().active_resources == 0).await;
    assert_eq!(backend.status().resource_bytes, 0);
    assert_eq!(backend.status().blocking_resources, 0);
    assert_eq!(Arc::strong_count(&source), 1);
    assert_eq!(Arc::strong_count(&database), 1);
    gate.release();
    holding.await.unwrap();
    let output = execute_turso_graphar_retained_on_backend(
        &backend,
        runtime,
        TursoBackendQuery {
            database,
            query,
            source,
            projection,
            limits: limits(),
            reserved_bytes: 2048,
        },
    )
    .await
    .unwrap();
    assert_eq!(output.get().rows().len(), 2);
    // Retained output holds its resource reservation, but has returned the
    // shared worker permit so another admitted physical operation can run.
    let alongside = backend.prepare_resource(8, || Ok(())).await.unwrap();
    assert_eq!(backend.status().active_resources, 2);
    assert_eq!(backend.status().blocking_resources, 0);
    drop(alongside);
    drop(output);
    backend.shutdown().await.unwrap();
    assert_eq!(backend.status().resource_bytes, 0);
}
