//! Native recovery must progress while accepted writes wait for one connection.
#![cfg(any(feature = "turso", feature = "duckdb"))]
#[path = "../support/held_provider.rs"]
mod held_provider;
use held_provider::{Gate, Held};
use mrr_data_backend::{
    AuthorityProposal, AuthorityStatus, Backend, BackendConfig, BackendError, Lifecycle,
    ProfilePort,
};
use mrr_data_content::ConditionalContentCommitOutcome;
use mrr_data_content::{
    CacheAdmission, ConditionalCommitPortError, ConditionalContentCommitPort,
    ConditionalContentWrite, ContentBlock, ContentCodec, PublishReceipt,
};
use std::{
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::task::JoinSet;

mod mixed_lifecycle;

#[cfg(feature = "turso")]
type Native = mrr_data_backend::providers::TursoProvider;
#[cfg(all(not(feature = "turso"), feature = "duckdb"))]
type Native = mrr_data_backend::providers::DuckDbProvider;
fn native(path: PathBuf) -> Native {
    #[cfg(feature = "turso")]
    {
        Native::new(path, tokio::runtime::Handle::current())
    }
    #[cfg(all(not(feature = "turso"), feature = "duckdb"))]
    {
        Native::new(path)
    }
}
fn write<'a>(scope: &'a str, operation_id: &'a str) -> ConditionalContentWrite<'a> {
    ConditionalContentWrite {
        scope,
        operation_id,
        expected: None,
        replacement: ContentBlock::new(ContentCodec::Raw, b"dispatch").cid(),
    }
}
fn ack(w: ConditionalContentWrite<'_>) -> PublishReceipt {
    PublishReceipt {
        cid: w.replacement,
        cache: CacheAdmission::Stored,
    }
}
#[test]
fn recovery_progresses_when_write_admission_is_saturated_with_two_host_threads() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let gate = Gate(Arc::new((Mutex::new(false), Condvar::new())));
        let entered = Arc::new(AtomicBool::new(false));
        let backend = Backend::open(
            BackendConfig {
                max_writes: 8,
                ..BackendConfig::default()
            },
            Held {
                native: native(dir.path().join("dispatch.db")),
                gate: gate.0.clone(),
                entered: entered.clone(),
            },
            tokio::runtime::Handle::current(),
        )
        .await
        .unwrap();
        let port = backend.profile("dispatch.v1", "tenant").unwrap();
        let seed = write("seed", "seed");
        port.commit(seed, Some(&ack(seed)), |_| Ok::<_, ()>(()))
            .await
            .unwrap();
        let mut jobs = JoinSet::new();
        let held_port = port.clone();
        jobs.spawn(async move {
            let held = write("held", "held");
            held_port
                .commit(held, Some(&ack(held)), |_| Ok::<_, ()>(()))
                .await
                .unwrap();
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            while !entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for id in 0..7 {
            let port = port.clone();
            jobs.spawn(async move {
                let scope = format!("queued-{id}");
                let w = write(&scope, "queued");
                port.commit(w, Some(&ack(w)), |_| Ok::<_, ()>(()))
                    .await
                    .unwrap();
            });
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            while backend.status().active_writes != 8 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(backend.status().blocking_writes, 1);
        let refused = write("refused", "refused");
        assert!(matches!(
            port.commit(refused, Some(&ack(refused)), |_| -> Result<(), ()> {
                panic!("saturated request reached validator")
            })
            .await,
            Err(ConditionalCommitPortError::BeforeCommit(
                BackendError::Saturated
            ))
        ));
        assert_eq!(backend.status().saturated_writes, 1);
        let recovered = tokio::time::timeout(Duration::from_millis(500), port.recover(seed)).await;
        gate.release();
        while let Some(job) = jobs.join_next().await {
            job.unwrap();
        }
        backend.shutdown().await.unwrap();
        assert!(
            recovered.is_ok(),
            "queued writers starved reserved recovery in the Host pool"
        );
        assert!(recovered.unwrap().unwrap().is_some());
        assert_eq!(backend.status().completed, 10);
        assert_eq!(backend.status().blocking_writes, 0);
        assert_eq!(backend.status().blocking_recoveries, 0);
        assert_eq!(backend.status().saturated_recoveries, 0);
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_cancellation_preserves_owned_authority_and_drains_content_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue-cancel.db");
    let gate = Gate(Arc::new((Mutex::new(false), Condvar::new())));
    let entered = Arc::new(AtomicBool::new(false));
    let backend = Backend::open(
        BackendConfig::default(),
        Held {
            native: native(path.clone()),
            gate: gate.0.clone(),
            entered: entered.clone(),
        },
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let port = backend.profile("dispatch.v1", "tenant").unwrap();
    let held_port = port.clone();
    let held = tokio::spawn(async move {
        let w = write("held", "held");
        held_port
            .commit(w, Some(&ack(w)), |_| Ok::<_, ()>(()))
            .await
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !entered.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let validated = Arc::new(AtomicBool::new(false));
    cancel_queued(&backend, &port, validated.clone()).await;
    assert!(!validated.load(Ordering::Acquire));
    assert_eq!(backend.status().active_writes, 3);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), backend.shutdown())
            .await
            .is_err()
    );
    assert_eq!(backend.status().lifecycle, Lifecycle::Draining);
    gate.release();
    held.await.unwrap();
    backend.shutdown().await.unwrap();
    assert_eq!(backend.status().completed, 3);
    assert!(!validated.load(Ordering::Acquire));
    let reopened = Backend::open(
        BackendConfig::default(),
        native(path),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let port = reopened.profile("dispatch.v1", "tenant").unwrap();
    assert!(
        port.recover(write("cancelled", "queued-content"))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        port.authority("authority-home", "policy")
            .await
            .unwrap()
            .unwrap()
            .generation,
        1
    );
    reopened.shutdown().await.unwrap();
}

async fn cancel_queued(backend: &Backend, port: &ProfilePort, validated: Arc<AtomicBool>) {
    let validate = validated.clone();
    let content_port = port.clone();
    let content = tokio::spawn(async move {
        let w = write("cancelled", "queued-content");
        content_port
            .commit(w, Some(&ack(w)), move |_| {
                validate.store(true, Ordering::Release);
                Ok::<_, ()>(())
            })
            .await
            .unwrap();
    });
    let admin_port = port.clone();
    let proposal = AuthorityProposal {
        authority_id: "policy".into(),
        expected: None,
        replacement: write("unused", "unused").replacement,
        status: AuthorityStatus::Active,
    };
    let admin = tokio::spawn(async move {
        admin_port
            .advance_authority("authority-home", proposal)
            .await
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while backend.status().active_writes != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    content.abort();
    admin.abort();
    assert!(content.await.unwrap_err().is_cancelled());
    assert!(admin.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn successful_responses_release_single_slot_admission_before_the_next_request() {
    let dir = tempfile::tempdir().unwrap();
    let backend = Backend::open(
        BackendConfig {
            max_writes: 1,
            max_recoveries: 1,
            ..BackendConfig::default()
        },
        native(dir.path().join("single-slot.db")),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let port = backend.profile("dispatch.v1", "tenant").unwrap();
    let mut expected = None;
    for index in 0..64 {
        let operation = format!("operation-{index}");
        let mut w = write("single-home", &operation);
        w.expected = expected;
        let physical = ack(w);
        let ConditionalContentCommitOutcome::Committed(receipt) = port
            .commit(w, Some(&physical), |_| Ok::<_, ()>(()))
            .await
            .unwrap()
        else {
            panic!("fresh operation replayed");
        };
        assert_eq!(backend.status().active_writes, 0);
        assert_eq!(backend.status().blocking_writes, 0);
        expected = Some(receipt.committed);
        assert_eq!(
            port.recover(w).await.unwrap().unwrap().committed,
            receipt.committed
        );
        assert_eq!(backend.status().active_recoveries, 0);
        assert_eq!(backend.status().blocking_recoveries, 0);
    }
    backend.shutdown().await.unwrap();
    assert_eq!(backend.status().completed, 128);
}

#[test]
fn recovery_progresses_while_resource_preparation_holds_the_shared_host_budget() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let gate = Gate(Arc::new((Mutex::new(false), Condvar::new())));
        let backend = Backend::open(
            BackendConfig::default(),
            native(dir.path().join("resources.db")),
            tokio::runtime::Handle::current(),
        )
        .await
        .unwrap();
        let port = backend.profile("resources.v1", "tenant").unwrap();
        let seed = write("seed", "seed");
        port.commit(seed, Some(&ack(seed)), |_| Ok::<_, ()>(()))
            .await
            .unwrap();
        let worker = backend.clone();
        let worker_gate = gate.0.clone();
        let entered = Arc::new(AtomicBool::new(false));
        let mark = entered.clone();
        let resource = tokio::spawn(async move {
            worker
                .prepare_resource(8, move || {
                    mark.store(true, Ordering::Release);
                    let (lock, changed) = &*worker_gate;
                    let released = lock.lock().unwrap();
                    let (released, _) = changed
                        .wait_timeout_while(released, Duration::from_secs(3), |released| !*released)
                        .unwrap();
                    if !*released {
                        return Err(BackendError::Unavailable);
                    }
                    Ok(())
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            while !entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let queued_port = port.clone();
        let queued = tokio::spawn(async move {
            let w = write("queued", "queued");
            queued_port
                .commit(w, Some(&ack(w)), |_| Ok::<_, ()>(()))
                .await
                .unwrap();
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            while backend.status().active_writes != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(backend.status().blocking_resources, 1);
        assert_eq!(backend.status().blocking_writes, 0);
        let recovered = tokio::time::timeout(Duration::from_millis(500), port.recover(seed)).await;
        gate.release();
        drop(resource.await.unwrap().unwrap());
        queued.await.unwrap();
        backend.shutdown().await.unwrap();
        assert!(recovered.is_ok(), "resource preparation starved recovery");
        assert!(recovered.unwrap().unwrap().is_some());
    });
}

#[cfg(feature = "arrow-query")]
#[test]
fn recovery_progresses_during_query_backpressure_with_queued_fresh_work() {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use mrr_data_backend::ArrowQueryLimits;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let backend = Backend::open(
            BackendConfig::default(),
            native(dir.path().join("query.db")),
            tokio::runtime::Handle::current(),
        )
        .await
        .unwrap();
        let port = backend.profile("query.v1", "tenant").unwrap();
        let seed = write("seed", "seed");
        port.commit(seed, Some(&ack(seed)), |_| Ok::<_, ()>(()))
            .await
            .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        let bytes = batch.get_array_memory_size();
        let mut query = backend
            .query_arrow(
                batch.schema(),
                ArrowQueryLimits {
                    max_rows: 4,
                    max_batches: 2,
                    max_batch_bytes: bytes,
                    max_retained_bytes: bytes,
                    channel_capacity: 1,
                },
                bytes,
                move |out| {
                    out.emit(|| Ok(batch.clone()))?;
                    out.emit(|| Ok(batch))
                },
            )
            .unwrap();
        let first = query.next_batch().await.unwrap().unwrap();
        let queued_port = port.clone();
        let queued = tokio::spawn(async move {
            let w = write("queued", "queued");
            queued_port
                .commit(w, Some(&ack(w)), |_| Ok::<_, ()>(()))
                .await
                .unwrap();
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            while backend.status().active_writes != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(backend.status().blocking_resources, 1);
        assert_eq!(backend.status().blocking_writes, 0);
        let recovered = tokio::time::timeout(Duration::from_millis(500), port.recover(seed)).await;
        drop(first);
        drop(query.next_batch().await.unwrap().unwrap());
        assert!(query.next_batch().await.unwrap().is_none());
        queued.await.unwrap();
        drop(query);
        backend.shutdown().await.unwrap();
        assert!(recovered.is_ok(), "query backpressure starved recovery");
        assert!(recovered.unwrap().unwrap().is_some());
    });
}
