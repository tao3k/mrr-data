//! Fallible terminals, buffer backpressure, cancellation and retained drain.
#![cfg(feature = "arrow-query")]
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use mrr_data_backend::{
    ArrowQueryError, ArrowQueryLimits, AuthorityCapability, Backend, BackendConfig, BackendError,
    Lifecycle, MetadataProvider, ProviderCapabilities, StoredOutcome, StoredRevision, StoredWrite,
    providers::ProviderResult,
};
use mrr_data_content::{ContentRevision, PublishReceipt};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
struct Release(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
impl Drop for Release {
    fn drop(&mut self) {
        *self.0.0.lock().unwrap() = true;
        self.0.1.notify_all();
    }
}
struct Provider;
impl MetadataProvider for Provider {
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            atomic_head_operation: true,
            durable_commit: true,
            historical_lookup: true,
            authority_versions: AuthorityCapability::Unsupported,
        }
    }
    fn open(&self) -> Result<(), BackendError> {
        Ok(())
    }
    fn close(&self) -> Result<(), BackendError> {
        Ok(())
    }
    fn commit(
        &self,
        _: &StoredWrite,
        _: Option<&PublishReceipt>,
        _: &mut dyn FnMut(Option<ContentRevision>) -> bool,
    ) -> ProviderResult<StoredOutcome> {
        panic!("query fixture never commits")
    }
    fn recover(&self, _: &StoredWrite) -> ProviderResult<Option<StoredRevision>> {
        Ok(None)
    }
}
fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
    )
    .unwrap()
}
fn limits() -> ArrowQueryLimits {
    ArrowQueryLimits {
        max_rows: 20,
        max_batches: 5,
        max_batch_bytes: 512,
        max_retained_bytes: 1024,
        channel_capacity: 1,
    }
}
async fn backend() -> Backend {
    Backend::open(
        BackendConfig::default(),
        Provider,
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap()
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn fallible_fetch_distinguishes_exact_limit_eof_from_late_failure_and_excess() {
    for terminal in [Ok(None), Err(ArrowQueryError::Driver), Ok(Some(batch()))] {
        let backend = backend().await;
        let expected = match &terminal {
            Ok(None) => None,
            Err(error) => Some(*error),
            Ok(Some(_)) => Some(ArrowQueryError::Limit),
        };
        let mut query = backend
            .query_arrow(
                batch().schema(),
                ArrowQueryLimits {
                    max_batches: 1,
                    max_rows: 4,
                    ..limits()
                },
                1024,
                move |out| {
                    assert!(out.emit_next(|| Ok(Some(batch())))?);
                    // Ignoring a late refusal cannot manufacture successful EOF.
                    let _ = out.emit_next(|| terminal);
                    if let Some(error) = expected {
                        assert_eq!(
                            out.emit_next(|| panic!("failed source fetched again")),
                            Err(error)
                        );
                    }
                    Ok(())
                },
            )
            .unwrap();
        let first = query.next_batch().await.unwrap().unwrap();
        assert_eq!(first.batch().num_rows(), 4);
        drop(first);
        if let Some(error) = expected {
            assert_eq!(query.next_batch().await.err(), Some(error));
            assert!(query.summary().is_none());
        } else {
            assert!(query.next_batch().await.unwrap().is_none());
            assert_eq!(query.summary().unwrap().batches, 1);
            assert_eq!(query.summary().unwrap().rows, 4);
        }
        drop(query);
        backend.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn empty_fallible_source_releases_reservation_and_reports_zero_batches() {
    let backend = backend().await;
    let mut query = backend
        .query_arrow(batch().schema(), limits(), 1024, |out| {
            assert!(!out.emit_next(|| Ok(None))?);
            Ok(())
        })
        .unwrap();
    assert!(query.next_batch().await.unwrap().is_none());
    assert_eq!(query.summary().unwrap().batches, 0);
    assert_eq!(backend.status().resource_bytes, 0);
    drop(query);
    backend.shutdown().await.unwrap();
}
#[tokio::test]
async fn fallible_eof_fetch_waits_for_retained_bytes_and_cancel_skips_fetch() {
    let backend = backend().await;
    let bytes = batch().get_array_memory_size();
    let fetched = Arc::new(AtomicUsize::new(0));
    let marker = fetched.clone();
    let mut query = backend
        .query_arrow(
            batch().schema(),
            ArrowQueryLimits {
                max_batch_bytes: bytes,
                max_retained_bytes: bytes,
                ..limits()
            },
            bytes,
            move |out| {
                assert!(out.emit_next(|| Ok(Some(batch())))?);
                let result = out.emit_next(|| {
                    marker.fetch_add(1, Ordering::SeqCst);
                    Ok(None)
                });
                assert_eq!(result, Err(ArrowQueryError::Cancelled));
                Ok(())
            },
        )
        .unwrap();
    let retained = query.next_batch().await.unwrap().unwrap();
    assert_eq!(fetched.load(Ordering::SeqCst), 0);
    query.cancel();
    until(|| backend.status().blocking_resources == 0).await;
    assert_eq!(fetched.load(Ordering::SeqCst), 0);
    assert_eq!(
        query.next_batch().await.err(),
        Some(ArrowQueryError::Cancelled)
    );
    assert!(query.summary().is_none());
    drop(retained);
    drop(query);
    backend.shutdown().await.unwrap();
}
#[tokio::test]
async fn late_errors_and_worker_panic_never_certify_partial_output() {
    for panic_worker in [false, true] {
        let backend = backend().await;
        let mut query = backend
            .query_arrow(batch().schema(), limits(), 1024, move |out| {
                out.emit(|| Ok(batch()))?;
                assert!(!panic_worker, "native producer lost");
                Err(ArrowQueryError::Driver)
            })
            .unwrap();
        assert_eq!(
            query
                .next_batch()
                .await
                .unwrap()
                .unwrap()
                .batch()
                .num_rows(),
            4
        );
        let error = query.next_batch().await.err().unwrap();
        assert_eq!(
            error,
            if panic_worker {
                ArrowQueryError::WorkerLost
            } else {
                ArrowQueryError::Driver
            }
        );
        assert!(query.summary().is_none());
        assert_eq!(query.next_batch().await.err(), Some(error));
        drop(query);
        backend.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn ignored_emitter_failure_remains_a_failed_terminal() {
    let backend = backend().await;
    let mut query = backend
        .query_arrow(batch().schema(), limits(), 1024, |out| {
            let wrong = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
                "other",
                DataType::Int64,
                false,
            )])));
            let _ = out.emit(|| Ok(wrong));
            Ok(())
        })
        .unwrap();
    assert_eq!(
        query.next_batch().await.err(),
        Some(ArrowQueryError::Schema)
    );
    assert!(query.summary().is_none());
    drop(query);
    backend.shutdown().await.unwrap();
}
#[tokio::test]
async fn final_batch_clone_holds_drain_after_successful_query_completion() {
    let backend = backend().await;
    let mut query = backend
        .query_arrow(batch().schema(), limits(), 1024, |out| {
            out.emit(|| Ok(batch()))
        })
        .unwrap();
    let first = query.next_batch().await.unwrap().unwrap();
    let last = first.clone();
    assert!(query.next_batch().await.unwrap().is_none());
    assert_eq!(query.summary().unwrap().rows, 4);
    assert_eq!(backend.status().blocking_resources, 0);
    assert_eq!(backend.status().active_resources, 1);
    let closing = backend.clone();
    let task = tokio::spawn(async move { closing.shutdown().await });
    until(|| backend.status().lifecycle == Lifecycle::Draining).await;
    assert!(matches!(
        backend.query_arrow(batch().schema(), limits(), 1024, |_| Ok(())),
        Err(ArrowQueryError::Backend(BackendError::NotReady))
    ));
    drop(query);
    drop(first);
    assert_eq!(last.batch().num_rows(), 4);
    assert!(!task.is_finished());
    drop(last);
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(backend.status().resource_bytes, 0);
}
#[tokio::test]
async fn byte_backpressure_precedes_fetch_and_consumer_drop_wakes_the_producer() {
    let backend = backend().await;
    let bytes = batch().get_array_memory_size();
    let limits = ArrowQueryLimits {
        max_batch_bytes: bytes,
        max_retained_bytes: bytes,
        ..limits()
    };
    let fetched = Arc::new(AtomicUsize::new(0));
    let marker = fetched.clone();
    let mut query = backend
        .query_arrow(batch().schema(), limits, bytes, move |out| {
            for _ in 0..3 {
                out.emit(|| {
                    marker.fetch_add(1, Ordering::SeqCst);
                    Ok(batch())
                })?;
            }
            Ok(())
        })
        .unwrap();
    let first = query.next_batch().await.unwrap().unwrap();
    let shared = first.clone();
    drop(first);
    assert_eq!(fetched.load(Ordering::SeqCst), 1);
    drop(shared);
    let second = query.next_batch().await.unwrap().unwrap();
    assert_eq!(fetched.load(Ordering::SeqCst), 2);
    drop(query);
    until(|| backend.status().blocking_resources == 0).await;
    assert_eq!(fetched.load(Ordering::SeqCst), 2);
    assert_eq!(backend.status().active_resources, 1);
    drop(second);
    backend.shutdown().await.unwrap();
}
#[tokio::test]
async fn panicking_native_interrupt_cannot_unwind_cancellation_or_consumer_drop() {
    for explicit in [false, true] {
        let backend = backend().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let marker = calls.clone();
        let mut query = backend
            .query_arrow(batch().schema(), limits(), 1024, move |out| {
                out.on_cancel(move || {
                    marker.fetch_add(1, Ordering::SeqCst);
                    panic!("failed native interrupt callback");
                })?;
                for _ in 0..3 {
                    out.emit(|| Ok(batch()))?;
                }
                Ok(())
            })
            .unwrap();
        let retained = query.next_batch().await.unwrap().unwrap();
        if explicit {
            query.cancel();
            assert_eq!(
                query.next_batch().await.err(),
                Some(ArrowQueryError::Cancelled)
            );
            assert!(query.summary().is_none());
        }
        drop(query);
        drop(retained);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        tokio::time::timeout(Duration::from_secs(3), backend.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(backend.status().resource_bytes, 0);
    }
}
#[tokio::test]
async fn configuration_row_and_batch_limits_refuse_without_complete_results() {
    let backend = backend().await;
    assert!(matches!(
        backend.query_arrow(batch().schema(), limits(), 1, |_| Ok(())),
        Err(ArrowQueryError::Limit)
    ));
    assert!(matches!(
        backend.query_arrow(
            batch().schema(),
            ArrowQueryLimits {
                channel_capacity: 0,
                ..limits()
            },
            1024,
            |_| Ok(())
        ),
        Err(ArrowQueryError::InvalidConfiguration)
    ));
    for bound in [
        ArrowQueryLimits {
            max_rows: 3,
            ..limits()
        },
        ArrowQueryLimits {
            max_batch_bytes: 1,
            ..limits()
        },
    ] {
        let mut query = backend
            .query_arrow(batch().schema(), bound, 1024, |out| {
                out.emit(|| Ok(batch()))
            })
            .unwrap();
        assert_eq!(query.next_batch().await.err(), Some(ArrowQueryError::Limit));
        assert!(query.summary().is_none());
    }
    backend.shutdown().await.unwrap();
}
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn native_duckdb_arrow_complete_count_and_late_execution_failure() {
    use mrr_data_backend::providers::{emit_duckdb_arrow, emit_duckdb_arrow_stream};
    for streaming in [false, true] {
        let backend = backend().await;
        let sql = "SELECT i::BIGINT AS id, CASE i%3 WHEN 0 THEN '实体/é' WHEN 1 THEN '' ELSE NULL END AS entity, CASE i%3 WHEN 0 THEN from_hex('00ff7061796c6f6164') WHEN 1 THEN from_hex('') ELSE NULL END AS payload FROM range(4097) t(i)";
        let mut input = duckdb::Connection::open_in_memory().unwrap();
        let schema = input
            .prepare(sql)
            .unwrap()
            .query_arrow([])
            .unwrap()
            .get_schema();
        let native_limits = ArrowQueryLimits {
            max_rows: 4097,
            max_batches: 8,
            max_batch_bytes: 262_144,
            max_retained_bytes: 524_288,
            channel_capacity: 1,
        };
        let mut query = backend
            .query_arrow(schema.clone(), native_limits, 524_288, move |out| {
                let mut statement = input.prepare(sql).map_err(|_| ArrowQueryError::Driver)?;
                if streaming {
                    emit_duckdb_arrow_stream(&mut statement, [], out)
                } else {
                    emit_duckdb_arrow(&mut statement, [], out)
                }
            })
            .unwrap();
        let mut expected = 0_i64;
        while let Some(batch) = query.next_batch().await.unwrap() {
            let ids = batch
                .batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let strings = batch
                .batch()
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            let binary = batch
                .batch()
                .column(2)
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .unwrap();
            for (row, value) in ids.values().iter().enumerate() {
                use arrow_array::Array;
                assert_eq!(*value, expected);
                match expected % 3 {
                    0 => {
                        assert_eq!(strings.value(row), "实体/é");
                        assert_eq!(binary.value(row), b"\0\xffpayload");
                    }
                    1 => {
                        assert_eq!(strings.value(row), "");
                        assert_eq!(binary.value(row), b"");
                    }
                    _ => {
                        assert!(strings.is_null(row));
                        assert!(binary.is_null(row));
                    }
                }
                expected += 1;
            }
        }
        assert_eq!(expected, 4097);
        assert_eq!(query.summary().unwrap().rows, 4097);
        drop(query);
        input = duckdb::Connection::open_in_memory().unwrap();
        let mut failed = backend
            .query_arrow(schema, native_limits, 524_288, move |out| {
                let mut statement = input
                    .prepare("SELECT error('sanitized-native-error') AS id")
                    .map_err(|_| ArrowQueryError::Driver)?;
                if streaming {
                    emit_duckdb_arrow_stream(&mut statement, [], out)
                } else {
                    emit_duckdb_arrow(&mut statement, [], out)
                }
            })
            .unwrap();
        assert_eq!(
            failed.next_batch().await.err(),
            Some(ArrowQueryError::Driver)
        );
        assert!(failed.summary().is_none());
        drop(failed);
        backend.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn cancelled_query_queue_skips_native_work_and_a_cancelled_receive_can_resume() {
    let backend = backend().await;
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let release = Release(gate.clone());
    let worker = backend.clone();
    let task = tokio::spawn(async move {
        worker
            .prepare_resource(1, move || {
                let state = gate.0.lock().unwrap();
                let (state, _) = gate
                    .1
                    .wait_timeout_while(state, Duration::from_secs(3), |state| !*state)
                    .unwrap();
                if !*state {
                    return Err(BackendError::Unavailable);
                }
                Ok(())
            })
            .await
    });
    until(|| backend.status().blocking_resources == 1).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let marker = calls.clone();
    let cancelled = backend
        .query_arrow(batch().schema(), limits(), 1024, move |_| {
            marker.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    drop(cancelled);
    until(|| backend.status().active_resources == 1).await;
    let mut live = backend
        .query_arrow(batch().schema(), limits(), 1024, |out| {
            out.emit(|| Ok(batch()))
        })
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), live.next_batch())
            .await
            .is_err()
    );
    drop(release);
    drop(task.await.unwrap().unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        live.next_batch().await.unwrap().unwrap().batch().num_rows(),
        4
    );
    assert!(live.next_batch().await.unwrap().is_none());
    drop(live);
    backend.shutdown().await.unwrap();
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn native_interrupt_cancels_running_execution_and_keeps_drain_owned() {
    for streaming in [false, true] {
        let backend = backend().await;
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let schema = conn
            .prepare("SELECT count(*) AS id FROM range(1)")
            .unwrap()
            .query_arrow([])
            .unwrap()
            .get_schema();
        let (started, start) = tokio::sync::oneshot::channel();
        let mut query = backend
            .query_arrow(schema, limits(), 1024, move |out| {
                let interrupt = conn.interrupt_handle();
                out.on_cancel(move || interrupt.interrupt())?;
                let mut statement = conn
                    .prepare("SELECT count(*) AS id FROM range(1000000000000) t(i) WHERE i%29!=0")
                    .map_err(|_| ArrowQueryError::Driver)?;
                let _ = started.send(());
                if streaming {
                    mrr_data_backend::providers::emit_duckdb_arrow_stream(&mut statement, [], out)
                } else {
                    mrr_data_backend::providers::emit_duckdb_arrow(&mut statement, [], out)
                }
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), start)
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        query.cancel();
        assert_eq!(
            query.next_batch().await.err(),
            Some(ArrowQueryError::Cancelled)
        );
        assert!(query.summary().is_none());
        until(|| backend.status().blocking_resources == 0).await;
        drop(query);
        backend.shutdown().await.unwrap();
    }
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn native_stream_late_fetch_failure_cannot_certify_delivered_prefix() {
    let backend = backend().await;
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch("SET threads=1").unwrap();
    let schema = conn
        .prepare("SELECT i::BIGINT AS id FROM range(1) t(i)")
        .unwrap()
        .query_arrow([])
        .unwrap()
        .get_schema();
    let mut query = backend.query_arrow(schema, ArrowQueryLimits {
        max_rows: 1_000_000, max_batches: 1024, max_batch_bytes: 65_536,
        max_retained_bytes: 131_072, channel_capacity: 1,
    }, 131_072, move |out| {
        let mut statement = conn.prepare("SELECT CASE WHEN i=999999 THEN error('private-fetch-detail') ELSE i END::BIGINT AS id FROM range(1000000) t(i)")
            .map_err(|_| ArrowQueryError::Driver)?;
        // A mistakenly ignored native refusal must still fail the terminal.
        let _ = mrr_data_backend::providers::emit_duckdb_arrow_stream(&mut statement, [], out);
        Ok(())
    }).unwrap();
    let mut delivered = 0_i64;
    loop {
        match tokio::time::timeout(Duration::from_secs(3), query.next_batch())
            .await
            .unwrap()
        {
            Ok(Some(batch)) => {
                let ids = batch
                    .batch()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for value in ids.values() {
                    assert_eq!(*value, delivered);
                    delivered += 1;
                }
            }
            Err(error) => {
                assert_eq!(error, ArrowQueryError::Driver);
                break;
            }
            Ok(None) => panic!("late native error became successful EOF"),
        }
    }
    assert!(delivered > 0 && delivered < 1_000_000);
    assert!(query.summary().is_none());
    assert_eq!(
        query.next_batch().await.err(),
        Some(ArrowQueryError::Driver)
    );
    drop(query);
    backend.shutdown().await.unwrap();
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn native_stream_empty_exact_limits_and_sticky_schema_or_row_refusal() {
    for (sql, rows, refusal) in [
        ("SELECT i::BIGINT AS id FROM range(0) t(i)", 0, None),
        ("SELECT i::BIGINT AS id FROM range(2048) t(i)", 2048, None),
        (
            "SELECT i::BIGINT AS id FROM range(2049) t(i)",
            0,
            Some(ArrowQueryError::Limit),
        ),
        (
            "SELECT 'wrong-type' AS id",
            0,
            Some(ArrowQueryError::Schema),
        ),
    ] {
        let backend = backend().await;
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("SET threads=1").unwrap();
        let schema = conn
            .prepare("SELECT i::BIGINT AS id FROM range(1) t(i)")
            .unwrap()
            .query_arrow([])
            .unwrap()
            .get_schema();
        let mut query = backend
            .query_arrow(
                schema,
                ArrowQueryLimits {
                    max_rows: 2048,
                    max_batches: 1,
                    max_batch_bytes: 65_536,
                    max_retained_bytes: 131_072,
                    channel_capacity: 1,
                },
                131_072,
                move |out| {
                    let mut statement = conn.prepare(sql).map_err(|_| ArrowQueryError::Driver)?;
                    let _ = mrr_data_backend::providers::emit_duckdb_arrow_stream(
                        &mut statement,
                        [],
                        out,
                    );
                    Ok(())
                },
            )
            .unwrap();
        let mut delivered = 0;
        loop {
            match query.next_batch().await {
                Ok(Some(batch)) => delivered += batch.batch().num_rows(),
                Ok(None) => {
                    assert!(refusal.is_none());
                    break;
                }
                Err(error) => {
                    assert_eq!(Some(error), refusal);
                    break;
                }
            }
        }
        if refusal.is_some() {
            assert!(query.summary().is_none());
            assert!(delivered <= 2048);
        } else {
            assert_eq!(delivered, rows);
            assert_eq!(query.summary().unwrap().rows, rows);
        }
        drop(query);
        backend.shutdown().await.unwrap();
    }
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn native_stream_batch_survives_result_teardown_and_holds_backend_drain() {
    let backend = backend().await;
    let conn = duckdb::Connection::open_in_memory().unwrap();
    let schema = conn
        .prepare("SELECT i::BIGINT AS id FROM range(1) t(i)")
        .unwrap()
        .query_arrow([])
        .unwrap()
        .get_schema();
    let mut query = backend
        .query_arrow(
            schema,
            ArrowQueryLimits {
                max_rows: 2048,
                max_batches: 1,
                max_batch_bytes: 65_536,
                max_retained_bytes: 131_072,
                channel_capacity: 1,
            },
            131_072,
            move |out| {
                let mut statement = conn
                    .prepare("SELECT i::BIGINT AS id FROM range(?) t(i)")
                    .map_err(|_| ArrowQueryError::Driver)?;
                mrr_data_backend::providers::emit_duckdb_arrow_stream(
                    &mut statement,
                    [2048_i64],
                    out,
                )
            },
        )
        .unwrap();
    let batch = query.next_batch().await.unwrap().unwrap();
    let clone = batch.clone();
    assert!(query.next_batch().await.unwrap().is_none());
    assert_eq!(query.summary().unwrap().rows, 2048);
    assert_eq!(backend.status().blocking_resources, 0);
    drop(query);
    drop(batch);
    let closing = backend.clone();
    let shutdown = tokio::spawn(async move { closing.shutdown().await });
    until(|| backend.status().lifecycle == Lifecycle::Draining).await;
    let ids = clone
        .batch()
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 0);
    assert_eq!(ids.value(2047), 2047);
    assert!(!shutdown.is_finished());
    drop(clone);
    tokio::time::timeout(Duration::from_secs(3), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(backend.status().resource_bytes, 0);
}
