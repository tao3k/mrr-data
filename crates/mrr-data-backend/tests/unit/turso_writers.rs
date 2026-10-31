//! Native exclusive ownership and discard tests for the private writer owner.
use super::provider::transact;
use crate::BackendError;
use crate::providers::{TransactionProvider, TursoProvider};
use mrr_data_content::ConditionalCommitPortError as PortError;
use tokio::runtime::Handle;

fn open(path: &std::path::Path, runtime: &Handle) -> TursoProvider {
    let p = TursoProvider::new(path.into(), runtime.clone())
        .with_reusable_concurrent_writes(1)
        .unwrap();
    p.open_storage().unwrap();
    p
}
#[test]
fn closed_writer_owner_cannot_be_resurrected() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let p = open(&dir.path().join("state.db"), runtime.handle());
    p.close_storage().unwrap();
    assert_eq!(p.open_storage(), Err(BackendError::InvalidConfiguration));
}
#[test]
fn driver_unknown_during_close_discards_the_lease_and_retains_prior_obligation() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    let old = open(&path, runtime.handle());
    let original = b"original-claim;original-reservation;original-history;unknown-held=4";
    old.transaction(&mut |tx| {
        tx.put("obligation", original)
            .map_err(PortError::BeforeCommit)
    })
    .unwrap();
    let lease = old.writers.checkout(runtime.handle()).unwrap();
    let conn = lease.connection().unwrap();
    assert_eq!(old.close_storage(), Err(BackendError::NotReady));
    let result = transact(conn, runtime.handle(), "BEGIN CONCURRENT", &mut |tx| {
        // Stage an invalid projection, then force a COMMIT error by aborting
        // before COMMIT. This controls the driver-Unknown branch, not reply loss.
        tx.put("obligation", b"invalid-held=0").unwrap();
        runtime.block_on(conn.execute("ROLLBACK", ())).unwrap();
        Ok(())
    });
    assert!(matches!(result, Err(PortError::Unknown(_))));
    drop(lease);
    assert_eq!(old.writers.counts(), (0, 0));
    old.close_storage().unwrap();
    let recovered = open(&path, runtime.handle());
    assert_eq!(
        recovered.read("obligation").unwrap().as_deref(),
        Some(original.as_slice())
    );
    recovered.close_storage().unwrap();
}
#[test]
fn late_successful_return_cannot_join_a_closed_or_replacement_owner() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    let old = open(&path, runtime.handle());
    let mut lease = old.writers.checkout(runtime.handle()).unwrap();
    let conn = lease.connection().unwrap();
    runtime
        .block_on(conn.execute("CREATE TEMP TABLE reuse_probe (id INTEGER)", ()))
        .unwrap();
    assert_eq!(old.close_storage(), Err(BackendError::NotReady));
    // Close is not revocation: this accepted lease can finish, but cannot return idle.
    transact(conn, runtime.handle(), "BEGIN CONCURRENT", &mut |tx| {
        tx.put(
            "original-obligation",
            b"burned; original-reservation; unknown-held=4",
        )
        .map_err(PortError::BeforeCommit)
    })
    .unwrap();
    lease.committed();
    drop(lease);
    assert_eq!(old.writers.counts(), (0, 0));
    old.close_storage().unwrap();
    assert_eq!(old.open_storage(), Err(BackendError::InvalidConfiguration));
    let replacement = open(&path, runtime.handle());
    old.close_storage().unwrap(); // Late old-owner close cannot address replacement resources.
    let writer = replacement.writers.checkout(runtime.handle()).unwrap();
    assert_eq!(probe(writer.connection().unwrap(), runtime.handle()), 0);
    drop(writer);
    assert_eq!(
        replacement.read("original-obligation").unwrap().as_deref(),
        Some(b"burned; original-reservation; unknown-held=4".as_slice())
    );
    replacement.close_storage().unwrap();
}
#[test]
fn concurrent_closes_refuse_live_lease_then_close_only_the_original_owner() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    let old = open(&path, runtime.handle());
    let lease = old.writers.checkout(runtime.handle()).unwrap();
    std::thread::scope(|scope| {
        let jobs: Vec<_> = (0..8)
            .map(|_| scope.spawn(|| old.close_storage()))
            .collect();
        for job in jobs {
            assert_eq!(job.join().unwrap(), Err(BackendError::NotReady));
        }
    });
    assert!(matches!(
        old.writers.checkout(runtime.handle()),
        Err(BackendError::NotReady)
    ));
    drop(lease);
    std::thread::scope(|scope| {
        let jobs: Vec<_> = (0..8)
            .map(|_| scope.spawn(|| old.close_storage()))
            .collect();
        for job in jobs {
            job.join().unwrap().unwrap();
        }
    });
    let replacement = open(&path, runtime.handle());
    old.close_storage().unwrap();
    assert_eq!(old.open_storage(), Err(BackendError::InvalidConfiguration));
    replacement
        .transaction(&mut |tx| {
            tx.put("new-owner", b"same original database")
                .map_err(PortError::BeforeCommit)
        })
        .unwrap();
    replacement.close_storage().unwrap();
}
fn probe(conn: &turso::Connection, runtime: &Handle) -> i64 {
    runtime.block_on(async {
        let mut rows = conn
            .query(
                "SELECT count(*) FROM sqlite_temp_schema WHERE name='reuse_probe'",
                (),
            )
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get(0).unwrap()
    })
}
#[test]
fn full_configuration_and_successful_session_are_reused_exclusively() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let p = open(&dir.path().join("state.db"), runtime.handle());
    let mut first = p.writers.checkout(runtime.handle()).unwrap();
    let conn = first.connection().unwrap();
    runtime
        .block_on(conn.execute("CREATE TEMP TABLE reuse_probe (id INTEGER)", ()))
        .unwrap();
    transact(conn, runtime.handle(), "BEGIN CONCURRENT", &mut |_| Ok(())).unwrap();
    first.committed();
    drop(first);
    let second = p.writers.checkout(runtime.handle()).unwrap();
    let conn = second.connection().unwrap();
    assert_eq!(probe(conn, runtime.handle()), 1);
    runtime.block_on(async {
        let mut rows = conn.query("PRAGMA synchronous", ()).await.unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            2
        );
    });
    assert!(matches!(
        p.writers.checkout(runtime.handle()),
        Err(BackendError::Saturated)
    ));
    drop(second); // Not marked committed: discard rather than recycling.
    let third = p.writers.checkout(runtime.handle()).unwrap();
    assert_eq!(probe(third.connection().unwrap(), runtime.handle()), 0);
    drop(third);
    p.close_storage().unwrap();
}
#[test]
fn close_refuses_live_lease_and_reopen_cannot_inherit_it() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let p = open(&dir.path().join("state.db"), runtime.handle());
    let mut lease = p.writers.checkout(runtime.handle()).unwrap();
    assert_eq!(p.close_storage(), Err(BackendError::NotReady));
    assert!(matches!(
        p.writers.checkout(runtime.handle()),
        Err(BackendError::NotReady)
    ));
    assert_eq!(p.open_storage(), Err(BackendError::InvalidConfiguration));
    lease.committed();
    drop(lease);
    p.close_storage().unwrap();
    assert_eq!(p.open_storage(), Err(BackendError::InvalidConfiguration));
    let recovered = open(&dir.path().join("state.db"), runtime.handle());
    let lease = recovered.writers.checkout(runtime.handle()).unwrap();
    assert!(lease.connection().unwrap().is_autocommit().unwrap());
    drop(lease);
    recovered.close_storage().unwrap();
}
#[test]
fn unknown_commit_and_unwinding_discard_sessions_and_return_capacity() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let p = open(&dir.path().join("state.db"), runtime.handle());
    let mut writer = p.writers.checkout(runtime.handle()).unwrap();
    let conn = writer.connection().unwrap();
    runtime
        .block_on(conn.execute("CREATE TEMP TABLE reuse_probe (id INTEGER)", ()))
        .unwrap();
    let result = transact(conn, runtime.handle(), "BEGIN CONCURRENT", &mut |_| {
        runtime.block_on(conn.execute("ROLLBACK", ())).unwrap();
        Ok(())
    });
    assert!(matches!(result, Err(PortError::Unknown(_))));
    // The provider only calls committed after Ok. Simulate that same decision.
    if result.is_ok() {
        writer.committed();
    }
    drop(writer);
    let next = p.writers.checkout(runtime.handle()).unwrap();
    assert_eq!(probe(next.connection().unwrap(), runtime.handle()), 0);
    drop(next);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        p.transaction(&mut |tx| {
            tx.put("uncommitted", b"unknown exposure remains held")
                .unwrap();
            panic!("worker failure")
        })
    }));
    assert!(panic.is_err());
    assert_eq!(p.writers.counts().0, 0);
    assert_eq!(p.read("uncommitted").unwrap(), None);
    p.close_storage().unwrap();
}
#[test]
fn validation_refusal_discards_partial_write_without_retry() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let p = open(&dir.path().join("state.db"), runtime.handle());
    let mut calls = 0;
    let result = p.transaction(&mut |tx| {
        calls += 1;
        tx.put("uncommitted", b"original reservation").unwrap();
        Err(PortError::Validation(()))
    });
    assert!(result.is_err());
    assert_eq!(calls, 1);
    assert_eq!(p.read("uncommitted").unwrap(), None);
    assert_eq!(p.writers.counts().1, 0);
    p.close_storage().unwrap();
    assert!(
        TursoProvider::new(dir.path().join("invalid.db"), runtime.handle().clone())
            .with_reusable_concurrent_writes(0)
            .is_err()
    );
    let opened = open(&dir.path().join("opened.db"), runtime.handle());
    assert!(opened.with_reusable_concurrent_writes(1).is_err());
}
