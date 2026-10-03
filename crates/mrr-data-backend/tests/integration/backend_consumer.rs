//! Independent consumers exercise real `SQLite` files, profile isolation and lifecycle.
use mrr_data_backend::{
    Backend, BackendConfig, BackendError, Lifecycle, MetadataProvider, ProviderCapabilities,
    StoredOutcome, StoredRevision, StoredWrite,
    providers::{ProviderResult, SqliteProvider},
};
use mrr_data_content::{
    CacheAdmission, ConditionalCommitError, ConditionalCommitPortError as PortError,
    ConditionalContentCommitOutcome as Outcome, ConditionalContentCommitPort,
    ConditionalContentWrite, ContentBlock, ContentCodec, ContentRevision, PublishReceipt,
};
use std::{
    path::Path,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
fn root(bytes: &[u8]) -> cid::Cid {
    ContentBlock::new(ContentCodec::Raw, bytes).cid()
}
fn write<'a>(
    operation_id: &'a str,
    expected: Option<ContentRevision>,
    bytes: &[u8],
) -> ConditionalContentWrite<'a> {
    ConditionalContentWrite {
        scope: "shared-home",
        operation_id,
        expected,
        replacement: root(bytes),
    }
}
fn ack(w: ConditionalContentWrite<'_>) -> PublishReceipt {
    PublishReceipt {
        cid: w.replacement,
        cache: CacheAdmission::Stored,
    }
}
fn committed(result: &Outcome<'_>) -> ContentRevision {
    match result {
        Outcome::Committed(r) => r.committed,
        Outcome::Replayed(_) => panic!("expected fresh commit"),
    }
}
async fn open(path: &Path) -> Backend {
    Backend::open(
        BackendConfig::default(),
        SqliteProvider::new(path.into()),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap()
}
#[tokio::test]
async fn independent_profiles_share_one_engine_and_restart_preserves_exact_history() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    let backend = open(&path).await;
    let commerce = backend.profile("commerce.v1", "tenant").unwrap();
    let privacy = backend.profile("privacy.v1", "tenant").unwrap();
    let first = write("one", None, b"first");
    let revision = committed(
        &commerce
            .commit(first, Some(&ack(first)), |_| Ok::<_, ()>(()))
            .await
            .unwrap(),
    );
    committed(
        &privacy
            .commit(first, Some(&ack(first)), |_| Ok::<_, ()>(()))
            .await
            .unwrap(),
    );
    let next = write("two", Some(revision), b"next");
    committed(
        &commerce
            .commit(next, Some(&ack(next)), |_| Ok::<_, ()>(()))
            .await
            .unwrap(),
    );
    assert!(matches!(
        commerce
            .commit(first, None, |_| -> Result<(), ()> {
                panic!("replay validation")
            })
            .await
            .unwrap(),
        Outcome::Replayed(_)
    ));
    backend.shutdown().await.unwrap();
    backend.shutdown().await.unwrap();
    assert_eq!(backend.status().lifecycle, Lifecycle::Closed);
    assert!(matches!(
        commerce.recover(first).await,
        Err(PortError::BeforeCommit(BackendError::NotReady))
    ));
    let reopened = open(&path).await;
    let port = reopened.profile("commerce.v1", "tenant").unwrap();
    assert_eq!(
        port.recover(first).await.unwrap().unwrap().committed,
        revision
    );
    assert!(matches!(
        port.commit(first, None, |_| Ok::<_, ()>(())).await.unwrap(),
        Outcome::Replayed(_)
    ));
    let conflicting = write("one", None, b"changed");
    assert!(matches!(
        port.recover(conflicting).await,
        Err(PortError::Protocol(
            ConditionalCommitError::OperationConflict
        ))
    ));
    assert!(
        reopened
            .profile("commerce.v1", "other-tenant")
            .unwrap()
            .recover(first)
            .await
            .unwrap()
            .is_none()
    );
    reopened.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_backend_instances_race_one_head_and_validation_refusal_leaves_no_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("race.db");
    let a = open(&path).await;
    let b = open(&path).await;
    let pa = a.profile("profile.v1", "tenant").unwrap();
    let pb = b.profile("profile.v1", "tenant").unwrap();
    let rejected = write("refused", None, b"bad");
    assert!(matches!(
        pa.commit(rejected, Some(&ack(rejected)), |_| Err("revoked"))
            .await,
        Err(PortError::Validation("revoked"))
    ));
    assert!(pa.recover(rejected).await.unwrap().is_none());
    let one = write("race-a", None, b"a");
    let two = write("race-b", None, b"b");
    let aa = ack(one);
    let ab = ack(two);
    let (r1, r2) = tokio::join!(
        pa.commit(one, Some(&aa), |_| Ok::<_, ()>(())),
        pb.commit(two, Some(&ab), |_| Ok::<_, ()>(()))
    );
    assert_eq!(usize::from(r1.is_ok()) + usize::from(r2.is_ok()), 1);
    let failure = if r1.is_err() { r1 } else { r2 };
    assert!(matches!(
        failure,
        Err(PortError::Protocol(
            ConditionalCommitError::RevisionConflict
        ))
    ));
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
struct HeldProvider {
    before_validation: bool,
    sqlite: SqliteProvider,
    entered: Arc<AtomicBool>,
    gate: Arc<(Mutex<bool>, Condvar)>,
}
impl MetadataProvider for HeldProvider {
    fn capabilities(&self) -> ProviderCapabilities {
        self.sqlite.capabilities()
    }
    fn open(&self) -> Result<(), BackendError> {
        self.sqlite.open()
    }
    fn commit(
        &self,
        w: &StoredWrite,
        p: Option<&PublishReceipt>,
        v: &mut dyn FnMut(Option<ContentRevision>) -> bool,
    ) -> ProviderResult<StoredOutcome> {
        self.sqlite.commit(w, p, &mut |head| {
            if self.before_validation {
                self.entered.store(true, Ordering::Release);
                let (lock, changed) = &*self.gate;
                let mut release = lock.lock().unwrap();
                while !*release {
                    release = changed.wait(release).unwrap();
                }
            }
            let accepted = v(head);
            if accepted {
                self.entered.store(true, Ordering::Release);
                let (lock, changed) = &*self.gate;
                let mut release = lock.lock().unwrap();
                while !*release {
                    release = changed.wait(release).unwrap();
                }
            }
            accepted
        })
    }
    fn recover(&self, w: &StoredWrite) -> ProviderResult<Option<StoredRevision>> {
        self.sqlite.recover(w)
    }
    fn close(&self) -> Result<(), BackendError> {
        self.sqlite.close()
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_keeps_owned_commit_drain_and_reserved_recovery_alive() {
    for config in [
        BackendConfig {
            max_writes: 1,
            ..BackendConfig::default()
        },
        BackendConfig {
            max_retained_bytes: 4096,
            ..BackendConfig::default()
        },
    ] {
        let dir = tempfile::tempdir().unwrap();
        let entered = Arc::new(AtomicBool::new(false));
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let backend = Backend::open(
            config,
            HeldProvider {
                before_validation: false,
                sqlite: SqliteProvider::new(dir.path().join("held.db")),
                entered: entered.clone(),
                gate: gate.clone(),
            },
            tokio::runtime::Handle::current(),
        )
        .await
        .unwrap();
        let port = backend.profile("profile.v1", "tenant").unwrap();
        let task_port = port.clone();
        let task = tokio::spawn(async move {
            let w = write("lost-ack", None, b"held");
            task_port
                .commit(w, Some(&ack(w)), |_| Ok::<_, ()>(()))
                .await
                .map(|r| committed(&r))
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            while !entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        let _ = task.await;
        assert_eq!(backend.status().active_writes, 1);
        let blocked = write("new", None, b"new");
        assert!(matches!(
            port.commit(blocked, Some(&ack(blocked)), |_| Ok::<_, ()>(()))
                .await,
            Err(PortError::BeforeCommit(BackendError::Saturated))
        ));
        // Reserved recovery lane uses a separate connection while the writer is held.
        assert!(
            port.recover(write("lost-ack", None, b"held"))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), backend.shutdown())
                .await
                .is_err()
        );
        assert_eq!(backend.status().lifecycle, Lifecycle::Draining);
        assert!(matches!(
            port.commit(blocked, Some(&ack(blocked)), |_| Ok::<_, ()>(()))
                .await,
            Err(PortError::BeforeCommit(BackendError::NotReady))
        ));
        {
            let (lock, changed) = &*gate;
            *lock.lock().unwrap() = true;
            changed.notify_all();
        }
        tokio::time::timeout(Duration::from_secs(3), backend.shutdown())
            .await
            .unwrap()
            .unwrap();
        let reopened = open(&dir.path().join("held.db")).await;
        assert!(
            reopened
                .profile("profile.v1", "tenant")
                .unwrap()
                .recover(write("lost-ack", None, b"held"))
                .await
                .unwrap()
                .is_some()
        );
        reopened.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn bounds_bad_configuration_and_corruption_refuse_before_ready_or_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bounds.db");
    assert!(matches!(
        Backend::open(
            BackendConfig {
                max_writes: 0,
                ..BackendConfig::default()
            },
            SqliteProvider::new(path.clone()),
            tokio::runtime::Handle::current()
        )
        .await,
        Err(BackendError::InvalidConfiguration)
    ));
    assert!(!path.exists());
    let backend = open(&path).await;
    let port = backend.profile("profile.v1", "tenant").unwrap();
    assert!(matches!(
        backend.profile("", "tenant"),
        Err(BackendError::Limit)
    ));
    let large = "x".repeat(257);
    let w = write(&large, None, b"a");
    assert!(matches!(
        port.commit(w, Some(&ack(w)), |_| Ok::<_, ()>(())).await,
        Err(PortError::BeforeCommit(BackendError::Limit))
    ));
    assert_eq!(backend.status().completed, 0);
    backend.shutdown().await.unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("PRAGMA user_version=99").unwrap();
    drop(db);
    assert!(matches!(
        Backend::open(
            BackendConfig::default(),
            SqliteProvider::new(path),
            tokio::runtime::Handle::current()
        )
        .await,
        Err(BackendError::Corrupt)
    ));
}
// Invoked in an independent process, never by the normal parent test environment.
#[test]
fn durable_child_write() {
    let Ok(path) = std::env::var("MRR_BACKEND_CHILD_DB") else {
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let backend = open(Path::new(&path)).await;
        let port = backend.profile("profile.v1", "tenant").unwrap();
        let first = write("first", None, b"first");
        committed(
            &port
                .commit(first, Some(&ack(first)), |_| Ok::<_, ()>(()))
                .await
                .unwrap(),
        );
        if std::env::var("MRR_BACKEND_CHILD_PARTIAL").is_ok() {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "BEGIN IMMEDIATE; DELETE FROM mrr_heads; DELETE FROM mrr_operations;",
            )
            .unwrap();
            std::process::exit(0); // Exit without COMMIT, rollback or Rust destructors.
        }
        std::process::exit(0); // Durable commit without backend shutdown/Drop.
    });
}
#[tokio::test]
async fn abrupt_process_exit_and_uncommitted_transaction_preserve_atomic_receipts() {
    for partial in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("crash.db");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap());
        child
            .args(["--exact", "durable_child_write", "--nocapture"])
            .env("MRR_BACKEND_CHILD_DB", &path);
        if partial {
            child.env("MRR_BACKEND_CHILD_PARTIAL", "1");
        }
        assert!(child.status().unwrap().success());
        let backend = open(&path).await;
        let port = backend.profile("profile.v1", "tenant").unwrap();
        let first = write("first", None, b"first");
        assert!(port.recover(first).await.unwrap().is_some());
        assert!(matches!(
            port.commit(first, None, |_| -> Result<(), ()> {
                panic!("historical replay")
            })
            .await
            .unwrap(),
            Outcome::Replayed(_)
        ));
        // Recovered head still rejects a competing absent-head proposal.
        let fresh = write("wrong", None, b"wrong");
        assert!(matches!(
            port.commit(fresh, Some(&ack(fresh)), |_| Ok::<_, ()>(()))
                .await,
            Err(PortError::Protocol(
                ConditionalCommitError::RevisionConflict
            ))
        ));
        backend.shutdown().await.unwrap();
    }
}

struct LostAck(SqliteProvider);
impl MetadataProvider for LostAck {
    fn capabilities(&self) -> ProviderCapabilities {
        self.0.capabilities()
    }
    fn open(&self) -> Result<(), BackendError> {
        self.0.open()
    }
    fn commit(
        &self,
        w: &StoredWrite,
        p: Option<&PublishReceipt>,
        v: &mut dyn FnMut(Option<ContentRevision>) -> bool,
    ) -> ProviderResult<StoredOutcome> {
        let result = self.0.commit(w, p, v)?;
        if result.replayed {
            Ok(result)
        } else {
            Err(PortError::Unknown(BackendError::Unavailable))
        }
    }
    fn recover(&self, w: &StoredWrite) -> ProviderResult<Option<StoredRevision>> {
        self.0.recover(w)
    }
    fn close(&self) -> Result<(), BackendError> {
        self.0.close()
    }
}
#[tokio::test]
async fn lost_ack_remains_unknown_then_recovers_original_receipt_without_validation() {
    let dir = tempfile::tempdir().unwrap();
    let backend = Backend::open(
        BackendConfig::default(),
        LostAck(SqliteProvider::new(dir.path().join("ack.db"))),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let port = backend.profile("profile.v1", "tenant").unwrap();
    let w = write("lost-ack", None, b"durable");
    assert!(matches!(
        port.commit(w, Some(&ack(w)), |_| Ok::<_, ()>(())).await,
        Err(PortError::Unknown(BackendError::Unavailable))
    ));
    assert_eq!(
        port.recover(w).await.unwrap().unwrap().committed.root,
        w.replacement
    );
    assert!(matches!(
        port.commit(w, None, |_| -> Result<(), ()> {
            panic!("replay validator")
        })
        .await
        .unwrap(),
        Outcome::Replayed(_)
    ));
    backend.shutdown().await.unwrap();
}
#[tokio::test]
async fn deleted_metadata_tables_are_not_recreated_as_fresh_authority() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing.db");
    let backend = open(&path).await;
    backend.shutdown().await.unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("DROP TABLE mrr_heads").unwrap();
    drop(db);
    assert!(matches!(
        Backend::open(
            BackendConfig::default(),
            SqliteProvider::new(path),
            tokio::runtime::Handle::current()
        )
        .await,
        Err(BackendError::Corrupt)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_before_validator_creates_no_head_or_operation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cancel.db");
    let entered = Arc::new(AtomicBool::new(false));
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let backend = Backend::open(
        BackendConfig::default(),
        HeldProvider {
            before_validation: true,
            sqlite: SqliteProvider::new(path.clone()),
            entered: entered.clone(),
            gate: gate.clone(),
        },
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let port = backend.profile("profile.v1", "tenant").unwrap();
    let caller = tokio::spawn(async move {
        let w = write("cancel", None, b"none");
        port.commit(w, Some(&ack(w)), |_| -> Result<(), ()> {
            panic!("canceled callback")
        })
        .await
        .map(|r| committed(&r))
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !entered.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    caller.abort();
    let _ = caller.await;
    {
        let (lock, changed) = &*gate;
        *lock.lock().unwrap() = true;
        changed.notify_all();
    }
    tokio::time::timeout(Duration::from_secs(3), backend.shutdown())
        .await
        .unwrap()
        .unwrap();
    let reopened = open(&path).await;
    let port = reopened.profile("profile.v1", "tenant").unwrap();
    let canceled = write("cancel", None, b"none");
    assert!(port.recover(canceled).await.unwrap().is_none());
    let fresh = write("fresh", None, b"fresh");
    committed(
        &port
            .commit(fresh, Some(&ack(fresh)), |_| Ok::<_, ()>(()))
            .await
            .unwrap(),
    );
    reopened.shutdown().await.unwrap();
}
