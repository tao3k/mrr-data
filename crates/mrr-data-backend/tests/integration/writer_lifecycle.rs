//! Native cancellation/retirement/shutdown with original opaque obligations.
use mrr_data_backend::{
    AuthorityExpectation, AuthorityProposal, AuthorityState, AuthorityStatus, Backend,
    BackendConfig, BackendError, Lifecycle, ProfilePort,
    providers::{TransactionProvider, TursoProvider},
};
use mrr_data_content::{
    ConditionalCommitPortError as PortError, ConditionalContentCommitOutcome as Outcome,
    ConditionalContentCommitPort, ContentBlock, ContentCodec, ContentRevision, ContentStore,
    FilesystemContentStore, MemoryContentStore, PublishReceipt, publish_content,
};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
#[path = "../support/commit_pause.rs"]
mod commit_pause;
#[path = "../support/retained_obligation.rs"]
mod retained_obligation;
use commit_pause::{Pause, PausePoint, PausedStorage, Release};
use retained_obligation::{AcknowledgedFiles, RetainedObligation, body, body_with_burn, write};
const SCOPE: &str = "original-budget/history-scope";
fn config() -> BackendConfig {
    BackendConfig {
        max_writes: 4,
        max_write_workers: 4,
        max_shared_workers: 4,
        ..BackendConfig::default()
    }
}
fn native(path: &Path) -> Arc<TursoProvider> {
    Arc::new(
        TursoProvider::new(path.into(), tokio::runtime::Handle::current())
            .with_reusable_concurrent_writes(4)
            .unwrap(),
    )
}
fn port(backend: &Backend, authority: AuthorityState) -> ProfilePort {
    backend
        .profile("gateway.admission.v1", "original-home")
        .unwrap()
        .with_authorities(&[AuthorityExpectation {
            authority_id: "policy".into(),
            state: authority,
        }])
        .unwrap()
}
fn retired(authority: AuthorityState, root: cid::Cid) -> AuthorityProposal {
    AuthorityProposal {
        authority_id: "policy".into(),
        expected: Some(authority),
        replacement: root,
        status: AuthorityStatus::Retired,
    }
}
struct Fixture {
    backend: Backend,
    native: Arc<TursoProvider>,
    pause: Arc<Pause>,
    _release: Release,
    closes: Arc<AtomicUsize>,
    store: AcknowledgedFiles,
    held: Vec<u8>,
    burned: Vec<u8>,
    physical: PublishReceipt,
    initial: ContentRevision,
    authority: AuthorityState,
}
impl Fixture {
    async fn open(dir: &Path, point: PausePoint) -> Self {
        let native = native(&dir.join("state.db"));
        let pause = Arc::new(Pause::default());
        let closes = Arc::new(AtomicUsize::new(0));
        let backend = Backend::open(
            config(),
            PausedStorage {
                native: native.clone(),
                pause: pause.clone(),
                point,
                closes: closes.clone(),
            },
            tokio::runtime::Handle::current(),
        )
        .await
        .unwrap();
        let store = AcknowledgedFiles(FilesystemContentStore::open(dir.join("bodies")).unwrap());
        let held = body_with_burn(false);
        let burned = body();
        let initial_body = publish_content(
            &MemoryContentStore::default(),
            &store,
            ContentBlock::new(ContentCodec::Raw, &held),
        )
        .await
        .unwrap();
        let physical = publish_content(
            &MemoryContentStore::default(),
            &store,
            ContentBlock::new(ContentCodec::Raw, &burned),
        )
        .await
        .unwrap();
        let raw = backend
            .profile("gateway.admission.v1", "original-home")
            .unwrap();
        let authority = raw
            .advance_authority(
                SCOPE,
                AuthorityProposal {
                    authority_id: "policy".into(),
                    expected: None,
                    replacement: physical.cid,
                    status: AuthorityStatus::Active,
                },
            )
            .await
            .unwrap();
        let guarded = port(&backend, authority);
        let outcome = guarded
            .commit(write("reservation", &held), Some(&initial_body), |_| {
                Ok::<_, ()>(())
            })
            .await
            .unwrap();
        let initial = match outcome {
            Outcome::Committed(r) => r.committed,
            Outcome::Replayed(_) => panic!("fresh reservation"),
        };
        Self {
            backend,
            native,
            _release: Release(pause.clone()),
            pause,
            closes,
            store,
            held,
            burned,
            physical,
            initial,
            authority,
        }
    }
    fn consume(&self) -> mrr_data_content::ConditionalContentWrite<'_> {
        let mut w = write("original-consume", &self.burned);
        w.expected = Some(self.initial);
        w
    }
    fn retained(&self, root: cid::Cid, burned: bool) {
        let bytes = self.store.0.get_bounded(&root, 8192).unwrap();
        let (scope, actual_burn, frame): (String, bool, RetainedObligation) =
            serde_json::from_slice(&bytes).unwrap();
        assert_eq!(scope, SCOPE);
        assert_eq!(actual_burn, burned);
        assert_eq!((frame.reserved, frame.held), (4, 4));
        assert_eq!(frame.claim, "original-claim");
        assert_eq!(frame.reservation, "original-reservation");
        assert_eq!(frame.unknown_exposure, "original-unknown-exposure");
        assert_eq!(frame.history, "original-history-prefix");
    }
    async fn recovered(&self, dir: &Path, consumed: bool, status: AuthorityStatus) {
        let replacement = native(&dir.join("state.db"));
        let backend = Backend::open(
            config(),
            PausedStorage {
                native: replacement,
                pause: Arc::new(Pause::default()),
                point: PausePoint::BeforeDecision,
                closes: Arc::new(AtomicUsize::new(0)),
            },
            tokio::runtime::Handle::current(),
        )
        .await
        .unwrap();
        self.native.close_storage().unwrap(); // Delayed old close is confined to the old instance.
        assert_eq!(
            self.native.open_storage(),
            Err(BackendError::InvalidConfiguration)
        );
        let p = port(&backend, self.authority);
        assert_eq!(
            p.authority(SCOPE, "policy").await.unwrap().unwrap().status,
            status
        );
        let w = self.consume();
        let result = p.recover(w).await.unwrap();
        assert_eq!(result.is_some(), consumed);
        if consumed {
            assert!(matches!(
                p.commit(w, None, |_| -> Result<(), ()> {
                    panic!("replay cannot consume again")
                })
                .await
                .unwrap(),
                Outcome::Replayed(_)
            ));
            self.retained(w.replacement, true);
            let delivery = p
                .publication_delivery(SCOPE, self.initial.revision + 1)
                .await
                .unwrap()
                .unwrap();
            p.acknowledge_publication(&delivery).await.unwrap();
        } else if status == AuthorityStatus::Retired {
            assert!(matches!(
                p.commit(w, Some(&self.physical), |_| -> Result<(), ()> {
                    panic!("restart cannot refresh an old authority")
                })
                .await,
                Err(PortError::BeforeCommit(BackendError::AuthorityRetired))
            ));
        }
        assert!(
            p.recover(write("reservation", &self.held))
                .await
                .unwrap()
                .is_some()
        );
        self.retained(self.initial.root, false);
        backend.shutdown().await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_before_validation_drains_without_consuming_or_releasing_original_hold() {
    let dir = tempfile::tempdir().unwrap();
    let f = Arc::new(Fixture::open(dir.path(), PausePoint::BeforeDecision).await);
    f.pause.arm();
    let work = f.clone();
    let caller = tokio::spawn(async move {
        port(&work.backend, work.authority)
            .commit(
                work.consume(),
                Some(&work.physical),
                |_| -> Result<(), ()> { panic!("canceled validator") },
            )
            .await
            .map(|_| ())
    });
    f.pause.entered().await;
    caller.abort();
    let _ = caller.await;
    assert_eq!(f.backend.status().active_writes, 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), f.backend.shutdown())
            .await
            .is_err()
    );
    assert_eq!(f.closes.load(Ordering::SeqCst), 0);
    f.pause.release();
    tokio::time::timeout(Duration::from_secs(3), f.backend.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(f.closes.load(Ordering::SeqCst), 1);
    f.recovered(dir.path(), false, AuthorityStatus::Active)
        .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_after_validation_and_concurrent_shutdown_keep_exact_unknown_consumption() {
    for point in [PausePoint::BeforeCommit, PausePoint::CommittedReplyLost] {
        cancellation_after_validation(point).await;
    }
}
async fn cancellation_after_validation(point: PausePoint) {
    let dir = tempfile::tempdir().unwrap();
    let f = Arc::new(Fixture::open(dir.path(), point).await);
    f.pause.arm();
    let work = f.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let caller = tokio::spawn(async move {
        port(&work.backend, work.authority)
            .commit(work.consume(), Some(&work.physical), |_| {
                count.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(())
            })
            .await
            .map(|_| ())
    });
    f.pause.entered().await;
    caller.abort();
    let _ = caller.await;
    let original = port(&f.backend, f.authority)
        .recover(f.consume())
        .await
        .unwrap()
        .is_some();
    assert_eq!(original, point == PausePoint::CommittedReplyLost);
    let mut closers = Vec::new();
    for _ in 0..4 {
        let backend = f.backend.clone();
        closers.push(tokio::spawn(async move { backend.shutdown().await }));
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.backend.status().lifecycle != Lifecycle::Draining {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.backend.status().active_writes, 1);
    assert_eq!(f.closes.load(Ordering::SeqCst), 0);
    f.pause.release();
    for close in closers {
        tokio::time::timeout(Duration::from_secs(3), close)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    assert_eq!(f.closes.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    f.recovered(dir.path(), true, AuthorityStatus::Active).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn known_commit_lost_reply_reuses_resource_but_never_revalidates_original_consume() {
    let dir = tempfile::tempdir().unwrap();
    let f = Arc::new(Fixture::open(dir.path(), PausePoint::CommittedReplyLost).await);
    f.pause.arm();
    let work = f.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let caller = tokio::spawn(async move {
        port(&work.backend, work.authority)
            .commit(work.consume(), Some(&work.physical), |_| {
                count.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(())
            })
            .await
            .map(|_| ())
    });
    f.pause.entered().await;
    let p = port(&f.backend, f.authority);
    let w = f.consume();
    assert!(matches!(
        p.commit(w, None, |_| -> Result<(), ()> {
            panic!("no repeat validation")
        })
        .await
        .unwrap(),
        Outcome::Replayed(_)
    ));
    p.advance_authority(SCOPE, retired(f.authority, f.physical.cid))
        .await
        .unwrap();
    let mut fresh = w;
    fresh.operation_id = "new-nonce-old-authority";
    fresh.expected = p.recover(w).await.unwrap().map(|r| r.committed);
    assert!(matches!(
        p.commit(fresh, Some(&f.physical), |_| -> Result<(), ()> {
            panic!("retired authority")
        })
        .await,
        Err(PortError::BeforeCommit(BackendError::AuthorityRetired))
    ));
    f.pause.release();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), caller)
            .await
            .unwrap()
            .unwrap(),
        Err(PortError::Unknown(BackendError::Unavailable))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    f.retained(w.replacement, true);
    f.backend.shutdown().await.unwrap();
    f.recovered(dir.path(), true, AuthorityStatus::Retired)
        .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn effective_retirement_before_seal_survives_fresh_owner_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let f = Arc::new(Fixture::open(dir.path(), PausePoint::BeforeDecision).await);
    let validation = Arc::new(Pause::default());
    let _release = Release(validation.clone());
    let work = f.clone();
    let wait = validation.clone();
    let caller = tokio::spawn(async move {
        port(&work.backend, work.authority)
            .commit(work.consume(), Some(&work.physical), |_| {
                wait.block().then_some(()).ok_or(())
            })
            .await
            .map(|_| ())
    });
    validation.entered().await;
    let p = port(&f.backend, f.authority);
    p.advance_authority(SCOPE, retired(f.authority, f.physical.cid))
        .await
        .unwrap();
    validation.release();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), caller)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(p.recover(f.consume()).await.unwrap().is_none());
    f.backend.shutdown().await.unwrap();
    f.recovered(dir.path(), false, AuthorityStatus::Retired)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sealed_intent_is_not_a_commit_or_an_effective_revocation_ack() {
    let dir = tempfile::tempdir().unwrap();
    let f = Arc::new(Fixture::open(dir.path(), PausePoint::BeforeCommit).await);
    f.pause.arm();
    let work = f.clone();
    let caller = tokio::spawn(async move {
        port(&work.backend, work.authority)
            .commit(work.consume(), Some(&work.physical), |_| Ok::<_, ()>(()))
            .await
            .map(|_| ())
    });
    f.pause.entered().await;
    let p = port(&f.backend, f.authority);
    let proposal = retired(f.authority, f.physical.cid);
    let retirement = tokio::time::timeout(
        Duration::from_secs(3),
        p.advance_authority(SCOPE, proposal.clone()),
    )
    .await
    .unwrap();
    f.pause.release();
    let consumed = tokio::time::timeout(Duration::from_secs(3), caller)
        .await
        .unwrap()
        .unwrap();
    assert!(
        retirement.is_err() || consumed.is_err(),
        "conflicting snapshots cannot both commit"
    );
    let observed = p.recover(f.consume()).await.unwrap();
    let committed = observed.is_some();
    assert!(
        consumed.is_err() || committed,
        "known success must recover exactly"
    );
    assert!(
        retirement.is_err() || !committed,
        "effective retirement cannot coexist with the stale consume"
    );
    // An unsuccessful update is not acknowledged as effective. Reconcile the
    // same original proposal/generation rather than inventing another identity.
    let exact = p.advance_authority(SCOPE, proposal).await.unwrap();
    assert_eq!(exact.status, AuthorityStatus::Retired);
    let mut retry = f.consume();
    retry.operation_id = "new-nonce-after-retirement";
    retry.expected = Some(observed.map_or(f.initial, |r| r.committed));
    assert!(matches!(
        p.commit(retry, Some(&f.physical), |_| -> Result<(), ()> {
            panic!("retired snapshot")
        })
        .await,
        Err(PortError::BeforeCommit(BackendError::AuthorityRetired))
    ));
    f.backend.shutdown().await.unwrap();
    f.recovered(dir.path(), committed, AuthorityStatus::Retired)
        .await;
}
