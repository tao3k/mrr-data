//! MVCC scheduling refines the existing home-sequence contract, not domain policy.
use mrr_data_backend::{
    AuthorityChange, AuthorityExpectation, AuthorityKey, AuthorityProposal, AuthorityStatus,
    BackendError, MetadataProvider, StoredWrite,
    providers::{ProviderResult, TransactionProvider, TursoProvider},
};
use mrr_data_content::{CacheAdmission, ContentBlock, ContentCodec, PublishReceipt};
use std::{
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

fn write(scope: &str, op: &str) -> StoredWrite {
    StoredWrite {
        profile: "gateway.v1".into(),
        namespace: "original".into(),
        scope: scope.into(),
        operation_id: op.into(),
        expected: None,
        replacement: ContentBlock::new(
            ContentCodec::Raw,
            b"original-burn-reservation-unknown-exposure",
        )
        .cid(),
        authorities: vec![],
    }
}
fn commit(
    provider: &TursoProvider,
    w: &StoredWrite,
    validate: &mut dyn FnMut(Option<mrr_data_content::ContentRevision>) -> bool,
) -> ProviderResult<mrr_data_backend::StoredOutcome> {
    provider.commit(
        w,
        Some(&PublishReceipt {
            cid: w.replacement,
            cache: CacheAdmission::Stored,
        }),
        validate,
    )
}
fn provider(path: &std::path::Path, handle: &tokio::runtime::Handle) -> Arc<TursoProvider> {
    let p = Arc::new(TursoProvider::new(path.into(), handle.clone()).with_concurrent_writes());
    p.open().unwrap();
    p
}
// Bounded rendezvous: release and join workers even if overlap is not observed.
fn overlap(
    p: &Arc<TursoProvider>,
    writes: [StoredWrite; 2],
) -> Vec<ProviderResult<mrr_data_backend::StoredOutcome>> {
    let (entered, observed) = mpsc::channel();
    let workers: Vec<_> = writes
        .into_iter()
        .map(|w| {
            let p = p.clone();
            let entered = entered.clone();
            let (release, wait) = mpsc::channel();
            let worker = thread::spawn(move || {
                let mut validations = 0;
                let result = commit(&p, &w, &mut |_| {
                    validations += 1;
                    entered.send(()).unwrap();
                    wait.recv_timeout(Duration::from_secs(5)).is_ok()
                });
                assert_eq!(
                    validations, 1,
                    "a conflicting snapshot must not retry validation"
                );
                result
            });
            (release, worker)
        })
        .collect();
    drop(entered);
    let first = observed.recv_timeout(Duration::from_secs(2));
    let second = observed.recv_timeout(Duration::from_secs(2));
    let results = workers
        .into_iter()
        .map(|(release, worker)| {
            let _ = release.send(());
            worker
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert!(
        first.is_ok() && second.is_ok(),
        "independent validation callbacks did not overlap"
    );
    results
}
#[test]
fn independent_homes_overlap_and_recover_exact_results() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    let p = provider(&path, runtime.handle());
    let writes = [write("a", "one"), write("b", "one")];
    let outcomes = overlap(&p, writes.clone());
    assert!(outcomes.iter().all(Result::is_ok), "{outcomes:?}");
    p.close().unwrap();
    let reopened = provider(&path, runtime.handle());
    for (w, result) in writes.iter().zip(outcomes) {
        assert_eq!(
            reopened.recover(w).unwrap(),
            Some(result.unwrap().committed)
        );
        assert!(
            reopened
                .commit(w, None, &mut |_| panic!(
                    "historical replay cannot revalidate"
                ))
                .unwrap()
                .replayed
        );
    }
    reopened.close().unwrap();
}
#[test]
fn same_home_conflict_does_not_retry_or_create_another_outcome() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    let p = provider(&path, runtime.handle());
    let writes = [write("shared-budget", "a"), write("shared-budget", "b")];
    let outcomes = overlap(&p, writes.clone());
    assert_eq!(
        outcomes.iter().filter(|r| r.is_ok()).count(),
        1,
        "{outcomes:?}"
    );
    p.close().unwrap();
    let reopened = provider(&path, runtime.handle());
    for (w, outcome) in writes.iter().zip(outcomes) {
        assert_eq!(
            reopened.recover(w).unwrap(),
            outcome.ok().map(|r| r.committed)
        );
    }
    reopened.close().unwrap();
}
#[test]
fn authority_change_invalidates_a_validated_snapshot_before_commit() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    let p = provider(&path, runtime.handle());
    let mut w = write("shared-budget", "burn");
    let key = AuthorityKey {
        profile: w.profile.clone(),
        namespace: w.namespace.clone(),
        scope: w.scope.clone(),
        authority_id: "policy".into(),
    };
    let mut change = AuthorityChange {
        key,
        proposal: AuthorityProposal {
            authority_id: "policy".into(),
            expected: None,
            replacement: w.replacement,
            status: AuthorityStatus::Active,
        },
    };
    let initial = p.advance_authority(&change).unwrap();
    w.authorities = vec![AuthorityExpectation {
        authority_id: "policy".into(),
        state: initial,
    }];
    let (entered, observed) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let running = p.clone();
    let original = w.clone();
    let worker = thread::spawn(move || {
        commit(&running, &original, &mut |_| {
            entered.send(()).unwrap();
            wait.recv_timeout(Duration::from_secs(5)).is_ok()
        })
    });
    let arrived = observed.recv_timeout(Duration::from_secs(2));
    change.proposal.expected = Some(initial);
    change.proposal.status = AuthorityStatus::Retired;
    let retired = p.advance_authority(&change);
    let _ = release.send(());
    let result = worker.join().unwrap();
    assert!(arrived.is_ok());
    assert!(retired.is_ok(), "{retired:?}");
    assert!(result.is_err(), "old authority snapshot committed");
    assert_eq!(p.recover(&w).unwrap(), None);
    assert!(
        commit(&p, &w, &mut |_| panic!(
            "retired authority must reject before validation"
        ))
        .is_err()
    );
    p.close().unwrap();
}
#[test]
fn snapshot_isolation_without_common_witness_allows_write_skew_counterexample() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    let p = provider(&path, runtime.handle());
    p.transaction(&mut |tx| {
        tx.put("raw-policy", b"active")
            .map_err(mrr_data_content::ConditionalCommitPortError::BeforeCommit)
    })
    .unwrap();
    let (entered, observed) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let running = p.clone();
    let worker = thread::spawn(move || {
        running.transaction(&mut |tx| {
            assert_eq!(
                tx.get("raw-policy").unwrap().as_deref(),
                Some(b"active".as_slice())
            );
            entered.send(()).unwrap();
            wait.recv_timeout(Duration::from_secs(5)).unwrap();
            tx.put("raw-effect", b"stale")
                .map_err(mrr_data_content::ConditionalCommitPortError::BeforeCommit)
        })
    });
    let arrived = observed.recv_timeout(Duration::from_secs(2));
    let changed = p.transaction(&mut |tx| {
        tx.put("raw-policy", b"retired")
            .map_err(mrr_data_content::ConditionalCommitPortError::BeforeCommit)
    });
    let _ = release.send(());
    let result = worker.join().unwrap();
    assert!(arrived.is_ok());
    changed.unwrap();
    result.unwrap();
    assert_eq!(
        p.read("raw-effect").unwrap().as_deref(),
        Some(b"stale".as_slice())
    );
    p.close().unwrap();
}
#[test]
fn journal_mode_cannot_be_silently_changed_on_reopen() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal.db");
    let mvcc = dir.path().join("mvcc.db");
    let serial = TursoProvider::new(wal.clone(), runtime.handle().clone());
    serial.open().unwrap();
    serial.close().unwrap();
    assert_eq!(
        TursoProvider::new(wal, runtime.handle().clone())
            .with_concurrent_writes()
            .open(),
        Err(BackendError::UnsupportedCapabilities)
    );
    provider(&mvcc, runtime.handle()).close().unwrap();
    assert_eq!(
        TursoProvider::new(mvcc, runtime.handle().clone()).open(),
        Err(BackendError::UnsupportedCapabilities)
    );
}
