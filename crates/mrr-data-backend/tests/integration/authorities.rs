//! Authority generations, mandatory guards and concurrent retirement consumers.
#[cfg(all(not(feature = "turso"), feature = "duckdb"))]
use super::{
    Arc, AtomicBool, BackendConfig, Condvar, Duration, HeldProvider, Mutex, Ordering, native,
};
use super::{
    Backend, BackendError, ConditionalCommitError, ContentRevision, Outcome, PortError, ack,
    committed, open, root, tamper, write,
};
use mrr_data_backend::{
    AuthorityExpectation as Guard, AuthorityProposal as Update, AuthorityState,
    AuthorityStatus as Status, ProfilePort,
};
use mrr_data_content::ConditionalContentCommitPort;
struct RetiredHome {
    base: ProfilePort,
    original: ProfilePort,
    current: ProfilePort,
    enroll: Update,
    one: AuthorityState,
    retired: AuthorityState,
    head: ContentRevision,
}
async fn rotate_and_retire(backend: &Backend) -> RetiredHome {
    let base = backend.profile("profile.v1", "tenant").unwrap();
    let enroll = Update {
        authority_id: "issuer".into(),
        expected: None,
        replacement: root(b"key-one"),
        status: Status::Active,
    };
    let one = base
        .advance_authority("shared-home", enroll.clone())
        .await
        .unwrap();
    let first = write("first", None, b"first");
    assert!(matches!(
        base.commit(first, Some(&ack(first)), |_| -> Result<(), ()> {
            panic!("unguarded bypass")
        })
        .await,
        Err(PortError::BeforeCommit(BackendError::AuthorityConflict))
    ));
    let original = base
        .with_authorities(&[Guard {
            authority_id: "issuer".into(),
            state: one,
        }])
        .unwrap();
    let head = committed(
        &original
            .commit(first, Some(&ack(first)), |_| Ok::<_, ()>(()))
            .await
            .unwrap(),
    );
    let two = base
        .advance_authority(
            "shared-home",
            Update {
                expected: Some(one),
                replacement: root(b"key-two"),
                ..enroll.clone()
            },
        )
        .await
        .unwrap();
    let second = write("second", Some(head), b"second");
    assert!(matches!(
        original
            .commit(second, Some(&ack(second)), |_| -> Result<(), ()> {
                panic!("stale authority")
            })
            .await,
        Err(PortError::BeforeCommit(BackendError::AuthorityConflict))
    ));
    let current = base
        .with_authorities(&[Guard {
            authority_id: "issuer".into(),
            state: two,
        }])
        .unwrap();
    let head = committed(
        &current
            .commit(second, Some(&ack(second)), |_| Ok::<_, ()>(()))
            .await
            .unwrap(),
    );
    let retired = base
        .advance_authority(
            "shared-home",
            Update {
                expected: Some(two),
                status: Status::Retired,
                ..enroll.clone()
            },
        )
        .await
        .unwrap();
    RetiredHome {
        base,
        original,
        current,
        enroll,
        one,
        retired,
        head,
    }
}
#[tokio::test]
async fn mandatory_authority_rotation_retirement_and_historical_replay_share_the_home() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.db");
    let backend = open(&path).await;
    let RetiredHome {
        base,
        original,
        current,
        enroll,
        one,
        retired,
        head,
    } = rotate_and_retire(&backend).await;
    let first = write("first", None, b"first");
    let terminal = base
        .with_authorities(&[Guard {
            authority_id: "issuer".into(),
            state: retired,
        }])
        .unwrap();
    let third = write("third", Some(head), b"third");
    assert!(matches!(
        terminal
            .commit(third, Some(&ack(third)), |_| -> Result<(), ()> {
                panic!("retired authority")
            })
            .await,
        Err(PortError::BeforeCommit(BackendError::AuthorityRetired))
    ));
    assert!(matches!(
        original
            .commit(first, None, |_| -> Result<(), ()> {
                panic!("historical authority")
            })
            .await
            .unwrap(),
        Outcome::Replayed(_)
    ));
    assert!(original.recover(first).await.unwrap().is_some());
    assert!(matches!(
        current.recover(first).await,
        Err(PortError::Protocol(
            ConditionalCommitError::OperationConflict
        ))
    ));
    assert_eq!(
        base.advance_authority("shared-home", enroll.clone())
            .await
            .unwrap(),
        one
    );
    assert_eq!(
        base.authority("shared-home", "issuer").await.unwrap(),
        Some(retired)
    );
    assert!(matches!(
        base.advance_authority(
            "shared-home",
            Update {
                expected: Some(retired),
                ..enroll.clone()
            }
        )
        .await,
        Err(PortError::BeforeCommit(BackendError::AuthorityRetired))
    ));
    assert!(matches!(
        base.advance_authority(
            "shared-home",
            Update {
                replacement: root(b"forged-first"),
                ..enroll
            }
        )
        .await,
        Err(PortError::BeforeCommit(BackendError::AuthorityConflict))
    ));
    backend.shutdown().await.unwrap();
    let reopened = open(&path).await;
    assert_eq!(
        reopened
            .profile("profile.v1", "tenant")
            .unwrap()
            .authority("shared-home", "issuer")
            .await
            .unwrap(),
        Some(retired)
    );
    assert!(
        reopened
            .profile("other.v1", "tenant")
            .unwrap()
            .authority("shared-home", "issuer")
            .await
            .unwrap()
            .is_none()
    );
    reopened.shutdown().await.unwrap();
}
#[tokio::test]
async fn authority_guard_order_aba_limits_and_membership_are_exact() {
    use mrr_data_backend::{
        AuthorityExpectation as Guard, AuthorityProposal as Update, AuthorityStatus as Status,
    };
    let dir = tempfile::tempdir().unwrap();
    let backend = open(&dir.path().join("guards.db")).await;
    let base = backend.profile("profile.v1", "tenant").unwrap();
    let mut guards = Vec::new();
    for i in 0..16 {
        let id = format!("authority-{i:02}");
        let state = base
            .advance_authority(
                "shared-home",
                Update {
                    authority_id: id.clone(),
                    expected: None,
                    replacement: root(b"same-key"),
                    status: Status::Active,
                },
            )
            .await
            .unwrap();
        guards.push(Guard {
            authority_id: id,
            state,
        });
    }
    assert!(matches!(
        base.advance_authority(
            "shared-home",
            Update {
                authority_id: "seventeenth".into(),
                expected: None,
                replacement: root(b"key"),
                status: Status::Active
            }
        )
        .await,
        Err(PortError::BeforeCommit(BackendError::Limit))
    ));
    assert!(
        base.authority("shared-home", "seventeenth")
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        base.with_authorities(&[guards[0].clone(), guards[0].clone()]),
        Err(BackendError::AuthorityConflict)
    ));
    let all = base.with_authorities(&guards).unwrap();
    let first = write("first", None, b"first");
    let head = committed(
        &all.commit(first, Some(&ack(first)), |_| Ok::<_, ()>(()))
            .await
            .unwrap(),
    );
    guards.reverse();
    assert!(matches!(
        base.with_authorities(&guards)
            .unwrap()
            .commit(first, None, |_| -> Result<(), ()> {
                panic!("guard ordering")
            })
            .await
            .unwrap(),
        Outcome::Replayed(_)
    ));
    let old = guards[0].state;
    let next = base
        .advance_authority(
            "shared-home",
            Update {
                authority_id: guards[0].authority_id.clone(),
                expected: Some(old),
                replacement: old.commitment,
                status: Status::Active,
            },
        )
        .await
        .unwrap();
    assert_eq!(next.commitment, old.commitment);
    assert_eq!(next.generation, old.generation + 1);
    let second = write("second", Some(head), b"second");
    assert!(matches!(
        all.commit(second, Some(&ack(second)), |_| -> Result<(), ()> {
            panic!("generation ABA")
        })
        .await,
        Err(PortError::BeforeCommit(BackendError::AuthorityConflict))
    ));
    guards[0].state = next;
    committed(
        &base
            .with_authorities(&guards)
            .unwrap()
            .commit(second, Some(&ack(second)), |_| Ok::<_, ()>(()))
            .await
            .unwrap(),
    );
    backend.shutdown().await.unwrap();
}

#[cfg(all(feature = "duckdb", not(feature = "turso")))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_snapshot_isolation_cannot_write_skew_past_committed_retirement() {
    use mrr_data_backend::{
        AuthorityExpectation as Guard, AuthorityProposal as Update, AuthorityStatus as Status,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("write-skew.db");
    let admin = open(&path).await;
    let authority = admin.profile("profile.v1", "tenant").unwrap();
    let proposal = Update {
        authority_id: "issuer".into(),
        expected: None,
        replacement: root(b"key"),
        status: Status::Active,
    };
    let state = authority
        .advance_authority("shared-home", proposal.clone())
        .await
        .unwrap();
    let entered = Arc::new(AtomicBool::new(false));
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let worker = Backend::open(
        BackendConfig::default(),
        HeldProvider {
            before_validation: false,
            native: native(path),
            entered: entered.clone(),
            gate: gate.clone(),
        },
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let guarded = worker
        .profile("profile.v1", "tenant")
        .unwrap()
        .with_authorities(&[Guard {
            authority_id: "issuer".into(),
            state,
        }])
        .unwrap();
    let request = tokio::spawn(async move {
        let w = write("race", None, b"race");
        guarded
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
    let retired = authority
        .advance_authority(
            "shared-home",
            Update {
                expected: Some(state),
                status: Status::Retired,
                ..proposal
            },
        )
        .await
        .unwrap();
    {
        let (lock, changed) = &*gate;
        *lock.lock().unwrap() = true;
        changed.notify_all();
    }
    assert!(request.await.unwrap().is_err());
    assert_eq!(
        authority.authority("shared-home", "issuer").await.unwrap(),
        Some(retired)
    );
    assert!(
        authority
            .with_authorities(&[Guard {
                authority_id: "issuer".into(),
                state
            }])
            .unwrap()
            .recover(write("race", None, b"race"))
            .await
            .unwrap()
            .is_none()
    );
    worker.shutdown().await.unwrap();
    admin.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_logical_head_cannot_be_reenrolled_as_fresh_authority() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lost-head.db");
    let backend = open(&path).await;
    let port = backend.profile("profile.v1", "tenant").unwrap();
    let first = write("first", None, b"first");
    committed(
        &port
            .commit(first, Some(&ack(first)), |_| Ok::<_, ()>(()))
            .await
            .unwrap(),
    );
    backend.shutdown().await.unwrap();
    tamper(
        &path,
        "DELETE FROM mrr_backend_kv WHERE key LIKE '%\"head\"%'",
        None,
        false,
    );
    let reopened = open(&path).await;
    let port = reopened.profile("profile.v1", "tenant").unwrap();
    assert!(port.recover(first).await.unwrap().is_some());
    let reset = write("reset", None, b"reset");
    assert!(matches!(
        port.commit(reset, Some(&ack(reset)), |_| -> Result<(), ()> {
            panic!("reset history")
        })
        .await,
        Err(PortError::BeforeCommit(BackendError::Corrupt))
    ));
    reopened.shutdown().await.unwrap();
}
