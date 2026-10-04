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

/// A single simulated Host owns all authority mutation and final disclosure.
/// The gate represents the Host's execution boundary, not a Backend permission
/// oracle. Multi-process Hosts must supply their own equivalent coordination.
struct SimulatedHost {
    port: ProfilePort,
    boundary: tokio::sync::Mutex<Vec<Vec<u8>>>,
}
impl SimulatedHost {
    async fn advance(&self, proposal: Update) -> AuthorityState {
        let _boundary = self.boundary.lock().await;
        self.port
            .advance_authority("shared-home", proposal)
            .await
            .unwrap()
    }

    async fn disclose(
        &self,
        guards: &[Guard],
        payload: &[u8],
        held: Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    ) -> bool {
        let mut emitted = self.boundary.lock().await;
        // Policy and key are both mandatory. Neither historical receipts nor
        // caller-supplied snapshots establish current disclosure permission.
        if guards.len() != 2
            || !["policy", "key"]
                .iter()
                .all(|id| guards.iter().filter(|g| g.authority_id == *id).count() == 1)
        {
            return false;
        }
        for guard in guards {
            if guard.state.status != Status::Active
                || self
                    .port
                    .authority("shared-home", &guard.authority_id)
                    .await
                    .unwrap()
                    != Some(guard.state)
            {
                return false;
            }
        }
        if let Some((entered, release)) = held {
            let _ = entered.send(());
            // Caller cancellation/drop never emits the protected payload.
            if release.await.is_err() {
                return false;
            }
        }
        emitted.push(payload.into());
        true
    }
}

async fn simulate_disclosure_retirement_race(
    host: std::sync::Arc<SimulatedHost>,
    guards: &[Guard],
) -> AuthorityState {
    // A canceled pending disclosure releases the boundary without emitting.
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let canceled = tokio::spawn({
        let host = host.clone();
        let guards = guards.to_vec();
        async move {
            host.disclose(&guards, b"canceled", Some((entered_tx, release_rx)))
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), entered_rx)
        .await
        .unwrap()
        .unwrap();
    canceled.abort();
    assert!(canceled.await.unwrap_err().is_cancelled());
    drop(release_tx);
    assert_eq!(host.boundary.lock().await.len(), 2);

    // An in-flight final disclosure finishes before retirement can enter the
    // same Host boundary. Once retirement completes, no later disclosure emits.
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let disclosure = tokio::spawn({
        let host = host.clone();
        let guards = guards.to_vec();
        async move {
            host.disclose(
                &guards,
                b"before-retirement",
                Some((entered_tx, release_rx)),
            )
            .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), entered_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(host.boundary.try_lock().is_err());
    let retirement = tokio::spawn({
        let host = host.clone();
        let expected = guards[0].state;
        async move {
            host.advance(Update {
                authority_id: "policy".into(),
                expected: Some(expected),
                replacement: expected.commitment,
                status: Status::Retired,
            })
            .await
        }
    });
    release_tx.send(()).unwrap();
    assert!(disclosure.await.unwrap());
    retirement.await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simulated_host_disclosure_rotation_retirement_and_replay_are_separate() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("simulated-host.db");
    let backend = open(&path).await;
    let host = std::sync::Arc::new(SimulatedHost {
        port: backend.profile("profile.v1", "tenant").unwrap(),
        boundary: tokio::sync::Mutex::new(Vec::new()),
    });
    let mut guards = Vec::new();
    for id in ["policy", "key"] {
        guards.push(Guard {
            authority_id: id.into(),
            state: host
                .advance(Update {
                    authority_id: id.into(),
                    expected: None,
                    replacement: root(id.as_bytes()),
                    status: Status::Active,
                })
                .await,
        });
    }
    let original = host.port.with_authorities(&guards).unwrap();
    let w = write("authorized-effect", None, b"protected");
    committed(
        &original
            .commit(w, Some(&ack(w)), |_| Ok::<_, ()>(()))
            .await
            .unwrap(),
    );
    assert!(!host.disclose(&guards[..1], b"omitted-key", None).await);
    assert!(host.disclose(&guards, b"initial", None).await);
    let old = guards.clone();
    guards[1].state = host
        .advance(Update {
            authority_id: "key".into(),
            expected: Some(guards[1].state),
            replacement: root(b"key-rotated"),
            status: Status::Active,
        })
        .await;
    assert!(!host.disclose(&old, b"stale-key", None).await);
    assert!(host.disclose(&guards, b"rotated", None).await);

    let retired = simulate_disclosure_retirement_race(host.clone(), &guards).await;
    assert!(!host.disclose(&guards, b"after-retirement", None).await);
    guards[0].state = retired;
    assert!(!host.disclose(&guards, b"retired-snapshot", None).await);
    // Exact historical receipts survive retirement without re-authorizing data.
    assert!(original.recover(w).await.unwrap().is_some());
    assert!(matches!(
        original
            .commit(w, None, |_| -> Result<(), ()> {
                panic!("historical replay must not validate a fresh effect")
            })
            .await
            .unwrap(),
        Outcome::Replayed(_)
    ));
    assert!(!host.disclose(&old, b"replayed-payload", None).await);
    assert_eq!(
        *host.boundary.lock().await,
        [
            b"initial".to_vec(),
            b"rotated".to_vec(),
            b"before-retirement".to_vec()
        ]
    );
    backend.shutdown().await.unwrap();
    let reopened = open(&path).await;
    let resumed = SimulatedHost {
        port: reopened.profile("profile.v1", "tenant").unwrap(),
        boundary: tokio::sync::Mutex::new(Vec::new()),
    };
    assert!(!resumed.disclose(&old, b"restart-bypass", None).await);
    assert_eq!(
        resumed
            .port
            .authority("shared-home", "policy")
            .await
            .unwrap(),
        Some(retired)
    );
    reopened.shutdown().await.unwrap();
}
