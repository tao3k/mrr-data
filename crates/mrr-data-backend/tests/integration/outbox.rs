//! Original native provider transactions, recovery and historical delivery.
use super::*;
use mrr_data_backend::providers::{MetadataTransaction, TransactionProvider};

#[tokio::test]
async fn replacement_replay_and_reordered_ack_preserve_exact_deliveries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("deliveries.db");
    let backend = open(&path).await;
    let port = backend.profile("publication.v1", "tenant").unwrap();
    let first = write("one", None, b"first");
    let r1 = committed(
        &port
            .commit(first, Some(&ack(first)), |_| Ok::<_, ()>(()))
            .await
            .unwrap(),
    );
    let d1 = port
        .publication_delivery(first.scope, 1)
        .await
        .unwrap()
        .unwrap();
    assert!(!d1.acknowledged);
    let next = write("two", Some(r1), b"next");
    let r2 = committed(
        &port
            .commit(next, Some(&ack(next)), |_| Ok::<_, ()>(()))
            .await
            .unwrap(),
    );
    let d2 = port
        .publication_delivery(first.scope, 2)
        .await
        .unwrap()
        .unwrap();
    port.acknowledge_publication(&d2).await.unwrap();
    port.acknowledge_publication(&d1).await.unwrap();
    port.acknowledge_publication(&d1).await.unwrap();
    assert!(matches!(
        port.commit(first, None, |_| -> Result<(), ()> {
            panic!("historical replay")
        })
        .await
        .unwrap(),
        Outcome::Replayed(_)
    ));
    let mut forged = d1.clone();
    forged.write.replacement = root(b"substituted");
    assert!(port.acknowledge_publication(&forged).await.is_err());
    backend.shutdown().await.unwrap();
    let reopened = open(&path).await;
    let port = reopened.profile("publication.v1", "tenant").unwrap();
    assert!(
        port.publication_delivery(first.scope, 1)
            .await
            .unwrap()
            .unwrap()
            .acknowledged
    );
    assert!(
        port.publication_delivery(first.scope, 2)
            .await
            .unwrap()
            .unwrap()
            .acknowledged
    );
    assert_eq!(port.recover(next).await.unwrap().unwrap().committed, r2);
    let stale = write("three", Some(r1), b"stale");
    assert!(
        port.commit(stale, Some(&ack(stale)), |_| Ok::<_, ()>(()))
            .await
            .is_err()
    );
    assert!(
        port.publication_delivery(first.scope, 3)
            .await
            .unwrap()
            .is_none()
    );
    reopened.shutdown().await.unwrap();
    println!("PUBLICATION-REPLACEMENT-REPLAY-ACK-OK");
}

struct CrashProvider {
    native: NativeProvider,
    point: usize,
}
struct CrashTransaction<'a> {
    tx: &'a mut dyn MetadataTransaction,
    writes: usize,
    point: usize,
}
impl MetadataTransaction for CrashTransaction<'_> {
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        self.tx.get(key)
    }
    fn put(&mut self, key: &str, value: &[u8]) -> Result<(), BackendError> {
        self.tx.put(key, value)?;
        self.writes += 1;
        if self.writes == self.point {
            std::process::exit(91);
        }
        Ok(())
    }
}
impl TransactionProvider for CrashProvider {
    fn open_storage(&self) -> Result<(), BackendError> {
        self.native.open_storage()
    }
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        self.native.read(key)
    }
    fn close_storage(&self) -> Result<(), BackendError> {
        self.native.close_storage()
    }
    fn transaction(
        &self,
        run: &mut dyn FnMut(&mut dyn MetadataTransaction) -> ProviderResult<()>,
    ) -> ProviderResult<()> {
        self.native.transaction(&mut |tx| {
            run(&mut CrashTransaction {
                tx,
                writes: 0,
                point: self.point,
            })
        })?;
        if self.point == 6 {
            std::process::exit(91);
        }
        Ok(())
    }
}
#[tokio::test]
async fn crash_worker() {
    let Ok(path) = std::env::var("MRR_PUBLICATION_CRASH_PATH") else {
        return;
    };
    let point = std::env::var("MRR_PUBLICATION_CRASH_POINT")
        .unwrap()
        .parse()
        .unwrap();
    let backend = Backend::open(
        BackendConfig::default(),
        CrashProvider {
            native: native(path.into()),
            point,
        },
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let port = backend.profile("publication.v1", "tenant").unwrap();
    let w = write("crash", None, b"exact");
    port.commit(w, Some(&ack(w)), |_| Ok::<_, ()>(()))
        .await
        .unwrap();
    panic!("crash point not reached");
}
#[tokio::test]
async fn process_death_at_every_tuple_write_and_after_commit_is_recoverable() {
    let dir = tempfile::tempdir().unwrap();
    for point in 1..=6 {
        let path = dir.path().join(format!("crash-{point}.db"));
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "outbox::crash_worker", "--nocapture"])
            .env("MRR_PUBLICATION_CRASH_PATH", &path)
            .env("MRR_PUBLICATION_CRASH_POINT", point.to_string())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(91));
        let backend = open(&path).await;
        let port = backend.profile("publication.v1", "tenant").unwrap();
        let w = write("crash", None, b"exact");
        if point < 6 {
            assert!(port.recover(w).await.unwrap().is_none());
            assert!(
                port.publication_delivery(w.scope, 1)
                    .await
                    .unwrap()
                    .is_none()
            );
            committed(
                &port
                    .commit(w, Some(&ack(w)), |_| Ok::<_, ()>(()))
                    .await
                    .unwrap(),
            );
        } else {
            assert!(port.recover(w).await.unwrap().is_some());
            assert!(
                port.publication_delivery(w.scope, 1)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert!(matches!(
                port.commit(w, None, |_| -> Result<(), ()> { panic!("recovery effect") })
                    .await
                    .unwrap(),
                Outcome::Replayed(_)
            ));
        }
        backend.shutdown().await.unwrap();
        println!("PUBLICATION-CRASH-RECOVERY-OK {point}");
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_delivery_refuses_historical_repair() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("corrupt.db");
    let backend = open(&path).await;
    let port = backend.profile("publication.v1", "tenant").unwrap();
    let w = write("one", None, b"first");
    committed(
        &port
            .commit(w, Some(&ack(w)), |_| Ok::<_, ()>(()))
            .await
            .unwrap(),
    );
    backend.shutdown().await.unwrap();
    tamper(
        &path,
        "DELETE FROM mrr_backend_kv WHERE CAST(key AS TEXT) LIKE '%publication-delivery%'",
        None,
        false,
    );
    let reopened = open(&path).await;
    let port = reopened.profile("publication.v1", "tenant").unwrap();
    assert!(port.recover(w).await.is_err());
    assert!(port.commit(w, None, |_| Ok::<_, ()>(())).await.is_err());
    let replacement = write(
        "replacement",
        Some(ContentRevision {
            revision: 1,
            root: w.replacement,
        }),
        b"next",
    );
    assert!(
        port.commit(replacement, Some(&ack(replacement)), |_| Ok::<_, ()>(()))
            .await
            .is_err()
    );
    assert!(
        port.publication_delivery(w.scope, 2)
            .await
            .unwrap()
            .is_none()
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn authority_and_validator_refusals_cannot_emit_delivery() {
    use mrr_data_backend::{AuthorityExpectation, AuthorityProposal, AuthorityStatus};
    let dir = tempfile::tempdir().unwrap();
    let backend = open(&dir.path().join("guards.db")).await;
    let base = backend.profile("publication.v1", "tenant").unwrap();
    let update = AuthorityProposal {
        authority_id: "grant".into(),
        expected: None,
        replacement: root(b"grant"),
        status: AuthorityStatus::Active,
    };
    let enrolled = base
        .advance_authority("shared-home", update.clone())
        .await
        .unwrap();
    let port = base
        .with_authorities(&[AuthorityExpectation {
            authority_id: "grant".into(),
            state: enrolled,
        }])
        .unwrap();
    let w = write("refused", None, b"payload");
    assert!(matches!(
        port.commit(w, Some(&ack(w)), |_| Err("native validator refused"))
            .await,
        Err(PortError::Validation(_))
    ));
    assert!(port.recover(w).await.unwrap().is_none());
    assert!(
        port.publication_delivery(w.scope, 1)
            .await
            .unwrap()
            .is_none()
    );
    base.advance_authority(
        w.scope,
        AuthorityProposal {
            expected: Some(enrolled),
            status: AuthorityStatus::Retired,
            ..update
        },
    )
    .await
    .unwrap();
    assert!(
        port.commit(w, Some(&ack(w)), |_| -> Result<(), ()> {
            panic!("retired grant")
        })
        .await
        .is_err()
    );
    assert!(port.recover(w).await.unwrap().is_none());
    assert!(
        port.publication_delivery(w.scope, 1)
            .await
            .unwrap()
            .is_none()
    );
    backend.shutdown().await.unwrap();
    println!("PUBLICATION-REFUSAL-NO-DELIVERY-OK");
}
