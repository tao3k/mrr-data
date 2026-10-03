//! Native durable graph publication/CAS/recovery; transport payloads are opaque.
#![cfg(all(feature = "graph-publish", any(feature = "turso", feature = "duckdb")))]
#[path = "../../../mrr-data-content/tests/support/graph_publication_fixture.rs"]
mod fixture;
use fixture::{Fixture, Remote};
use mrr_data_backend::{
    AuthorityExpectation, AuthorityProposal, AuthorityStatus, Backend, BackendConfig, BackendError,
};
use mrr_data_content::{
    ConditionalCommitPortError as Error, ConditionalContentCommitOutcome as Outcome,
    ConditionalContentCommitPort, ConditionalContentWrite, ContentRevision, publish_graph_dataset,
};
#[cfg(feature = "turso")]
fn native(path: &std::path::Path) -> mrr_data_backend::providers::TursoProvider {
    mrr_data_backend::providers::TursoProvider::new(path.into(), tokio::runtime::Handle::current())
}
#[cfg(all(not(feature = "turso"), feature = "duckdb"))]
fn native(path: &std::path::Path) -> mrr_data_backend::providers::DuckDbProvider {
    mrr_data_backend::providers::DuckDbProvider::new(path.into())
}
async fn open(path: &std::path::Path) -> Backend {
    Backend::open(
        BackendConfig::default(),
        native(path),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap()
}
fn write(
    operation_id: &str,
    expected: Option<ContentRevision>,
    replacement: cid::Cid,
) -> ConditionalContentWrite<'_> {
    ConditionalContentWrite {
        scope: "graph",
        operation_id,
        expected,
        replacement,
    }
}
#[tokio::test]
async fn complete_ack_protected_cas_and_restart_recover_exact_operation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("graph.db");
    let backend = open(&path).await;
    let f = Fixture::new();
    let resource = backend
        .prepare_resource(fixture::limits().max_total_bytes, move || Ok(f.prepare()))
        .await
        .unwrap();
    let base = backend.profile("graph.v1", "tenant").unwrap();
    let enrollment = AuthorityProposal {
        authority_id: "policy".into(),
        expected: None,
        replacement: *resource.get().root(),
        status: AuthorityStatus::Active,
    };
    let state = base
        .advance_authority("graph", enrollment.clone())
        .await
        .unwrap();
    let expectations = [AuthorityExpectation {
        authority_id: "policy".into(),
        state,
    }];
    let port = base.with_authorities(&expectations).unwrap();
    let operation = write("publish-one", None, *resource.get().root());
    assert!(
        port.commit_graph_publication(operation, None, |_| -> Result<(), ()> {
            panic!("missing ACK validator")
        })
        .await
        .is_err()
    );
    assert!(port.recover(operation).await.unwrap().is_none());
    let remote = Remote::default();
    let publication = publish_graph_dataset(
        resource.get(),
        &mrr_data_content::MemoryContentStore::default(),
        &remote,
        &remote,
        || async { Ok(()) },
    )
    .await
    .unwrap();
    assert!(
        port.commit_graph_publication(operation, Some(&publication), |_| Err("policy denied"))
            .await
            .is_err()
    );
    assert!(port.recover(operation).await.unwrap().is_none());
    let Outcome::Committed(receipt) = port
        .commit_graph_publication(operation, Some(&publication), |_| Ok::<_, ()>(()))
        .await
        .unwrap()
    else {
        panic!("fresh CAS");
    };
    let head = receipt.committed;
    base.advance_authority(
        "graph",
        AuthorityProposal {
            expected: Some(state),
            status: AuthorityStatus::Retired,
            ..enrollment
        },
    )
    .await
    .unwrap();
    let next = write("publish-two", Some(head), operation.replacement);
    assert!(matches!(
        port.commit_graph_publication(next, Some(&publication), |_| -> Result<(), ()> {
            panic!("retired validator")
        })
        .await,
        Err(Error::BeforeCommit(BackendError::AuthorityRetired))
    ));
    assert!(port.recover(next).await.unwrap().is_none());
    drop(resource);
    backend.shutdown().await.unwrap();
    let backend = open(&path).await;
    let port = backend
        .profile("graph.v1", "tenant")
        .unwrap()
        .with_authorities(&expectations)
        .unwrap();
    assert_eq!(
        port.recover(operation).await.unwrap().unwrap().committed,
        head
    );
    assert!(matches!(
        port.commit_graph_publication(operation, None, |_| -> Result<(), ()> {
            panic!("historical replay validator")
        })
        .await
        .unwrap(),
        Outcome::Replayed(_)
    ));
    backend.shutdown().await.unwrap();
}
#[tokio::test]
async fn lost_root_ack_leaves_original_operation_absent_then_exact_content_retry_commits() {
    let directory = tempfile::tempdir().unwrap();
    let backend = open(&directory.path().join("graph.db")).await;
    let f = Fixture::new();
    let prepared = f.prepare();
    let port = backend.profile("graph.v1", "tenant").unwrap();
    let operation = write("lost-ack", None, *prepared.root());
    let remote = Remote::default();
    *remote.lost_ack.lock().unwrap() = Some(operation.replacement);
    assert!(
        publish_graph_dataset(&prepared, &f.local, &remote, &remote, || async { Ok(()) })
            .await
            .is_err()
    );
    assert!(port.recover(operation).await.unwrap().is_none());
    *remote.lost_ack.lock().unwrap() = None;
    let publication =
        publish_graph_dataset(&prepared, &f.local, &remote, &remote, || async { Ok(()) })
            .await
            .unwrap();
    let wrong = write("wrong-root", None, *f.snapshot.cid());
    assert!(
        port.commit_graph_publication(wrong, Some(&publication), |_| -> Result<(), ()> {
            panic!("wrong root validator")
        })
        .await
        .is_err()
    );
    assert!(port.recover(wrong).await.unwrap().is_none());
    assert!(matches!(
        port.commit_graph_publication(operation, Some(&publication), |_| Ok::<_, ()>(()))
            .await
            .unwrap(),
        Outcome::Committed(_)
    ));
    backend.shutdown().await.unwrap();
}
