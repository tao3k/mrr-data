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

#[cfg(feature = "turso")]
type Native = mrr_data_backend::providers::TursoProvider;
#[cfg(all(not(feature = "turso"), feature = "duckdb"))]
type Native = mrr_data_backend::providers::DuckDbProvider;
const CRASH_EXIT: i32 = 37;

/// File-backed transport simulation survives child process exit. This does not
/// attest deployed object durability or machine power-loss persistence.
struct ProcessRemote {
    directory: std::path::PathBuf,
    root: cid::Cid,
    inventory: cid::Cid,
    snapshot: cid::Cid,
    phase: String,
}
impl mrr_data_content::RemoteContentStore for ProcessRemote {
    fn get<'a>(
        &'a self,
        cid: &'a cid::Cid,
        max_bytes: usize,
    ) -> mrr_data_content::RemoteFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            match std::fs::read(self.directory.join(cid.to_string())) {
                Ok(bytes) if bytes.len() <= max_bytes => Ok(Some(bytes)),
                Ok(_) => Err(mrr_data_content::RemoteError::TooLarge),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(_) => Err(mrr_data_content::RemoteError::Unavailable),
            }
        })
    }
    fn put<'a>(
        &'a self,
        block: mrr_data_content::ContentBlock<'a>,
    ) -> mrr_data_content::RemoteFuture<'a, ()> {
        Box::pin(async move {
            std::fs::write(self.directory.join(block.cid().to_string()), block.bytes())
                .map_err(|_| mrr_data_content::RemoteError::Unavailable)?;
            if (self.phase == "child-ack" && block.cid() != self.root)
                || (self.phase == "inventory-ack" && block.cid() == self.inventory)
                || (self.phase == "snapshot-ack" && block.cid() == self.snapshot)
                || (self.phase == "root-ack" && block.cid() == self.root)
            {
                std::process::exit(CRASH_EXIT);
            }
            Ok(())
        })
    }
}

struct ProcessProvider {
    native: Native,
    phase: String,
}
impl mrr_data_backend::MetadataProvider for ProcessProvider {
    fn authority(
        &self,
        key: &mrr_data_backend::AuthorityKey,
    ) -> mrr_data_backend::providers::ProviderResult<Option<mrr_data_backend::AuthorityState>> {
        self.native.authority(key)
    }
    fn advance_authority(
        &self,
        change: &mrr_data_backend::AuthorityChange,
    ) -> mrr_data_backend::providers::ProviderResult<mrr_data_backend::AuthorityState> {
        self.native.advance_authority(change)
    }
    fn capabilities(&self) -> mrr_data_backend::ProviderCapabilities {
        self.native.capabilities()
    }
    fn open(&self) -> Result<(), BackendError> {
        self.native.open()
    }
    fn close(&self) -> Result<(), BackendError> {
        self.native.close()
    }
    fn recover(
        &self,
        write: &mrr_data_backend::StoredWrite,
    ) -> mrr_data_backend::providers::ProviderResult<Option<mrr_data_backend::StoredRevision>> {
        self.native.recover(write)
    }
    fn commit(
        &self,
        write: &mrr_data_backend::StoredWrite,
        physical: Option<&mrr_data_content::PublishReceipt>,
        validate: &mut dyn FnMut(Option<ContentRevision>) -> bool,
    ) -> mrr_data_backend::providers::ProviderResult<mrr_data_backend::StoredOutcome> {
        if self.phase == "before-cas" {
            std::process::exit(CRASH_EXIT);
        }
        let outcome = self.native.commit(write, physical, validate)?;
        if self.phase == "after-commit" {
            std::process::exit(CRASH_EXIT);
        }
        Ok(outcome)
    }
}

fn baseline_root() -> cid::Cid {
    mrr_data_content::ContentBlock::new(mrr_data_content::ContentCodec::Raw, b"prior-head").cid()
}
async fn prior_head(port: &mrr_data_backend::ProfilePort) -> ContentRevision {
    port.recover(write("prior", None, baseline_root()))
        .await
        .unwrap()
        .unwrap()
        .committed
}

async fn protected_port(backend: &Backend) -> mrr_data_backend::ProfilePort {
    let base = backend.profile("graph.v1", "tenant").unwrap();
    let state = base.authority("graph", "policy").await.unwrap().unwrap();
    base.with_authorities(&[AuthorityExpectation {
        authority_id: "policy".into(),
        state,
    }])
    .unwrap()
}

#[test]
fn graph_publication_crash_child() {
    let Ok(directory) = std::env::var("MRR_GRAPH_CRASH_DIRECTORY") else {
        return;
    };
    let phase = std::env::var("MRR_GRAPH_CRASH_PHASE").unwrap();
    eprintln!("publication child entering {phase}");
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let directory = std::path::Path::new(&directory);
        let backend = Backend::open(
            BackendConfig::default(),
            ProcessProvider {
                native: native(&directory.join("graph.db")),
                phase: phase.clone(),
            },
            tokio::runtime::Handle::current(),
        )
        .await
        .unwrap();
        let port = backend.profile("graph.v1", "tenant").unwrap();
        let prior = prior_head(&port).await;
        let port = protected_port(&backend).await;
        let f = Fixture::new();
        let prepared = f.prepare();
        let remote = ProcessRemote {
            directory: directory.join("objects"),
            root: *prepared.root(),
            inventory: *f.binding.inventory_root(),
            snapshot: *f.binding.snapshot_root(),
            phase: phase.clone(),
        };
        let publication = publish_graph_dataset(&prepared, &f.local, &remote, &remote, || async {
            if phase == "before-root" {
                std::process::exit(CRASH_EXIT);
            }
            Ok(())
        })
        .await
        .unwrap();
        if phase == "complete-ack" {
            std::process::exit(CRASH_EXIT);
        }
        let operation = write("crash-publication", Some(prior), *prepared.root());
        port.commit_graph_publication(operation, Some(&publication), |_| Ok::<_, ()>(()))
            .await
            .unwrap();
        panic!("configured crash point was not reached");
    });
}

async fn run_crash_child(directory: &std::path::Path, phase: &str) {
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "graph_publication_crash_child", "--nocapture"])
        .env("MRR_GRAPH_CRASH_DIRECTORY", directory)
        .env("MRR_GRAPH_CRASH_PHASE", phase)
        .spawn()
        .unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert_eq!(status.code(), Some(CRASH_EXIT), "phase {phase}");
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("publication child stalled at {phase}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

async fn seed_prior_head(path: &std::path::Path) {
    let backend = open(path).await;
    let port = backend.profile("graph.v1", "tenant").unwrap();
    let operation = write("prior", None, baseline_root());
    let ack = mrr_data_content::PublishReceipt {
        cid: operation.replacement,
        cache: mrr_data_content::CacheAdmission::Stored,
    };
    port.commit(operation, Some(&ack), |_| Ok::<_, ()>(()))
        .await
        .unwrap();
    port.advance_authority(
        "graph",
        AuthorityProposal {
            authority_id: "policy".into(),
            expected: None,
            replacement: baseline_root(),
            status: AuthorityStatus::Active,
        },
    )
    .await
    .unwrap();
    backend.shutdown().await.unwrap();
}

async fn recover_crashed_publication(directory: &std::path::Path, phase: &str) {
    let backend = open(&directory.join("graph.db")).await;
    let port = backend.profile("graph.v1", "tenant").unwrap();
    let prior = prior_head(&port).await;
    let port = protected_port(&backend).await;
    let f = Fixture::new();
    let prepared = f.prepare();
    let operation = write("crash-publication", Some(prior), *prepared.root());
    let recovered = port.recover(operation).await.unwrap();
    assert_eq!(recovered.is_some(), phase == "after-commit", "{phase}");
    let remote = ProcessRemote {
        directory: directory.join("objects"),
        root: operation.replacement,
        inventory: *f.binding.inventory_root(),
        snapshot: *f.binding.snapshot_root(),
        phase: String::new(),
    };
    // A complete binding root survives only after root upload. Never admit a
    // partial child closure as a head. Cold restore rechecks the entire closure.
    let restored = mrr_data_content::restore_graph_dataset(
        &mrr_data_content::MemoryContentStore::default(),
        &remote,
        &operation.replacement,
        &f.query,
        &f.relations,
        &f.entities,
        (
            fixture::limits(),
            mrr_data_core::GraphInventoryLimits::default(),
        ),
    )
    .await;
    assert_eq!(
        restored.is_ok(),
        ["root-ack", "complete-ack", "before-cas", "after-commit"].contains(&phase),
        "{phase}"
    );
    if let Ok(restored) = restored {
        assert_eq!(restored.root(), prepared.root());
        assert_eq!(restored.block_count(), prepared.block_count());
        assert_eq!(restored.total_bytes(), prepared.total_bytes());
    }
    if let Some(receipt) = recovered {
        assert_eq!(receipt.committed.root, operation.replacement);
        assert_eq!(receipt.committed.revision, prior.revision + 1);
        assert!(matches!(
            port.commit_graph_publication(operation, None, |_| -> Result<(), ()> {
                panic!("recovered commit must replay without fresh validation")
            })
            .await
            .unwrap(),
            Outcome::Replayed(_)
        ));
    } else {
        // Exact expected prior head must still accept this original operation.
        // Re-publish the same closure to complete any interrupted transport.
        let publication =
            publish_graph_dataset(&prepared, &f.local, &remote, &remote, || async { Ok(()) })
                .await
                .unwrap();
        assert!(matches!(
            port.commit_graph_publication(operation, Some(&publication), |_| Ok::<_, ()>(()))
                .await
                .unwrap(),
            Outcome::Committed(_)
        ));
    }
    backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn publication_process_exit_preserves_original_operation_and_complete_head() {
    for phase in [
        "child-ack",
        "inventory-ack",
        "snapshot-ack",
        "before-root",
        "root-ack",
        "complete-ack",
        "before-cas",
        "after-commit",
    ] {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join("objects")).unwrap();
        seed_prior_head(&directory.path().join("graph.db")).await;
        run_crash_child(directory.path(), phase).await;
        recover_crashed_publication(directory.path(), phase).await;
        eprintln!("publication recovery verified {phase}");
    }
}
