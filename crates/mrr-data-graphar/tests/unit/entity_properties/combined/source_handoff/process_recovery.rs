//! Real process termination over a test-only single-writer file transaction fixture.
//! This qualifies process recovery, not database concurrency or power-loss durability.
use super::{RESERVED, authority, selective};
use crate::tests::entity_properties::combined::{fixture::Fixture, remote::Remote};
use mrr_data_backend::{
    Backend, BackendConfig, BackendError,
    providers::{MetadataTransaction, ProviderResult, TransactionProvider},
};
use mrr_data_content::{ConditionalCommitPortError, ConditionalContentCommitPort};
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
};
const CHILD: &str = "tests::entity_properties::combined::source_handoff::backend::process_recovery::original_source_recovery_child";
const FIXTURE: &str = "tests::entity_properties::combined::source_handoff::backend::selective::resources::fixture::original_source_resource_fixture";
struct FileMetadata {
    path: PathBuf,
    writer: Mutex<()>,
}
struct Working(BTreeMap<String, Vec<u8>>);
impl MetadataTransaction for Working {
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        Ok(self.0.get(key).cloned())
    }
    fn put(&mut self, key: &str, value: &[u8]) -> Result<(), BackendError> {
        if value.len() > 65_536 {
            return Err(BackendError::Limit);
        }
        self.0.insert(key.into(), value.to_vec());
        Ok(())
    }
}
impl FileMetadata {
    fn load(&self) -> Result<BTreeMap<String, Vec<u8>>, BackendError> {
        match std::fs::read(&self.path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|_| BackendError::Corrupt),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
            Err(_) => Err(BackendError::Unavailable),
        }
    }
}
impl TransactionProvider for FileMetadata {
    fn open_storage(&self) -> Result<(), BackendError> {
        self.load().map(|_| ())
    }
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        Ok(self.load()?.get(key).cloned())
    }
    fn transaction(
        &self,
        run: &mut dyn FnMut(&mut dyn MetadataTransaction) -> ProviderResult<()>,
    ) -> ProviderResult<()> {
        let _writer = self.writer.lock().unwrap();
        let mut working = Working(
            self.load()
                .map_err(ConditionalCommitPortError::BeforeCommit)?,
        );
        run(&mut working)?;
        let staging = self.path.with_extension("next");
        let before = |_| ConditionalCommitPortError::BeforeCommit(BackendError::Unavailable);
        let unknown = |_| ConditionalCommitPortError::Unknown(BackendError::Unavailable);
        let mut file = std::fs::File::create(&staging).map_err(before)?;
        serde_json::to_writer(&mut file, &working.0)
            .map_err(|_| ConditionalCommitPortError::BeforeCommit(BackendError::Unavailable))?;
        file.sync_all().map_err(before)?;
        std::fs::rename(staging, &self.path).map_err(unknown)?;
        std::fs::File::open(self.path.parent().unwrap())
            .and_then(|f| f.sync_all())
            .map_err(unknown)?;
        Ok(())
    }
    fn close_storage(&self) -> Result<(), BackendError> {
        Ok(())
    }
}
pub(super) fn checkpoint(f: &Fixture, remote: &Remote, phase: &str) {
    if std::env::var("MRR_DATA_SOURCE_CRASH_PHASE").ok().as_deref() != Some(phase) {
        return;
    }
    let path = PathBuf::from(std::env::var("MRR_DATA_SOURCE_RECOVERY").unwrap());
    let blocks: Vec<_> = remote
        .blocks
        .lock()
        .unwrap()
        .iter()
        .map(|(cid, b)| (cid.to_string(), b.clone()))
        .collect();
    let mut file = std::fs::File::create(path.join("remote.json")).unwrap();
    serde_json::to_writer(&mut file, &blocks).unwrap();
    file.sync_all().unwrap();
    println!("SOURCE-CRASH-READY {phase} {}", f.query.snapshot_root());
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}
#[tokio::test]
#[ignore = "independent original-source process; run through recovery matrix"]
async fn original_source_recovery_child() {
    let home = PathBuf::from(std::env::var("MRR_DATA_SOURCE_RECOVERY").unwrap());
    println!("original-source recovery child loading immutable fixture");
    let f = selective::process_fixture("uniform");
    let backend = Backend::open(
        BackendConfig {
            max_resource_bytes: 3 * RESERVED,
            ..BackendConfig::default()
        },
        FileMetadata {
            path: home.join("metadata.json"),
            writer: Mutex::new(()),
        },
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let remote = Arc::new(Remote::default());
    if std::env::var("MRR_DATA_SOURCE_CRASH_PHASE").is_ok() {
        authority::publish(&f, &backend, remote.as_ref()).await;
        panic!("required crash checkpoint was not reached");
    }
    let blocks: Vec<(String, Vec<u8>)> =
        serde_json::from_slice(&std::fs::read(home.join("remote.json")).unwrap()).unwrap();
    *remote.blocks.lock().unwrap() = blocks
        .into_iter()
        .map(|(cid, b)| (cid.parse().unwrap(), b))
        .collect();
    let base = backend.profile("healthcare", "simulation").unwrap();
    let policy = base.authority("dataset", "policy").await.unwrap().unwrap();
    let guarded = base
        .with_authorities(&[mrr_data_backend::AuthorityExpectation {
            authority_id: "policy".into(),
            state: policy,
        }])
        .unwrap();
    let write = authority::operation(*f.query.snapshot_root());
    let recovered = guarded.recover(write).await.unwrap();
    let phase = std::env::var("MRR_DATA_SOURCE_EXPECT_PHASE").unwrap();
    assert_eq!(recovered.is_some(), phase == "after-commit");
    let receipt = mrr_data_content::publish_combined_graph(
        &f.prepare(),
        &f.local,
        remote.as_ref(),
        remote.as_ref(),
        || async { Ok(()) },
    )
    .await
    .unwrap();
    let outcome = guarded
        .commit(write, Some(&receipt), |_| Ok::<_, ()>(()))
        .await
        .unwrap();
    assert_eq!(
        matches!(
            outcome,
            mrr_data_content::ConditionalContentCommitOutcome::Replayed(_)
        ),
        phase == "after-commit"
    );
    let result = selective::process_query_transport(
        &f,
        &backend,
        remote,
        Arc::new(mrr_data_content::MemoryContentStore::default()),
        &base,
        policy,
    )
    .await;
    result
        .get()
        .verify(
            &f.query,
            super::result_limits(),
            std::num::NonZeroUsize::new(1 << 20).unwrap(),
        )
        .unwrap();
    drop(result);
    assert_eq!(backend.status().resource_bytes, 0);
    authority::retire_and_recover(&f, &base, policy, &receipt).await;
    assert!(!authority::disclose(&base, policy).await);
    backend.shutdown().await.unwrap();
    println!(
        "SOURCE-RECOVERY {}",
        serde_json::json!({"phase":phase,"snapshot_root":f.query.snapshot_root().to_string(),"source_digest":super::super::SOURCE_DIGEST,"rows":4,"cleanup_bytes":0,"retired_disclosure_refused":true})
    );
}
fn spawn(test: &str, directory: &std::path::Path, phase: Option<&str>) -> std::process::Child {
    Command::new(std::env::current_exe().unwrap())
        .args([
            test,
            "--exact",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("MRR_DATA_SOURCE_RECOVERY", directory)
        .env("MRR_DATA_SOURCE_EXPECT_PHASE", phase.unwrap_or(""))
        .env("MRR_DATA_SOURCE_SHAPE", "uniform")
        .env("MRR_DATA_SOURCE_FIXTURE", directory.join("fixture.json"))
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap()
}
fn drain(mut child: std::process::Child) -> Vec<String> {
    let lines = BufReader::new(child.stdout.take().unwrap())
        .lines()
        .map(|l| {
            let l = l.unwrap();
            println!("{l}");
            l
        })
        .collect();
    assert!(child.wait().unwrap().success());
    lines
}
#[test]
#[ignore = "parent must initialize neither Gambit nor Tokio; supervise actual children"]
fn original_source_process_recovery_matrix() {
    use std::os::unix::process::ExitStatusExt;
    for phase in ["before-commit", "after-commit"] {
        let directory = tempfile::tempdir().unwrap();
        drain(spawn(FIXTURE, directory.path(), None));
        // The Rust parent has no native runtime and retains the actual SIGKILL wait status.
        let mut command = Command::new(std::env::current_exe().unwrap());
        let mut child = command
            .args([
                CHILD,
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("MRR_DATA_SOURCE_RECOVERY", directory.path())
            .env("MRR_DATA_SOURCE_SHAPE", "uniform")
            .env(
                "MRR_DATA_SOURCE_FIXTURE",
                directory.path().join("fixture.json"),
            )
            .env("MRR_DATA_SOURCE_CRASH_PHASE", phase)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut root = None;
        for line in BufReader::new(child.stdout.take().unwrap()).lines() {
            let line = line.unwrap();
            println!("{line}");
            if line.starts_with(&format!("SOURCE-CRASH-READY {phase} ")) {
                root = Some(
                    line.strip_prefix(&format!("SOURCE-CRASH-READY {phase} "))
                        .unwrap()
                        .to_owned(),
                );
                break;
            }
        }
        let root = root.expect("required actual crash checkpoint");
        assert!(root.parse::<cid::Cid>().is_ok());
        child.kill().unwrap();
        assert_eq!(child.wait().unwrap().signal(), Some(9));
        let lines = drain(spawn(CHILD, directory.path(), Some(phase)));
        let records: Vec<_> = lines
            .iter()
            .filter_map(|l| l.strip_prefix("SOURCE-RECOVERY "))
            .collect();
        assert_eq!(records.len(), 1);
        let record: serde_json::Value = serde_json::from_str(records[0]).unwrap();
        assert_eq!(record["phase"], phase);
        assert_eq!(record["snapshot_root"], root);
        assert_eq!(record["source_digest"], super::super::SOURCE_DIGEST);
        assert_eq!(record["retired_disclosure_refused"], true);
        assert_eq!(record["rows"], 4);
        assert_eq!(record["cleanup_bytes"], 0);
    }
}
