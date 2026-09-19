use super::{S3Wire, snapshot_fixture, snapshot_limits};
use crate::{KacheContentStore, S3Config, S3ContentStore};
use anyhow::Result;
use std::{
    path::Path,
    process::{Child, Command, Stdio},
    time::Instant,
};
use std::{sync::Arc, time::Duration};
use tempfile::tempdir;

// Own every spawned process, including assertion/error paths.
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn child(endpoint: &str, cache: &Path, mode: &str) -> Result<Process> {
    Ok(Process(
        Command::new(std::env::current_exe()?)
            .args([
                "--ignored",
                "--exact",
                "tests::contracts::restart::snapshot_process_child",
                "--nocapture",
            ])
            .env("MRR_RESTART_ENDPOINT", endpoint)
            .env("MRR_RESTART_CACHE", cache)
            .env("MRR_RESTART_MODE", mode)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?,
    ))
}

#[tokio::test]
async fn killed_publisher_restarts_from_persistent_cache_and_restores_same_root() -> Result<()> {
    let wire = S3Wire::start().await?;
    let cache = tempdir()?;
    let fixture = snapshot_fixture::fixture();
    let root_path = format!("/probe/blocks/{}", fixture.snapshot.cid());
    *wire.objects.pause_put_path.lock().unwrap() = Some(root_path.clone());
    let endpoint = wire.endpoint.clone();
    let path = cache.path().to_owned();
    let objects = Arc::clone(&wire.objects);
    let waiting_root = root_path.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut process = child(&endpoint, &path, "seed")?;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            anyhow::ensure!(
                process.0.try_wait()?.is_none(),
                "publisher exited before root PUT"
            );
            if objects.writes.lock().unwrap().contains(&waiting_root) {
                break;
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "publisher never reached root PUT"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        process.0.kill()?;
        anyhow::ensure!(
            !process.0.wait()?.success(),
            "publisher was not interrupted"
        );
        Ok(())
    })
    .await??;
    assert!(!wire.objects.bytes.lock().unwrap().contains_key(&root_path));
    assert_eq!(
        wire.objects.bytes.lock().unwrap().len(),
        fixture.snapshot.manifest().referenced_cids().len()
    );
    *wire.objects.pause_put_path.lock().unwrap() = None;
    wire.objects.writes.lock().unwrap().clear();
    let endpoint = wire.endpoint.clone();
    let path = cache.path().to_owned();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut process = child(&endpoint, &path, "recover")?;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = process.0.try_wait()? {
                anyhow::ensure!(status.success(), "recovery process failed: {status}");
                break;
            }
            anyhow::ensure!(Instant::now() < deadline, "recovery process timed out");
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    })
    .await??;
    let mut expected: Vec<_> = fixture
        .snapshot
        .manifest()
        .referenced_cids()
        .iter()
        .map(|cid| format!("/probe/blocks/{cid}"))
        .collect();
    expected.push(root_path.clone());
    assert_eq!(*wire.objects.writes.lock().unwrap(), expected);
    assert_eq!(
        wire.objects.bytes.lock().unwrap()[&root_path],
        fixture.snapshot.bytes()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "subprocess entry point invoked by the restart contract"]
async fn snapshot_process_child() -> Result<()> {
    let endpoint = std::env::var("MRR_RESTART_ENDPOINT")?;
    let path = std::env::var("MRR_RESTART_CACHE")?;
    let seed = std::env::var("MRR_RESTART_MODE")? == "seed";
    let (source, fixture) = tokio::task::spawn_blocking(move || -> Result<_> {
        let source = KacheContentStore::open(path, 16 * 1024 * 1024)?;
        let fixture = snapshot_fixture::fixture();
        // Recovery must use persisted blocks; it cannot repopulate the source.
        if seed {
            fixture.seed(&source)?;
        }
        Ok((crate::BlockingContentStore::new(source), fixture))
    })
    .await??;
    let remote = S3ContentStore::new(
        S3Config::default()
            .bucket("probe")
            .region("us-east-1")
            .endpoint(&endpoint)
            .access_key_id("local-key")
            .secret_access_key("local-secret")
            .disable_config_load()
            .disable_ec2_metadata(),
        crate::http_client_builder().build()?,
        Duration::from_secs(30),
    )?;
    let control = mrr_data_content::TransferSession::new(
        Duration::from_secs(30),
        mrr_data_content::RemoteTransferLimits {
            operations: 100,
            bytes: 64 * 1024 * 1024,
            attempts_per_operation: 2,
            retry_delay: Duration::ZERO,
        },
    )?;
    let receipt = control
        .publish_snapshot(
            &source,
            &remote,
            &fixture.snapshot,
            &fixture.relations,
            &fixture.entities,
            snapshot_limits(),
        )
        .await?;
    anyhow::ensure!(!seed, "seed process unexpectedly completed publication");
    assert_eq!(receipt.root(), fixture.snapshot.cid());
    let target = tempdir()?;
    let path = target.path().to_owned();
    let target_store = tokio::task::spawn_blocking(move || {
        KacheContentStore::open(path, 16 * 1024 * 1024).map(crate::BlockingContentStore::new)
    })
    .await??;
    let restored = control
        .restore_snapshot(
            &target_store,
            &remote,
            receipt.root(),
            &fixture.relations,
            &fixture.entities,
            snapshot_limits(),
        )
        .await?;
    assert_eq!(restored.snapshot(), &fixture.snapshot);
    let ipc = &restored.children()[&mrr_data_core::raw_cid(&fixture.ipc)];
    assert_eq!(
        mrr_data_arrow::ipc_to_facts(
            &fixture.relation,
            ipc,
            mrr_data_arrow::IpcImportLimits::new(ipc.len(), 10, 100)
        )
        .unwrap(),
        fixture.facts
    );
    Ok(())
}
