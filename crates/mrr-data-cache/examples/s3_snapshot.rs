//! Publish real Arrow facts and restore them through a second empty Kache cache.
//! Supply the same S3 environment variables as the `s3_cache` example.
#[path = "support/snapshot_fixture.rs"]
mod snapshot_fixture;

use mrr_data_cache::{
    BlockingContentStore, KacheContentStore, S3Config, S3ContentStore, http_client_builder,
};
use mrr_data_content::{RemoteTransferLimits, SnapshotTransferLimits, TransferSession};
use std::{env, fs, time::Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = S3Config::default()
        .bucket(&env::var("S3_BUCKET")?)
        .root(&env::var("S3_ROOT").unwrap_or_default())
        .endpoint(&env::var("S3_ENDPOINT")?)
        .region(&env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".into()));
    let mut client = http_client_builder();
    if let Ok(path) = env::var("S3_CA_PEM") {
        client = client.tls_certs_merge([reqwest::Certificate::from_pem(&fs::read(path)?)?]);
    }
    let remote = S3ContentStore::new(config, client.build()?, Duration::from_mins(1))?;
    let a = tempfile::tempdir()?;
    let b = tempfile::tempdir()?;
    let a_path = a.path().to_owned();
    let b_path = b.path().to_owned();
    let (source, target, fixture) = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let source = KacheContentStore::open(a_path, 16 * 1024 * 1024)?;
        let target = KacheContentStore::open(b_path, 16 * 1024 * 1024)?;
        let fixture = snapshot_fixture::fixture();
        fixture.seed(&source)?;
        Ok((
            BlockingContentStore::new(source),
            BlockingContentStore::new(target),
            fixture,
        ))
    })
    .await??;
    let control = TransferSession::new(
        Duration::from_mins(1),
        RemoteTransferLimits {
            operations: 100,
            bytes: 64 * 1024 * 1024,
            attempts_per_operation: 3,
            retry_delay: Duration::from_millis(100),
        },
    )?;
    let limits = SnapshotTransferLimits::new(64 * 1024, 100, 1024 * 1024, 8 * 1024 * 1024);
    let receipt = control
        .publish_snapshot(
            &source,
            &remote,
            &fixture.snapshot,
            &fixture.relations,
            &fixture.entities,
            limits,
        )
        .await?;
    let restored = control
        .restore_snapshot(
            &target,
            &remote,
            receipt.root(),
            &fixture.relations,
            &fixture.entities,
            limits,
        )
        .await?;
    anyhow::ensure!(
        restored.snapshot() == &fixture.snapshot,
        "snapshot identity drift"
    );
    let ipc = &restored.children()[&mrr_data_core::raw_cid(&fixture.ipc)];
    let facts = mrr_data_arrow::ipc_to_facts(
        &fixture.relation,
        ipc,
        mrr_data_arrow::IpcImportLimits::new(ipc.len(), 10, 100),
    )
    .map_err(|e| anyhow::anyhow!("Arrow import: {e:?}"))?;
    anyhow::ensure!(facts == fixture.facts, "Arrow fact drift");
    println!(
        "Restored {} facts from {}; transfer: {:?}",
        facts.len(),
        receipt.root(),
        control.stats()
    );
    Ok(())
}
