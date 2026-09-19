//! Runs only through tools/s3-conformance/run.py against an independent server.
use crate::{
    BlockingContentStore, KacheContentStore, S3Config, S3ContentStore, http_client_builder,
};
use anyhow::Result;
use mrr_data_content::{ContentBlock, ContentCodec, RemoteContentStore, RemoteError};
use opendal::{HttpTransporter, OperationContext, Operator};
use opendal_http_transport_reqwest::ReqwestTransport;
use std::{env, fs, time::Duration};

use super::contracts::snapshot_fixture;
const KEY: &str = "local-conformance-key";
const SECRET: &str = "local-conformance-secret";

fn config(endpoint: &str, root: &str) -> S3Config {
    S3Config::default()
        .bucket("conformance")
        .region("us-east-1")
        .endpoint(endpoint)
        .root(root)
        .access_key_id(KEY)
        .secret_access_key(SECRET)
        .disable_config_load()
        .disable_ec2_metadata()
}
fn client(ca: Option<&[u8]>) -> Result<reqwest::Client> {
    let mut builder = http_client_builder().no_proxy();
    if let Some(ca) = ca {
        builder = builder.add_root_certificate(reqwest::Certificate::from_pem(ca)?);
    }
    Ok(builder.build()?)
}
fn remote(config: S3Config, ca: Option<&[u8]>) -> Result<S3ContentStore> {
    Ok(S3ContentStore::new(
        config,
        client(ca)?,
        Duration::from_secs(5),
    )?)
}

async fn authentication_and_trust(endpoint: &str, ca: &[u8]) -> Result<()> {
    let block = ContentBlock::new(ContentCodec::Raw, b"authenticated and trusted");
    let good = remote(config(endpoint, "trust"), Some(ca))?;
    good.put(block).await?;
    assert_eq!(
        good.get(&block.cid(), 100).await?,
        Some(block.bytes().to_vec())
    );
    for bad in [
        config(endpoint, "trust").access_key_id("unknown-key"),
        config(endpoint, "trust").secret_access_key("incorrect-secret"),
    ] {
        let denied = remote(bad, Some(ca))?;
        assert_eq!(
            denied.get(&block.cid(), 100).await,
            Err(RemoteError::PermissionDenied)
        );
        assert_eq!(denied.put(block).await, Err(RemoteError::PermissionDenied));
    }
    let untrusted = remote(config(endpoint, "trust"), None)?;
    assert_eq!(
        untrusted.get(&block.cid(), 100).await,
        Err(RemoteError::Unavailable)
    );
    // The CA is trusted, but the leaf has only the 127.0.0.1 IP SAN.
    let mismatch = remote(
        config(&endpoint.replace("127.0.0.1", "localhost"), "trust"),
        Some(ca),
    )?;
    assert_eq!(
        mismatch.get(&block.cid(), 100).await,
        Err(RemoteError::Unavailable)
    );
    // Negative results must not be mistaken for an outage of the good endpoint.
    assert_eq!(
        good.get(&block.cid(), 100).await?,
        Some(block.bytes().to_vec())
    );
    Ok(())
}

async fn namespaces_and_conditional_writes(endpoint: &str, ca: &[u8]) -> Result<()> {
    let block = ContentBlock::new(ContentCodec::Raw, b"immutable");
    let remote = remote(config(endpoint, "tenant/a"), Some(ca))?;
    remote.put(block).await?;
    remote.put(block).await?;
    let other = S3ContentStore::new(
        config(endpoint, "tenant/b"),
        client(Some(ca))?,
        Duration::from_secs(5),
    )?;
    assert_eq!(other.get(&block.cid(), 100).await?, None);
    assert_eq!(
        remote.get(&block.cid(), 1).await,
        Err(RemoteError::TooLarge)
    );
    let context = OperationContext::new().with_http_transport(HttpTransporter::new(
        ReqwestTransport::new(client(Some(ca))?),
    ));
    let raw = Operator::new(config(endpoint, "tenant/a"))?.with_context(context);
    let key = format!("blocks/{}", block.cid());
    // Independently verify that this service enforces If-None-Match.
    let error = raw
        .write_with(&key, b"different".to_vec())
        .if_not_exists(true)
        .await
        .unwrap_err();
    assert!(matches!(
        error.kind(),
        opendal::ErrorKind::ConditionNotMatch | opendal::ErrorKind::AlreadyExists
    ));
    assert_eq!(raw.read(&key).await?.to_vec(), block.bytes());
    // Simulate an externally poisoned object: adapter PUT must not replace it.
    raw.write(&key, b"corrupted".to_vec()).await?;
    assert!(remote.put(block).await.is_err());
    assert_eq!(raw.read(&key).await?.to_vec(), b"corrupted");
    Ok(())
}

async fn snapshot_roundtrip(endpoint: &str, ca: &[u8]) -> Result<()> {
    let source_dir = tempfile::tempdir()?;
    let target_dir = tempfile::tempdir()?;
    let a = source_dir.path().to_owned();
    let b = target_dir.path().to_owned();
    let (source, target, fixture) = tokio::task::spawn_blocking(move || -> Result<_> {
        let fixture = snapshot_fixture::fixture();
        let source = KacheContentStore::open(a, 1_000_000)?;
        fixture.seed(&source)?;
        Ok((
            BlockingContentStore::new(source),
            BlockingContentStore::new(KacheContentStore::open(b, 1_000_000)?),
            fixture,
        ))
    })
    .await??;
    let remote = remote(config(endpoint, "snapshots/agent-a"), Some(ca))?;
    let session = mrr_data_content::TransferSession::new(
        Duration::from_secs(30),
        mrr_data_content::RemoteTransferLimits {
            operations: 100,
            bytes: 64 * 1024 * 1024,
            attempts_per_operation: 2,
            retry_delay: Duration::from_millis(10),
        },
    )?;
    let limits =
        mrr_data_content::SnapshotTransferLimits::new(64 * 1024, 100, 1024 * 1024, 8 * 1024 * 1024);
    let receipt = session
        .publish_snapshot(
            &source,
            &remote,
            &fixture.snapshot,
            &fixture.relations,
            &fixture.entities,
            limits,
        )
        .await?;
    let restored = session
        .restore_snapshot(
            &target,
            &remote,
            receipt.root(),
            &fixture.relations,
            &fixture.entities,
            limits,
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

#[tokio::test]
#[ignore = "requires independent SigV4/TLS server; run tools/s3-conformance/run.py"]
async fn local_s3_tls_conformance() -> Result<()> {
    let endpoint = env::var("MRR_S3_CONFORMANCE_ENDPOINT")?;
    let ca = fs::read(env::var("MRR_S3_CONFORMANCE_CA")?)?;
    anyhow::ensure!(
        endpoint.starts_with("https://127.0.0.1:"),
        "local TLS endpoint required"
    );
    authentication_and_trust(&endpoint, &ca).await?;
    namespaces_and_conditional_writes(&endpoint, &ca).await?;
    snapshot_roundtrip(&endpoint, &ca).await?;
    Ok(())
}
