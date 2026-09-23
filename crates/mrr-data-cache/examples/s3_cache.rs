//! Publish one bounded file and restore it through the public content protocol.
//! Set `S3_BUCKET`, `S3_ENDPOINT`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
use std::{env, fs, io::Read, time::Duration};

use mrr_data_cache::{
    BlockingContentStore, KacheContentStore, S3Config, S3ContentStore, http_client_builder,
};
use mrr_data_content::{ContentBlock, ContentCodec, publish_content, read_through};

const MAX_FILE_BYTES: usize = 64 * 1024 * 1024;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let file = env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("expected a file path"))?;
    let config = S3Config::default()
        .bucket(&env::var("S3_BUCKET")?)
        .root(&env::var("S3_ROOT").unwrap_or_default())
        .endpoint(&env::var("S3_ENDPOINT")?)
        .region(&env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".into()));
    let ca_path = env::var("S3_CA_PEM").ok();
    let (source_root, target_root, source, target, bytes, ca) =
        tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let input = fs::File::open(file)?;
            anyhow::ensure!(
                input.metadata()?.len() <= MAX_FILE_BYTES as u64,
                "file exceeds 64 MiB"
            );
            let mut bytes = Vec::new();
            input
                .take(MAX_FILE_BYTES as u64 + 1)
                .read_to_end(&mut bytes)?;
            anyhow::ensure!(bytes.len() <= MAX_FILE_BYTES, "file grew beyond 64 MiB");
            let source_root = tempfile::tempdir()?;
            let target_root = tempfile::tempdir()?;
            let source = BlockingContentStore::new(KacheContentStore::open(
                source_root.path(),
                MAX_FILE_BYTES as u64,
            )?);
            let target = BlockingContentStore::new(KacheContentStore::open(
                target_root.path(),
                MAX_FILE_BYTES as u64,
            )?);
            let ca = ca_path.map(fs::read).transpose()?;
            Ok((source_root, target_root, source, target, bytes, ca))
        })
        .await??;
    let mut client = http_client_builder();
    if let Some(ca) = ca {
        client = client.tls_certs_merge([reqwest::Certificate::from_pem(&ca)?]);
    }
    let remote = S3ContentStore::new(config, client.build()?, Duration::from_mins(1))?;
    let receipt = publish_content(
        &source,
        &remote,
        ContentBlock::new(ContentCodec::Raw, &bytes),
    )
    .await?;
    let fetched = read_through(&target, &remote, &receipt.cid, bytes.len())
        .await?
        .ok_or_else(|| anyhow::anyhow!("published content was missing"))?;
    anyhow::ensure!(fetched.bytes == bytes, "roundtrip mismatch");
    println!("Restored {} bytes: {}", fetched.bytes.len(), receipt.cid);
    tokio::task::spawn_blocking(move || {
        drop((source, target));
        drop((source_root, target_root));
    })
    .await?;
    Ok(())
}
