//! Dedicated one-request process owner; blocking setup precedes the async runtime.
use crate::protocol::{MAX_INPUT, PROFILE, Request};
use anyhow::{Context as _, Result, ensure};
use mrr_data_cache::{
    BlockingContentStore, KacheContentStore, S3Config, S3ContentStore, http_client_builder,
};
use std::{
    env,
    io::{self, Read},
    time::Duration,
};

/// Read one bounded data envelope and execute it in the dedicated worker.
/// # Errors
/// Rejects invalid requests, configuration and any transfer or admission failure.
pub fn run_cli() -> Result<String> {
    let mut input = Vec::new();
    io::stdin()
        .take(MAX_INPUT as u64 + 1)
        .read_to_end(&mut input)?;
    execute(&input)
}

/// Execute a bounded request using this process's runtime configuration.
/// # Errors
/// Returns input, configuration, transfer or semantic admission failures.
pub fn execute(input: &[u8]) -> Result<String> {
    ensure!(input.len() <= MAX_INPUT, "request exceeds 1 MiB");
    let request: Request = serde_json::from_slice(input)?;
    ensure!(request.profile == PROFILE, "unsupported consumer profile");
    crate::semantic::Context::new(&request.source, &request.revision)?;
    let (local, remote) = stores()?;
    WorkerRuntime::new()?.execute(input, local, remote)
}

fn stores() -> Result<(BlockingContentStore<KacheContentStore>, S3ContentStore)> {
    let path = env::var("MRR_CACHE_DIR").context("MRR_CACHE_DIR required")?;
    let local = KacheContentStore::open(path, 32 * 1024 * 1024)?;
    let local = BlockingContentStore::new(local);
    let config = S3Config::default()
        .bucket(&env::var("S3_BUCKET")?)
        .endpoint(&env::var("S3_ENDPOINT")?)
        .region(&env::var("S3_REGION")?)
        .root(&env::var("S3_ROOT")?);
    let mut client = http_client_builder();
    if let Ok(path) = env::var("S3_CA_PEM") {
        client =
            client.add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(path)?)?);
    }
    let remote = S3ContentStore::new(config, client.build()?, Duration::from_secs(5))?;
    Ok((local, remote))
}

struct WorkerRuntime(tokio::runtime::Runtime);
impl WorkerRuntime {
    fn new() -> Result<Self> {
        Ok(Self(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?,
        ))
    }
    fn execute(
        self,
        input: &[u8],
        local: BlockingContentStore<KacheContentStore>,
        remote: S3ContentStore,
    ) -> Result<String> {
        self.0
            .block_on(crate::worker::execute(input, local, remote))
    }
}
