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

const DEFAULT_CACHE_MAX_BYTES: u64 = 32 * 1024 * 1024;
const MIN_CACHE_MAX_BYTES: u64 = 1024 * 1024;
const MAX_CACHE_MAX_BYTES: u64 = 1024 * 1024 * 1024 * 1024 * 1024;
const DEFAULT_WORKER_THREADS: usize = 2;
const MAX_WORKER_THREADS: usize = 256;
const DEFAULT_S3_TIMEOUT_SECS: u64 = 5;
const MAX_S3_TIMEOUT_SECS: u64 = 30;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RuntimeConfig {
    cache_max_bytes: u64,
    worker_threads: usize,
    s3_timeout: Duration,
}

impl RuntimeConfig {
    fn from_env() -> Result<Self> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let cache_max_bytes = bounded_u64(
            "MRR_CACHE_MAX_BYTES",
            lookup("MRR_CACHE_MAX_BYTES"),
            DEFAULT_CACHE_MAX_BYTES,
            MIN_CACHE_MAX_BYTES,
            MAX_CACHE_MAX_BYTES,
        )?;
        let worker_threads = bounded_u64(
            "MRR_WORKER_THREADS",
            lookup("MRR_WORKER_THREADS"),
            DEFAULT_WORKER_THREADS as u64,
            1,
            MAX_WORKER_THREADS as u64,
        )?
        .try_into()
        .context("MRR_WORKER_THREADS unsupported on this platform")?;
        let s3_timeout_secs = bounded_u64(
            "MRR_S3_REQUEST_TIMEOUT_SECS",
            lookup("MRR_S3_REQUEST_TIMEOUT_SECS"),
            DEFAULT_S3_TIMEOUT_SECS,
            1,
            MAX_S3_TIMEOUT_SECS,
        )?;
        Ok(Self {
            cache_max_bytes,
            worker_threads,
            s3_timeout: Duration::from_secs(s3_timeout_secs),
        })
    }
}

fn bounded_u64(
    name: &str,
    value: Option<String>,
    default: u64,
    minimum: u64,
    maximum: u64,
) -> Result<u64> {
    let Some(value) = value else {
        return Ok(default);
    };
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("{name} must be an unsigned integer"))?;
    ensure!(
        (minimum..=maximum).contains(&parsed),
        "{name} must be between {minimum} and {maximum}"
    );
    Ok(parsed)
}

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
    let config = RuntimeConfig::from_env()?;
    let (local, remote) = stores(config)?;
    WorkerRuntime::new(config.worker_threads)?.execute(input, local, remote)
}

fn stores(
    runtime_config: RuntimeConfig,
) -> Result<(BlockingContentStore<KacheContentStore>, S3ContentStore)> {
    let path = env::var("MRR_CACHE_DIR").context("MRR_CACHE_DIR required")?;
    let local = KacheContentStore::open(path, runtime_config.cache_max_bytes)?;
    let local = BlockingContentStore::new(local);
    let s3_config = S3Config::default()
        .bucket(&env::var("S3_BUCKET")?)
        .endpoint(&env::var("S3_ENDPOINT")?)
        .region(&env::var("S3_REGION")?)
        .root(&env::var("S3_ROOT")?);
    let mut client = http_client_builder();
    if let Ok(path) = env::var("S3_CA_PEM") {
        client =
            client.add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(path)?)?);
    }
    let remote = S3ContentStore::new(s3_config, client.build()?, runtime_config.s3_timeout)?;
    Ok((local, remote))
}

struct WorkerRuntime(tokio::runtime::Runtime);
impl WorkerRuntime {
    fn new(worker_threads: usize) -> Result<Self> {
        Ok(Self(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads)
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

#[cfg(test)]
#[path = "../tests/unit/runtime.rs"]
mod tests;
