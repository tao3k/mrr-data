//! Opt-in native measurements with exact recovery and lifecycle assertions.
#![cfg(any(feature = "turso", feature = "duckdb"))]
use mrr_data_backend::{Backend, BackendConfig, Lifecycle, MetadataProvider, ProfilePort};
use mrr_data_content::{
    CacheAdmission, ConditionalContentCommitOutcome, ConditionalContentCommitPort,
    ConditionalContentWrite, ContentBlock, ContentCodec, ContentRevision, PublishReceipt,
};
use std::{path::PathBuf, sync::Arc, time::Instant};
use tokio::{sync::Mutex, task::JoinSet};

const OPERATIONS: usize = 64;
const WARMUP: usize = 8;
const ROUNDS: usize = 3;

struct Record {
    scope: String,
    operation: String,
    expected: Option<ContentRevision>,
    committed: ContentRevision,
}
impl Record {
    fn write(&self) -> ConditionalContentWrite<'_> {
        ConditionalContentWrite {
            scope: &self.scope,
            operation_id: &self.operation,
            expected: self.expected,
            replacement: self.committed.root,
        }
    }
}

async fn lane(
    port: ProfilePort,
    head: Arc<Mutex<Option<ContentRevision>>>,
    lane_id: usize,
    hot: bool,
) -> (Vec<Record>, Vec<u128>) {
    let scope = format!("home-{}", if hot { 0 } else { lane_id });
    let root = ContentBlock::new(ContentCodec::Raw, b"backend-measurement").cid();
    let mut records = Vec::new();
    let mut samples = Vec::new();
    for operation in 0..WARMUP + OPERATIONS {
        let id = format!("lane-{lane_id}-operation-{operation}");
        // End-to-end latency includes application sequencing of a single home.
        let start = Instant::now();
        let mut current = head.lock().await;
        let expected = *current;
        let write = ConditionalContentWrite {
            scope: &scope,
            operation_id: &id,
            expected,
            replacement: root,
        };
        let ack = PublishReceipt {
            cid: root,
            cache: CacheAdmission::Stored,
        };
        let result = port
            .commit(write, Some(&ack), |_| Ok::<_, ()>(()))
            .await
            .unwrap();
        let ConditionalContentCommitOutcome::Committed(receipt) = result else {
            panic!("fresh operation replayed");
        };
        let committed = receipt.committed;
        *current = Some(committed);
        drop(current);
        if operation >= WARMUP {
            samples.push(start.elapsed().as_nanos());
        }
        records.push(Record {
            scope: scope.clone(),
            operation: id,
            expected,
            committed,
        });
    }
    (records, samples)
}

fn report(
    provider: &str,
    scenario: &str,
    phase: &str,
    round: usize,
    workers: usize,
    elapsed_ns: u128,
    mut samples: Vec<u128>,
) {
    samples.sort_unstable();
    let wall_operations = samples.len()
        + if phase == "commit" {
            workers * WARMUP
        } else {
            0
        };
    let percentile = |percent: usize| samples[(samples.len() * percent).div_ceil(100) - 1];
    println!(
        "{}",
        serde_json::json!({
            "provider": provider, "scenario": scenario, "phase": phase,
            "round": round, "workers": workers, "samples": samples.len(),
            "elapsed_ns": elapsed_ns, "p50_ns": percentile(50),
            "p95_ns": percentile(95), "p99_ns": percentile(99),
            "wall_operations": wall_operations,
            "wall_operations_per_second": u128::try_from(wall_operations).unwrap() * 1_000_000_000 / elapsed_ns,
            "errors": 0
        })
    );
}

async fn closed(backend: &Backend, operations: usize) -> u128 {
    let start = Instant::now();
    backend.shutdown().await.unwrap();
    backend.shutdown().await.unwrap();
    let status = backend.status();
    assert_eq!(status.lifecycle, Lifecycle::Closed);
    assert_eq!(status.active_writes, 0);
    assert_eq!(status.active_recoveries, 0);
    assert_eq!(status.blocking_writes, 0);
    assert_eq!(status.blocking_recoveries, 0);
    assert_eq!(status.retained_bytes, 0);
    assert_eq!(status.completed, u64::try_from(operations).unwrap());
    start.elapsed().as_nanos()
}

async fn scenario<P: MetadataProvider>(
    provider_name: &str,
    factory: &impl Fn(PathBuf) -> P,
    round: usize,
    workers: usize,
    hot: bool,
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("backend.db");
    let config = BackendConfig::default();
    let backend = Backend::open(
        config,
        factory(path.clone()),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let name = if hot { "hot-home" } else { "independent-homes" };
    let shared = Arc::new(Mutex::new(None));
    let mut jobs = JoinSet::new();
    let start = Instant::now();
    for id in 0..workers {
        let head = if hot {
            shared.clone()
        } else {
            Arc::new(Mutex::new(None))
        };
        jobs.spawn(lane(
            backend.profile("measurement.v1", "tenant").unwrap(),
            head,
            id,
            hot,
        ));
    }
    let mut records = Vec::new();
    let mut samples = Vec::new();
    while let Some(job) = jobs.join_next().await {
        let (mut lane_records, mut lane_samples) = job.unwrap();
        records.append(&mut lane_records);
        samples.append(&mut lane_samples);
    }
    // Wall time deliberately includes warmup: throughput uses the full count.
    let elapsed_ns = start.elapsed().as_nanos();
    let shutdown_ns = closed(&backend, records.len()).await;
    report(
        provider_name,
        name,
        "commit",
        round,
        workers,
        elapsed_ns,
        samples,
    );
    println!(
        "{}",
        serde_json::json!({"provider": provider_name, "scenario": name,
        "round": round, "workers": workers, "commits": records.len(),
        "commit_wall_ns_including_warmup": elapsed_ns, "shutdown_ns": shutdown_ns})
    );

    let reopened = Backend::open(config, factory(path), tokio::runtime::Handle::current())
        .await
        .unwrap();
    let port = reopened.profile("measurement.v1", "tenant").unwrap();
    let start = Instant::now();
    let mut samples = Vec::new();
    for record in &records {
        let operation_start = Instant::now();
        let receipt = port.recover(record.write()).await.unwrap().unwrap();
        assert_eq!(receipt.committed, record.committed);
        samples.push(operation_start.elapsed().as_nanos());
    }
    report(
        provider_name,
        name,
        "recovery-serial",
        round,
        workers,
        start.elapsed().as_nanos(),
        samples,
    );
    closed(&reopened, records.len()).await;
}

async fn suite<P: MetadataProvider>(name: &str, factory: impl Fn(PathBuf) -> P) {
    let config = BackendConfig::default();
    println!(
        "{}",
        serde_json::json!({"provider": name,
        "os": std::env::consts::OS, "arch": std::env::consts::ARCH,
        "required_cargo_profile": "test", "rounds": ROUNDS,
        "operations_per_lane": OPERATIONS, "warmup_per_lane": WARMUP,
            "runtime_workers": 2, "max_writes": config.max_writes, "max_recoveries": config.max_recoveries,
            "max_write_workers": config.max_write_workers, "max_recovery_workers": config.max_recovery_workers,
            "max_retained_bytes": config.max_retained_bytes, "sequencing": "application-per-home-mutex",
        "block_publication": "synthetic-ack-metadata-only"})
    );
    for round in 0..ROUNDS {
        for workers in [1, 4] {
            // Alternate order to reduce systematic cache/thermal order bias.
            for hot in if round % 2 == 0 {
                [true, false]
            } else {
                [false, true]
            } {
                scenario(name, &factory, round, workers, hot).await;
            }
        }
    }
}

#[cfg(feature = "turso")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "native performance measurement; run alone with --ignored --nocapture"]
async fn turso_measurement() {
    suite("turso", |path| {
        mrr_data_backend::providers::TursoProvider::new(path, tokio::runtime::Handle::current())
    })
    .await;
}

#[cfg(feature = "duckdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "native performance measurement; run alone with --ignored --nocapture"]
async fn duckdb_measurement() {
    suite("duckdb", mrr_data_backend::providers::DuckDbProvider::new).await;
}
