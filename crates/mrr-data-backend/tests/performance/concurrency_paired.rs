//! Paired native metadata scheduling benchmark. No implicit retry or new home.
use mrr_data_backend::{Backend, BackendConfig, ProfilePort, providers::TursoProvider};
use mrr_data_content::{
    CacheAdmission, ConditionalCommitPortError as PortError,
    ConditionalContentCommitOutcome as Outcome, ConditionalContentCommitPort,
    ConditionalContentWrite, ContentBlock, ContentCodec, ContentRevision, PublishReceipt,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Barrier, Mutex};
const ATTEMPTS: usize = 64;
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
struct Samples {
    latency: Vec<u128>,
    records: Vec<Record>,
    failures: usize,
    unknowns: usize,
    refusals: usize,
}
async fn lane(
    port: ProfilePort,
    lane: usize,
    hot: bool,
    head: Arc<Mutex<Option<ContentRevision>>>,
    start: Arc<Barrier>,
    hold: Duration,
) -> Samples {
    let mut samples = Samples {
        latency: vec![],
        records: vec![],
        failures: 0,
        unknowns: 0,
        refusals: 0,
    };
    let scope = format!("canonical-home-{}", if hot { 0 } else { lane });
    let root = ContentBlock::new(
        ContentCodec::Raw,
        b"original-reservation-and-unknown-exposure-reference",
    )
    .cid();
    let physical = PublishReceipt {
        cid: root,
        cache: CacheAdmission::Stored,
    };
    start.wait().await;
    for n in 0..ATTEMPTS {
        let operation = format!("lane-{lane}-op-{n}");
        let begin = Instant::now();
        let mut current = head.lock().await;
        let expected = *current;
        let w = ConditionalContentWrite {
            scope: &scope,
            operation_id: &operation,
            expected,
            replacement: root,
        };
        let result = port
            .commit(w, Some(&physical), |_| {
                std::thread::sleep(hold);
                Ok::<_, ()>(())
            })
            .await;
        match result {
            Ok(Outcome::Committed(r)) => {
                let committed = r.committed;
                *current = Some(committed);
                samples.latency.push(begin.elapsed().as_nanos());
                samples.records.push(Record {
                    scope: scope.clone(),
                    operation,
                    expected,
                    committed,
                });
            }
            Err(PortError::Unknown(_)) => {
                samples.unknowns += 1;
                samples.failures += 1;
                break;
            }
            Err(_) => {
                samples.refusals += 1;
                samples.failures += 1;
                break;
            }
            Ok(Outcome::Replayed(_)) => {
                samples.failures += 1;
                break;
            }
        }
    }
    samples
}
fn config(cpus: usize) -> BackendConfig {
    BackendConfig {
        max_writes: cpus,
        max_write_workers: cpus,
        max_shared_workers: cpus,
        max_recoveries: 4,
        max_recovery_workers: 4,
        max_retained_bytes: (cpus + 4) * 65536,
        ..BackendConfig::default()
    }
}
async fn open(path: &std::path::Path, cpus: usize, concurrent: bool) -> Backend {
    let provider = TursoProvider::new(path.into(), tokio::runtime::Handle::current());
    let provider = if concurrent {
        provider.with_concurrent_writes()
    } else {
        provider
    };
    Backend::open(config(cpus), provider, tokio::runtime::Handle::current())
        .await
        .unwrap()
}
async fn case(cpus: usize, concurrent: bool, hot: bool, hold: Duration, pair: usize, order: usize) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("paired.db");
    let backend = open(&path, cpus, concurrent).await;
    let shared = Arc::new(Mutex::new(None));
    let barrier = Arc::new(Barrier::new(cpus + 1));
    let mut jobs = tokio::task::JoinSet::new();
    for i in 0..cpus {
        let head = if hot {
            shared.clone()
        } else {
            Arc::new(Mutex::new(None))
        };
        jobs.spawn(lane(
            backend.profile("gateway.v1", "original").unwrap(),
            i,
            hot,
            head,
            barrier.clone(),
            hold,
        ));
    }
    let start = Instant::now();
    barrier.wait().await;
    let collected = tokio::time::timeout(Duration::from_secs(30), async {
        let mut results = vec![];
        while let Some(result) = jobs.join_next().await {
            results.push(result.unwrap());
        }
        results
    })
    .await;
    // A failed fixed bound still drains every accepted job before shutdown.
    while jobs.join_next().await.is_some() {}
    let elapsed = start.elapsed();
    let close = Instant::now();
    backend.shutdown().await.unwrap();
    let close_ns = close.elapsed().as_nanos();
    let results = collected.expect("unchanged 30-second collection bound");
    let failures: usize = results.iter().map(|s| s.failures).sum();
    let unknowns: usize = results.iter().map(|s| s.unknowns).sum();
    let refusals: usize = results.iter().map(|s| s.refusals).sum();
    let mut latency: Vec<_> = results
        .iter()
        .flat_map(|s| s.latency.iter().copied())
        .collect();
    latency.sort_unstable();
    let records: Vec<_> = results.into_iter().flat_map(|s| s.records).collect();
    let reopened = open(&path, cpus, concurrent).await;
    let port = reopened.profile("gateway.v1", "original").unwrap();
    for record in &records {
        assert_eq!(
            port.recover(record.write())
                .await
                .unwrap()
                .map(|r| r.committed),
            Some(record.committed)
        );
    }
    reopened.shutdown().await.unwrap();
    let percentile = |p: usize| {
        latency
            .get((latency.len() * p).div_ceil(100).saturating_sub(1))
            .copied()
    };
    let successes = u32::try_from(records.len()).expect("bounded benchmark operation count");
    println!(
        "{}",
        serde_json::json!({"pair": pair, "order": order, "concurrent": concurrent,
        "hot_home": hot, "host_cpus": cpus, "workers": cpus, "validation_hold_ns": hold.as_nanos(),
        "attempts_per_lane": ATTEMPTS, "successes": records.len(), "failures": failures,
        "unknowns": unknowns, "refusals": refusals,
        "elapsed_ns": elapsed.as_nanos(), "successes_per_second": f64::from(successes) / elapsed.as_secs_f64(),
        "p50_ns": percentile(50), "p95_ns": percentile(95), "p99_ns": percentile(99),
        "exact_recoveries": records.len(), "close_ns": close_ns, "retries": 0,
        "content_ack": "synthetic-metadata-only", "synchronous": "FULL"})
    );
    assert_eq!(
        failures, 0,
        "paired workload cannot hide failed/unknown attempts"
    );
    assert_eq!(records.len(), cpus * ATTEMPTS);
}
#[test]
#[ignore = "run alone; real paired native metadata benchmark, not an LLM throughput claim"]
fn turso_paired_concurrency() {
    let cpus = std::thread::available_parallelism().unwrap().get();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cpus)
        .max_blocking_threads(cpus + 4)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for hot in [false, true] {
            for hold in [Duration::ZERO, Duration::from_millis(1)] {
                for pair in 0..3 {
                    let modes = match pair % 2 {
                        0 => [false, true],
                        _ => [true, false],
                    };
                    for (order, concurrent) in modes.into_iter().enumerate() {
                        case(cpus, concurrent, hot, hold, pair, order).await;
                    }
                }
            }
        }
    });
}
