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
#[path = "../support/bound_obligation.rs"]
mod bound_obligation;
#[path = "../support/storage_timing.rs"]
mod storage_timing;
use bound_obligation::BoundObligation;
use storage_timing::{PhaseSample, TimedStorage, Timings};
#[derive(Clone, Copy, PartialEq)]
enum WriterMode {
    Wal,
    Fresh,
    Reuse,
}
impl WriterMode {
    fn name(self) -> &'static str {
        match self {
            Self::Wal => "wal",
            Self::Fresh => "fresh-mvcc",
            Self::Reuse => "reused-mvcc",
        }
    }
}
#[derive(Clone)]
enum Validation {
    Hold(Duration),
    Bound(Arc<BoundObligation>),
}
struct Record {
    scope: String,
    operation: String,
    expected: Option<ContentRevision>,
    committed: ContentRevision,
    guards: Vec<mrr_data_backend::AuthorityExpectation>,
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
    validation: Validation,
) -> Samples {
    let mut samples = Samples {
        latency: vec![],
        records: vec![],
        failures: 0,
        unknowns: 0,
        refusals: 0,
    };
    let scope = format!("canonical-home-{}", if hot { 0 } else { lane });
    let root = match &validation {
        Validation::Bound(bound) => bound.root(),
        Validation::Hold(_) => ContentBlock::new(
            ContentCodec::Raw,
            b"original-reservation-and-unknown-exposure-reference",
        )
        .cid(),
    };
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
            .commit(w, Some(&physical), |current| match &validation {
                Validation::Hold(hold) => {
                    std::thread::sleep(*hold);
                    Ok(())
                }
                Validation::Bound(bound) => bound.validate(current, expected),
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
                    guards: match &validation {
                        Validation::Bound(bound) => bound.guards().to_vec(),
                        Validation::Hold(_) => vec![],
                    },
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
async fn open(path: &std::path::Path, cpus: usize, mode: WriterMode) -> (Backend, Arc<Timings>) {
    let provider = TursoProvider::new(path.into(), tokio::runtime::Handle::current());
    let provider = match mode {
        WriterMode::Wal => provider,
        WriterMode::Fresh => provider.with_concurrent_writes(),
        WriterMode::Reuse => provider.with_reusable_concurrent_writes(cpus).unwrap(),
    };
    let timings = Arc::new(Timings::default());
    let provider = TimedStorage {
        provider,
        timings: timings.clone(),
    };
    let backend = Backend::open(config(cpus), provider, tokio::runtime::Handle::current())
        .await
        .unwrap();
    (backend, timings)
}
#[derive(Clone, Copy)]
struct Case {
    cpus: usize,
    mode: WriterMode,
    hot: bool,
    hold: Option<Duration>,
    pair: usize,
    order: usize,
}
async fn prepare(
    backend: &Backend,
    path: &std::path::Path,
    cpus: usize,
    hot: bool,
    hold: Option<Duration>,
) -> Vec<(ProfilePort, Validation)> {
    let mut prepared = Vec::new();
    for i in 0..cpus {
        let port = backend.profile("gateway.v1", "original").unwrap();
        let (port, validation) = if let Some(hold) = hold {
            (port, Validation::Hold(hold))
        } else {
            let scope = format!("canonical-home-{}", if hot { 0 } else { i });
            let (port, body) = BoundObligation::prepare(port, path, &scope).await;
            (port, Validation::Bound(body))
        };
        prepared.push((port, validation));
    }
    prepared
}
async fn case(config: Case) {
    let Case {
        cpus,
        mode,
        hot,
        hold,
        ..
    } = config;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("paired.db");
    let (backend, timings) = open(&path, cpus, mode).await;
    let shared = Arc::new(Mutex::new(None));
    let barrier = Arc::new(Barrier::new(cpus + 1));
    let mut jobs = tokio::task::JoinSet::new();
    let prepared = prepare(&backend, &dir.path().join("bodies"), cpus, hot, hold).await;
    // Complete authority publication/body preparation before starting any lane.
    for (i, (port, validation)) in prepared.iter().enumerate() {
        let head = if hot {
            shared.clone()
        } else {
            Arc::new(Mutex::new(None))
        };
        jobs.spawn(lane(
            port.clone(),
            i,
            hot,
            head,
            barrier.clone(),
            validation.clone(),
        ));
    }
    timings.reset();
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
    let phases = timings.snapshot();
    let close = Instant::now();
    backend.shutdown().await.unwrap();
    let close_ns = close.elapsed().as_nanos();
    let results = collected.expect("unchanged 30-second collection bound");
    let records: Vec<_> = results.iter().flat_map(|s| &s.records).collect();
    let (reopened, _) = open(&path, cpus, mode).await;
    let port = reopened.profile("gateway.v1", "original").unwrap();
    for record in &records {
        assert_eq!(
            port.with_authorities(&record.guards)
                .unwrap()
                .recover(record.write())
                .await
                .unwrap()
                .map(|r| r.committed),
            Some(record.committed)
        );
    }
    for (_, validation) in &prepared {
        if let Validation::Bound(bound) = validation {
            bound.validate(None, None).unwrap();
        }
    }
    reopened.shutdown().await.unwrap();
    report(config, &results, elapsed, close_ns, phases);
}
fn report(
    config: Case,
    results: &[Samples],
    elapsed: Duration,
    close_ns: u128,
    phases: PhaseSample,
) {
    let Case {
        cpus,
        mode,
        hot,
        hold,
        pair,
        order,
    } = config;
    let failures: usize = results.iter().map(|s| s.failures).sum();
    let unknowns: usize = results.iter().map(|s| s.unknowns).sum();
    let refusals: usize = results.iter().map(|s| s.refusals).sum();
    let mut latency: Vec<_> = results.iter().flat_map(|s| &s.latency).copied().collect();
    latency.sort_unstable();
    let records: Vec<_> = results.iter().flat_map(|s| &s.records).collect();
    let percentile = |p: usize| {
        latency
            .get((latency.len() * p).div_ceil(100).saturating_sub(1))
            .copied()
    };
    let successes = u32::try_from(records.len()).expect("bounded benchmark operation count");
    println!(
        "{}",
        serde_json::json!({"pair": pair, "order": order, "concurrent": mode != WriterMode::Wal, "writer_mode": mode.name(),
        "hot_home": hot, "host_cpus": cpus, "workers": cpus, "validation_hold_ns": hold.map(|d| d.as_nanos()),
        "validation": if hold.is_some() { "simulated-hold" } else { "verified-filesystem-obligation-and-authority" },
        "attempts_per_lane": ATTEMPTS, "successes": records.len(), "failures": failures,
        "unknowns": unknowns, "refusals": refusals,
        "elapsed_ns": elapsed.as_nanos(), "successes_per_second": f64::from(successes) / elapsed.as_secs_f64(),
        "p50_ns": percentile(50), "p95_ns": percentile(95), "p99_ns": percentile(99),
        "exact_recoveries": records.len(), "close_ns": close_ns, "retries": 0,
        "storage_phases": phases,
        "content_ack": if hold.is_some() { "synthetic-metadata-only" } else { "durable-local-body-before-measurement" }, "synchronous": "FULL"})
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
                        let mode = if concurrent {
                            WriterMode::Fresh
                        } else {
                            WriterMode::Wal
                        };
                        case(Case {
                            cpus,
                            mode,
                            hot,
                            hold: Some(hold),
                            pair,
                            order,
                        })
                        .await;
                    }
                }
            }
        }
    });
}

#[test]
#[ignore = "run alone; real bounded content verification and native authority checks"]
fn turso_paired_writer_reuse() {
    let cpus = std::thread::available_parallelism().unwrap().get();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cpus)
        .max_blocking_threads(cpus + 4)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for hot in [false, true] {
            for pair in 0..3 {
                let mut modes = [WriterMode::Wal, WriterMode::Fresh, WriterMode::Reuse];
                modes.rotate_left(pair);
                for (order, mode) in modes.into_iter().enumerate() {
                    case(Case {
                        cpus,
                        mode,
                        hot,
                        hold: None,
                        pair,
                        order,
                    })
                    .await;
                }
            }
        }
    });
}
