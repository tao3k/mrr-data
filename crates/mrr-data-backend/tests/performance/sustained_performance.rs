//! Finite continuous producers: successful latency and overload refusal are separate.
#![cfg(any(feature = "turso", feature = "duckdb"))]
use mrr_data_backend::{
    Backend, BackendConfig, BackendError, Lifecycle, MetadataProvider, ProfilePort,
};
use mrr_data_content::{
    CacheAdmission, ConditionalCommitPortError, ConditionalContentCommitOutcome,
    ConditionalContentCommitPort, ConditionalContentWrite, ContentBlock, ContentCodec,
    ContentRevision, PublishReceipt,
};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{sync::Barrier, task::JoinSet};

const WRITERS: usize = 8;
const READERS: usize = 4;
const ATTEMPTS: usize = 64;
const MIN_READS: usize = 64;
const MAX_READS: usize = 8192;

fn write(scope: &str) -> ConditionalContentWrite<'_> {
    ConditionalContentWrite {
        scope,
        operation_id: scope,
        expected: None,
        replacement: ContentBlock::new(ContentCodec::Raw, b"sustained-metadata").cid(),
    }
}
fn ack(w: ConditionalContentWrite<'_>) -> PublishReceipt {
    PublishReceipt {
        cid: w.replacement,
        cache: CacheAdmission::Stored,
    }
}
#[derive(Default)]
struct Samples {
    committed: Vec<(String, ContentRevision)>,
    refused: Vec<String>,
    success_ns: Vec<u128>,
    refusal_ns: Vec<u128>,
    reads_ns: Vec<u128>,
    reads_during_writes: usize,
}
struct Load {
    start: Barrier,
    remaining: AtomicUsize,
}
async fn writer(port: ProfilePort, load: Arc<Load>, lane: usize) -> Samples {
    let mut samples = Samples::default();
    load.start.wait().await;
    for index in 0..ATTEMPTS {
        let scope = format!("lane-{lane}-operation-{index}");
        let w = write(&scope);
        let physical = ack(w);
        let start = Instant::now();
        match port.commit(w, Some(&physical), |_| Ok::<_, ()>(())).await {
            Ok(ConditionalContentCommitOutcome::Committed(receipt)) => {
                let revision = receipt.committed;
                samples.success_ns.push(start.elapsed().as_nanos());
                samples.committed.push((scope, revision));
            }
            Err(ConditionalCommitPortError::BeforeCommit(BackendError::Saturated)) => {
                samples.refusal_ns.push(start.elapsed().as_nanos());
                samples.refused.push(scope);
            }
            result => panic!("unexpected write result: {result:?}"),
        }
        tokio::task::yield_now().await;
    }
    load.remaining.fetch_sub(1, Ordering::AcqRel);
    samples
}
async fn reader(port: ProfilePort, load: Arc<Load>, expected: ContentRevision) -> Samples {
    let mut samples = Samples::default();
    load.start.wait().await;
    while load.remaining.load(Ordering::Acquire) != 0 || samples.reads_ns.len() < MIN_READS {
        assert!(
            samples.reads_ns.len() < MAX_READS,
            "writer progress exhausted read ceiling"
        );
        let during = load.remaining.load(Ordering::Acquire) != 0;
        let start = Instant::now();
        assert_eq!(
            port.recover(write("seed"))
                .await
                .unwrap()
                .unwrap()
                .committed,
            expected
        );
        samples.reads_ns.push(start.elapsed().as_nanos());
        samples.reads_during_writes += usize::from(during);
        tokio::task::yield_now().await;
    }
    samples
}
fn distribution(samples: &mut [u128]) -> serde_json::Value {
    samples.sort_unstable();
    if samples.is_empty() {
        return serde_json::json!({"samples": 0});
    }
    let percentile = |p: usize| samples[(samples.len() * p).div_ceil(100) - 1];
    serde_json::json!({"samples": samples.len(), "p50_ns": percentile(50),
        "p95_ns": percentile(95), "p99_ns": percentile(99)})
}
fn closed(backend: &Backend, completed: usize) {
    let s = backend.status();
    assert_eq!(s.lifecycle, Lifecycle::Closed);
    assert_eq!((s.active_writes, s.active_recoveries), (0, 0));
    assert_eq!((s.blocking_writes, s.blocking_recoveries), (0, 0));
    assert_eq!(s.retained_bytes, 0);
    assert_eq!(s.completed, u64::try_from(completed).unwrap());
}
async fn collect(jobs: &mut JoinSet<Samples>) -> Samples {
    let mut total = Samples::default();
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(job) = jobs.join_next().await {
            let s = job.unwrap();
            total.committed.extend(s.committed);
            total.refused.extend(s.refused);
            total.success_ns.extend(s.success_ns);
            total.refusal_ns.extend(s.refusal_ns);
            total.reads_ns.extend(s.reads_ns);
            total.reads_during_writes += s.reads_during_writes;
        }
    })
    .await
    .expect("continuous producers did not finish within 30s");
    total
}
async fn verify_reopen<P: MetadataProvider>(
    factory: &impl Fn(PathBuf) -> P,
    path: PathBuf,
    ledger: &[(String, ContentRevision)],
    refused: &[String],
) {
    let backend = Backend::open(
        BackendConfig::default(),
        factory(path),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let port = backend.profile("sustained.v1", "tenant").unwrap();
    for (scope, expected) in ledger {
        assert_eq!(
            port.recover(write(scope)).await.unwrap().unwrap().committed,
            *expected
        );
    }
    for scope in refused {
        assert!(port.recover(write(scope)).await.unwrap().is_none());
    }
    backend.shutdown().await.unwrap();
    closed(&backend, ledger.len() + refused.len());
}
async fn scenario<P: MetadataProvider>(name: &str, factory: &impl Fn(PathBuf) -> P, round: usize) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sustained.db");
    let backend = Backend::open(
        BackendConfig {
            max_writes: 4,
            max_recoveries: READERS,
            ..BackendConfig::default()
        },
        factory(path.clone()),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let port = backend.profile("sustained.v1", "tenant").unwrap();
    let seed = write("seed");
    let physical = ack(seed);
    let ConditionalContentCommitOutcome::Committed(seed) = port
        .commit(seed, Some(&physical), |_| Ok::<_, ()>(()))
        .await
        .unwrap()
    else {
        panic!("fresh seed replayed");
    };
    let mut ledger = vec![("seed".into(), seed.committed)];
    let load = Arc::new(Load {
        start: Barrier::new(WRITERS + READERS),
        remaining: AtomicUsize::new(WRITERS),
    });
    let mut jobs = JoinSet::new();
    let start = Instant::now();
    for lane in 0..WRITERS {
        jobs.spawn(writer(port.clone(), load.clone(), lane));
    }
    for _ in 0..READERS {
        jobs.spawn(reader(port.clone(), load.clone(), seed.committed));
    }
    let mut total = collect(&mut jobs).await;
    let elapsed_ns = start.elapsed().as_nanos();
    assert_eq!(
        total.success_ns.len() + total.refusal_ns.len(),
        WRITERS * ATTEMPTS
    );
    assert_ne!(total.success_ns, [] as [u128; 0]);
    assert!(total.reads_during_writes > 0);
    assert_eq!(
        backend.status().saturated_writes,
        u64::try_from(total.refusal_ns.len()).unwrap()
    );
    assert_eq!(backend.status().saturated_recoveries, 0);
    ledger.extend(total.committed);
    let drain = Instant::now();
    backend.shutdown().await.unwrap();
    let quiescent_close_ns = drain.elapsed().as_nanos();
    closed(&backend, ledger.len() + total.reads_ns.len());
    verify_reopen(factory, path, &ledger, &total.refused).await;
    println!(
        "{}",
        serde_json::json!({"provider": name, "scenario": "continuous-producers",
        "round": round, "elapsed_ns": elapsed_ns, "write_attempts": WRITERS * ATTEMPTS,
        "committed_writes": total.success_ns.len(), "saturated_writes": total.refusal_ns.len(),
        "committed_writes_per_second": u128::try_from(total.success_ns.len()).unwrap() * 1_000_000_000 / elapsed_ns,
        "recoveries_per_second": u128::try_from(total.reads_ns.len()).unwrap() * 1_000_000_000 / elapsed_ns,
        "successful_commit_latency": distribution(&mut total.success_ns),
        "saturation_refusal_latency": distribution(&mut total.refusal_ns),
        "recovery_latency": distribution(&mut total.reads_ns),
        "recoveries_started_while_producers_active": total.reads_during_writes,
        "quiescent_close_ns": quiescent_close_ns, "exact_recoveries_after_reopen": ledger.len(),
        "absent_refused_operations_after_reopen": total.refused.len(),
        "unexpected_errors": 0, "unknown_outcomes": 0, "replays": 0})
    );
}
fn suite<P: MetadataProvider>(name: &str, factory: impl Fn(PathBuf) -> P) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        println!(
            "{}",
            serde_json::json!({"provider": name, "host_max_blocking_threads": 2,
            "runtime_workers": 2, "max_writes": 4, "max_recoveries": READERS,
            "max_write_workers": 1, "max_recovery_workers": 1, "writer_lanes": WRITERS,
            "reader_lanes": READERS, "attempts_per_writer": ATTEMPTS, "rounds": 3,
            "required_cargo_profile": "test", "block_publication": "synthetic-ack-metadata-only",
            "retries": 0, "minimum_reads_per_lane": MIN_READS, "maximum_reads_per_lane": MAX_READS})
        );
        for round in 0..3 {
            scenario(name, &factory, round).await;
        }
    });
}
#[cfg(feature = "turso")]
#[test]
#[ignore = "native continuous-load measurement; run alone with --ignored --nocapture"]
fn turso_sustained_measurement() {
    suite("turso", |path| {
        mrr_data_backend::providers::TursoProvider::new(path, tokio::runtime::Handle::current())
    });
}
#[cfg(feature = "duckdb")]
#[test]
#[ignore = "native continuous-load measurement; run alone with --ignored --nocapture"]
fn duckdb_sustained_measurement() {
    suite("duckdb", mrr_data_backend::providers::DuckDbProvider::new);
}
