//! Historical recovery under stalled native writes and a two-thread Host pool.
#![cfg(any(feature = "turso", feature = "duckdb"))]
#[path = "../support/held_provider.rs"]
mod held_provider;
use held_provider::{Gate, Held};
use mrr_data_backend::{Backend, BackendConfig, Lifecycle, MetadataProvider, ProfilePort};
use mrr_data_content::{
    CacheAdmission, ConditionalContentCommitOutcome, ConditionalContentCommitPort,
    ConditionalContentWrite, ContentBlock, ContentCodec, ContentRevision, PublishReceipt,
};
use std::{
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::task::JoinSet;

const READS: usize = 32;
const ROUNDS: usize = 3;

fn write(scope: &str) -> ConditionalContentWrite<'_> {
    ConditionalContentWrite {
        scope,
        operation_id: scope,
        expected: None,
        replacement: ContentBlock::new(ContentCodec::Raw, b"mixed-metadata").cid(),
    }
}
async fn commit(port: &ProfilePort, scope: &str) -> ContentRevision {
    let w = write(scope);
    let ack = PublishReceipt {
        cid: w.replacement,
        cache: CacheAdmission::Stored,
    };
    let ConditionalContentCommitOutcome::Committed(receipt) = port
        .commit(w, Some(&ack), |_| Ok::<_, ()>(()))
        .await
        .unwrap()
    else {
        panic!("fresh operation replayed");
    };
    receipt.committed
}
async fn wait_for(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}
fn closed(backend: &Backend, completed: usize) {
    let status = backend.status();
    assert_eq!(status.lifecycle, Lifecycle::Closed);
    assert_eq!(status.active_writes, 0);
    assert_eq!(status.active_recoveries, 0);
    assert_eq!(status.blocking_writes, 0);
    assert_eq!(status.blocking_recoveries, 0);
    assert_eq!(status.retained_bytes, 0);
    assert_eq!(status.completed, u64::try_from(completed).unwrap());
}
async fn recover_lane(port: ProfilePort, expected: ContentRevision) -> Vec<u128> {
    let seed = write("seed");
    let mut samples = Vec::new();
    for _ in 0..READS {
        let start = Instant::now();
        let receipt = port.recover(seed).await.unwrap().unwrap();
        assert_eq!(receipt.committed, expected);
        samples.push(start.elapsed().as_nanos());
    }
    samples
}
async fn recovery(
    port: &ProfilePort,
    expected: ContentRevision,
    lanes: usize,
) -> (u128, Vec<u128>) {
    let mut jobs = JoinSet::new();
    let start = Instant::now();
    for _ in 0..lanes {
        jobs.spawn(recover_lane(port.clone(), expected));
    }
    let mut samples = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(job) = jobs.join_next().await {
            samples.extend(job.unwrap());
        }
    })
    .await
    .expect("recovery starved behind queued native writes");
    (start.elapsed().as_nanos(), samples)
}

async fn scenario<P: MetadataProvider>(
    name: &str,
    factory: &impl Fn(PathBuf) -> P,
    round: usize,
    lanes: usize,
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mixed.db");
    let gate = Gate(Arc::new((Mutex::new(false), Condvar::new())));
    let entered = Arc::new(AtomicBool::new(false));
    let backend = Backend::open(
        BackendConfig::default(),
        Held {
            native: factory(path.clone()),
            gate: gate.0.clone(),
            entered: entered.clone(),
        },
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let port = backend.profile("mixed.v1", "tenant").unwrap();
    let seed = commit(&port, "seed").await;
    let mut ledger = vec![("seed".to_owned(), seed)];
    let mut writes = JoinSet::new();
    let held_port = port.clone();
    writes.spawn(async move { ("held".to_owned(), commit(&held_port, "held").await) });
    wait_for(|| entered.load(Ordering::Acquire)).await;
    for id in 0..7 {
        let port = port.clone();
        writes.spawn(async move {
            let scope = format!("queued-{id}");
            let revision = commit(&port, &scope).await;
            (scope, revision)
        });
    }
    wait_for(|| backend.status().active_writes == 8).await;
    assert_eq!(backend.status().blocking_writes, 1);
    let (elapsed_ns, mut samples) = recovery(&port, seed, lanes).await;
    assert!(!*gate.0.0.lock().unwrap());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), backend.shutdown())
            .await
            .is_err()
    );
    assert_eq!(backend.status().lifecycle, Lifecycle::Draining);
    let drain = Instant::now();
    gate.release();
    while let Some(job) = writes.join_next().await {
        ledger.push(job.unwrap());
    }
    backend.shutdown().await.unwrap();
    backend.shutdown().await.unwrap();
    let drain_ns = drain.elapsed().as_nanos();
    closed(&backend, ledger.len() + samples.len());
    let reopened = Backend::open(
        BackendConfig::default(),
        factory(path),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let port = reopened.profile("mixed.v1", "tenant").unwrap();
    for (scope, expected) in &ledger {
        assert_eq!(
            port.recover(write(scope)).await.unwrap().unwrap().committed,
            *expected
        );
    }
    reopened.shutdown().await.unwrap();
    closed(&reopened, ledger.len());
    samples.sort_unstable();
    let percentile = |p: usize| samples[(samples.len() * p).div_ceil(100) - 1];
    println!(
        "{}",
        serde_json::json!({"provider": name, "scenario": "stalled-writer",
        "round": round, "recovery_lanes": lanes, "samples": samples.len(), "elapsed_ns": elapsed_ns,
        "operations_per_second": u128::try_from(samples.len()).unwrap() * 1_000_000_000 / elapsed_ns,
        "p50_ns": percentile(50), "p95_ns": percentile(95), "p99_ns": percentile(99),
        "accepted_writes_while_stalled": 8, "submitted_writes_while_stalled": 1,
        "released_stall_to_close_ns": drain_ns, "commits": ledger.len(),
        "exact_recoveries_after_reopen": ledger.len(), "errors": 0})
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
        let config = BackendConfig::default();
        println!("{}", serde_json::json!({"provider": name, "runtime_workers": 2,
            "host_max_blocking_threads": 2, "max_write_workers": config.max_write_workers,
            "max_recovery_workers": config.max_recovery_workers, "max_writes": config.max_writes,
            "max_recoveries": config.max_recoveries, "max_retained_bytes": config.max_retained_bytes,
            "required_cargo_profile": "test", "rounds": ROUNDS, "reads_per_lane": READS,
            "warmup_recoveries": 0, "block_publication": "synthetic-ack-metadata-only"}));
        for round in 0..ROUNDS {
            for lanes in if round % 2 == 0 { [1, 4, 8] } else { [8, 4, 1] } {
                scenario(name, &factory, round, lanes).await;
            }
        }
    });
}
#[cfg(feature = "turso")]
#[test]
#[ignore = "native mixed-load measurement; run alone with --ignored --nocapture"]
fn turso_mixed_measurement() {
    suite("turso", |path| {
        mrr_data_backend::providers::TursoProvider::new(path, tokio::runtime::Handle::current())
    });
}
#[cfg(feature = "duckdb")]
#[test]
#[ignore = "native mixed-load measurement; run alone with --ignored --nocapture"]
fn duckdb_mixed_measurement() {
    suite("duckdb", mrr_data_backend::providers::DuckDbProvider::new);
}
