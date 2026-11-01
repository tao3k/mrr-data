//! Sequential public-engine control; not an alternate Host execution API.
use super::{BoundObligation, TimedDecisions, TimedStorage, Timings, WriterMode, open};
use mrr_data_backend::{MetadataProvider, StoredWrite, providers::TursoProvider};
use mrr_data_content::{CacheAdmission, ConditionalContentCommitPort, PublishReceipt};
use std::{sync::Arc, time::Instant};

#[test]
#[ignore = "run alone; sequential real metadata/validator control, not end-to-end throughput"]
fn turso_worker_local_validator_control() {
    let cpus = std::thread::available_parallelism().unwrap().get().max(4);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cpus)
        .max_blocking_threads(cpus + 4)
        .enable_all()
        .build()
        .unwrap();
    for round in 0..3 {
        let mut modes = [WriterMode::Wal, WriterMode::Fresh, WriterMode::Reuse];
        modes.rotate_left(round);
        for mode in modes {
            measure(&runtime, cpus, mode, round);
        }
    }
}
fn measure(runtime: &tokio::runtime::Runtime, cpus: usize, mode: WriterMode, round: usize) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("worker-control.db");
    let bound = runtime.block_on(async {
        let (backend, _) = open(&path, cpus, mode).await;
        let port = backend.profile("gateway.v1", "original").unwrap();
        let (_, bound) =
            BoundObligation::prepare(port, &dir.path().join("bodies"), "canonical-home-0").await;
        backend.shutdown().await.unwrap();
        bound
    });
    let provider = TursoProvider::new(path, runtime.handle().clone());
    let provider = match mode {
        WriterMode::Wal => provider,
        WriterMode::Fresh => provider.with_concurrent_writes(),
        WriterMode::Reuse => provider.with_reusable_concurrent_writes(cpus).unwrap(),
    };
    let timings = Arc::new(Timings::default());
    let provider = TimedDecisions(TimedStorage {
        provider,
        timings: timings.clone(),
    });
    provider.open().unwrap();
    let physical = PublishReceipt {
        cid: bound.root(),
        cache: CacheAdmission::Stored,
    };
    let mut prior = None;
    let mut records = Vec::new();
    let mut validation_ns = 0;
    let mut samples = Vec::new();
    timings.reset();
    let start = Instant::now();
    for attempt in 0..cpus * super::ATTEMPTS {
        let write = StoredWrite {
            profile: "gateway.v1".into(),
            namespace: "original".into(),
            scope: "canonical-home-0".into(),
            operation_id: format!("worker-control-{attempt}"),
            expected: prior,
            replacement: bound.root(),
            authorities: bound.guards().to_vec(),
        };
        let tick = Instant::now();
        let receipt = provider
            .commit(&write, Some(&physical), &mut |current| {
                let tick = Instant::now();
                let valid = bound.validate(current, prior.map(Into::into)).is_ok();
                validation_ns += tick.elapsed().as_nanos();
                valid
            })
            .unwrap();
        assert!(!receipt.replayed);
        prior = Some(receipt.committed);
        samples.push(tick.elapsed().as_nanos());
        records.push((write, receipt.committed));
    }
    let elapsed_ns = start.elapsed().as_nanos();
    let phases = timings.snapshot();
    phases.assert_balanced(records.len());
    provider.close().unwrap();
    provider.open().expect_err("old owner must remain terminal");
    // Reconstruct resource ownership; recover every original operation/guard.
    runtime.block_on(async {
        let (fresh, _) = open(&dir.path().join("worker-control.db"), cpus, mode).await;
        let port = fresh
            .profile("gateway.v1", "original")
            .unwrap()
            .with_authorities(bound.guards())
            .unwrap();
        for (write, committed) in &records {
            assert_eq!(
                port.recover(write.content_write())
                    .await
                    .unwrap()
                    .unwrap()
                    .committed,
                (*committed).into()
            );
        }
        bound.validate(None, None).unwrap();
        fresh.shutdown().await.unwrap();
    });
    println!(
        "{}",
        serde_json::json!({"control":"sequential-public-engine-owned-immutable-validator",
        "round":round,"writer_mode":mode.name(),"host_cpus":std::thread::available_parallelism().unwrap().get(),
        "runtime_workers":cpus,"active_transaction_lanes":1,"successes":records.len(),
        "exact_recoveries":records.len(),"elapsed_ns":elapsed_ns,"validation_ns":validation_ns,
        "request_ns":samples,"storage_phases":phases,"synchronous":"FULL","retries":0})
    );
}
