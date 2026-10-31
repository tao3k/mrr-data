//! Component costs mirror the public driver operations used by `TursoProvider`.
use mrr_data_backend::turso_driver::Builder;
use std::time::{Duration, Instant};
const SAMPLES: usize = 512;
fn summary(mut samples: Vec<u128>) -> serde_json::Value {
    samples.sort_unstable();
    serde_json::json!({"samples": samples.len(), "p50_ns": samples[samples.len()/2],
        "p95_ns": samples[(samples.len()*95).div_ceil(100)-1],
        "mean_ns": samples.iter().sum::<u128>() / u128::try_from(samples.len()).unwrap()})
}

fn transaction_sample(runtime: &tokio::runtime::Runtime, reuse: bool, round: usize) {
    let dir = tempfile::tempdir().unwrap();
    let db = runtime
        .block_on(Builder::new_local(dir.path().join("phases.db").to_str().unwrap()).build())
        .unwrap();
    let init = db.connect().unwrap();
    runtime.block_on(async {
        init.pragma_update("journal_mode", "'mvcc'").await.unwrap();
        init.execute(
            "CREATE TABLE setup_probe (id INTEGER PRIMARY KEY, value INTEGER)",
            (),
        )
        .await
        .unwrap();
        init.execute("INSERT INTO setup_probe VALUES (1,0)", ())
            .await
            .unwrap();
    });
    let mut idle = None;
    let mut setup = Vec::new();
    let mut begin = Vec::new();
    let mut read = Vec::new();
    let mut write = Vec::new();
    let mut commit = Vec::new();
    let mut release = Vec::new();
    for _ in 0..SAMPLES {
        let start = Instant::now();
        let conn = if let Some(conn) = idle.take() {
            conn
        } else {
            let conn = db.connect().unwrap();
            conn.busy_timeout(Duration::from_millis(250)).unwrap();
            runtime.block_on(async {
                conn.execute("PRAGMA synchronous=FULL", ()).await.unwrap();
                let mut rows = conn.query("PRAGMA synchronous", ()).await.unwrap();
                assert_eq!(
                    rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
                    2
                );
            });
            conn
        };
        setup.push(start.elapsed().as_nanos());
        let start = Instant::now();
        runtime
            .block_on(conn.execute("BEGIN CONCURRENT", ()))
            .unwrap();
        begin.push(start.elapsed().as_nanos());
        let start = Instant::now();
        runtime.block_on(async {
            let mut rows = conn
                .query("SELECT value FROM setup_probe WHERE id=1", ())
                .await
                .unwrap();
            assert!(rows.next().await.unwrap().is_some());
        });
        read.push(start.elapsed().as_nanos());
        let start = Instant::now();
        runtime
            .block_on(conn.execute("UPDATE setup_probe SET value=value+1 WHERE id=1", ()))
            .unwrap();
        write.push(start.elapsed().as_nanos());
        let start = Instant::now();
        runtime.block_on(conn.execute("COMMIT", ())).unwrap();
        commit.push(start.elapsed().as_nanos());
        assert!(conn.is_autocommit().unwrap());
        let start = Instant::now();
        if reuse {
            idle = Some(conn);
        } else {
            drop(conn);
        }
        release.push(start.elapsed().as_nanos());
    }
    println!(
        "{}",
        serde_json::json!({"round": round, "reused": reuse,
        "setup": summary(setup), "begin": summary(begin), "read": summary(read),
        "write": summary(write), "commit": summary(commit), "release": summary(release), "synchronous": "FULL"})
    );
}
#[test]
#[ignore = "run alone; sequential physical MVCC phase diagnosis with real FULL commits"]
fn turso_writer_transaction_warmup() {
    let cpus = std::thread::available_parallelism().unwrap().get().max(4);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cpus)
        .enable_all()
        .build()
        .unwrap();
    for round in 0..3 {
        let modes = match round % 2 {
            0 => [false, true],
            _ => [true, false],
        };
        for reuse in modes {
            transaction_sample(&runtime, reuse, round);
        }
    }
}
#[test]
#[ignore = "run alone; sequential setup component costs, not aggregate machine throughput"]
fn turso_writer_setup_costs() {
    let cpus = std::thread::available_parallelism().unwrap().get().max(4);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cpus)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = runtime
        .block_on(Builder::new_local(dir.path().join("setup.db").to_str().unwrap()).build())
        .unwrap();
    let control = db.connect().unwrap();
    runtime
        .block_on(control.pragma_update("journal_mode", "'mvcc'"))
        .unwrap();
    for round in 0..3 {
        let mut connect = Vec::new();
        let mut busy = Vec::new();
        let mut full = Vec::new();
        let mut drop_time = Vec::new();
        let mut autocommit = Vec::new();
        let mut cycle = Vec::new();
        for _ in 0..SAMPLES {
            let begin = Instant::now();
            let part = Instant::now();
            let conn = db.connect().unwrap();
            connect.push(part.elapsed().as_nanos());
            let part = Instant::now();
            conn.busy_timeout(Duration::from_millis(250)).unwrap();
            busy.push(part.elapsed().as_nanos());
            let part = Instant::now();
            runtime.block_on(async {
                conn.execute("PRAGMA synchronous=FULL", ()).await.unwrap();
                let mut rows = conn.query("PRAGMA synchronous", ()).await.unwrap();
                assert_eq!(
                    rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
                    2
                );
            });
            full.push(part.elapsed().as_nanos());
            let part = Instant::now();
            assert!(conn.is_autocommit().unwrap());
            autocommit.push(part.elapsed().as_nanos());
            let part = Instant::now();
            drop(conn);
            drop_time.push(part.elapsed().as_nanos());
            cycle.push(begin.elapsed().as_nanos());
        }
        println!(
            "{}",
            serde_json::json!({"round": round, "host_cpus": std::thread::available_parallelism().unwrap().get(),
            "connect": summary(connect), "busy_timeout": summary(busy), "full_set_and_verify": summary(full),
            "autocommit": summary(autocommit), "drop": summary(drop_time), "cycle": summary(cycle)})
        );
    }
}
