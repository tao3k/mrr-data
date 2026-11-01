//! Unit-test-only clocks inside the original provider transaction. No runtime API.
use super::connection::failure;
use crate::BackendError;
use std::{
    cell::Cell,
    future::{Future, poll_fn},
    pin::pin,
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    task::Poll,
    time::Instant,
};
use tokio::runtime::Handle;
#[path = "../performance/concurrency_paired.rs"]
mod workload;

const SQL: &str = "SELECT CASE WHEN length(value)<=65536 THEN value ELSE NULL END FROM mrr_backend_kv WHERE key=?1";
const MAX_READS: usize = 100_000;
static FORM: AtomicU8 = AtomicU8::new(0);
static SAMPLES: OnceLock<Vec<Mutex<Vec<ReadSample>>>> = OnceLock::new();
static GENERATION: AtomicU64 = AtomicU64::new(0);
static NEXT_SLOT: AtomicUsize = AtomicUsize::new(0);
static READS: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static LOCAL_SLOT: Cell<(u64, usize)> = const { Cell::new((0, usize::MAX)) };
}

#[derive(Default, Debug, serde::Serialize)]
struct Stage {
    wall_ns: u64,
    poll_ns: u64,
    pending_gap_ns: u64,
    polls: u64,
    pending: u64,
}
fn ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap()
}
async fn measured<F: Future>(future: F) -> (F::Output, Stage) {
    let mut future = pin!(future);
    let mut stage = Stage::default();
    let mut last_pending = None::<Instant>;
    let start = Instant::now();
    let output = poll_fn(|cx| {
        let tick = Instant::now();
        if let Some(end) = last_pending.take() {
            stage.pending_gap_ns += u64::try_from(tick.duration_since(end).as_nanos()).unwrap();
        }
        let result = future.as_mut().poll(cx);
        stage.poll_ns += ns(tick);
        stage.polls += 1;
        if result.is_pending() {
            stage.pending += 1;
            last_pending = Some(Instant::now());
        }
        result
    })
    .await;
    stage.wall_ns = ns(start);
    assert!(stage.poll_ns + stage.pending_gap_ns <= stage.wall_ns);
    assert_eq!(stage.polls, stage.pending + 1);
    (output, stage)
}
#[derive(Default, serde::Serialize)]
struct ReadSample {
    sequence: usize,
    split: bool,
    key_bytes: usize,
    value_bytes: usize,
    found: bool,
    ok: bool,
    total_ns: u64,
    observer_append_ns: u64,
    prepare: Stage,
    query: Stage,
    next: Stage,
    owned_value_ns: u64,
    cleanup_ns: u64,
}
impl ReadSample {
    fn assert_balanced(&self) {
        assert!(
            self.prepare.wall_ns
                + self.query.wall_ns
                + self.next.wall_ns
                + self.owned_value_ns
                + self.cleanup_ns
                <= self.total_ns
        );
    }
}
pub(crate) fn read_if_armed(
    conn: &turso::Connection,
    runtime: &Handle,
    key: &str,
) -> Option<Result<Option<Vec<u8>>, BackendError>> {
    let form = FORM.load(Ordering::Acquire);
    if form == 0 {
        return None;
    }
    let (result, mut sample) = profile_read(conn, runtime, key, form == 2);
    let append = Instant::now();
    LOCAL_SLOT.with(|local| {
        let generation = GENERATION.load(Ordering::Relaxed);
        let slot = match local.get() {
            (epoch, slot) if epoch == generation => slot,
            _ => {
                let slot = NEXT_SLOT.fetch_add(1, Ordering::Relaxed);
                local.set((generation, slot));
                slot
            }
        };
        sample.sequence = READS.fetch_add(1, Ordering::Relaxed);
        assert!(
            sample.sequence < MAX_READS,
            "bounded per-case read observer"
        );
        // Only this OS thread appends to its slot. The collector reads it after
        // all accepted jobs drain, so there is no cross-worker append lock.
        let mut samples = SAMPLES.get().unwrap()[slot].lock().unwrap();
        samples.push(sample);
        samples.last_mut().unwrap().observer_append_ns = ns(append);
    });
    Some(result)
}
fn profile_read(
    conn: &turso::Connection,
    runtime: &Handle,
    key: &str,
    split: bool,
) -> (Result<Option<Vec<u8>>, BackendError>, ReadSample) {
    let start = Instant::now();
    let mut sample = ReadSample {
        split,
        key_bytes: key.len(),
        ..ReadSample::default()
    };
    let result = runtime.block_on(async {
        let mut rows = if sample.split {
            // Provider uses explicit SQL BEGIN/COMMIT, never the driver's dangling
            // Transaction guard. Split is diagnostic, not an accepted replacement.
            let (stmt, stage) = measured(conn.prepare(SQL)).await;
            sample.prepare = stage;
            let mut stmt = stmt.map_err(failure)?;
            let (rows, stage) = measured(stmt.query([key])).await;
            sample.query = stage;
            rows.map_err(failure)?
        } else {
            let (rows, stage) = measured(conn.query(SQL, [key])).await;
            sample.query = stage;
            rows.map_err(failure)?
        };
        let (row, stage) = measured(rows.next()).await;
        sample.next = stage;
        let row = row.map_err(failure)?;
        let tick = Instant::now();
        let value = match &row {
            Some(row) => match row.get_value(0).map_err(failure)? {
                turso::Value::Blob(bytes) => {
                    sample.found = true;
                    sample.value_bytes = bytes.len();
                    Some(bytes)
                }
                _ => return Err(BackendError::Corrupt),
            },
            None => None,
        };
        sample.owned_value_ns = ns(tick);
        let tick = Instant::now();
        drop(row);
        drop(rows);
        sample.cleanup_ns = ns(tick);
        Ok(value)
    });
    sample.total_ns = ns(start);
    sample.ok = result.is_ok();
    sample.assert_balanced();
    (result, sample)
}
fn begin_direct() {
    arm(1);
}
fn begin_split() {
    arm(2);
}
fn arm(form: u8) {
    assert_eq!(FORM.load(Ordering::Acquire), 0);
    let cpus = std::thread::available_parallelism().unwrap().get().max(4);
    let capacity = cpus * workload::ATTEMPTS * 9;
    assert!(
        capacity <= MAX_READS,
        "observer does not shrink workers to fit"
    );
    let slots = SAMPLES.get_or_init(|| {
        (0..cpus + 4)
            .map(|_| Mutex::new(Vec::with_capacity(capacity)))
            .collect()
    });
    assert!(slots.iter().all(|slot| slot.lock().unwrap().is_empty()));
    NEXT_SLOT.store(0, Ordering::Relaxed);
    READS.store(0, Ordering::Relaxed);
    GENERATION.fetch_add(1, Ordering::Relaxed);
    FORM.store(form, Ordering::Release);
}
fn finish() {
    assert_ne!(FORM.swap(0, Ordering::AcqRel), 0);
}

#[test]
fn split_and_direct_preserve_absence_blob_bound_and_corruption() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(std::thread::available_parallelism().unwrap().get().max(4))
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = runtime
        .block_on(turso::Builder::new_local(dir.path().join("read.db").to_str().unwrap()).build())
        .unwrap();
    let conn = db.connect().unwrap();
    runtime.block_on(async {
        conn.execute(
            "CREATE TABLE mrr_backend_kv (key TEXT PRIMARY KEY, value BLOB)",
            (),
        )
        .await
        .unwrap();
        for len in [0, 65536, 65537] {
            conn.execute(
                "INSERT INTO mrr_backend_kv VALUES (?1, ?2)",
                [
                    turso::Value::Text(len.to_string()),
                    turso::Value::Blob(vec![7; len]),
                ],
            )
            .await
            .unwrap();
        }
        conn.execute(
            "INSERT INTO mrr_backend_kv VALUES ('text', 'not a blob')",
            (),
        )
        .await
        .unwrap();
        conn.execute("BEGIN IMMEDIATE", ()).await.unwrap();
    });
    for split in [false, true] {
        for len in [0, 65536] {
            assert_eq!(
                profile_read(&conn, runtime.handle(), &len.to_string(), split)
                    .0
                    .unwrap(),
                Some(vec![7; len])
            );
        }
        assert_eq!(
            profile_read(&conn, runtime.handle(), "absent", split)
                .0
                .unwrap(),
            None
        );
        for key in ["65537", "text"] {
            assert!(matches!(
                profile_read(&conn, runtime.handle(), key, split).0,
                Err(BackendError::Corrupt)
            ));
        }
    }
    runtime.block_on(conn.execute("ROLLBACK", ())).unwrap();
    assert!(conn.is_autocommit().unwrap());
}

#[test]
fn poll_clock_distinguishes_ready_work_and_pending_gap() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (value, ready) = measured(std::future::ready(7)).await;
        assert_eq!(value, 7);
        assert_eq!(ready.pending, 0);
        assert_eq!(ready.pending_gap_ns, 0);
        let mut yielded = false;
        let ((), pending) = measured(poll_fn(|cx| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }))
        .await;
        assert_eq!(pending.pending, 1);
        assert_eq!(pending.polls, 2);
    });
}

#[test]
#[ignore = "run alone; full original transactions with bounded per-read diagnostic clocks"]
fn turso_original_transaction_read_stages() {
    use workload::{Case, WriterMode, case_observed};
    let cpus = std::thread::available_parallelism().unwrap().get().max(4);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cpus)
        .max_blocking_threads(cpus + 4)
        .enable_all()
        .build()
        .unwrap();
    for pair in 0..3 {
        let mut modes = [WriterMode::Wal, WriterMode::Fresh, WriterMode::Reuse];
        modes.rotate_left(pair);
        for (order, mode) in modes.into_iter().enumerate() {
            for split in [pair % 2 != 0, pair % 2 == 0] {
                runtime.block_on(case_observed(
                    Case {
                        cpus,
                        mode,
                        hot: false,
                        hold: None,
                        pair,
                        order,
                    },
                    if split { begin_split } else { begin_direct },
                    finish,
                ));
                let mut samples: Vec<_> = SAMPLES
                    .get()
                    .unwrap()
                    .iter()
                    .flat_map(|slot| slot.lock().unwrap().drain(..).collect::<Vec<_>>())
                    .collect();
                samples.sort_unstable_by_key(|sample| sample.sequence);
                assert_eq!(samples.len(), READS.load(Ordering::Relaxed));
                assert!(
                    samples
                        .iter()
                        .enumerate()
                        .all(|(index, sample)| index == sample.sequence)
                );
                assert!(!samples.is_empty());
                assert!(samples.iter().all(|sample| sample.ok));
                println!(
                    "{}",
                    serde_json::json!({"read_stages": true, "pair": pair, "order": order,
                    "mode": match mode { WriterMode::Wal => "wal", WriterMode::Fresh => "fresh-mvcc", WriterMode::Reuse => "reused-mvcc" },
                    "split": split, "workers": cpus, "samples": samples})
                );
            }
        }
    }
}
