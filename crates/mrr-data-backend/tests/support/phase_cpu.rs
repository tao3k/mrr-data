//! Test-only synchronous thread CPU spans and bounded, worker-sharded receipts.
use mrr_data_backend::StoredWrite;
use nix::sys::resource::{UsageWho, getrusage};
use nix::time::{ClockId, clock_gettime};
use std::{
    cell::{Cell, RefCell},
    future::{Future, poll_fn},
    pin::pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Waker},
    time::Instant,
};
#[path = "phase_cpu/io.rs"]
mod io;
#[path = "phase_cpu/validation.rs"]
mod validation;
#[path = "phase_cpu/wake.rs"]
mod wake;
use io::IoScope;
pub use io::{FileRole, IoEvent, IoKind, io_call};
pub use validation::{validate_io, validate_records};
use wake::{ObservedWake, WakeDetail};
static ON: AtomicBool = AtomicBool::new(false);
static EPOCH: OnceLock<Instant> = OnceLock::new();
static GENERATION: AtomicU64 = AtomicU64::new(0);
static NEXT: AtomicUsize = AtomicUsize::new(0);
static COUNT: AtomicUsize = AtomicUsize::new(0);
static RECORDS: OnceLock<Vec<Mutex<Vec<Record>>>> = OnceLock::new();
const LIMIT: usize = 100_000;
const WAKE_LIMIT: usize = 64;
thread_local! {
    static SLOT: Cell<(u64, usize)> = const { Cell::new((0, usize::MAX)) };
    static CURRENT: RefCell<Option<Worker>> = const { RefCell::new(None) };
    static USAGE: Cell<ProcessUsage> = const { Cell::new(ProcessUsage { input_blocks: 0, output_blocks: 0, voluntary_switches: 0, involuntary_switches: 0 }) };
}
#[derive(Clone, Copy, serde::Serialize)]
pub struct ProcessUsage {
    input_blocks: i64,
    output_blocks: i64,
    voluntary_switches: i64,
    involuntary_switches: i64,
}
fn usage() -> ProcessUsage {
    let usage = getrusage(UsageWho::RUSAGE_SELF).unwrap();
    ProcessUsage {
        input_blocks: usage.block_reads(),
        output_blocks: usage.block_writes(),
        voluntary_switches: usage.voluntary_context_switches(),
        involuntary_switches: usage.involuntary_context_switches(),
    }
}
#[must_use]
pub fn process_usage() -> ProcessUsage {
    USAGE.with(Cell::get)
}
#[derive(Clone, Copy)]
pub struct Stamp {
    wall: Instant,
    cpu: u64,
}
fn cpu() -> u64 {
    let time = clock_gettime(ClockId::CLOCK_THREAD_CPUTIME_ID).unwrap();
    u64::try_from(time.tv_sec()).unwrap() * 1_000_000_000 + u64::try_from(time.tv_nsec()).unwrap()
}
fn stamp() -> Stamp {
    Stamp {
        wall: Instant::now(),
        cpu: cpu(),
    }
}
fn ns(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap()
}
#[derive(Default, serde::Serialize)]
pub struct Span {
    #[serde(rename = "start_ns")]
    start: u64,
    #[serde(rename = "end_ns")]
    end: u64,
    #[serde(rename = "wall_ns")]
    wall: u64,
    #[serde(rename = "cpu_ns")]
    cpu: u64,
    #[serde(rename = "off_cpu_ns")]
    off_cpu: i128,
}
impl Span {
    fn between(start: Stamp, end: Stamp) -> Self {
        let wall_ns = ns(end.wall.duration_since(start.wall));
        let cpu_ns = end
            .cpu
            .checked_sub(start.cpu)
            .expect("one synchronous OS thread");
        Self {
            start: ns(start.wall.duration_since(*EPOCH.get().unwrap())),
            end: ns(end.wall.duration_since(*EPOCH.get().unwrap())),
            wall: wall_ns,
            cpu: cpu_ns,
            off_cpu: i128::from(wall_ns) - i128::from(cpu_ns),
        }
    }
}
#[derive(Default, serde::Serialize)]
pub struct Commit {
    total: Span,
    poll_wall_ns: u64,
    poll_cpu_ns: u64,
    pending_gap_ns: u64,
    polls: u64,
    pending: u64,
    windows: Vec<WaitWindow>,
    window_overflow: bool,
    wakes: Vec<u64>,
    wake_count: usize,
    wake_unpublished: usize,
    wake_overflow: bool,
    wake_details: Vec<WakeDetail>,
    wake_details_unpublished: usize,
    io: Vec<IoEvent>,
}
#[derive(serde::Serialize)]
struct WaitWindow {
    poll_start: u64,
    pending_start: u64,
    next_poll: u64,
}
fn at(instant: Instant) -> u64 {
    ns(instant.duration_since(*EPOCH.get().unwrap()))
}
struct Worker {
    start: Stamp,
    key: (String, String),
    body_start: Option<Stamp>,
    body_end: Option<Stamp>,
    body: Span,
    bridge: Span,
    commit: Commit,
}
#[derive(serde::Serialize)]
pub enum Record {
    Worker {
        scope: String,
        operation: String,
        total: Span,
        body: Span,
        post: Span,
        bridge: Span,
        commit: Box<Commit>,
    },
    Caller {
        scope: String,
        operation: String,
        validation: Span,
    },
    Receipt {
        scope: String,
        operation: String,
        at_ns: u64,
    },
}
fn append(record: Record) {
    SLOT.with(|local| {
        let epoch = GENERATION.load(Ordering::Relaxed);
        let slot = match local.get() {
            (generation, slot) if generation == epoch => slot,
            _ => {
                let slot = NEXT.fetch_add(1, Ordering::Relaxed);
                local.set((epoch, slot));
                slot
            }
        };
        assert!(COUNT.fetch_add(1, Ordering::Relaxed) < LIMIT);
        RECORDS.get().unwrap()[slot].lock().unwrap().push(record);
    });
}
pub struct WorkerSpan(bool);
impl WorkerSpan {
    /// # Panics
    /// Refuses a different fixture profile or nested worker context.
    pub fn enter(write: &StoredWrite) -> Self {
        let on = ON.load(Ordering::Acquire);
        if on {
            assert_eq!(
                (&*write.profile, &*write.namespace),
                ("gateway.v1", "original")
            );
            CURRENT.with(|current| {
                let key = (write.scope.clone(), write.operation_id.clone());
                assert!(
                    current
                        .borrow_mut()
                        .replace(Worker {
                            start: stamp(),
                            key,
                            body_start: None,
                            body_end: None,
                            body: Span::default(),
                            bridge: Span::default(),
                            commit: Commit::default()
                        })
                        .is_none()
                );
            });
        }
        Self(on)
    }
}
impl Drop for WorkerSpan {
    fn drop(&mut self) {
        if self.0 {
            let end = stamp();
            let worker = CURRENT.with(|current| current.borrow_mut().take().unwrap());
            append(Record::Worker {
                scope: worker.key.0,
                operation: worker.key.1,
                total: Span::between(worker.start, end),
                body: worker.body,
                post: worker
                    .body_end
                    .map_or_else(Span::default, |body_end| Span::between(body_end, end)),
                bridge: worker.bridge,
                commit: Box::new(worker.commit),
            });
        }
    }
}
pub fn caller_start() -> Option<Stamp> {
    ON.load(Ordering::Acquire).then(stamp)
}
pub fn caller_finish(scope: &str, operation: &str, start: Option<Stamp>) {
    if let Some(start) = start {
        let validation = Span::between(start, stamp());
        append(Record::Caller {
            scope: scope.into(),
            operation: operation.into(),
            validation,
        });
    }
}
/// # Panics
/// Requires the initialized, bounded case collector.
pub fn receipt(scope: &str, operation: &str) {
    if ON.load(Ordering::Acquire) {
        let at_ns = ns(EPOCH.get().unwrap().elapsed());
        append(Record::Receipt {
            scope: scope.into(),
            operation: operation.into(),
            at_ns,
        });
    }
}
#[must_use]
pub fn bridge_start() -> Option<Stamp> {
    caller_start()
}
/// # Panics
/// Requires the active original worker context.
pub fn bridge_finish(start: Option<Stamp>) {
    if let Some(start) = start {
        let span = Span::between(start, stamp());
        CURRENT.with(|current| current.borrow_mut().as_mut().unwrap().bridge = span);
    }
}
/// # Panics
/// Requires the active original worker context.
pub fn body_start() {
    if ON.load(Ordering::Acquire) {
        let now = stamp();
        CURRENT.with(|current| current.borrow_mut().as_mut().unwrap().body_start = Some(now));
    }
}
/// # Panics
/// Requires one matching body start on the original worker.
pub fn body_finish() {
    if ON.load(Ordering::Acquire) {
        let now = stamp();
        CURRENT.with(|current| {
            let mut current = current.borrow_mut();
            let worker = current.as_mut().unwrap();
            worker.body = Span::between(worker.body_start.unwrap(), now);
            worker.body_end = Some(now);
        });
    }
}
/// # Panics
/// Refuses OS-thread migration, invalid clocks or inconsistent poll boundaries.
pub async fn observe_commit<F: Future>(future: F) -> F::Output {
    if !ON.load(Ordering::Acquire) {
        return future.await;
    }
    let mut future = pin!(future);
    let io = IoScope::enter();
    let start = stamp();
    let mut commit = Commit::default();
    let mut waiting = None::<Instant>;
    let mut previous_poll = None::<Instant>;
    let mut proxy = None::<Arc<ObservedWake>>;
    let thread = std::thread::current().id();
    let output = poll_fn(|cx| {
        assert_eq!(
            thread,
            std::thread::current().id(),
            "Handle.block_on retains its OS thread"
        );
        let begin = stamp();
        if let Some(end) = waiting.take() {
            commit.pending_gap_ns += ns(begin.wall.duration_since(end));
            if commit.windows.len() < WAKE_LIMIT {
                commit.windows.push(WaitWindow {
                    poll_start: at(previous_poll.unwrap()),
                    pending_start: at(end),
                    next_poll: at(begin.wall),
                });
            } else {
                commit.window_overflow = true;
            }
        }
        let proxy = proxy.get_or_insert_with(|| Arc::new(ObservedWake::new(cx.waker().clone())));
        assert!(
            proxy.target.will_wake(cx.waker()),
            "pinned Handle.block_on target remains stable"
        );
        let observed = Waker::from(proxy.clone());
        let result = future.as_mut().poll(&mut Context::from_waker(&observed));
        let end = stamp();
        commit.poll_wall_ns += ns(end.wall.duration_since(begin.wall));
        commit.poll_cpu_ns += end.cpu - begin.cpu;
        commit.polls += 1;
        if result.is_pending() {
            commit.pending += 1;
            waiting = Some(end.wall);
            previous_poll = Some(begin.wall);
        }
        result
    })
    .await;
    commit.total = Span::between(start, stamp());
    commit.io = io.finish();
    proxy.unwrap().snapshot(&mut commit);
    assert_eq!(commit.polls, commit.pending + 1);
    assert!(commit.poll_wall_ns + commit.pending_gap_ns <= commit.total.wall);
    CURRENT.with(|current| current.borrow_mut().as_mut().unwrap().commit = commit);
    output
}
/// # Panics
/// Refuses overlap, uncleared observations or an exceeded observer budget.
pub fn begin() {
    assert!(!ON.load(Ordering::Acquire));
    EPOCH.get_or_init(Instant::now);
    USAGE.with(|start| start.set(usage()));
    let cpus = std::thread::available_parallelism().unwrap().get().max(4);
    let capacity = cpus * super::ATTEMPTS * 3;
    assert!(capacity <= LIMIT);
    let slots = RECORDS.get_or_init(|| {
        (0..2 * cpus + 4)
            .map(|_| Mutex::new(Vec::with_capacity(capacity)))
            .collect()
    });
    assert!(slots.iter().all(|slot| slot.lock().unwrap().is_empty()));
    NEXT.store(0, Ordering::Relaxed);
    COUNT.store(0, Ordering::Relaxed);
    GENERATION.fetch_add(1, Ordering::Relaxed);
    ON.store(true, Ordering::Release);
}
/// # Panics
/// Requires one active case after accepted jobs drain.
pub fn finish() {
    assert!(ON.swap(false, Ordering::AcqRel));
    let end = usage();
    USAGE.with(|start| {
        let before = start.get();
        start.set(ProcessUsage {
            input_blocks: end.input_blocks - before.input_blocks,
            output_blocks: end.output_blocks - before.output_blocks,
            voluntary_switches: end.voluntary_switches - before.voluntary_switches,
            involuntary_switches: end.involuntary_switches - before.involuntary_switches,
        });
    });
}
/// # Panics
/// Refuses a running or uninitialized collector.
pub fn drain() -> Vec<Record> {
    assert!(!ON.load(Ordering::Acquire));
    let records: Vec<_> = RECORDS
        .get()
        .unwrap()
        .iter()
        .flat_map(|slot| slot.lock().unwrap().drain(..).collect::<Vec<_>>())
        .collect();
    records
}

/// # Panics
/// Requires a supported monotonic per-thread CPU clock.
#[must_use]
pub fn calibration() -> Vec<Span> {
    EPOCH.get_or_init(Instant::now);
    (0..256)
        .map(|_| {
            let start = stamp();
            Span::between(start, stamp())
        })
        .collect()
}

#[test]
fn thread_cpu_is_not_a_sleep_wall_clock() {
    EPOCH.get_or_init(Instant::now);
    let start = stamp();
    std::thread::sleep(std::time::Duration::from_millis(20));
    let elapsed = Span::between(start, stamp());
    assert!(elapsed.wall >= 20_000_000);
    assert!(
        elapsed.cpu < elapsed.wall,
        "sleep has positive off-CPU time"
    );
}
