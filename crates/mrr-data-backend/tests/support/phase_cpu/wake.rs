//! Bounded forwarding observations. Publication may lag the forwarded wake.
use super::{CURRENT, Commit, EPOCH, WAKE_LIMIT, at, stamp};
use std::{
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::{Wake, Waker},
    time::Instant,
};
pub(super) struct ObservedWake {
    pub(super) target: Waker,
    next: AtomicUsize,
    times: [AtomicU64; WAKE_LIMIT],
    details: [OnceLock<WakeDetail>; WAKE_LIMIT],
}
#[derive(Clone, serde::Serialize)]
pub(super) struct WakeDetail {
    pub(super) start_ns: u64,
    pub(super) end_ns: u64,
    pub(super) cpu_ns: u64,
    pub(super) origin_context: Option<(String, String)>,
    pub(super) in_commit: bool,
}
impl ObservedWake {
    pub(super) fn new(target: Waker) -> Self {
        Self {
            target,
            next: AtomicUsize::new(0),
            times: std::array::from_fn(|_| AtomicU64::new(0)),
            details: std::array::from_fn(|_| OnceLock::new()),
        }
    }
    fn forward(&self) {
        let index = self.next.fetch_add(1, Ordering::Relaxed);
        let start = stamp();
        if let Some(slot) = self.times.get(index) {
            slot.store(at(start.wall) + 1, Ordering::Release);
        }
        let origin_context =
            CURRENT.with(|current| current.borrow().as_ref().map(|worker| worker.key.clone()));
        let in_commit = super::io::in_commit();
        // Observation saturation must never suppress or invent a scheduler wake.
        self.target.wake_by_ref();
        let end = stamp();
        if let Some(slot) = self.details.get(index) {
            let _ = slot.set(WakeDetail {
                start_ns: at(start.wall),
                end_ns: at(end.wall),
                cpu_ns: end.cpu - start.cpu,
                origin_context,
                in_commit,
            });
        }
    }
    pub(super) fn snapshot(&self, commit: &mut Commit) {
        commit.wake_count = self.next.load(Ordering::Acquire);
        commit.wake_overflow = commit.wake_count > WAKE_LIMIT;
        for slot in self.times.iter().take(commit.wake_count.min(WAKE_LIMIT)) {
            match slot.load(Ordering::Acquire).checked_sub(1) {
                Some(time) => commit.wakes.push(time),
                None => commit.wake_unpublished += 1,
            }
        }
        commit.wakes.sort_unstable();
        commit.wake_details.extend(
            self.details
                .iter()
                .take(commit.wake_count.min(WAKE_LIMIT))
                .filter_map(|slot| slot.get().cloned()),
        );
        commit.wake_details_unpublished =
            commit.wake_count.min(WAKE_LIMIT) - commit.wake_details.len();
    }
}
impl Wake for ObservedWake {
    fn wake(self: Arc<Self>) {
        self.forward();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.forward();
    }
}

#[test]
fn forwarding_waker_never_suppresses_notifications_when_observation_is_full() {
    struct Target(AtomicUsize);
    impl Wake for Target {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    EPOCH.get_or_init(Instant::now);
    let target = Arc::new(Target(AtomicUsize::new(0)));
    let proxy = Arc::new(ObservedWake::new(Waker::from(target.clone())));
    let waker = Waker::from(proxy.clone());
    let workers = std::thread::available_parallelism().unwrap().get().max(4);
    std::thread::scope(|scope| {
        for _ in 0..workers {
            let waker = waker.clone();
            scope.spawn(move || {
                for _ in 0..WAKE_LIMIT + 2 {
                    waker.wake_by_ref();
                }
            });
        }
    });
    waker.wake();
    assert_eq!(
        target.0.load(Ordering::Relaxed),
        workers * (WAKE_LIMIT + 2) + 1
    );
    let mut snapshot = Commit::default();
    proxy.snapshot(&mut snapshot);
    assert!(snapshot.wake_overflow);
    assert_eq!(snapshot.wakes.len(), WAKE_LIMIT);
    assert_eq!(snapshot.wake_unpublished, 0);
}
