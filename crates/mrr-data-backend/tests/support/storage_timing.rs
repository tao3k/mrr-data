//! Test-only timing of the unchanged physical provider and opaque record seam.
use mrr_data_backend::{
    BackendError,
    providers::{MetadataTransaction, ProviderResult, TransactionProvider, TursoProvider},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
#[derive(Default)]
pub struct Timings {
    transactions: AtomicU64,
    bodies: AtomicU64,
    bridges: AtomicU64,
    bridge: AtomicU64,
    total: AtomicU64,
    body: AtomicU64,
    pre_body: AtomicU64,
    post_body: AtomicU64,
    get: AtomicU64,
    put: AtomicU64,
    gets: AtomicU64,
    puts: AtomicU64,
}
#[derive(Clone, Copy, serde::Serialize)]
pub struct PhaseSample {
    transactions: u64,
    bodies: u64,
    bridges: u64,
    bridge_ns: u64,
    total_ns: u64,
    body_ns: u64,
    pre_body_ns: u64,
    post_body_ns: u64,
    get_ns: u64,
    put_ns: u64,
    gets: u64,
    puts: u64,
}
fn record(counter: &AtomicU64, start: Instant) {
    counter.fetch_add(
        u64::try_from(start.elapsed().as_nanos()).unwrap(),
        Ordering::Relaxed,
    );
}
impl Timings {
    pub fn bridge(&self, run: impl FnOnce() -> bool) -> bool {
        let start = Instant::now();
        let result = run();
        record(&self.bridge, start);
        self.bridges.fetch_add(1, Ordering::Relaxed);
        result
    }
    pub fn reset(&self) {
        for c in [
            &self.transactions,
            &self.bodies,
            &self.bridges,
            &self.bridge,
            &self.total,
            &self.body,
            &self.pre_body,
            &self.post_body,
            &self.get,
            &self.put,
            &self.gets,
            &self.puts,
        ] {
            c.store(0, Ordering::Relaxed);
        }
    }
    pub fn snapshot(&self) -> PhaseSample {
        PhaseSample {
            transactions: self.transactions.load(Ordering::Relaxed),
            bodies: self.bodies.load(Ordering::Relaxed),
            bridges: self.bridges.load(Ordering::Relaxed),
            bridge_ns: self.bridge.load(Ordering::Relaxed),
            total_ns: self.total.load(Ordering::Relaxed),
            body_ns: self.body.load(Ordering::Relaxed),
            pre_body_ns: self.pre_body.load(Ordering::Relaxed),
            post_body_ns: self.post_body.load(Ordering::Relaxed),
            get_ns: self.get.load(Ordering::Relaxed),
            put_ns: self.put.load(Ordering::Relaxed),
            gets: self.gets.load(Ordering::Relaxed),
            puts: self.puts.load(Ordering::Relaxed),
        }
    }
}
impl PhaseSample {
    pub fn assert_balanced(&self, expected: usize) {
        assert_eq!(self.transactions, u64::try_from(expected).unwrap());
        assert_eq!(self.bodies, self.transactions, "no hidden callback retry");
        assert_eq!(
            self.bridges, self.transactions,
            "one original validation bridge"
        );
        assert_eq!(
            self.total_ns,
            self.pre_body_ns + self.body_ns + self.post_body_ns
        );
        assert!(self.get_ns + self.put_ns <= self.body_ns);
        assert!(self.get_ns + self.put_ns + self.bridge_ns <= self.body_ns);
    }
}
pub struct TimedStorage {
    pub provider: TursoProvider,
    pub timings: Arc<Timings>,
}
struct TimedTransaction<'a> {
    tx: &'a mut dyn MetadataTransaction,
    timings: &'a Timings,
}
impl MetadataTransaction for TimedTransaction<'_> {
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        let start = Instant::now();
        let result = self.tx.get(key);
        record(&self.timings.get, start);
        self.timings.gets.fetch_add(1, Ordering::Relaxed);
        result
    }
    fn put(&mut self, key: &str, value: &[u8]) -> Result<(), BackendError> {
        let start = Instant::now();
        let result = self.tx.put(key, value);
        record(&self.timings.put, start);
        self.timings.puts.fetch_add(1, Ordering::Relaxed);
        result
    }
}
impl TransactionProvider for TimedStorage {
    fn open_storage(&self) -> Result<(), BackendError> {
        self.provider.open_storage()
    }
    fn close_storage(&self) -> Result<(), BackendError> {
        self.provider.close_storage()
    }
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError> {
        self.provider.read(key)
    }
    fn transaction(
        &self,
        run: &mut dyn FnMut(&mut dyn MetadataTransaction) -> ProviderResult<()>,
    ) -> ProviderResult<()> {
        let start = Instant::now();
        let mut end_body = None;
        let result = self.provider.transaction(&mut |tx| {
            let begin = Instant::now();
            self.timings.pre_body.fetch_add(
                u64::try_from(begin.duration_since(start).as_nanos()).unwrap(),
                Ordering::Relaxed,
            );
            let result = run(&mut TimedTransaction {
                tx,
                timings: &self.timings,
            });
            let end = Instant::now();
            self.timings.body.fetch_add(
                u64::try_from(end.duration_since(begin).as_nanos()).unwrap(),
                Ordering::Relaxed,
            );
            self.timings.bodies.fetch_add(1, Ordering::Relaxed);
            end_body = Some(end);
            result
        });
        let finish = Instant::now();
        if let Some(end) = end_body {
            self.timings.post_body.fetch_add(
                u64::try_from(finish.duration_since(end).as_nanos()).unwrap(),
                Ordering::Relaxed,
            );
        }
        self.timings.total.fetch_add(
            u64::try_from(finish.duration_since(start).as_nanos()).unwrap(),
            Ordering::Relaxed,
        );
        self.timings.transactions.fetch_add(1, Ordering::Relaxed);
        result
    }
}
