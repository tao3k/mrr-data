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
    total: AtomicU64,
    body: AtomicU64,
    get: AtomicU64,
    put: AtomicU64,
    gets: AtomicU64,
    puts: AtomicU64,
}
#[derive(Clone, Copy, serde::Serialize)]
pub struct PhaseSample {
    transactions: u64,
    total_ns: u64,
    body_ns: u64,
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
    pub fn reset(&self) {
        for c in [
            &self.transactions,
            &self.total,
            &self.body,
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
            total_ns: self.total.load(Ordering::Relaxed),
            body_ns: self.body.load(Ordering::Relaxed),
            get_ns: self.get.load(Ordering::Relaxed),
            put_ns: self.put.load(Ordering::Relaxed),
            gets: self.gets.load(Ordering::Relaxed),
            puts: self.puts.load(Ordering::Relaxed),
        }
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
        let result = self.provider.transaction(&mut |tx| {
            let begin = Instant::now();
            let result = run(&mut TimedTransaction {
                tx,
                timings: &self.timings,
            });
            record(&self.timings.body, begin);
            result
        });
        record(&self.timings.total, start);
        self.timings.transactions.fetch_add(1, Ordering::Relaxed);
        result
    }
}
