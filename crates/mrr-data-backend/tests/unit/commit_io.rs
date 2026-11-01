//! Transparent test-only delegation through the pinned public Rust IO interface.
use super::workload::phase_cpu::{FileRole, IoKind, io_call};
use std::{ptr::NonNull, sync::Arc, time::Duration};
use turso::core::{
    Buffer, Clock, Completion, File, IO, MemoryIO, MonotonicInstant, OpenFlags, PlatformIO, Result,
    WallClockInstant,
    io::{FileId, FileSyncType, SharedWalLockKind, SharedWalMappedRegion},
};

pub(super) fn enabled() -> bool {
    match std::env::var("MRR_DATA_BENCH_IO").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("0") => false,
        Ok("1") => true,
        _ => panic!("MRR_DATA_BENCH_IO must be absent, 0 or 1"),
    }
}
pub(crate) fn builder(builder: turso::Builder) -> turso::Builder {
    if enabled() {
        builder.with_io_impl(Arc::new(MeasuredIo(Arc::new(PlatformIO::new().unwrap()))))
    } else {
        builder
    }
}
struct MeasuredIo(Arc<dyn IO>);
struct MeasuredFile {
    inner: Arc<dyn File>,
    role: FileRole,
}
fn measured_file(inner: Arc<dyn File>, path: &str) -> Arc<dyn File> {
    let role = if path.ends_with("-wal") {
        FileRole::Wal
    } else if path.ends_with("-shm") {
        FileRole::SharedWal
    } else if path.ends_with("-log") {
        FileRole::MvccLog
    } else {
        FileRole::DatabaseOrOther
    };
    Arc::new(MeasuredFile { inner, role })
}
impl Clock for MeasuredIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.0.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.0.current_time_wall_clock()
    }
}
impl IO for MeasuredIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        self.0
            .open_file(path, flags, direct)
            .map(|file| measured_file(file, path))
    }
    fn open_shared_wal_file(&self, path: &str) -> Result<Arc<dyn File>> {
        self.0
            .open_shared_wal_file(path)
            .map(|file| measured_file(file, path))
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.0.remove_file(path)
    }
    fn supports_shared_wal_coordination(&self) -> bool {
        self.0.supports_shared_wal_coordination()
    }
    fn step(&self) -> Result<()> {
        io_call(IoKind::Step, FileRole::Driver, || self.0.step())
    }
    fn cancel(&self, c: &[Completion]) -> Result<()> {
        io_call(IoKind::Cancel, FileRole::Driver, || self.0.cancel(c))
    }
    fn drain_completions(&self, c: &[Completion]) -> Result<()> {
        io_call(IoKind::Drain, FileRole::Driver, || {
            self.0.drain_completions(c)
        })
    }
    fn wait_for_completion(&self, c: Completion) -> Result<()> {
        io_call(IoKind::Wait, FileRole::Driver, || {
            self.0.wait_for_completion(c)
        })
    }
    fn generate_random_number(&self) -> i64 {
        self.0.generate_random_number()
    }
    fn fill_bytes(&self, dest: &mut [u8]) {
        self.0.fill_bytes(dest);
    }
    fn get_memory_io(&self) -> Arc<MemoryIO> {
        self.0.get_memory_io()
    }
    fn register_fixed_buffer(&self, ptr: NonNull<u8>, len: usize) -> Result<u32> {
        self.0.register_fixed_buffer(ptr, len)
    }
    fn yield_now(&self) {
        io_call(IoKind::Yield, FileRole::Driver, || {
            self.0.yield_now();
            Ok(())
        })
        .unwrap();
    }
    fn sleep(&self, duration: Duration) {
        io_call(IoKind::Sleep, FileRole::Driver, || {
            self.0.sleep(duration);
            Ok(())
        })
        .unwrap();
    }
    fn file_id(&self, path: &str) -> Result<FileId> {
        self.0.file_id(path)
    }
}
impl File for MeasuredFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        io_call(IoKind::Lock, self.role, || self.inner.lock_file(exclusive))
    }
    fn unlock_file(&self) -> Result<()> {
        io_call(IoKind::Unlock, self.role, || self.inner.unlock_file())
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        io_call(IoKind::Read, self.role, || self.inner.pread(pos, c))
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        io_call(IoKind::Write, self.role, || {
            self.inner.pwrite(pos, buffer, c)
        })
    }
    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        io_call(IoKind::Write, self.role, || {
            self.inner.pwritev(pos, buffers, c)
        })
    }
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        io_call(IoKind::Sync, self.role, || self.inner.sync(c, sync_type))
    }
    fn size(&self) -> Result<u64> {
        io_call(IoKind::Size, self.role, || self.inner.size())
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        io_call(IoKind::Truncate, self.role, || self.inner.truncate(len, c))
    }
    fn has_hole(&self, pos: usize, len: usize) -> Result<bool> {
        self.inner.has_hole(pos, len)
    }
    fn punch_hole(&self, pos: usize, len: usize) -> Result<()> {
        self.inner.punch_hole(pos, len)
    }
    fn shared_wal_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<()> {
        self.inner.shared_wal_lock_byte(offset, exclusive, kind)
    }
    fn shared_wal_try_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        self.inner.shared_wal_try_lock_byte(offset, exclusive, kind)
    }
    fn shared_wal_probe_exclusive_byte(
        &self,
        offset: u64,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        self.inner.shared_wal_probe_exclusive_byte(offset, kind)
    }
    fn shared_wal_probe_exclusive_while_shared_byte(
        &self,
        offset: u64,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        self.inner
            .shared_wal_probe_exclusive_while_shared_byte(offset, kind)
    }
    fn shared_wal_unlock_byte(&self, offset: u64, kind: SharedWalLockKind) -> Result<()> {
        self.inner.shared_wal_unlock_byte(offset, kind)
    }
    fn shared_wal_set_len(&self, len: u64) -> Result<()> {
        self.inner.shared_wal_set_len(len)
    }
    fn shared_wal_map(&self, offset: u64, len: usize) -> Result<Box<dyn SharedWalMappedRegion>> {
        self.inner.shared_wal_map(offset, len)
    }
}

#[test]
fn delegated_sync_keeps_the_original_completion_and_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observed.db");
    let io = MeasuredIo(Arc::new(PlatformIO::new().unwrap()));
    let file = io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = calls.clone();
    let original = Completion::new_write(move |result| {
        assert!(result.is_ok());
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    });
    let completion = file.sync(original.clone(), FileSyncType::Fsync).unwrap();
    assert!(completion.finished());
    assert!(original.finished());
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    io.wait_for_completion(completion).unwrap();
    file.unlock_file().unwrap();
    assert!(path.exists());
}
