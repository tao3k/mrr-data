//! Synchronous IO observation lifetime; never substitutes a completion.
use super::{Span, stamp};
use std::cell::RefCell;
const IO_LIMIT: usize = 256;
thread_local! {
    static COMMIT_IO: RefCell<Option<Vec<IoEvent>>> = const { RefCell::new(None) };
}
pub(super) fn in_commit() -> bool {
    COMMIT_IO.with(|current| current.borrow().is_some())
}
#[derive(Clone, Copy, serde::Serialize)]
pub enum IoKind {
    Read,
    Write,
    Sync,
    Truncate,
    Size,
    Lock,
    Unlock,
    Step,
    Cancel,
    Drain,
    Wait,
    Yield,
    Sleep,
}
#[derive(Clone, Copy, serde::Serialize)]
pub enum FileRole {
    Wal,
    SharedWal,
    MvccLog,
    DatabaseOrOther,
    Driver,
}
#[derive(serde::Serialize)]
pub struct IoEvent {
    pub(super) kind: IoKind,
    file: FileRole,
    pub(super) span: Span,
    ok: bool,
}
/// Measures a synchronous delegated call, never an inferred device completion.
/// # Errors
/// Returns the original delegated operation error without retry or translation.
/// # Panics
/// Refuses overflow of the original COMMIT's fixed observation budget.
pub fn io_call<T>(
    kind: IoKind,
    file: FileRole,
    call: impl FnOnce() -> turso::core::Result<T>,
) -> turso::core::Result<T> {
    if !COMMIT_IO.with(|current| current.borrow().is_some()) {
        return call();
    }
    let start = stamp();
    let output = call();
    let span = Span::between(start, stamp());
    COMMIT_IO.with(|current| {
        let mut current = current.borrow_mut();
        let events = current.as_mut().unwrap();
        assert!(events.len() < IO_LIMIT, "bounded COMMIT IO observation");
        events.push(IoEvent {
            kind,
            file,
            span,
            ok: output.is_ok(),
        });
    });
    output
}
pub(super) struct IoScope;
impl IoScope {
    pub(super) fn enter() -> Self {
        COMMIT_IO.with(|current| {
            assert!(
                current
                    .borrow_mut()
                    .replace(Vec::with_capacity(IO_LIMIT))
                    .is_none()
            );
        });
        Self
    }
    pub(super) fn finish(self) -> Vec<IoEvent> {
        let records = COMMIT_IO.with(|current| current.borrow_mut().take().unwrap());
        drop(self);
        records
    }
}
impl Drop for IoScope {
    fn drop(&mut self) {
        COMMIT_IO.with(|current| {
            current.borrow_mut().take();
        });
    }
}

#[test]
fn io_scope_unwinds_without_retaining_a_later_commit_context() {
    let result = std::panic::catch_unwind(|| {
        let _scope = IoScope::enter();
        panic!("injected IO observation unwind");
    });
    assert!(result.is_err());
    assert!(COMMIT_IO.with(|current| current.borrow().is_none()));
    let scope = IoScope::enter();
    assert!(scope.finish().is_empty());
}
