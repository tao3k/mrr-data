//! Asynchronous local-storage boundary; execution placement belongs to adapters.
use crate::{ContentBlock, ContentError, ContentStore};
use cid::Cid;
use std::{
    future::{Future, ready},
    pin::Pin,
};

pub type LocalFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ContentError>> + Send + 'a>>;

/// Local operations usable by async coordinators. Disk providers should use a
/// blocking-executor adapter. The blanket synchronous implementation is inline,
/// suitable for memory stores or callers already on a blocking thread.
pub trait AsyncContentStore {
    fn load<'a>(&'a self, cid: &'a Cid, max_bytes: usize) -> LocalFuture<'a, Vec<u8>>;
    fn store<'a>(&'a self, block: ContentBlock<'a>) -> LocalFuture<'a, Cid>;
}
impl<S: ContentStore + ?Sized> AsyncContentStore for S {
    fn load<'a>(&'a self, cid: &'a Cid, max_bytes: usize) -> LocalFuture<'a, Vec<u8>> {
        Box::pin(ready(self.get_bounded(cid, max_bytes)))
    }
    fn store<'a>(&'a self, block: ContentBlock<'a>) -> LocalFuture<'a, Cid> {
        Box::pin(ready(self.put(block)))
    }
}
