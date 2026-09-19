//! Bounded blocking execution for synchronous local content stores.
use cid::Cid;
use mrr_data_content::{AsyncContentStore, ContentBlock, ContentError, ContentStore, LocalFuture};
use std::{
    io::ErrorKind,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;

/// One blocking job per store, shared across clones. Cancellation can detach a
/// running disk operation, but the job retains its permit until it finishes;
/// it cannot cause an unbounded queue of detached workers or publish remotely.
pub struct BlockingContentStore<S> {
    store: Arc<Mutex<S>>,
    permit: Arc<Semaphore>,
}
impl<S> Clone for BlockingContentStore<S> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            permit: Arc::clone(&self.permit),
        }
    }
}
impl<S> BlockingContentStore<S> {
    #[must_use]
    pub fn new(store: S) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
            permit: Arc::new(Semaphore::new(1)),
        }
    }
}
fn worker_error() -> ContentError {
    ContentError::Io {
        operation: "local blocking worker",
        kind: ErrorKind::Other,
    }
}
impl<S: ContentStore + Send + 'static> AsyncContentStore for BlockingContentStore<S> {
    fn load<'a>(&'a self, cid: &'a Cid, max_bytes: usize) -> LocalFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let permit = Arc::clone(&self.permit)
                .acquire_owned()
                .await
                .map_err(|_| worker_error())?;
            let store = Arc::clone(&self.store);
            let cid = *cid;
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                store
                    .lock()
                    .map_err(|_| ContentError::LockPoisoned)?
                    .get_bounded(&cid, max_bytes)
            })
            .await
            .map_err(|_| worker_error())?
        })
    }
    fn store<'a>(&'a self, block: ContentBlock<'a>) -> LocalFuture<'a, Cid> {
        Box::pin(async move {
            let permit = Arc::clone(&self.permit)
                .acquire_owned()
                .await
                .map_err(|_| worker_error())?;
            let store = Arc::clone(&self.store);
            let bytes = block.bytes().to_vec();
            let codec = block.codec();
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                store
                    .lock()
                    .map_err(|_| ContentError::LockPoisoned)?
                    .put(ContentBlock::new(codec, &bytes))
            })
            .await
            .map_err(|_| worker_error())?
        })
    }
}
