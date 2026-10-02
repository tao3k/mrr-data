//! Optional Tokio transfer sessions: one deadline, cancellation and retry ledger.
use crate::{ContentBlock, RemoteContentStore, RemoteError, RemoteFuture, SnapshotTransferError};
use cid::Cid;
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{Semaphore, watch},
    time::Instant,
};

/// Limits apply across all operations/retries using one session. GET attempts
/// reserve their requested maximum; failed attempts keep that reservation. A
/// successful GET is charged its actual logical bytes. This is not wire traffic
/// accounting: provider-internal requests, framing and transport chunks differ.
#[derive(Clone, Copy, Debug)]
pub struct RemoteTransferLimits {
    pub operations: usize,
    pub bytes: usize,
    pub attempts_per_operation: usize,
    pub retry_delay: Duration,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransferStats {
    pub operations: usize,
    pub charged_bytes: usize,
    /// Automatic retries within remote method calls; explicit caller retries
    /// are still included in operations and charged bytes.
    pub retries: usize,
}
struct State {
    deadline: Instant,
    limits: RemoteTransferLimits,
    cancelled: watch::Sender<bool>,
    stats: Mutex<TransferStats>,
    permit: Semaphore,
}

/// Clones share cancellation, deadline and counters. Reuse the session for an
/// explicit retry to preserve its budget. A new process/session gets a fresh
/// budget and must revalidate the same immutable root; no resume log is trusted.
#[derive(Clone)]
pub struct TransferSession {
    state: Arc<State>,
}
impl TransferSession {
    /// Starts the whole-operation clock now, not separately for each block.
    /// # Errors
    /// Rejects zero attempts or a timeout that overflows the clock.
    pub fn new(timeout: Duration, limits: RemoteTransferLimits) -> Result<Self, RemoteError> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(RemoteError::InvalidConfiguration)?;
        if limits.attempts_per_operation == 0 {
            return Err(RemoteError::InvalidConfiguration);
        }
        let (cancelled, _) = watch::channel(false);
        Ok(Self {
            state: Arc::new(State {
                deadline,
                limits,
                cancelled,
                stats: Mutex::new(TransferStats::default()),
                permit: Semaphore::new(1),
            }),
        })
    }
    pub fn cancel(&self) {
        self.state.cancelled.send_replace(true);
    }
    #[must_use]
    pub fn stats(&self) -> TransferStats {
        *self
            .state
            .stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    #[must_use]
    pub fn remote<'a, R: RemoteContentStore + ?Sized>(
        &'a self,
        remote: &'a R,
    ) -> BudgetedRemote<'a, R> {
        BudgetedRemote {
            session: self,
            remote,
            attempts_per_operation: self.state.limits.attempts_per_operation,
        }
    }
    /// Decorates one remote operation with this session's deadline and budget,
    /// but never retries it automatically. Use for an effect whose retry must
    /// first re-enter a caller-owned authorization check.
    #[must_use]
    pub fn remote_once<'a, R: RemoteContentStore + ?Sized>(
        &'a self,
        remote: &'a R,
    ) -> BudgetedRemote<'a, R> {
        BudgetedRemote {
            session: self,
            remote,
            attempts_per_operation: 1,
        }
    }
    /// Publishes with both whole-operation control and remote retry accounting.
    /// # Errors
    /// Returns snapshot validation/transport errors, cancellation or expiry.
    pub async fn publish_snapshot(
        &self,
        local: &dyn crate::AsyncContentStore,
        remote: &dyn RemoteContentStore,
        snapshot: &mrr_data_core::SnapshotBlock,
        relations: &meta_relational_reasoning::RelationCatalog,
        entities: &meta_relational_reasoning::EntityCatalog,
        limits: crate::SnapshotTransferLimits,
    ) -> Result<crate::SnapshotPublication, SnapshotTransferError> {
        let budgeted = self.remote(remote);
        self.run(crate::publish_snapshot(
            local, &budgeted, snapshot, relations, entities, limits,
        ))
        .await
    }

    /// Publishes children with session retries, checks a caller-owned gate,
    /// then attempts the root once under the same deadline and budget. A root
    /// retry must re-enter this method and its gate.
    /// # Errors
    /// Returns a snapshot failure or the caller's root-gate error.
    pub async fn publish_snapshot_with_root_gate<E, F>(
        &self,
        inputs: crate::SnapshotPublishInputs<'_>,
        before_root: F,
    ) -> Result<crate::SnapshotPublication, E>
    where
        E: From<SnapshotTransferError>,
        F: FnOnce() -> Result<(), E>,
    {
        let budgeted = self.remote(inputs.remote);
        let root_once = self.remote_once(inputs.remote);
        self.run(crate::snapshot::publish_snapshot_with_root_gate(
            crate::SnapshotPublishInputs {
                local: inputs.local,
                remote: &budgeted,
                snapshot: inputs.snapshot,
                relations: inputs.relations,
                entities: inputs.entities,
                limits: inputs.limits,
            },
            &root_once,
            before_root,
        ))
        .await
    }

    /// Restores with both whole-operation control and remote retry accounting.
    /// # Errors
    /// Returns snapshot validation/transport errors, cancellation or expiry.
    pub async fn restore_snapshot(
        &self,
        local: &(impl crate::AsyncContentStore + ?Sized),
        remote: &(impl RemoteContentStore + ?Sized),
        root: &Cid,
        relations: &meta_relational_reasoning::RelationCatalog,
        entities: &meta_relational_reasoning::EntityCatalog,
        limits: crate::SnapshotTransferLimits,
    ) -> Result<crate::RestoredSnapshot, SnapshotTransferError> {
        let budgeted = self.remote(remote);
        self.run(crate::restore_snapshot(
            local, &budgeted, root, relations, entities, limits,
        ))
        .await
    }

    fn check(&self) -> Result<(), RemoteError> {
        if *self.state.cancelled.borrow() {
            return Err(RemoteError::Cancelled);
        }
        if Instant::now() >= self.state.deadline {
            return Err(RemoteError::DeadlineExceeded);
        }
        Ok(())
    }
    async fn guard<T>(
        &self,
        work: impl Future<Output = Result<T, RemoteError>>,
    ) -> Result<T, RemoteError> {
        let mut cancelled = self.state.cancelled.subscribe();
        self.check()?;
        let result = tokio::select! {
            biased;
            _ = cancelled.changed() => Err(RemoteError::Cancelled),
            () = tokio::time::sleep_until(self.state.deadline) => Err(RemoteError::DeadlineExceeded),
            result = work => result,
        };
        self.check()?;
        result
    }
    /// Runs an entire snapshot future, including local staging, under this
    /// deadline/cancellation scope. Use `self.remote(...)` inside that future
    /// for retry accounting and checks between remote operations. Use a blocking
    /// local adapter for disk I/O; synchronous code cannot be forcibly preempted.
    /// # Errors
    /// Returns the operation error, cancellation or whole-operation expiry.
    pub async fn run<T, E>(&self, work: impl Future<Output = Result<T, E>>) -> Result<T, E>
    where
        E: From<SnapshotTransferError>,
    {
        // Keep the operation's typed error instead of reducing it to transport failure.
        let result = self.guard(async { Ok(work.await) }).await;
        match result {
            Ok(result) => result,
            Err(RemoteError::Cancelled) => Err(SnapshotTransferError::Cancelled.into()),
            Err(_) => Err(SnapshotTransferError::DeadlineExceeded.into()),
        }
    }
    fn charge(&self, bytes: usize, retry: bool) -> Result<(), RemoteError> {
        self.check()?;
        let mut stats = self
            .state
            .stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if stats.operations >= self.state.limits.operations {
            return Err(RemoteError::RequestBudgetExceeded);
        }
        let total = stats
            .charged_bytes
            .checked_add(bytes)
            .filter(|n| *n <= self.state.limits.bytes)
            .ok_or(RemoteError::TransferBudgetExceeded)?;
        stats.operations += 1;
        stats.charged_bytes = total;
        stats.retries += usize::from(retry);
        Ok(())
    }
    async fn attempts<T, F, Fut>(
        &self,
        reserved: usize,
        attempts_per_operation: usize,
        mut operation: F,
        actual: impl Fn(&T) -> usize,
    ) -> Result<T, RemoteError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, RemoteError>>,
    {
        self.guard(async {
            // One remote operation in flight per session, including retries.
            let _permit = self
                .state
                .permit
                .acquire()
                .await
                .map_err(|_| RemoteError::Unavailable)?;
            for attempt in 0..attempts_per_operation {
                self.charge(reserved, attempt > 0)?;
                match operation().await {
                    Ok(result) => {
                        let actual = actual(&result);
                        if actual > reserved {
                            return Err(RemoteError::TooLarge);
                        }
                        self.state
                            .stats
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .charged_bytes -= reserved - actual;
                        return Ok(result);
                    }
                    Err(error)
                        if matches!(
                            error,
                            RemoteError::Unavailable | RemoteError::DeadlineExceeded
                        ) && attempt + 1 < attempts_per_operation =>
                    {
                        tokio::time::sleep(self.state.limits.retry_delay).await;
                    }
                    Err(error) => return Err(error),
                }
            }
            unreachable!("constructor rejects zero attempts")
        })
        .await
    }
}

/// Runtime-only transport decoration; storage format and credentials remain
/// owned by the underlying provider. There are no detached remote tasks.
pub struct BudgetedRemote<'a, R: ?Sized> {
    session: &'a TransferSession,
    remote: &'a R,
    attempts_per_operation: usize,
}
impl<R: RemoteContentStore + ?Sized> RemoteContentStore for BudgetedRemote<'_, R> {
    fn get<'a>(&'a self, cid: &'a Cid, max_bytes: usize) -> RemoteFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            self.session
                .attempts(
                    max_bytes,
                    self.attempts_per_operation,
                    || self.remote.get(cid, max_bytes),
                    |bytes| bytes.as_ref().map_or(0, Vec::len),
                )
                .await
        })
    }
    fn put<'a>(&'a self, block: ContentBlock<'a>) -> RemoteFuture<'a, ()> {
        Box::pin(async move {
            self.session
                .attempts(
                    block.bytes().len(),
                    self.attempts_per_operation,
                    || self.remote.put(block),
                    |()| block.bytes().len(),
                )
                .await
        })
    }
}
