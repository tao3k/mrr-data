//! Physical candidates have explicit terminal errors and retained batch ownership.
use crate::{BackendError, scheduler::ResourceLease};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use std::sync::{Arc, Condvar, Mutex};
use tokio::{
    runtime::Handle,
    sync::{Semaphore, mpsc, oneshot},
};

/// Sanitized physical failures. None grants semantic admission or disclosure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArrowQueryError {
    Backend(BackendError),
    InvalidConfiguration,
    Limit,
    Schema,
    Driver,
    Incomplete,
    Cancelled,
    WorkerLost,
}
impl std::fmt::Display for ArrowQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Arrow query: {self:?}")
    }
}
impl std::error::Error for ArrowQueryError {}
/// Output bounds are distinct from native engine memory/cache/spill budgets.
#[derive(Clone, Copy, Debug)]
pub struct ArrowQueryLimits {
    pub max_rows: usize,
    pub max_batches: usize,
    pub max_batch_bytes: usize,
    pub max_retained_bytes: usize,
    pub channel_capacity: usize,
}
impl ArrowQueryLimits {
    pub(crate) fn validate(self) -> Result<(), ArrowQueryError> {
        if self.max_rows == 0
            || self.max_batches == 0
            || self.max_batch_bytes == 0
            || self.max_retained_bytes < self.max_batch_bytes
            || !(1..=256).contains(&self.channel_capacity)
        {
            return Err(ArrowQueryError::InvalidConfiguration);
        }
        Ok(())
    }
}
/// Complete physical output only; final MRR admission remains required.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArrowQuerySummary {
    pub rows: usize,
    pub batches: usize,
}
struct Budget {
    limit: usize,
    state: Mutex<BudgetState>,
    changed: Condvar,
}
struct BudgetState {
    used: usize,
    cancelled: bool,
    cancel_hook: Option<Arc<dyn Fn() + Send + Sync>>,
}
impl Budget {
    fn reserve(self: &Arc<Self>, bytes: usize) -> Result<BytesLease, ArrowQueryError> {
        let mut state = self.state.lock().map_err(|_| ArrowQueryError::WorkerLost)?;
        while !state.cancelled && bytes > self.limit - state.used {
            state = self
                .changed
                .wait(state)
                .map_err(|_| ArrowQueryError::WorkerLost)?;
        }
        if state.cancelled {
            return Err(ArrowQueryError::Cancelled);
        }
        state.used += bytes;
        Ok(BytesLease {
            budget: self.clone(),
            bytes,
        })
    }
    fn cancel(&self) {
        let hook = {
            let mut state = self.state.lock().expect("query budget lock");
            state.cancelled = true;
            state.cancel_hook.take()
        };
        self.changed.notify_all();
        if let Some(hook) = hook {
            interrupt(hook.as_ref());
        }
    }
}
// A Host callback must not unwind out of query cancellation or Drop.
fn interrupt(hook: &(dyn Fn() + Send + Sync)) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(hook));
}
struct BytesLease {
    budget: Arc<Budget>,
    bytes: usize,
}
impl BytesLease {
    fn shrink(&mut self, bytes: usize) {
        let mut state = self.budget.state.lock().expect("query budget lock");
        state.used -= self.bytes - bytes;
        self.bytes = bytes;
        self.budget.changed.notify_all();
    }
}
impl Drop for BytesLease {
    fn drop(&mut self) {
        self.budget.state.lock().expect("query budget lock").used -= self.bytes;
        self.budget.changed.notify_all();
    }
}
struct RetainedBatch {
    batch: RecordBatch,
    _bytes: BytesLease,
    _resource: Arc<Mutex<ResourceLease>>,
}
/// Clone this lease when sharing a batch. Borrow its Arrow data while a lease lives.
/// Independent `RecordBatch`/array clones can escape accounting; the Host must keep
/// a lease with every such shared buffer until its final consumer releases it.
#[derive(Clone)]
pub struct ArrowBatchLease(Arc<RetainedBatch>);
impl ArrowBatchLease {
    #[must_use]
    pub fn batch(&self) -> &RecordBatch {
        &self.0.batch
    }
}
/// Bounded blocking-producer sink; it never runs native work on async workers.
pub struct ArrowQueryEmitter {
    schema: SchemaRef,
    limits: ArrowQueryLimits,
    summary: ArrowQuerySummary,
    sender: mpsc::Sender<ArrowBatchLease>,
    budget: Arc<Budget>,
    resource: Arc<Mutex<ResourceLease>>,
    failure: Option<ArrowQueryError>,
}
impl ArrowQueryEmitter {
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    /// Register the native driver's thread-safe interrupt before execution.
    /// The hook must be short and nonblocking. Registration races are handled;
    /// an already-cancelled consumer invokes the hook and refuses execution.
    /// Hooks are removed before native worker/resource completion.
    /// # Errors
    /// Refuses registration after cancellation or a poisoned budget lock.
    pub fn on_cancel(
        &mut self,
        cancel: impl Fn() + Send + Sync + 'static,
    ) -> Result<(), ArrowQueryError> {
        let retained = self.resource.clone();
        let hook: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let _lease = &retained;
            cancel();
        });
        let cancelled = {
            let mut state = self
                .budget
                .state
                .lock()
                .map_err(|_| ArrowQueryError::WorkerLost)?;
            if state.cancelled {
                true
            } else {
                state.cancel_hook = Some(hook.clone());
                false
            }
        };
        if cancelled {
            interrupt(hook.as_ref());
            self.failure = Some(ArrowQueryError::Cancelled);
            Err(ArrowQueryError::Cancelled)
        } else {
            Ok(())
        }
    }
    /// Refuse a known native result size before exposing any batches.
    /// # Errors
    /// Refuses output beyond the admitted total row limit.
    pub fn check_rows(&self, rows: usize) -> Result<(), ArrowQueryError> {
        if rows > self.limits.max_rows {
            Err(ArrowQueryError::Limit)
        } else {
            Ok(())
        }
    }
    #[cfg(feature = "duckdb")]
    pub(crate) fn fail(&mut self, error: ArrowQueryError) {
        self.failure.get_or_insert(error);
    }
    /// Reserve the maximum batch budget BEFORE fetching/allocating its payload.
    /// Only one producer uses this emitter. Native allocations inside `fetch`
    /// need the Host's separate engine budget and may exceed a rejected batch.
    /// # Errors
    /// Propagates fetch errors, schema/row/byte limits, and consumer cancellation.
    /// Every refusal is sticky even if a producer mistakenly ignores it.
    pub fn emit(
        &mut self,
        fetch: impl FnOnce() -> Result<RecordBatch, ArrowQueryError>,
    ) -> Result<(), ArrowQueryError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        // Known-size producers refuse before fetching an extra batch.
        if self.summary.batches >= self.limits.max_batches {
            self.failure = Some(ArrowQueryError::Limit);
            return Err(ArrowQueryError::Limit);
        }
        self.emit_next(|| fetch().map(Some)).map(|_| ())
    }
    /// Fetch one batch from a fallible source, reserving output bytes first.
    /// Returns `true` for a delivered batch and `false` only for explicit EOF.
    /// The source must distinguish EOF from fetch/conversion errors; wrapping an
    /// iterator that hides errors in `None` does not satisfy this contract.
    /// At the batch limit, one bounded fetch is necessary to distinguish exact
    /// completion from excess output. An excess batch is never delivered.
    /// As with `emit`, this reservation does not bound native engine allocations.
    /// # Errors
    /// Fetch errors and output refusals are sticky, including errors after the
    /// final allowed batch. Consumer cancellation wakes a pending reservation.
    pub fn emit_next(
        &mut self,
        fetch: impl FnOnce() -> Result<Option<RecordBatch>, ArrowQueryError>,
    ) -> Result<bool, ArrowQueryError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        let result = self.emit_next_inner(fetch);
        if let Err(error) = result {
            self.failure = Some(error);
        }
        result
    }
    fn emit_next_inner(
        &mut self,
        fetch: impl FnOnce() -> Result<Option<RecordBatch>, ArrowQueryError>,
    ) -> Result<bool, ArrowQueryError> {
        let mut bytes = self.budget.reserve(self.limits.max_batch_bytes)?;
        let Some(batch) = fetch()? else {
            return Ok(false);
        };
        if self.summary.batches >= self.limits.max_batches {
            return Err(ArrowQueryError::Limit);
        }
        if batch.schema() != self.schema {
            return Err(ArrowQueryError::Schema);
        }
        let rows = self
            .summary
            .rows
            .checked_add(batch.num_rows())
            .ok_or(ArrowQueryError::Limit)?;
        self.check_rows(rows)?;
        let size = batch.get_array_memory_size();
        if size > self.limits.max_batch_bytes {
            return Err(ArrowQueryError::Limit);
        }
        bytes.shrink(size);
        self.sender
            .blocking_send(ArrowBatchLease(Arc::new(RetainedBatch {
                batch,
                _bytes: bytes,
                _resource: self.resource.clone(),
            })))
            .map_err(|_| ArrowQueryError::Cancelled)?;
        self.summary.rows = rows;
        self.summary.batches += 1;
        Ok(true)
    }
}
/// Consume physical batches, releasing old leases to permit more output.
/// `summary` exists only after explicit successful terminal completion.
pub struct ArrowQuery {
    schema: SchemaRef,
    receiver: mpsc::Receiver<ArrowBatchLease>,
    terminal: Option<oneshot::Receiver<Result<ArrowQuerySummary, ArrowQueryError>>>,
    completed: Option<Result<ArrowQuerySummary, ArrowQueryError>>,
    budget: Arc<Budget>,
}
impl ArrowQuery {
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    #[must_use]
    pub fn summary(&self) -> Option<ArrowQuerySummary> {
        self.completed.and_then(Result::ok)
    }
    /// Cancel output and wake native output waits. A running native call retains
    /// its shared worker/resource lease until it returns. Already delivered
    /// batches retain their leases; this query cannot report successful completion.
    pub fn cancel(&mut self) {
        if self.completed.is_some() {
            return;
        }
        self.budget.cancel();
        self.receiver.close();
        while self.receiver.try_recv().is_ok() {}
        self.completed = Some(Err(ArrowQueryError::Cancelled));
    }
    /// Only a successful producer terminal report can return EOF. Cancellation
    /// during this wait preserves the receiver and terminal channel for retry.
    /// # Errors
    /// Late driver errors, missing terminal reports and worker panic never become EOF.
    pub async fn next_batch(&mut self) -> Result<Option<ArrowBatchLease>, ArrowQueryError> {
        if let Some(result) = self.completed {
            return result.map(|_| None);
        }
        if let Some(batch) = self.receiver.recv().await {
            return Ok(Some(batch));
        }
        let Some(terminal) = self.terminal.as_mut() else {
            self.completed = Some(Err(ArrowQueryError::WorkerLost));
            return Err(ArrowQueryError::WorkerLost);
        };
        let result = terminal.await.unwrap_or(Err(ArrowQueryError::WorkerLost));
        self.terminal = None;
        self.completed = Some(result);
        result.map(|_| None)
    }
}
impl Drop for ArrowQuery {
    fn drop(&mut self) {
        self.budget.cancel();
        self.receiver.close();
    }
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn dispatch(
    runtime: &Handle,
    slots: Arc<Semaphore>,
    shared: Arc<Semaphore>,
    lease: ResourceLease,
    schema: SchemaRef,
    limits: ArrowQueryLimits,
    run: impl FnOnce(&mut ArrowQueryEmitter) -> Result<(), ArrowQueryError> + Send + 'static,
) -> ArrowQuery {
    let (sender, receiver) = mpsc::channel(limits.channel_capacity);
    let (terminal_tx, terminal_rx) = oneshot::channel();
    let budget = Arc::new(Budget {
        limit: limits.max_retained_bytes,
        state: Mutex::new(BudgetState {
            used: 0,
            cancelled: false,
            cancel_hook: None,
        }),
        changed: Condvar::new(),
    });
    let query = ArrowQuery {
        schema: schema.clone(),
        receiver,
        terminal: Some(terminal_rx),
        completed: None,
        budget: budget.clone(),
    };
    let blocking = runtime.clone();
    runtime.spawn(async move {
        let permit = tokio::select! {
            biased;
            () = sender.closed() => return,
            permit = slots.acquire_owned() => permit.expect("resource slots never closed"),
        };
        let shared_permit = tokio::select! {
            biased;
            () = sender.closed() => return,
            permit = shared.acquire_owned() => permit.expect("shared slots never closed"),
        };
        let resource = Arc::new(Mutex::new(lease.submitted()));
        blocking.spawn_blocking(move || {
            let mut emitter = ArrowQueryEmitter {
                schema,
                limits,
                summary: ArrowQuerySummary {
                    rows: 0,
                    batches: 0,
                },
                sender,
                budget,
                resource: resource.clone(),
                failure: None,
            };
            let outcome = if emitter.sender.is_closed() {
                Err(ArrowQueryError::Cancelled)
            } else {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&mut emitter)))
                    .unwrap_or(Err(ArrowQueryError::WorkerLost))
            };
            let result = outcome.and_then(|()| emitter.failure.map_or(Ok(emitter.summary), Err));
            let hook = emitter
                .budget
                .state
                .lock()
                .expect("query budget lock")
                .cancel_hook
                .take();
            drop(hook);
            drop(emitter);
            resource.lock().expect("resource lease lock").finished();
            drop(resource);
            drop(shared_permit);
            drop(permit);
            let _ = terminal_tx.send(result);
        });
    });
    query
}
