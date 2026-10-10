//! Runtime borrowing, bounded admission and cancellation-safe worker ownership.
use crate::{
    BackendConfig, BackendError, BackendStatus, Lifecycle, MetadataProvider, StoredOutcome,
    StoredWrite, scheduler::Scheduler,
};
use mrr_data_content::{
    ConditionalCommitFuture, ConditionalCommitPortError as PortError,
    ConditionalContentCommitOutcome, ConditionalContentCommitPort, ConditionalContentReceipt,
    ConditionalContentWrite, ContentRevision, PublishReceipt,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::{runtime::Handle, sync::oneshot};
struct Inner {
    provider: Arc<dyn MetadataProvider>,
    scheduler: Arc<Scheduler>,
    runtime: Handle,
    dispatcher: crate::dispatch::Dispatcher,
    close_started: AtomicBool,
    close_result: Mutex<Option<Result<(), BackendError>>>,
}
/// Shared engine. Drop does not promise async shutdown; the embedding Host keeps
/// its runtime alive until `shutdown` completes. Clone shares all resource limits.
#[derive(Clone)]
pub struct Backend {
    inner: Arc<Inner>,
}
impl Backend {
    /// Borrow an executor and open/recover the provider on a blocking worker.
    /// No process signals or independent runtime are created.
    /// # Errors
    /// Invalid limits/capabilities, corrupt storage or failed opening refuse Ready.
    pub async fn open(
        config: BackendConfig,
        provider: impl MetadataProvider,
        runtime: Handle,
    ) -> Result<Self, BackendError> {
        let config = config.validate()?;
        let capabilities = provider.capabilities();
        if !capabilities.atomic_head_operation
            || !capabilities.durable_commit
            || !capabilities.historical_lookup
        {
            return Err(BackendError::UnsupportedCapabilities);
        }
        let provider: Arc<dyn MetadataProvider> = Arc::new(provider);
        let opening = provider.clone();
        runtime
            .spawn_blocking(move || opening.open())
            .await
            .map_err(|_| BackendError::WorkerLost)??;
        Ok(Self {
            inner: Arc::new(Inner {
                provider,
                scheduler: Scheduler::new(config),
                dispatcher: crate::dispatch::Dispatcher::new(config, runtime.clone()),
                runtime,
                close_started: AtomicBool::new(false),
                close_result: Mutex::new(None),
            }),
        })
    }
    /// Prepare a retained physical resource on the Host blocking executor.
    /// Admission is shared across all profiles. Cancelling a queued request skips
    /// preparation; a running worker retains its lease until it finishes. Returned
    /// handles participate in shutdown until their final clone is dropped.
    /// # Errors
    /// Refuses lifecycle/count/byte limits, preparation failure or lost workers.
    pub async fn prepare_resource<T: Send + Sync + 'static>(
        &self,
        reserved_bytes: usize,
        prepare: impl FnOnce() -> Result<T, BackendError> + Send + 'static,
    ) -> Result<crate::ResourceHandle<T>, BackendError> {
        self.prepare_resource_fallible(reserved_bytes, prepare)
            .await
            .map_err(|error| match error {
                crate::ResourcePreparationError::Backend(error)
                | crate::ResourcePreparationError::Preparation(error) => error,
            })
    }
    /// Prepare retained output while preserving the driver's typed error.
    /// Queued cancellation skips preparation; running work retains admission
    /// through cleanup. Success retains its reservation until the final handle
    /// is dropped. Failure releases admission before returning its error.
    /// # Errors
    /// Distinguishes Backend admission/worker failures from driver refusals.
    pub async fn prepare_resource_fallible<T: Send + Sync + 'static, E: Send + 'static>(
        &self,
        reserved_bytes: usize,
        prepare: impl FnOnce() -> Result<T, E> + Send + 'static,
    ) -> Result<crate::ResourceHandle<T>, crate::ResourcePreparationError<E>> {
        use crate::ResourcePreparationError;
        let lease = self
            .inner
            .scheduler
            .admit_resource(reserved_bytes)
            .map_err(ResourcePreparationError::Backend)?;
        self.inner
            .dispatcher
            .prepare(lease, None, prepare)
            .await
            .map_err(|_| ResourcePreparationError::Backend(BackendError::WorkerLost))?
            .map_err(ResourcePreparationError::Preparation)
    }
    /// Prepare retained output with cooperative stop checkpoints.
    /// Dropping the waiter signals cancellation. Running driver work owns its
    /// lease through cleanup. The driver must check control between native calls;
    /// queued deadlines use the Host runtime timer; running native calls are
    /// not forcibly interrupted.
    /// # Errors
    /// Returns typed stops/refusals or shared Backend admission/worker failures.
    pub async fn prepare_resource_controlled<T, E>(
        &self,
        reserved_bytes: usize,
        control: crate::ResourceControl,
        prepare: impl FnOnce(&crate::ResourceControl) -> Result<T, E> + Send + 'static,
    ) -> Result<crate::ResourceHandle<T>, crate::ResourcePreparationError<E>>
    where
        T: Send + Sync + 'static,
        E: From<crate::ResourceStop> + Send + 'static,
    {
        use crate::ResourcePreparationError;
        let mut cancellation = crate::control::CancelOnDrop::new(control.clone());
        let lease = self
            .inner
            .scheduler
            .admit_resource(reserved_bytes)
            .map_err(ResourcePreparationError::Backend)?;
        let worker_control = control.clone();
        let result = self
            .inner
            .dispatcher
            .prepare(lease, Some((control, E::from)), move || {
                worker_control.check().map_err(E::from)?;
                let value = prepare(&worker_control)?;
                worker_control.check().map_err(E::from)?;
                Ok(value)
            })
            .await
            .map_err(|_| ResourcePreparationError::Backend(BackendError::WorkerLost))?
            .map_err(ResourcePreparationError::Preparation);
        cancellation.disarm();
        result
    }
    /// Prepare retained resources using async provider I/O on the borrowed Host runtime.
    /// Admission and worker permits are shared with physical preparation. Dropping
    /// the waiter signals cancellation; started work keeps its lease through driver
    /// cleanup and must check control between provider calls. No task is aborted.
    /// # Errors
    /// Preserves typed driver stops and shared admission/worker failures.
    pub async fn prepare_resource_async_controlled<T, E, F>(
        &self,
        reserved_bytes: usize,
        control: crate::ResourceControl,
        prepare: impl FnOnce(crate::ResourceControl) -> F + Send + 'static,
    ) -> Result<crate::ResourceHandle<T>, crate::ResourcePreparationError<E>>
    where
        T: Send + Sync + 'static,
        E: From<crate::ResourceStop> + Send + 'static,
        F: std::future::Future<Output = Result<T, E>> + Send + 'static,
    {
        use crate::ResourcePreparationError;
        let mut cancellation = crate::control::CancelOnDrop::new(control.clone());
        let lease = self
            .inner
            .scheduler
            .admit_resource(reserved_bytes)
            .map_err(ResourcePreparationError::Backend)?;
        let result = self
            .inner
            .dispatcher
            .prepare_async(lease, control, prepare)
            .await
            .map_err(|_| ResourcePreparationError::Backend(BackendError::WorkerLost))?
            .map_err(ResourcePreparationError::Preparation);
        cancellation.disarm();
        result
    }
    /// Run one physical operation on the bounded resource worker lane and
    /// return its owned result. The reservation covers the worker, not the
    /// returned value; the Host must bound result size before returning it.
    /// Cancelling a queued waiter skips work. A running worker holds its lease
    /// until completion, including after waiter cancellation.
    /// # Errors
    /// Refuses lifecycle/count/byte limits or reports a lost worker.
    pub async fn run_resource<T: Send + 'static>(
        &self,
        reserved_bytes: usize,
        run: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, BackendError> {
        let lease = self.inner.scheduler.admit_resource(reserved_bytes)?;
        self.inner
            .dispatcher
            .run_resource(lease, run)
            .await
            .map_err(|_| BackendError::WorkerLost)
    }
    /// Run a fallible physical Arrow producer on shared resource admission.
    /// The Host admits the semantic query/schema and budgets native memory first.
    /// Reservations include retained batches, input/native state and one driver fetch.
    /// Retained batch clones hold the same drain barrier. Dropping the consumer
    /// cancels queued work and wakes output waits; running native calls must finish.
    /// # Errors
    /// Refuses invalid limits, undersized reservations and sealed/saturated admission.
    #[cfg(feature = "arrow-query")]
    pub fn query_arrow(
        &self,
        schema: arrow_schema::SchemaRef,
        limits: crate::ArrowQueryLimits,
        reserved_bytes: usize,
        run: impl FnOnce(&mut crate::ArrowQueryEmitter) -> Result<(), crate::ArrowQueryError>
        + Send
        + 'static,
    ) -> Result<crate::ArrowQuery, crate::ArrowQueryError> {
        limits.validate()?;
        if reserved_bytes < limits.max_retained_bytes {
            return Err(crate::ArrowQueryError::Limit);
        }
        let lease = self
            .inner
            .scheduler
            .admit_resource(reserved_bytes)
            .map_err(crate::ArrowQueryError::Backend)?;
        Ok(self.inner.dispatcher.query(lease, schema, limits, run))
    }
    /// Select Host-enrolled profile and stable deployment namespace. Versioning
    /// must never silently move pending authority to a fresh namespace.
    /// # Errors
    /// Empty/oversized bindings or a sealed lifecycle refuse new bindings.
    pub fn profile(&self, profile: &str, namespace: &str) -> Result<ProfilePort, BackendError> {
        self.check_ids(&[profile, namespace])?;
        if self.status().lifecycle != Lifecycle::Ready {
            return Err(BackendError::NotReady);
        }
        Ok(ProfilePort {
            backend: self.clone(),
            profile: profile.into(),
            namespace: namespace.into(),
            authorities: Vec::new(),
        })
    }
    #[must_use]
    pub fn status(&self) -> BackendStatus {
        self.inner.scheduler.status()
    }
    fn check_ids(&self, ids: &[&str]) -> Result<(), BackendError> {
        if ids
            .iter()
            .any(|id| id.is_empty() || id.len() > self.inner.scheduler.config.max_identity_bytes)
        {
            return Err(BackendError::Limit);
        }
        Ok(())
    }
    /// Seal admission atomically, finish every accepted worker, then close provider
    /// handles. Cancellation of this waiter does not cancel the owned drain task.
    /// Concurrent/repeated callers receive the same cached terminal report.
    /// # Errors
    /// Failed close reports Faulted; it does not establish a durability barrier.
    pub async fn shutdown(&self) -> Result<(), BackendError> {
        self.inner.scheduler.drain();
        if !self.inner.close_started.swap(true, Ordering::AcqRel) {
            let inner = self.inner.clone();
            self.inner.runtime.spawn(async move {
                finish_close(inner).await;
            });
        }
        loop {
            let changed = self.inner.scheduler.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(result) = *self
                .inner
                .close_result
                .lock()
                .map_err(|_| BackendError::Unavailable)?
            {
                return result;
            }
            changed.await;
        }
    }
}
async fn finish_close(inner: Arc<Inner>) {
    loop {
        let changed = inner.scheduler.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let status = inner.scheduler.status();
        if status.active_writes == 0
            && status.active_recoveries == 0
            && status.active_resources == 0
        {
            break;
        }
        changed.await;
    }
    inner.scheduler.lifecycle(Lifecycle::Closing);
    let provider = inner.provider.clone();
    let result = inner
        .runtime
        .spawn_blocking(move || provider.close())
        .await
        .unwrap_or(Err(BackendError::WorkerLost));
    *inner.close_result.lock().expect("close report lock") = Some(result);
    inner.scheduler.lifecycle(if result.is_ok() {
        Lifecycle::Closed
    } else {
        Lifecycle::Faulted
    });
}
/// Domain-neutral content port. Validators may borrow Host state, but must keep
/// its authority guard alive through commit completion. Never await this port
/// from the validator itself: it executes while the provider transaction is open.
#[derive(Clone)]
pub struct ProfilePort {
    backend: Backend,
    profile: String,
    namespace: String,
    authorities: Vec<crate::AuthorityExpectation>,
}
impl ProfilePort {
    fn owned(&self, write: ConditionalContentWrite<'_>) -> StoredWrite {
        StoredWrite {
            profile: self.profile.clone(),
            namespace: self.namespace.clone(),
            scope: write.scope.into(),
            operation_id: write.operation_id.into(),
            expected: write.expected.map(Into::into),
            replacement: write.replacement,
            authorities: self.authorities.clone(),
        }
    }
    fn size(&self, write: ConditionalContentWrite<'_>) -> Result<usize, BackendError> {
        self.backend.check_ids(&[write.scope, write.operation_id])?;
        Ok(2048
            + 8 * (self.profile.len()
                + self.namespace.len()
                + write.scope.len()
                + write.operation_id.len())
            + self
                .authorities
                .iter()
                .map(|g| 1024 + 8 * g.authority_id.len())
                .sum::<usize>())
    }
}
fn outcome(
    write: ConditionalContentWrite<'_>,
    stored: StoredOutcome,
) -> ConditionalContentCommitOutcome<'_> {
    let receipt = ConditionalContentReceipt {
        write,
        committed: stored.committed.into(),
    };
    if stored.replayed {
        ConditionalContentCommitOutcome::Replayed(receipt)
    } else {
        ConditionalContentCommitOutcome::Committed(receipt)
    }
}
fn port_error<V>(error: PortError<BackendError, ()>) -> PortError<BackendError, V> {
    match error {
        PortError::Protocol(e) => PortError::Protocol(e),
        PortError::BeforeCommit(e) => PortError::BeforeCommit(e),
        PortError::Unknown(e) => PortError::Unknown(e),
        PortError::Validation(()) => PortError::BeforeCommit(BackendError::Cancelled),
    }
}
impl ConditionalContentCommitPort for ProfilePort {
    type Error = BackendError;
    fn commit<'a, V, F>(
        &'a self,
        write: ConditionalContentWrite<'a>,
        physical: Option<&'a PublishReceipt>,
        validate_current: F,
    ) -> ConditionalCommitFuture<'a, ConditionalContentCommitOutcome<'a>, Self::Error, V>
    where
        V: Send + 'a,
        F: FnOnce(Option<ContentRevision>) -> Result<(), V> + Send + 'a,
    {
        Box::pin(async move {
            let bytes = self.size(write).map_err(PortError::BeforeCommit)?;
            let lease = self
                .backend
                .inner
                .scheduler
                .admit(false, bytes)
                .map_err(PortError::BeforeCommit)?;
            let owned = self.owned(write);
            // Only the acknowledged CID participates in this metadata protocol.
            // Cache diagnostic strings may be unbounded and are not retained.
            let physical = physical.map(|p| PublishReceipt {
                cid: p.cid,
                cache: mrr_data_content::CacheAdmission::Stored,
            });
            let provider = self.backend.inner.provider.clone();
            let (request_tx, request_rx) = oneshot::channel();
            let (answer_tx, answer_rx) = oneshot::channel();
            let result_rx = self.backend.inner.dispatcher.run(false, lease, move || {
                let mut request = Some((request_tx, answer_rx));
                provider.commit(&owned, physical.as_ref(), &mut |current| {
                    let Some((tx, rx)) = request.take() else {
                        return false;
                    };
                    tx.send(current).is_ok() && rx.blocking_recv().unwrap_or(false)
                })
            });
            if let Ok(current) = request_rx.await {
                match validate_current(current) {
                    Ok(()) => {
                        let _ = answer_tx.send(true);
                    }
                    Err(error) => {
                        let _ = answer_tx.send(false);
                        return Err(PortError::Validation(error));
                    }
                }
            }
            let result = result_rx
                .await
                .map_err(|_| PortError::Unknown(BackendError::WorkerLost))?
                .map_err(port_error)?;
            Ok(outcome(write, result))
        })
    }
    fn recover<'a>(
        &'a self,
        write: ConditionalContentWrite<'a>,
    ) -> ConditionalCommitFuture<'a, Option<ConditionalContentReceipt<'a>>, Self::Error> {
        Box::pin(async move {
            let bytes = self.size(write).map_err(PortError::BeforeCommit)?;
            let lease = self
                .backend
                .inner
                .scheduler
                .admit(true, bytes)
                .map_err(PortError::BeforeCommit)?;
            let owned = self.owned(write);
            let provider = self.backend.inner.provider.clone();
            let rx = self
                .backend
                .inner
                .dispatcher
                .run(true, lease, move || provider.recover(&owned));
            let result = rx
                .await
                .map_err(|_| PortError::BeforeCommit(BackendError::WorkerLost))?
                .map_err(port_error)?;
            Ok(result.map(|committed| ConditionalContentReceipt {
                write,
                committed: committed.into(),
            }))
        })
    }
}

impl ProfilePort {
    /// Bind validated authority snapshots. Canonical ordering makes exact retries
    /// independent of caller list order. All durable home authorities remain
    /// mandatory, including when a caller uses the original unguarded port.
    /// # Errors
    /// Refuses unsupported providers, duplicate/oversized IDs or over 16 guards.
    pub fn with_authorities(
        &self,
        expected: &[crate::AuthorityExpectation],
    ) -> Result<Self, BackendError> {
        if self
            .backend
            .inner
            .provider
            .capabilities()
            .authority_versions
            != crate::AuthorityCapability::Transactional
        {
            return Err(BackendError::UnsupportedCapabilities);
        }
        if expected.len() > 16 {
            return Err(BackendError::Limit);
        }
        for guard in expected {
            self.backend.check_ids(&[&guard.authority_id])?;
        }
        let mut authorities = expected.to_vec();
        authorities.sort_by(|a, b| a.authority_id.cmp(&b.authority_id));
        if authorities
            .windows(2)
            .any(|w| w[0].authority_id == w[1].authority_id)
        {
            return Err(BackendError::AuthorityConflict);
        }
        let mut port = self.clone();
        port.authorities = authorities;
        Ok(port)
    }
    fn authority_key(
        &self,
        scope: &str,
        authority_id: &str,
    ) -> Result<crate::AuthorityKey, BackendError> {
        self.backend.check_ids(&[scope, authority_id])?;
        Ok(crate::AuthorityKey {
            profile: self.profile.clone(),
            namespace: self.namespace.clone(),
            scope: scope.into(),
            authority_id: authority_id.into(),
        })
    }
    async fn metadata_work<T: Send + 'static>(
        &self,
        recovery: bool,
        reserved_bytes: usize,
        run: impl FnOnce(&dyn MetadataProvider) -> crate::providers::ProviderResult<T> + Send + 'static,
    ) -> crate::providers::ProviderResult<T> {
        if self
            .backend
            .inner
            .provider
            .capabilities()
            .authority_versions
            != crate::AuthorityCapability::Transactional
        {
            return Err(PortError::BeforeCommit(
                BackendError::UnsupportedCapabilities,
            ));
        }
        let lease = self
            .backend
            .inner
            .scheduler
            .admit(recovery, reserved_bytes)
            .map_err(PortError::BeforeCommit)?;
        let provider = self.backend.inner.provider.clone();
        let rx = self
            .backend
            .inner
            .dispatcher
            .run(recovery, lease, move || run(&*provider));
        rx.await.map_err(|_| {
            if recovery {
                PortError::BeforeCommit(BackendError::WorkerLost)
            } else {
                PortError::Unknown(BackendError::WorkerLost)
            }
        })?
    }
    /// Read the Host-owned durable current generation; failed lookup is not absence.
    /// `authority_id` deliberately remains an opaque Host string, bounded before
    /// copying and interpreted only within this configured profile/namespace/scope.
    /// # Errors
    /// Invalid keys, sealed/saturated engine or failed provider observation.
    pub async fn authority(
        &self,
        scope: &str,
        authority_id: &str,
    ) -> crate::providers::ProviderResult<Option<crate::AuthorityState>> {
        let key = self
            .authority_key(scope, authority_id)
            .map_err(PortError::BeforeCommit)?;
        self.metadata_work(true, 16384, move |p| p.authority(&key))
            .await
    }
    /// Host administrative CAS. Enrolling the first record makes its ID mandatory
    /// for every fresh content write in this stable home. No deletion/reset exists.
    /// Dropping the caller after acceptance leaves the update owned by the engine.
    /// # Errors
    /// Invalid proposal, conflict, terminal retirement, saturation or uncertain ACK.
    pub async fn advance_authority(
        &self,
        scope: &str,
        proposal: crate::AuthorityProposal,
    ) -> crate::providers::ProviderResult<crate::AuthorityState> {
        let key = self
            .authority_key(scope, &proposal.authority_id)
            .map_err(PortError::BeforeCommit)?;
        let change = crate::AuthorityChange { key, proposal };
        change.next().map_err(PortError::BeforeCommit)?;
        self.metadata_work(false, 16384, move |p| p.advance_authority(&change))
            .await
    }
}

impl ProfilePort {
    /// Observe an exact durable delivery, without fresh use permission.
    /// # Errors
    /// Rejects invalid homes, failed lookup and corrupt history.
    pub async fn publication_delivery(
        &self,
        scope: &str,
        revision: u64,
    ) -> crate::providers::ProviderResult<Option<crate::PublicationDelivery>> {
        self.backend
            .check_ids(&[scope])
            .map_err(PortError::BeforeCommit)?;
        if revision == 0 {
            return Err(PortError::BeforeCommit(BackendError::Limit));
        }
        let home = [self.profile.clone(), self.namespace.clone(), scope.into()];
        self.metadata_work(true, 65536, move |p| {
            p.publication_delivery(&home, revision)
        })
        .await
    }
    /// Acknowledge delivery; revalidate use authority at each external effect.
    /// # Errors
    /// Substitution, saturated scheduler, corrupt state or Unknown COMMIT.
    pub async fn acknowledge_publication(
        &self,
        row: &crate::PublicationDelivery,
    ) -> crate::providers::ProviderResult<()> {
        if row.write.profile != self.profile || row.write.namespace != self.namespace {
            return Err(PortError::BeforeCommit(BackendError::AuthorityConflict));
        }
        self.backend
            .check_ids(&[&row.write.scope, &row.write.operation_id])
            .map_err(PortError::BeforeCommit)?;
        if row.committed.revision == 0 || row.write.authorities.len() > 16 {
            return Err(PortError::BeforeCommit(BackendError::Limit));
        }
        for a in &row.write.authorities {
            self.backend
                .check_ids(&[&a.authority_id])
                .map_err(PortError::BeforeCommit)?;
        }
        crate::scheme_record::encode(row).map_err(PortError::BeforeCommit)?;
        let owned = row.clone();
        self.metadata_work(false, 65536, move |p| p.acknowledge_publication(&owned))
            .await
    }
}
