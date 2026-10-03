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
        if status.active_writes == 0 && status.active_recoveries == 0 {
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
    async fn authority_work<T: Send + 'static>(
        &self,
        recovery: bool,
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
            .admit(recovery, 16384)
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
        self.authority_work(true, move |p| p.authority(&key)).await
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
        self.authority_work(false, move |p| p.advance_authority(&change))
            .await
    }
}
