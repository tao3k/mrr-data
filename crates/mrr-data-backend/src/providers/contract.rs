//! Blocking metadata capabilities; the engine isolates them from executor threads.
use crate::{BackendError, ProviderCapabilities, StoredOutcome, StoredRevision, StoredWrite};
use mrr_data_content::{ConditionalCommitPortError, ContentRevision, PublishReceipt};
/// Separate known refusal, validation failure and ambiguous commit.
pub type ProviderResult<T> = Result<T, ConditionalCommitPortError<BackendError, ()>>;
/// Implementations serialize head/operation decisions and invoke the validator
/// inside that protected transaction, only for a new write. The Host additionally
/// holds authority synchronization through completion; this lock protects metadata
/// and does not magically lock an external key/revocation registry.
/// Provider constructors are configuration only. Opening must recover trusted
/// metadata before success. Failures during COMMIT are Unknown. Historical replay
/// never calls the validator and never emits a fresh-effect permission.
pub trait MetadataProvider: Send + Sync + 'static {
    fn capabilities(&self) -> ProviderCapabilities;
    /// # Errors
    /// Refuses inaccessible, corrupt or incompatible provider state.
    fn open(&self) -> Result<(), BackendError>;
    /// # Errors
    /// Preserves protocol, known no-write and uncertain outcomes.
    fn commit(
        &self,
        write: &StoredWrite,
        physical: Option<&PublishReceipt>,
        validate: &mut dyn FnMut(Option<ContentRevision>) -> bool,
    ) -> ProviderResult<StoredOutcome>;
    /// # Errors
    /// Failed lookup is not confirmed absence.
    fn recover(&self, write: &StoredWrite) -> ProviderResult<Option<StoredRevision>>;
    /// Close after all accepted workers finish. Repeated engine shutdown caches
    /// the same report. Other backend instances remain independently owned.
    /// # Errors
    /// A failed barrier/close does not establish clean shutdown.
    fn close(&self) -> Result<(), BackendError>;
}
