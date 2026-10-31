//! Database-neutral physical key/value transaction seam. Logical records, guard
//! comparisons and operation decisions belong to the shared engine.
use super::ProviderResult;
use crate::BackendError;
/// A provider-owned transaction held through validation and commit. It exposes
/// bounded opaque records, not SQL, schema names or domain-specific tables.
pub trait MetadataTransaction {
    /// # Errors
    /// Failed/corrupt lookup cannot be interpreted as absence. Limit before copy.
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, BackendError>;
    /// # Errors
    /// Rejects oversized values; all writes roll back if the callback refuses.
    fn put(&mut self, key: &str, value: &[u8]) -> Result<(), BackendError>;
}
/// Physical provider only. Constructors have no I/O. Implementations own native
/// connection pools, isolation, barrier verification and error translation.
/// The Host protects storage against replacement/rollback and controls access.
pub trait TransactionProvider: Send + Sync + 'static {
    /// # Errors
    /// Refuse incompatible/corrupt storage before exposing a ready provider.
    fn open_storage(&self) -> Result<(), BackendError>;
    /// Separate historical read path; it must not wait on a held local writer.
    /// # Errors
    /// Returns confirmed absence only from a successful bounded lookup.
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, BackendError>;
    /// Invoke the callback once while native isolation protects observations.
    /// Roll back on callback failure. COMMIT ambiguity is always Unknown.
    /// # Errors
    /// Preserves known no-write/refusal versus an uncertain durable outcome.
    fn transaction(
        &self,
        run: &mut dyn FnMut(&mut dyn MetadataTransaction) -> ProviderResult<()>,
    ) -> ProviderResult<()>;
    /// # Errors
    /// Failed native close/barrier is not a clean shutdown report.
    fn close_storage(&self) -> Result<(), BackendError>;
}
