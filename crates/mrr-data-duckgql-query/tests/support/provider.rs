use mrr_data_backend::{
    AuthorityCapability, BackendError, MetadataProvider, ProviderCapabilities, StoredOutcome,
    StoredRevision, StoredWrite, providers::ProviderResult,
};
use mrr_data_content::{ContentRevision, PublishReceipt};
pub(crate) struct Provider;
impl MetadataProvider for Provider {
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            atomic_head_operation: true,
            durable_commit: true,
            historical_lookup: true,
            authority_versions: AuthorityCapability::Unsupported,
        }
    }
    fn open(&self) -> Result<(), BackendError> {
        Ok(())
    }
    fn close(&self) -> Result<(), BackendError> {
        Ok(())
    }
    fn commit(
        &self,
        _: &StoredWrite,
        _: Option<&PublishReceipt>,
        _: &mut dyn FnMut(Option<ContentRevision>) -> bool,
    ) -> ProviderResult<StoredOutcome> {
        panic!("resource-only fixture")
    }
    fn recover(&self, _: &StoredWrite) -> ProviderResult<Option<StoredRevision>> {
        panic!("resource-only fixture")
    }
}
