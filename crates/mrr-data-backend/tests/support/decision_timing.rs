//! Delegate the existing semantic engine; time its unchanged validator bridge.
use super::storage_timing::TimedStorage;
use mrr_data_backend::{
    AuthorityChange, AuthorityKey, AuthorityState, BackendError, MetadataProvider,
    ProviderCapabilities, PublicationDelivery, StoredOutcome, StoredRevision, StoredWrite,
    providers::ProviderResult,
};
use mrr_data_content::{ContentRevision, PublishReceipt};
pub struct TimedDecisions(pub TimedStorage);
impl MetadataProvider for TimedDecisions {
    fn capabilities(&self) -> ProviderCapabilities {
        self.0.capabilities()
    }
    fn open(&self) -> Result<(), BackendError> {
        self.0.open()
    }
    fn close(&self) -> Result<(), BackendError> {
        self.0.close()
    }
    fn recover(&self, write: &StoredWrite) -> ProviderResult<Option<StoredRevision>> {
        self.0.recover(write)
    }
    fn commit(
        &self,
        write: &StoredWrite,
        physical: Option<&PublishReceipt>,
        validate: &mut dyn FnMut(Option<ContentRevision>) -> bool,
    ) -> ProviderResult<StoredOutcome> {
        self.0.commit(write, physical, &mut |current| {
            self.0.timings.bridge(|| validate(current))
        })
    }
    fn authority(&self, key: &AuthorityKey) -> ProviderResult<Option<AuthorityState>> {
        self.0.authority(key)
    }
    fn advance_authority(&self, change: &AuthorityChange) -> ProviderResult<AuthorityState> {
        self.0.advance_authority(change)
    }
    fn publication_delivery(
        &self,
        home: &[String; 3],
        revision: u64,
    ) -> ProviderResult<Option<PublicationDelivery>> {
        self.0.publication_delivery(home, revision)
    }
    fn acknowledge_publication(&self, row: &PublicationDelivery) -> ProviderResult<()> {
        self.0.acknowledge_publication(row)
    }
}
