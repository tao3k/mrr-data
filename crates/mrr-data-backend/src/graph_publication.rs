//! Complete physical graph ACKs feed the existing protected head transaction.
use crate::{BackendError, ProfilePort};
use mrr_data_content::{
    ConditionalCommitFuture, ConditionalContentCommitOutcome, ConditionalContentCommitPort,
    ConditionalContentWrite, ContentRevision, GraphPublication,
};
impl ProfilePort {
    /// Select a complete graph closure through this profile's enrolled authority
    /// versions and fresh validator. None permits only exact historical replay.
    /// Host guards stay held through completion; an Unknown outcome is recovered
    /// under the original operation identity, never a new replacement operation.
    /// # Errors
    /// Preserves missing/wrong ACK, stale authority, validation, conflict and Unknown.
    pub fn commit_graph_publication<'a, V, F>(
        &'a self,
        write: ConditionalContentWrite<'a>,
        publication: Option<&'a GraphPublication>,
        validate: F,
    ) -> ConditionalCommitFuture<'a, ConditionalContentCommitOutcome<'a>, BackendError, V>
    where
        V: Send + 'a,
        F: FnOnce(Option<ContentRevision>) -> Result<(), V> + Send + 'a,
    {
        self.commit(write, publication.map(GraphPublication::receipt), validate)
    }
}
