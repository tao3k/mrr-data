//! Shared opaque Host obligation fixture, not a budget or authorization model.
use mrr_data_content::{
    ConditionalContentWrite, ContentBlock, ContentCodec, ContentStore, FilesystemContentStore,
    RemoteContentStore, RemoteError, RemoteFuture,
};
#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct RetainedObligation {
    pub(crate) claim: String,
    pub(crate) reservation: String,
    pub(crate) unknown_exposure: String,
    pub(crate) history: String,
    pub(crate) reserved: u64,
    pub(crate) held: u64,
}

// Acknowledged file storage for this native experiment only. It delegates all
// content addressing, bounded reads and durable writes to MRR's existing store.
pub(crate) struct AcknowledgedFiles(pub(crate) FilesystemContentStore);
impl RemoteContentStore for AcknowledgedFiles {
    fn get<'a>(&'a self, cid: &'a cid::Cid, limit: usize) -> RemoteFuture<'a, Option<Vec<u8>>> {
        let store = self.0.clone();
        let cid = *cid;
        Box::pin(async move {
            tokio::task::spawn_blocking(move || store.get_bounded(&cid, limit))
                .await
                .map_err(|_| RemoteError::Unavailable)?
                .map(Some)
                .map_err(|_| RemoteError::Unavailable)
        })
    }
    fn put<'a>(&'a self, block: ContentBlock<'a>) -> RemoteFuture<'a, ()> {
        if block.bytes().len() > 8192 {
            return Box::pin(async { Err(RemoteError::TooLarge) });
        }
        let store = self.0.clone();
        let codec = block.codec();
        let bytes = block.bytes().to_vec();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || store.put(ContentBlock::new(codec, &bytes)))
                .await
                .map_err(|_| RemoteError::Unavailable)?
                .map(|_| ())
                .map_err(|_| RemoteError::Unavailable)
        })
    }
}
pub(crate) fn body() -> Vec<u8> {
    body_with_burn(true)
}
pub(crate) fn body_with_burn(burned: bool) -> Vec<u8> {
    let exposure = RetainedObligation {
        claim: "original-claim".into(),
        reservation: "original-reservation".into(),
        unknown_exposure: "original-unknown-exposure".into(),
        history: "original-history-prefix".into(),
        reserved: 4,
        held: 4,
    };
    // Opaque Host data: the backend preserves identity and bytes, not budget arithmetic.
    serde_json::to_vec(&("original-budget/history-scope", burned, exposure)).expect("typed body")
}
pub(crate) fn write<'a>(operation: &'a str, bytes: &[u8]) -> ConditionalContentWrite<'a> {
    ConditionalContentWrite {
        scope: "original-budget/history-scope",
        operation_id: operation,
        expected: None,
        replacement: ContentBlock::new(ContentCodec::Raw, bytes).cid(),
    }
}
