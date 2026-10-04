//! All file and dataset ACKs precede the publication authority gate and root ACK.
use super::PreparedCombinedGraph;
use crate::{
    AsyncContentStore, ContentBlock, ContentCodec, GraphTransferError as Error, PublishReceipt,
    RemoteContentStore, publish_content,
};
use std::future::Future;

/// A successful gate is a simulation/Host policy check; `root_remote` retains the
/// actual authority guard across root PUT. Protected discovery CAS is separate.
/// # Errors
/// Returns no complete receipt for failed/lost ACKs, cancellation or denied root.
pub async fn publish_combined_graph<F, Fut>(
    prepared: &PreparedCombinedGraph,
    local: &(dyn AsyncContentStore + Sync),
    remote: &dyn RemoteContentStore,
    root_remote: &dyn RemoteContentStore,
    before_root: F,
) -> Result<PublishReceipt, Error>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), Error>>,
{
    for (cid, bytes) in &prepared.blocks {
        if cid == prepared.root() || *cid == prepared.dataset_root {
            continue;
        }
        publish_content(
            local,
            remote,
            ContentBlock::new(ContentCodec::from_cid(cid)?, bytes),
        )
        .await?;
    }
    publish_content(
        local,
        remote,
        ContentBlock::new(
            ContentCodec::DagCbor,
            &prepared.blocks[&prepared.dataset_root],
        ),
    )
    .await?;
    before_root().await?;
    Ok(publish_content(
        local,
        root_remote,
        ContentBlock::new(ContentCodec::DagCbor, prepared.snapshot.bytes()),
    )
    .await?)
}
