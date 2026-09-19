//! Provider-neutral read-through and remote publication protocol.

use std::{future::Future, pin::Pin};

use cid::Cid;

use crate::{AsyncContentStore, ContentBlock, ContentError, store::codec_for};

/// Runtime-independent future returned by a remote adapter.
pub type RemoteFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, RemoteError>> + Send + 'a>>;

/// Transport failures are never interpreted as a missing object.
/// Credentials and provider response bodies must not be included here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoteError {
    Unavailable,
    PermissionDenied,
    InvalidConfiguration,
    DeadlineExceeded,
    Conflict,
    Corrupt,
    TooLarge,
    Cancelled,
    RequestBudgetExceeded,
    TransferBudgetExceeded,
}

impl std::fmt::Display for RemoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "remote content error: {self:?}")
    }
}

impl std::error::Error for RemoteError {}

/// Immutable, CID-addressed remote content. Endpoint, credentials, TLS and retry
/// configuration belong to the adapter, never to content manifests.
///
/// An adapter may use S3 objects or Kache packs internally. Keys must preserve
/// the complete CID (including codec); logical bytes are the original block,
/// not a compressed pack. Implementations own transport deadlines and staging.
pub trait RemoteContentStore: Send + Sync {
    /// `None` means confirmed absence only. Enforce `max_bytes` while receiving
    /// and decompressing, before allocating an oversized logical block. The
    /// coordinator independently checks the returned bytes and CID.
    fn get<'a>(&'a self, cid: &'a Cid, max_bytes: usize) -> RemoteFuture<'a, Option<Vec<u8>>>;

    /// Success means the provider acknowledged the complete readable object
    /// (including any index/manifest). Repeating the same block is idempotent.
    /// Never overwrite different content at the same address. On cancellation
    /// or error, remote persistence may have occurred; retry by the same CID.
    fn put<'a>(&'a self, block: ContentBlock<'a>) -> RemoteFuture<'a, ()>;
}

/// Cache persistence is independent of verified content availability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CacheAdmission {
    Stored,
    Failed(ContentError),
}

/// Read provenance and the outcome of filling the disposable local cache.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContentSource {
    Local,
    Remote(CacheAdmission),
}

/// Verified bytes returned by the protocol.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContentRead {
    pub bytes: Vec<u8>,
    pub source: ContentSource,
}

/// A receipt is returned only after remote acknowledgement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishReceipt {
    pub cid: Cid,
    pub cache: CacheAdmission,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContentProtocolError {
    Content(ContentError),
    Remote(RemoteError),
    TooLarge { limit: usize, actual: usize },
}

impl std::fmt::Display for ContentProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "content protocol error: {self:?}")
    }
}

impl std::error::Error for ContentProtocolError {}

fn verify(cid: &Cid, bytes: &[u8], limit: usize) -> Result<(), ContentProtocolError> {
    if bytes.len() > limit {
        return Err(ContentProtocolError::TooLarge {
            limit,
            actual: bytes.len(),
        });
    }
    let codec = codec_for(cid).map_err(ContentProtocolError::Content)?;
    let actual = ContentBlock::new(codec, bytes).cid();
    if actual != *cid {
        return Err(ContentProtocolError::Content(ContentError::CidMismatch {
            expected: Box::new(*cid),
            actual: Box::new(actual),
        }));
    }
    Ok(())
}

async fn admit(
    local: &(impl AsyncContentStore + ?Sized),
    block: ContentBlock<'_>,
) -> CacheAdmission {
    match local.store(block).await {
        Ok(actual) if actual == block.cid() => CacheAdmission::Stored,
        Ok(actual) => CacheAdmission::Failed(ContentError::CidMismatch {
            expected: Box::new(block.cid()),
            actual: Box::new(actual),
        }),
        Err(error) => CacheAdmission::Failed(error),
    }
}

/// Reads local content first, then fetches and verifies a remote miss before
/// cache admission. Local corruption is explicit; callers decide repair policy.
/// `max_bytes` is passed to both stores before data is buffered; returned bytes
/// are independently checked before they are accepted.
/// Concurrent misses may fetch twice; coalescing/eviction belongs to the cache.
///
/// # Errors
/// Returns local integrity/I/O errors, remote failures, or invalid/oversized data.
pub async fn read_through(
    local: &(impl AsyncContentStore + ?Sized),
    remote: &(impl RemoteContentStore + ?Sized),
    cid: &Cid,
    max_bytes: usize,
) -> Result<Option<ContentRead>, ContentProtocolError> {
    let codec = codec_for(cid).map_err(ContentProtocolError::Content)?;
    match local.load(cid, max_bytes).await {
        Ok(bytes) => {
            verify(cid, &bytes, max_bytes)?;
            return Ok(Some(ContentRead {
                bytes,
                source: ContentSource::Local,
            }));
        }
        Err(ContentError::NotFound(missing)) if *missing == *cid => {}
        Err(error) => return Err(ContentProtocolError::Content(error)),
    }
    let Some(bytes) = remote
        .get(cid, max_bytes)
        .await
        .map_err(ContentProtocolError::Remote)?
    else {
        return Ok(None);
    };
    verify(cid, &bytes, max_bytes)?;
    let admission = admit(local, ContentBlock::new(codec, &bytes)).await;
    Ok(Some(ContentRead {
        bytes,
        source: ContentSource::Remote(admission),
    }))
}

/// Publishes immutable content and then opportunistically fills the local cache.
/// A failed cache admission does not erase the remote acknowledgement.
///
/// # Errors
/// Returns a remote failure; no success receipt is issued for a local-only write.
pub async fn publish_content(
    local: &(impl AsyncContentStore + ?Sized),
    remote: &(impl RemoteContentStore + ?Sized),
    block: ContentBlock<'_>,
) -> Result<PublishReceipt, ContentProtocolError> {
    remote
        .put(block)
        .await
        .map_err(ContentProtocolError::Remote)?;
    Ok(PublishReceipt {
        cid: block.cid(),
        cache: admit(local, block).await,
    })
}
