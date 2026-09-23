use std::sync::atomic::{AtomicUsize, Ordering};

use cid::Cid;

use crate::{
    CacheAdmission, ContentBlock, ContentCodec, ContentError, ContentProtocolError, ContentSource,
    ContentStore, MemoryContentStore, RemoteContentStore, RemoteError, RemoteFuture,
    publish_content, read_through,
};

struct Remote {
    response: Result<Option<Vec<u8>>, RemoteError>,
    write: Result<(), RemoteError>,
    reads: AtomicUsize,
}

impl Remote {
    fn new(response: Result<Option<Vec<u8>>, RemoteError>) -> Self {
        Self {
            response,
            write: Ok(()),
            reads: AtomicUsize::new(0),
        }
    }
}

impl RemoteContentStore for Remote {
    fn get<'a>(&'a self, _: &'a Cid, _: usize) -> RemoteFuture<'a, Option<Vec<u8>>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { self.response.clone() })
    }

    fn put<'a>(&'a self, _: ContentBlock<'a>) -> RemoteFuture<'a, ()> {
        Box::pin(async { self.write })
    }
}

fn block() -> ContentBlock<'static> {
    ContentBlock::new(ContentCodec::Raw, b"verified content")
}

#[tokio::test]
async fn cold_read_fills_cache_and_warm_read_skips_remote() {
    let local = MemoryContentStore::default();
    let remote = Remote::new(Ok(Some(block().bytes().to_vec())));
    // Exercise dynamic dispatch: adapters need not be known to the caller.
    let remote_store: &dyn RemoteContentStore = &remote;
    let first = read_through(&local, remote_store, &block().cid(), 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.bytes, block().bytes());
    assert_eq!(first.source, ContentSource::Remote(CacheAdmission::Stored));
    let second = read_through(&local, remote_store, &block().cid(), 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.source, ContentSource::Local);
    assert_eq!(remote.reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn absence_and_transport_failure_are_distinct() {
    let local = MemoryContentStore::default();
    assert_eq!(
        read_through(&local, &Remote::new(Ok(None)), &block().cid(), 100).await,
        Ok(None)
    );
    for error in [
        RemoteError::Unavailable,
        RemoteError::PermissionDenied,
        RemoteError::DeadlineExceeded,
    ] {
        assert_eq!(
            read_through(&local, &Remote::new(Err(error)), &block().cid(), 100).await,
            Err(ContentProtocolError::Remote(error))
        );
    }
}

#[tokio::test]
async fn corrupt_and_oversized_remote_blocks_never_enter_cache() {
    let local = MemoryContentStore::default();
    let corrupt = Remote::new(Ok(Some(b"wrong content".to_vec())));
    assert!(matches!(
        read_through(&local, &corrupt, &block().cid(), 100).await,
        Err(ContentProtocolError::Content(
            ContentError::CidMismatch { .. }
        ))
    ));
    let oversized = Remote::new(Ok(Some(block().bytes().to_vec())));
    assert!(matches!(
        read_through(&local, &oversized, &block().cid(), 1).await,
        Err(ContentProtocolError::TooLarge { limit: 1, .. })
    ));
    assert!(matches!(
        local.get(&block().cid()),
        Err(ContentError::NotFound(_))
    ));
}

struct BrokenCache {
    read_error: Option<ContentError>,
}

impl ContentStore for BrokenCache {
    fn get_bounded(&self, cid: &Cid, _: usize) -> Result<Vec<u8>, ContentError> {
        Err(self
            .read_error
            .clone()
            .unwrap_or(ContentError::NotFound(Box::new(*cid))))
    }
    fn put(&self, _: ContentBlock<'_>) -> Result<Cid, ContentError> {
        Err(ContentError::LockPoisoned)
    }
}

#[tokio::test]
async fn cache_failure_does_not_discard_valid_remote_data_or_acknowledgement() {
    let local = BrokenCache { read_error: None };
    let remote = Remote::new(Ok(Some(block().bytes().to_vec())));
    let read = read_through(&local, &remote, &block().cid(), 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.bytes, block().bytes());
    assert_eq!(
        read.source,
        ContentSource::Remote(CacheAdmission::Failed(ContentError::LockPoisoned))
    );
    let receipt = publish_content(&local, &remote, block()).await.unwrap();
    assert_eq!(receipt.cid, block().cid());
    assert_eq!(
        receipt.cache,
        CacheAdmission::Failed(ContentError::LockPoisoned)
    );
}

#[tokio::test]
async fn local_errors_are_not_hidden_by_remote_fallback() {
    let local = BrokenCache {
        read_error: Some(ContentError::LockPoisoned),
    };
    let remote = Remote::new(Ok(Some(block().bytes().to_vec())));
    assert_eq!(
        read_through(&local, &remote, &block().cid(), 100).await,
        Err(ContentProtocolError::Content(ContentError::LockPoisoned))
    );
    assert_eq!(remote.reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn failed_publication_never_returns_success_even_with_a_warm_cache() {
    let local = MemoryContentStore::default();
    local.put(block()).unwrap();
    let mut remote = Remote::new(Ok(None));
    remote.write = Err(RemoteError::Unavailable);
    assert_eq!(
        publish_content(&local, &remote, block()).await,
        Err(ContentProtocolError::Remote(RemoteError::Unavailable))
    );
}

#[tokio::test]
async fn codec_is_part_of_remote_identity_and_empty_blocks_are_valid() {
    let local = MemoryContentStore::default();
    let raw = ContentBlock::new(ContentCodec::Raw, b"");
    let cbor = ContentBlock::new(ContentCodec::DagCbor, b"");
    assert_ne!(raw.cid(), cbor.cid());
    let remote = Remote::new(Ok(Some(Vec::new())));
    for block in [raw, cbor] {
        assert!(
            read_through(&local, &remote, &block.cid(), 0)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(local.get(&block.cid()).unwrap(), b"");
    }
    assert_eq!(remote.reads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn local_hit_enforces_budget_without_remote_fallback() {
    let local = MemoryContentStore::default();
    local.put(block()).unwrap();
    let remote = Remote::new(Ok(None));
    assert_eq!(
        read_through(&local, &remote, &block().cid(), 1).await,
        Err(ContentProtocolError::Content(ContentError::BlockTooLarge {
            limit: 1,
            actual: block().bytes().len() as u64,
        }))
    );
    assert_eq!(remote.reads.load(Ordering::SeqCst), 0);
}
