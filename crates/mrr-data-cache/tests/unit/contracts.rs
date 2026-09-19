use crate::{KacheContentStore, S3Config, S3ContentStore};
use anyhow::Result;
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::State,
    http::{Method, Request, Response, StatusCode},
    routing::any,
};
use mrr_data_content::{
    CacheAdmission, ContentBlock, ContentCodec, ContentProtocolError, ContentSource, ContentStore,
    RemoteContentStore, RemoteError, publish_content, read_through,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tempfile::tempdir;
#[derive(Default)]
struct Objects {
    bytes: Mutex<BTreeMap<String, Vec<u8>>>,
    gets: AtomicUsize,
    reject_writes: AtomicBool,
    reject_reads: AtomicBool,
    stall: AtomicBool,
    stall_body: AtomicBool,
    lose_ack: AtomicBool,
    reject_path: Mutex<Option<String>>,
    pause_put_path: Mutex<Option<String>>,
    writes: Mutex<Vec<String>>,
}

impl Objects {
    async fn record_put(&self, path: &str) {
        self.writes.lock().unwrap().push(path.to_owned());
        let pause = self.pause_put_path.lock().unwrap().as_deref() == Some(path);
        if pause {
            std::future::pending::<()>().await;
        }
    }
}

struct S3Wire {
    objects: Arc<Objects>,
    endpoint: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for S3Wire {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl S3Wire {
    async fn start() -> Result<Self> {
        let objects = Arc::new(Objects::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let app = Router::new()
            .fallback(any(object_request))
            .with_state(Arc::clone(&objects));
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Ok(Self {
            objects,
            endpoint,
            task,
        })
    }
    fn remote(&self) -> S3ContentStore {
        self.with_deadline(Duration::from_secs(5))
    }
    fn with_deadline(&self, deadline: Duration) -> S3ContentStore {
        S3ContentStore::new(
            S3Config::default()
                .bucket("probe")
                .region("us-east-1")
                .endpoint(&self.endpoint)
                .access_key_id("local-key")
                .secret_access_key("local-secret")
                .disable_config_load()
                .disable_ec2_metadata(),
            crate::http_client_builder().build().unwrap(),
            deadline,
        )
        .unwrap()
    }
}

async fn object_request(
    State(objects): State<Arc<Objects>>,
    request: Request<Body>,
) -> Response<Body> {
    let path = request.uri().path().to_owned();
    // Wire test only: require the real client's SigV4 header, without pretending
    // to validate credentials or reproduce a hosted provider's authentication.
    if !request
        .headers()
        .get("authorization")
        .is_some_and(|h| h.as_bytes().starts_with(b"AWS4-HMAC-SHA256"))
    {
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(Body::empty())
            .unwrap();
    }
    if objects.reject_reads.load(Ordering::SeqCst) && request.method() == Method::GET {
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(Body::empty())
            .unwrap();
    }
    if objects.stall.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    let create_only = request
        .headers()
        .get("if-none-match")
        .is_some_and(|v| v == "*");
    let method = request.method().clone();
    if method == Method::PUT {
        objects.record_put(&path).await;
        if objects.reject_path.lock().unwrap().as_ref() == Some(&path) {
            return Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body(Body::empty())
                .unwrap();
        }
        if objects.reject_writes.load(Ordering::SeqCst) {
            return Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body(Body::empty())
                .unwrap();
        }
        if create_only && objects.bytes.lock().unwrap().contains_key(&path) {
            return Response::builder()
                .status(StatusCode::PRECONDITION_FAILED)
                .body(Body::empty())
                .unwrap();
        }
        let bytes = to_bytes(request.into_body(), 16 * 1024 * 1024)
            .await
            .unwrap();
        let mut stored = objects.bytes.lock().unwrap();
        if create_only && stored.contains_key(&path) {
            return Response::builder()
                .status(StatusCode::PRECONDITION_FAILED)
                .body(Body::empty())
                .unwrap();
        }
        stored.insert(path, bytes.to_vec());
        if objects.lose_ack.load(Ordering::SeqCst) {
            return Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(Body::empty())
                .unwrap();
        }
        return Response::builder()
            .status(StatusCode::OK)
            .header("etag", "\"probe\"")
            .body(Body::empty())
            .unwrap();
    }
    if method == Method::GET {
        objects.gets.fetch_add(1, Ordering::SeqCst);
    }
    let Some(bytes) = objects.bytes.lock().unwrap().get(&path).cloned() else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
    };
    let length = bytes.len();
    let body = if method == Method::HEAD {
        Body::empty()
    } else if objects.stall_body.load(Ordering::SeqCst) {
        let first = bytes[..1].to_vec();
        let remainder = bytes[1..].to_vec();
        let stream = futures::stream::once(async move { Ok::<_, std::io::Error>(first) });
        let tail = futures::stream::once(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok::<_, std::io::Error>(remainder)
        });
        Body::from_stream(futures::StreamExt::chain(stream, tail))
    } else {
        Body::from(bytes)
    };
    Response::builder()
        .status(StatusCode::OK)
        .header("content-length", length)
        .header("etag", "\"probe\"")
        .body(body)
        .unwrap()
}

#[tokio::test]
async fn public_protocol_roundtrip_reopens_kache_and_keeps_warm_reads_off_s3() -> Result<()> {
    let wire = S3Wire::start().await?;
    let remote = wire.remote();
    let a = tempdir()?;
    let b = tempdir()?;
    let source = KacheContentStore::open(a.path(), 1_000_000)?;
    let target = KacheContentStore::open(b.path(), 1_000_000)?;
    // Both supported codecs and empty objects go through the real adapters.
    for block in [
        ContentBlock::new(ContentCodec::Raw, b"payload"),
        ContentBlock::new(ContentCodec::DagCbor, &[0xa1, 0x61, b'v', 0x01]),
        ContentBlock::new(ContentCodec::Raw, b""),
    ] {
        let receipt = publish_content(&source, &remote, block).await?;
        assert_eq!(receipt.cache, CacheAdmission::Stored);
        let read = read_through(&target, &remote, &receipt.cid, 100)
            .await?
            .unwrap();
        assert_eq!(read.bytes, block.bytes());
        assert_eq!(read.source, ContentSource::Remote(CacheAdmission::Stored));
        // Conditional create fails on the second PUT; matching content verifies idempotence.
        publish_content(&source, &remote, block).await?;
    }
    drop(target);
    let target = KacheContentStore::open(b.path(), 1_000_000)?;
    let gets = wire.objects.gets.load(Ordering::SeqCst);
    wire.objects.reject_reads.store(true, Ordering::SeqCst);
    let block = ContentBlock::new(ContentCodec::Raw, b"payload");
    let read = read_through(&target, &remote, &block.cid(), 100)
        .await?
        .unwrap();
    assert_eq!(read.source, ContentSource::Local);
    assert_eq!(wire.objects.gets.load(Ordering::SeqCst), gets);
    target.maintain()?;
    Ok(())
}

#[tokio::test]
async fn failed_put_is_not_acknowledged_and_retry_succeeds() -> Result<()> {
    let wire = S3Wire::start().await?;
    let remote = wire.remote();
    let root = tempdir()?;
    let local = KacheContentStore::open(root.path(), 1_000_000)?;
    let block = ContentBlock::new(ContentCodec::Raw, b"retry");
    wire.objects.reject_writes.store(true, Ordering::SeqCst);
    assert_eq!(
        publish_content(&local, &remote, block).await,
        Err(ContentProtocolError::Remote(RemoteError::PermissionDenied))
    );
    assert!(matches!(
        local.get(&block.cid()),
        Err(mrr_data_content::ContentError::NotFound(_))
    ));
    wire.objects.reject_writes.store(false, Ordering::SeqCst);
    publish_content(&local, &remote, block).await?;
    Ok(())
}

#[tokio::test]
async fn corrupt_oversized_missing_and_denied_objects_are_distinct() -> Result<()> {
    let wire = S3Wire::start().await?;
    let remote = wire.remote();
    let root = tempdir()?;
    let local = KacheContentStore::open(root.path(), 1_000_000)?;
    let block = ContentBlock::new(ContentCodec::Raw, b"good");
    assert_eq!(
        read_through(&local, &remote, &block.cid(), 100).await?,
        None
    );
    remote.put(block).await?;
    assert_eq!(
        remote.get(&block.cid(), 3).await,
        Err(RemoteError::TooLarge)
    );
    wire.objects.reject_reads.store(true, Ordering::SeqCst);
    assert_eq!(
        remote.get(&block.cid(), 100).await,
        Err(RemoteError::PermissionDenied)
    );
    wire.objects.reject_reads.store(false, Ordering::SeqCst);
    wire.objects
        .bytes
        .lock()
        .unwrap()
        .insert(format!("/probe/blocks/{}", block.cid()), b"evil".to_vec());
    assert_eq!(
        read_through(&local, &remote, &block.cid(), 100).await,
        Err(ContentProtocolError::Remote(RemoteError::Corrupt))
    );
    assert!(matches!(
        local.get(&block.cid()),
        Err(mrr_data_content::ContentError::NotFound(_))
    ));
    assert!(remote.put(block).await.is_err());
    assert_eq!(
        wire.objects.bytes.lock().unwrap()[&format!("/probe/blocks/{}", block.cid())],
        b"evil"
    );
    Ok(())
}

#[tokio::test]
async fn total_deadline_bounds_a_stalled_endpoint() -> Result<()> {
    let wire = S3Wire::start().await?;
    let remote = S3ContentStore::new(
        S3Config::default()
            .bucket("probe")
            .region("us-east-1")
            .endpoint(&wire.endpoint)
            .access_key_id("local-key")
            .secret_access_key("local-secret")
            .disable_config_load()
            .disable_ec2_metadata(),
        crate::http_client_builder().build()?,
        Duration::from_millis(30),
    )?;
    wire.objects.stall.store(true, Ordering::SeqCst);
    assert_eq!(
        remote
            .put(ContentBlock::new(ContentCodec::Raw, b"timeout"))
            .await,
        Err(RemoteError::DeadlineExceeded)
    );
    Ok(())
}

#[test]
fn local_corruption_is_detected_by_the_production_adapter() -> Result<()> {
    use std::io::Write;
    // Kache's content-addressed blob layout is upstream-owned; discover the leaf.
    fn replace_blob(path: &std::path::Path) -> Result<bool> {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if replace_blob(&entry.path())? {
                    return Ok(true);
                }
            } else if std::fs::read(entry.path())? == b"good" {
                let mut replacement = tempfile::NamedTempFile::new_in(path)?;
                replacement.write_all(b"evil")?;
                replacement.persist(entry.path())?;
                return Ok(true);
            }
        }
        Ok(false)
    }
    let root = tempdir()?;
    let local = KacheContentStore::open(root.path(), 1_000_000)?;
    let block = ContentBlock::new(ContentCodec::Raw, b"good");
    local.put(block)?;
    assert!(replace_blob(&root.path().join("store/blobs"))?);
    assert!(matches!(
        local.get(&block.cid()),
        Err(mrr_data_content::ContentError::CidMismatch { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn response_body_stall_does_not_fill_cache() -> Result<()> {
    let wire = S3Wire::start().await?;
    let remote = wire.remote();
    let block = ContentBlock::new(ContentCodec::Raw, b"body-stall");
    remote.put(block).await?;
    wire.objects.stall_body.store(true, Ordering::SeqCst);
    let limited = wire.with_deadline(Duration::from_millis(100));
    let root = tempdir()?;
    let local = KacheContentStore::open(root.path(), 1_000_000)?;
    assert_eq!(
        read_through(&local, &limited, &block.cid(), 100).await,
        Err(ContentProtocolError::Remote(RemoteError::DeadlineExceeded))
    );
    assert!(matches!(
        local.get(&block.cid()),
        Err(mrr_data_content::ContentError::NotFound(_))
    ));
    Ok(())
}

#[tokio::test]
async fn lost_acknowledgement_can_be_retried_without_overwriting() -> Result<()> {
    let wire = S3Wire::start().await?;
    let remote = wire.remote();
    let root = tempdir()?;
    let local = KacheContentStore::open(root.path(), 1_000_000)?;
    let block = ContentBlock::new(ContentCodec::Raw, b"persisted without ack");
    wire.objects.lose_ack.store(true, Ordering::SeqCst);
    assert_eq!(
        publish_content(&local, &remote, block).await,
        Err(ContentProtocolError::Remote(RemoteError::Unavailable))
    );
    assert_eq!(wire.objects.bytes.lock().unwrap().len(), 1);
    assert!(matches!(
        local.get(&block.cid()),
        Err(mrr_data_content::ContentError::NotFound(_))
    ));
    wire.objects.lose_ack.store(false, Ordering::SeqCst);
    let receipt = publish_content(&local, &remote, block).await?;
    assert_eq!(receipt.cache, CacheAdmission::Stored);
    assert_eq!(wire.objects.bytes.lock().unwrap().len(), 1);
    Ok(())
}

#[tokio::test]
async fn concurrent_publication_of_one_cid_is_idempotent() -> Result<()> {
    let wire = S3Wire::start().await?;
    let remote = wire.remote();
    let block = ContentBlock::new(ContentCodec::Raw, b"concurrent");
    let (left, right) = tokio::join!(remote.put(block), remote.put(block));
    left?;
    right?;
    assert_eq!(wire.objects.bytes.lock().unwrap().len(), 1);
    assert_eq!(
        remote.get(&block.cid(), 100).await?,
        Some(block.bytes().to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn warm_kache_reads_obey_the_same_budget_as_remote_reads() -> Result<()> {
    let wire = S3Wire::start().await?;
    let remote = wire.remote();
    let root = tempdir()?;
    let local = KacheContentStore::open(root.path(), 1_000_000)?;
    let block = ContentBlock::new(ContentCodec::Raw, b"larger than budget");
    publish_content(&local, &remote, block).await?;
    let gets = wire.objects.gets.load(Ordering::SeqCst);
    assert_eq!(
        read_through(&local, &remote, &block.cid(), 1).await,
        Err(ContentProtocolError::Content(
            mrr_data_content::ContentError::BlockTooLarge {
                limit: 1,
                actual: block.bytes().len() as u64,
            }
        ))
    );
    assert_eq!(wire.objects.gets.load(Ordering::SeqCst), gets);
    Ok(())
}

#[path = "../../examples/support/snapshot_fixture.rs"]
pub(super) mod snapshot_fixture;

fn snapshot_limits() -> mrr_data_content::SnapshotTransferLimits {
    mrr_data_content::SnapshotTransferLimits::new(64 * 1024, 10, 1024 * 1024, 8 * 1024 * 1024)
}

#[tokio::test]
async fn real_arrow_snapshot_roundtrips_through_s3_and_reopened_kache() -> Result<()> {
    let wire = S3Wire::start().await?;
    let remote = wire.remote();
    let a = tempdir()?;
    let b = tempdir()?;
    let source = KacheContentStore::open(a.path(), 16 * 1024 * 1024)?;
    let target = KacheContentStore::open(b.path(), 16 * 1024 * 1024)?;
    let fixture = snapshot_fixture::fixture();
    fixture.seed(&source)?;
    #[cfg(feature = "blocking")]
    let source = crate::BlockingContentStore::new(source);
    #[cfg(feature = "blocking")]
    let target = crate::BlockingContentStore::new(target);
    let control = mrr_data_content::TransferSession::new(
        Duration::from_secs(5),
        mrr_data_content::RemoteTransferLimits {
            operations: 100,
            bytes: 64 * 1024 * 1024,
            attempts_per_operation: 2,
            retry_delay: Duration::ZERO,
        },
    )?;
    let receipt = control
        .publish_snapshot(
            &source,
            &remote,
            &fixture.snapshot,
            &fixture.relations,
            &fixture.entities,
            snapshot_limits(),
        )
        .await?;
    let mut expected: Vec<_> = fixture
        .snapshot
        .manifest()
        .referenced_cids()
        .iter()
        .map(|cid| format!("/probe/blocks/{cid}"))
        .collect();
    expected.push(format!("/probe/blocks/{}", fixture.snapshot.cid()));
    assert_eq!(*wire.objects.writes.lock().unwrap(), expected);
    let restored = control
        .restore_snapshot(
            &target,
            &remote,
            receipt.root(),
            &fixture.relations,
            &fixture.entities,
            snapshot_limits(),
        )
        .await?;
    assert_eq!(restored.snapshot(), &fixture.snapshot);
    let ipc = &restored.children()[&mrr_data_core::raw_cid(&fixture.ipc)];
    assert_eq!(
        mrr_data_arrow::ipc_to_facts(
            &fixture.relation,
            ipc,
            mrr_data_arrow::IpcImportLimits::new(ipc.len(), 10, 100)
        )
        .unwrap(),
        fixture.facts
    );
    assert_eq!(wire.objects.gets.load(Ordering::SeqCst), expected.len());
    drop(target);
    let reopened = KacheContentStore::open(b.path(), 16 * 1024 * 1024)?;
    #[cfg(feature = "blocking")]
    let reopened = crate::BlockingContentStore::new(reopened);
    wire.objects.reject_reads.store(true, Ordering::SeqCst);
    let restored = control
        .restore_snapshot(
            &reopened,
            &remote,
            receipt.root(),
            &fixture.relations,
            &fixture.entities,
            snapshot_limits(),
        )
        .await?;
    assert!(
        restored
            .sources()
            .values()
            .all(|source| *source == ContentSource::Local)
    );
    assert_eq!(wire.objects.gets.load(Ordering::SeqCst), expected.len());
    Ok(())
}

#[tokio::test]
async fn failed_snapshot_publication_and_missing_child_restore_never_report_success() -> Result<()>
{
    use mrr_data_content::{SnapshotTransferError, publish_snapshot, restore_snapshot};
    let wire = S3Wire::start().await?;
    let remote = wire.remote();
    let a = tempdir()?;
    let b = tempdir()?;
    let source = KacheContentStore::open(a.path(), 16 * 1024 * 1024)?;
    let target = KacheContentStore::open(b.path(), 16 * 1024 * 1024)?;
    let fixture = snapshot_fixture::fixture();
    fixture.seed(&source)?;
    let child = fixture.snapshot.manifest().referenced_cids()[0];
    let child_path = format!("/probe/blocks/{child}");
    let root_path = format!("/probe/blocks/{}", fixture.snapshot.cid());
    for reject in [&child_path, &root_path] {
        *wire.objects.reject_path.lock().unwrap() = Some(reject.clone());
        assert!(
            publish_snapshot(
                &source,
                &remote,
                &fixture.snapshot,
                &fixture.relations,
                &fixture.entities,
                snapshot_limits()
            )
            .await
            .is_err()
        );
        assert!(!wire.objects.bytes.lock().unwrap().contains_key(&root_path));
        if reject == &child_path {
            assert!(!wire.objects.writes.lock().unwrap().contains(&root_path));
        }
    }
    *wire.objects.reject_path.lock().unwrap() = None;
    publish_snapshot(
        &source,
        &remote,
        &fixture.snapshot,
        &fixture.relations,
        &fixture.entities,
        snapshot_limits(),
    )
    .await?;
    let saved = wire
        .objects
        .bytes
        .lock()
        .unwrap()
        .remove(&child_path)
        .unwrap();
    assert_eq!(
        restore_snapshot(
            &target,
            &remote,
            fixture.snapshot.cid(),
            &fixture.relations,
            &fixture.entities,
            snapshot_limits()
        )
        .await
        .unwrap_err(),
        SnapshotTransferError::MissingBlock(Box::new(child))
    );
    wire.objects.bytes.lock().unwrap().insert(child_path, saved);
    assert_eq!(
        restore_snapshot(
            &target,
            &remote,
            fixture.snapshot.cid(),
            &fixture.relations,
            &fixture.entities,
            snapshot_limits()
        )
        .await?
        .snapshot(),
        &fixture.snapshot
    );
    Ok(())
}

#[cfg(feature = "blocking")]
#[tokio::test]
async fn whole_snapshot_deadline_cancels_s3_and_fresh_session_recovers() -> Result<()> {
    use mrr_data_content::{RemoteTransferLimits, SnapshotTransferError, TransferSession};
    let wire = S3Wire::start().await?;
    let remote = wire.remote();
    let root = tempdir()?;
    let local = KacheContentStore::open(root.path(), 16 * 1024 * 1024)?;
    let fixture = snapshot_fixture::fixture();
    fixture.seed(&local)?;
    let local = crate::BlockingContentStore::new(local);
    wire.objects.stall.store(true, Ordering::SeqCst);
    let budgets = RemoteTransferLimits {
        operations: 100,
        bytes: 64 * 1024 * 1024,
        attempts_per_operation: 2,
        retry_delay: Duration::ZERO,
    };
    let session = TransferSession::new(Duration::from_secs(1), budgets)?;
    assert_eq!(
        session
            .publish_snapshot(
                &local,
                &remote,
                &fixture.snapshot,
                &fixture.relations,
                &fixture.entities,
                snapshot_limits()
            )
            .await,
        Err(SnapshotTransferError::DeadlineExceeded)
    );
    assert!(
        !wire
            .objects
            .bytes
            .lock()
            .unwrap()
            .contains_key(&format!("/probe/blocks/{}", fixture.snapshot.cid()))
    );
    wire.objects.stall.store(false, Ordering::SeqCst);
    let fresh = TransferSession::new(Duration::from_secs(10), budgets)?;
    let receipt = fresh
        .publish_snapshot(
            &local,
            &remote,
            &fixture.snapshot,
            &fixture.relations,
            &fixture.entities,
            snapshot_limits(),
        )
        .await?;
    assert_eq!(receipt.root(), fixture.snapshot.cid());
    assert_eq!(
        fresh.stats().operations,
        fixture.snapshot.manifest().referenced_cids().len() + 1
    );
    Ok(())
}

#[cfg(feature = "blocking")]
#[path = "restart.rs"]
mod restart;
