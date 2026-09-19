//! Adapter contracts; locking, removal and maintenance use upstream Kache.
use super::{KacheContentStore, key};
use crate::BlockingContentStore;
use anyhow::Result;
use cid::Cid;
use mrr_data_content::{
    CacheAdmission, ContentBlock, ContentCodec, ContentError, ContentSource, ContentStore,
    RemoteContentStore, RemoteError, RemoteFuture, read_through,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tempfile::tempdir;
use tokio::{
    sync::{Barrier, Notify},
    task::JoinSet,
};

struct Remote {
    blocks: BTreeMap<Cid, Vec<u8>>,
    calls: AtomicUsize,
    bytes: AtomicUsize,
    barrier: Option<Barrier>,
    pause_once: AtomicBool,
    entered: Notify,
}
impl Remote {
    fn new(payloads: &[Vec<u8>], callers: Option<usize>) -> Self {
        Self {
            blocks: payloads
                .iter()
                .map(|bytes| {
                    (
                        ContentBlock::new(ContentCodec::Raw, bytes).cid(),
                        bytes.clone(),
                    )
                })
                .collect(),
            calls: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            barrier: callers.map(Barrier::new),
            pause_once: AtomicBool::new(false),
            entered: Notify::new(),
        }
    }
}
impl RemoteContentStore for Remote {
    fn get<'a>(&'a self, cid: &'a Cid, limit: usize) -> RemoteFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.pause_once.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            if let Some(barrier) = &self.barrier {
                barrier.wait().await;
            }
            let bytes = self.blocks.get(cid).cloned();
            if bytes.as_ref().is_some_and(|b| b.len() > limit) {
                return Err(RemoteError::TooLarge);
            }
            self.bytes
                .fetch_add(bytes.as_ref().map_or(0, Vec::len), Ordering::SeqCst);
            Ok(bytes)
        })
    }
    fn put<'a>(&'a self, _: ContentBlock<'a>) -> RemoteFuture<'a, ()> {
        Box::pin(async { Err(RemoteError::PermissionDenied) })
    }
}

#[tokio::test]
async fn concurrent_cold_and_reopened_warm_reads_preserve_content() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let root = tempdir()?;
        let payloads: Vec<_> = (0..5).map(|i| vec![i; 4096]).collect();
        let remote = Arc::new(Remote::new(&payloads, Some(8)));
        let requests = [0, 0, 0, 0, 1, 2, 3, 4];
        for warm in [false, true] {
            let mut jobs = JoinSet::new();
            for index in requests {
                let path = root.path().to_owned();
                let local =
                    tokio::task::spawn_blocking(move || KacheContentStore::open(path, 1_000_000))
                        .await??;
                let local = BlockingContentStore::new(local);
                let remote = Arc::clone(&remote);
                let bytes = payloads[index].clone();
                jobs.spawn(async move {
                    let cid = ContentBlock::new(ContentCodec::Raw, &bytes).cid();
                    let read = read_through(&local, &*remote, &cid, bytes.len())
                        .await?
                        .unwrap();
                    assert_eq!(read.bytes, bytes);
                    if warm {
                        assert_eq!(read.source, ContentSource::Local);
                    }
                    Ok::<_, anyhow::Error>(())
                });
            }
            while let Some(result) = jobs.join_next().await {
                result??;
            }
            assert_eq!(remote.calls.load(Ordering::SeqCst), 8);
            assert_eq!(remote.bytes.load(Ordering::SeqCst), 8 * 4096);
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn cancelled_reader_does_not_break_same_or_other_cid_readers() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let root = tempdir()?;
        let payloads = vec![b"same".to_vec(), b"other".to_vec()];
        let remote = Arc::new(Remote::new(&payloads, None));
        remote.pause_once.store(true, Ordering::SeqCst);
        let path = root.path().to_owned();
        let local = BlockingContentStore::new(
            tokio::task::spawn_blocking(move || KacheContentStore::open(path, 1_000_000)).await??,
        );
        let waiting = local.clone();
        let transport = Arc::clone(&remote);
        let cid = ContentBlock::new(ContentCodec::Raw, &payloads[0]).cid();
        let job = tokio::spawn(async move { read_through(&waiting, &*transport, &cid, 100).await });
        remote.entered.notified().await;
        job.abort();
        assert!(job.await.unwrap_err().is_cancelled());
        for bytes in &payloads {
            let cid = ContentBlock::new(ContentCodec::Raw, bytes).cid();
            assert_eq!(
                read_through(&local, &*remote, &cid, 100)
                    .await?
                    .unwrap()
                    .bytes,
                *bytes
            );
        }
        assert_eq!(remote.calls.load(Ordering::SeqCst), 3);
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn upstream_lock_contention_preserves_remote_bytes() -> Result<()> {
    let root = tempdir()?;
    let path = root.path().to_owned();
    let bytes = b"verified despite admission contention".to_vec();
    let cid = ContentBlock::new(ContentCodec::Raw, &bytes).cid();
    let (local, lock) = tokio::task::spawn_blocking(move || -> Result<_> {
        let local = KacheContentStore::open(path, 1_000_000)?;
        let lock = local.store.try_lock(&key(&cid))?.unwrap();
        Ok((BlockingContentStore::new(local), lock))
    })
    .await??;
    let remote = Remote::new(std::slice::from_ref(&bytes), None);
    let read = read_through(&local, &remote, &cid, 100).await?.unwrap();
    assert_eq!(read.bytes, bytes);
    assert!(matches!(
        read.source,
        ContentSource::Remote(CacheAdmission::Failed(ContentError::Io {
            kind: std::io::ErrorKind::WouldBlock,
            ..
        }))
    ));
    drop(lock);
    assert_eq!(
        read_through(&local, &remote, &cid, 100)
            .await?
            .unwrap()
            .source,
        ContentSource::Remote(CacheAdmission::Stored)
    );
    assert_eq!(
        read_through(&local, &remote, &cid, 100)
            .await?
            .unwrap()
            .source,
        ContentSource::Local
    );
    Ok(())
}

#[tokio::test]
async fn upstream_maintenance_and_removal_races_recover_and_reopen() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let root = tempdir()?;
        let bytes = vec![7; 4096];
        let cid = ContentBlock::new(ContentCodec::Raw, &bytes).cid();
        let path = root.path().to_owned();
        let payload = bytes.clone();
        let maintenance = tokio::task::spawn_blocking(move || -> Result<_> {
            let store = KacheContentStore::open(path, 1)?;
            store.put(ContentBlock::new(ContentCodec::Raw, &payload))?;
            store.maintain()?;
            // Upstream's recent-entry grace makes this a soft quota.
            assert_eq!(store.store.physical_size()?, 4096);
            store.store.remove_entry(&key(&cid))?;
            assert!(matches!(store.get(&cid), Err(ContentError::NotFound(_))));
            Ok(store)
        })
        .await??;
        let path = root.path().to_owned();
        let local = BlockingContentStore::new(
            tokio::task::spawn_blocking(move || KacheContentStore::open(path, 1)).await??,
        );
        let remote = Remote::new(std::slice::from_ref(&bytes), None);
        assert_eq!(
            read_through(&local, &remote, &cid, 4096)
                .await?
                .unwrap()
                .bytes,
            bytes
        );
        let worker = tokio::task::spawn_blocking(move || -> Result<()> {
            for _ in 0..32 {
                maintenance.maintain()?;
                // Exercise upstream removal without waiting out its 120s grace.
                maintenance.store.remove_entry(&key(&cid))?;
            }
            Ok(())
        });
        for _ in 0..32 {
            assert_eq!(
                read_through(&local, &remote, &cid, 4096)
                    .await?
                    .unwrap()
                    .bytes,
                bytes
            );
        }
        worker.await??;
        read_through(&local, &remote, &cid, 4096).await?.unwrap();
        drop(local);
        let path = root.path().to_owned();
        let reopened = BlockingContentStore::new(
            tokio::task::spawn_blocking(move || KacheContentStore::open(path, 1)).await??,
        );
        let calls = remote.calls.load(Ordering::SeqCst);
        assert_eq!(
            read_through(&reopened, &remote, &cid, 4096)
                .await?
                .unwrap()
                .source,
            ContentSource::Local
        );
        assert_eq!(remote.calls.load(Ordering::SeqCst), calls);
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}
