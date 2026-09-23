use crate::BlockingContentStore;
use cid::Cid;
use mrr_data_content::{
    AsyncContentStore, ContentBlock, ContentCodec, ContentError, ContentStore,
    RemoteTransferLimits, SnapshotTransferError, TransferSession,
};
use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Disk {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<(Mutex<bool>, Condvar)>,
    calls: Arc<AtomicUsize>,
}
impl ContentStore for Disk {
    fn get_bounded(&self, _: &Cid, _: usize) -> Result<Vec<u8>, ContentError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        let (flag, event) = &*self.release;
        let mut released = flag.lock().unwrap();
        while !*released {
            released = event.wait(released).unwrap();
        }
        Ok(Vec::new())
    }
    fn put(&self, block: ContentBlock<'_>) -> Result<Cid, ContentError> {
        Ok(block.cid())
    }
}
struct Release(Arc<(Mutex<bool>, Condvar)>);
impl Drop for Release {
    fn drop(&mut self) {
        *self.0.0.lock().unwrap() = true;
        self.0.1.notify_all();
    }
}

#[tokio::test]
async fn slow_disk_is_off_executor_and_cancelled_jobs_keep_their_permit() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let cleanup = Release(Arc::clone(&release));
    let store = BlockingContentStore::new(Disk {
        entered: Arc::clone(&entered),
        calls: Arc::clone(&calls),
        release,
    });
    let control = TransferSession::new(
        Duration::from_secs(10),
        RemoteTransferLimits {
            operations: 1,
            bytes: 1,
            attempts_per_operation: 1,
            retry_delay: Duration::ZERO,
        },
    )
    .unwrap();
    let worker = store.clone();
    let cancellation = control.clone();
    let job = tokio::spawn(async move {
        let cid = ContentBlock::new(ContentCodec::Raw, b"").cid();
        control
            .run(async {
                worker
                    .load(&cid, 0)
                    .await
                    .map_err(SnapshotTransferError::from)
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    // This test uses a current-thread runtime: a blocked executor would not
    // reach this timer/cancellation while the disk call waits on the condvar.
    tokio::time::sleep(Duration::from_millis(5)).await;
    cancellation.cancel();
    assert_eq!(job.await.unwrap(), Err(SnapshotTransferError::Cancelled));
    let worker = store.clone();
    let next = tokio::spawn(async move {
        let cid = ContentBlock::new(ContentCodec::Raw, b"").cid();
        worker.load(&cid, 0).await
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(!next.is_finished());
    drop(cleanup);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), next)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        b""
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
