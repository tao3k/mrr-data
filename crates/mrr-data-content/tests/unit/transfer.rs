use super::snapshot::{limits, local, snapshot_fixture};
use crate::{
    ContentBlock, ContentCodec, MemoryContentStore, RemoteContentStore, RemoteError, RemoteFuture,
    RemoteTransferLimits, SnapshotTransferError, TransferSession, publish_snapshot,
    restore_snapshot,
};
use cid::Cid;
use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Default)]
struct Remote {
    blocks: Mutex<BTreeMap<Cid, Vec<u8>>>,
    calls: AtomicUsize,
    failures: AtomicUsize,
    delay: Duration,
    active: AtomicUsize,
    peak: AtomicUsize,
    error: Option<RemoteError>,
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl Remote {
    async fn step(&self) -> Result<(), RemoteError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let _active = Active(&self.active);
        tokio::time::sleep(self.delay).await;
        if self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(self.error.unwrap_or(RemoteError::Unavailable));
        }
        Ok(())
    }
}
impl RemoteContentStore for Remote {
    fn get<'a>(&'a self, cid: &'a Cid, max: usize) -> RemoteFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            self.step().await?;
            let blocks = self.blocks.lock().unwrap();
            if blocks.get(cid).is_some_and(|b| b.len() > max) {
                return Err(RemoteError::TooLarge);
            }
            Ok(blocks.get(cid).cloned())
        })
    }
    fn put<'a>(&'a self, block: ContentBlock<'a>) -> RemoteFuture<'a, ()> {
        Box::pin(async move {
            self.step().await?;
            self.blocks
                .lock()
                .unwrap()
                .insert(block.cid(), block.bytes().to_vec());
            Ok(())
        })
    }
}
fn session(timeout: u64, ops: usize, bytes: usize, attempts: usize) -> TransferSession {
    TransferSession::new(
        Duration::from_millis(timeout),
        RemoteTransferLimits {
            operations: ops,
            bytes,
            attempts_per_operation: attempts,
            retry_delay: Duration::from_millis(2),
        },
    )
    .unwrap()
}

#[tokio::test(start_paused = true)]
async fn one_deadline_covers_children_and_root_and_fresh_session_converges() {
    let (snapshot, children, relations, entities) = snapshot_fixture();
    let local = local(&children);
    let remote = Remote {
        delay: Duration::from_millis(10),
        ..Remote::default()
    };
    let control = session(25, 20, 1_000_000, 1);
    let budgeted = control.remote(&remote);
    assert_eq!(
        control
            .run(publish_snapshot(
                &local,
                &budgeted,
                &snapshot,
                &relations,
                &entities,
                limits()
            ))
            .await,
        Err(SnapshotTransferError::DeadlineExceeded)
    );
    assert_eq!(remote.blocks.lock().unwrap().len(), children.len());
    assert!(!remote.blocks.lock().unwrap().contains_key(snapshot.cid()));
    let fresh = session(100, 20, 1_000_000, 1);
    let budgeted = fresh.remote(&remote);
    fresh
        .run(publish_snapshot(
            &local,
            &budgeted,
            &snapshot,
            &relations,
            &entities,
            limits(),
        ))
        .await
        .unwrap();
    let restored = restore_snapshot(
        &MemoryContentStore::default(),
        &remote,
        snapshot.cid(),
        &relations,
        &entities,
        limits(),
    )
    .await
    .unwrap();
    assert_eq!(restored.snapshot(), &snapshot);
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_child_prevents_root_and_pre_cancel_does_no_io() {
    let (snapshot, children, relations, entities) = snapshot_fixture();
    let local = local(&children);
    let remote = Remote {
        delay: Duration::from_millis(10),
        ..Remote::default()
    };
    let control = session(100, 20, 1_000_000, 1);
    let budgeted = control.remote(&remote);
    let cancel = async {
        tokio::time::sleep(Duration::from_millis(5)).await;
        control.cancel();
    };
    let (result, ()) = tokio::join!(
        control.run(publish_snapshot(
            &local,
            &budgeted,
            &snapshot,
            &relations,
            &entities,
            limits()
        )),
        cancel
    );
    assert_eq!(result, Err(SnapshotTransferError::Cancelled));
    assert!(remote.blocks.lock().unwrap().is_empty());
    let calls = remote.calls.load(Ordering::SeqCst);
    assert_eq!(
        control
            .run(publish_snapshot(
                &local,
                &budgeted,
                &snapshot,
                &relations,
                &entities,
                limits()
            ))
            .await,
        Err(SnapshotTransferError::Cancelled)
    );
    assert_eq!(calls, remote.calls.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn retries_charge_failures_and_share_the_session_budget() {
    let remote = Remote {
        failures: AtomicUsize::new(2),
        ..Remote::default()
    };
    let control = session(100, 4, 12, 3);
    let budgeted = control.remote(&remote);
    let block = ContentBlock::new(ContentCodec::Raw, b"four");
    budgeted.put(block).await.unwrap();
    assert_eq!(control.stats().operations, 3);
    assert_eq!(control.stats().retries, 2);
    assert_eq!(control.stats().charged_bytes, 12);
    assert_eq!(
        budgeted.put(block).await,
        Err(RemoteError::TransferBudgetExceeded)
    );
    assert_eq!(remote.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn failed_get_reservations_are_not_refunded_and_denials_are_not_retried() {
    let block = ContentBlock::new(ContentCodec::Raw, b"four");
    let remote = Remote {
        failures: AtomicUsize::new(1),
        ..Remote::default()
    };
    remote
        .blocks
        .lock()
        .unwrap()
        .insert(block.cid(), block.bytes().to_vec());
    let control = session(100, 3, 20, 3);
    let budgeted = control.remote(&remote);
    assert_eq!(
        budgeted.get(&block.cid(), 10).await.unwrap(),
        Some(block.bytes().to_vec())
    );
    assert_eq!(control.stats().charged_bytes, 14);
    let denied = Remote {
        failures: AtomicUsize::new(3),
        error: Some(RemoteError::PermissionDenied),
        ..Remote::default()
    };
    let other = session(100, 10, 100, 3);
    assert_eq!(
        other.remote(&denied).get(&block.cid(), 10).await,
        Err(RemoteError::PermissionDenied)
    );
    assert_eq!(denied.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn request_exhaustion_and_backoff_deadline_stop_before_another_attempt() {
    let remote = Remote {
        failures: AtomicUsize::new(100),
        ..Remote::default()
    };
    let block = ContentBlock::new(ContentCodec::Raw, b"one");
    let control = session(100, 1, 100, 3);
    assert_eq!(
        control.remote(&remote).put(block).await,
        Err(RemoteError::RequestBudgetExceeded)
    );
    assert_eq!(remote.calls.load(Ordering::SeqCst), 1);
    let control = session(1, 100, 1000, 3);
    assert_eq!(
        control.remote(&remote).put(block).await,
        Err(RemoteError::DeadlineExceeded)
    );
    assert_eq!(remote.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_restore_never_returns_a_partial_snapshot() {
    let (snapshot, children, relations, entities) = snapshot_fixture();
    let remote = Remote::default();
    publish_snapshot(
        &local(&children),
        &remote,
        &snapshot,
        &relations,
        &entities,
        limits(),
    )
    .await
    .unwrap();
    let remote = Remote {
        delay: Duration::from_millis(10),
        ..remote
    };
    let target = MemoryContentStore::default();
    let control = session(100, 10, 1_000_000, 1);
    let budgeted = control.remote(&remote);
    let cancel = async {
        tokio::time::sleep(Duration::from_millis(15)).await;
        control.cancel();
    };
    let (result, ()) = tokio::join!(
        control.run(restore_snapshot(
            &target,
            &budgeted,
            snapshot.cid(),
            &relations,
            &entities,
            limits()
        )),
        cancel
    );
    assert_eq!(result, Err(SnapshotTransferError::Cancelled));
    assert_eq!(
        restore_snapshot(
            &target,
            &remote,
            snapshot.cid(),
            &relations,
            &entities,
            limits()
        )
        .await
        .unwrap()
        .snapshot(),
        &snapshot
    );
}

#[tokio::test(start_paused = true)]
async fn one_session_limits_remote_concurrency_even_with_multiple_callers() {
    let remote = Remote {
        delay: Duration::from_millis(10),
        ..Remote::default()
    };
    let control = session(100, 10, 1000, 1);
    let budgeted = control.remote(&remote);
    let block = ContentBlock::new(ContentCodec::Raw, b"shared budget");
    let (a, b) = tokio::join!(budgeted.put(block), budgeted.put(block));
    a.unwrap();
    b.unwrap();
    assert_eq!(remote.peak.load(Ordering::SeqCst), 1);
    assert_eq!(remote.active.load(Ordering::SeqCst), 0);
    assert_eq!(control.stats().operations, 2);
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_root_put_returns_no_receipt_and_retry_converges() {
    let (snapshot, children, relations, entities) = snapshot_fixture();
    let local = local(&children);
    let remote = Remote {
        delay: Duration::from_millis(10),
        ..Remote::default()
    };
    let control = session(100, 20, 1_000_000, 1);
    let cancel = async {
        tokio::time::sleep(Duration::from_millis(children.len() as u64 * 10 + 5)).await;
        assert_eq!(remote.calls.load(Ordering::SeqCst), children.len() + 1);
        assert_eq!(remote.active.load(Ordering::SeqCst), 1);
        control.cancel();
    };
    let (result, ()) = tokio::join!(
        control.publish_snapshot(&local, &remote, &snapshot, &relations, &entities, limits()),
        cancel
    );
    assert_eq!(result, Err(SnapshotTransferError::Cancelled));
    assert_eq!(remote.active.load(Ordering::SeqCst), 0);
    assert_eq!(remote.blocks.lock().unwrap().len(), children.len());
    let fresh = session(100, 20, 1_000_000, 1);
    let receipt = fresh
        .publish_snapshot(&local, &remote, &snapshot, &relations, &entities, limits())
        .await
        .unwrap();
    assert_eq!(receipt.root(), snapshot.cid());
}
