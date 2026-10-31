//! Caller lifecycle stops surround real cold read-through operations.
use super::{
    fixture::{Fixture, capture_limits, transfer_limits},
    remote::Remote,
};
use mrr_data_content::{
    ContentBlock, ContentProtocolError, GraphTransferError, MemoryContentStore, RemoteContentStore,
    RemoteError, RemoteFuture, publish_combined_graph, restore_combined_graph_checked,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
enum Failure {
    Stopped,
    Transfer(GraphTransferError),
}
impl From<GraphTransferError> for Failure {
    fn from(error: GraphTransferError) -> Self {
        Self::Transfer(error)
    }
}
struct Observed<'a> {
    remote: &'a Remote,
    reads: AtomicUsize,
    fail: bool,
}
impl RemoteContentStore for Observed<'_> {
    fn get<'a>(&'a self, cid: &'a cid::Cid, cap: usize) -> RemoteFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(RemoteError::Unavailable);
            }
            self.remote.get(cid, cap).await
        })
    }
    fn put<'a>(&'a self, block: ContentBlock<'a>) -> RemoteFuture<'a, ()> {
        self.remote.put(block)
    }
}
#[tokio::test]
async fn cold_restore_stops_before_first_and_after_every_real_remote_read() {
    let f = Fixture::new();
    let prepared = f.prepare();
    let remote = Remote::default();
    publish_combined_graph(&prepared, &f.local, &remote, &remote, || async { Ok(()) })
        .await
        .unwrap();
    let observed = Observed {
        remote: &remote,
        reads: AtomicUsize::new(0),
        fail: false,
    };
    for stop_at in 0..=prepared.block_count() {
        observed.reads.store(0, Ordering::SeqCst);
        let result = restore_combined_graph_checked(
            &MemoryContentStore::default(),
            &observed,
            &f.query,
            (&f.relations, &f.entities),
            (transfer_limits(), capture_limits().dataset),
            || {
                if observed.reads.load(Ordering::SeqCst) >= stop_at {
                    Err(Failure::Stopped)
                } else {
                    Ok(())
                }
            },
        )
        .await;
        assert!(
            matches!(result, Err(Failure::Stopped)),
            "stop after {stop_at} reads"
        );
        assert_eq!(observed.reads.load(Ordering::SeqCst), stop_at);
        println!("cold restore refused after {stop_at} actual remote reads");
    }
    observed.reads.store(0, Ordering::SeqCst);
    let restored = restore_combined_graph_checked(
        &MemoryContentStore::default(),
        &observed,
        &f.query,
        (&f.relations, &f.entities),
        (transfer_limits(), capture_limits().dataset),
        || Ok::<(), Failure>(()),
    )
    .await
    .unwrap();
    assert_eq!(restored.total_bytes(), prepared.total_bytes());
    assert_eq!(
        observed.reads.load(Ordering::SeqCst),
        prepared.block_count()
    );
}
#[tokio::test]
async fn cold_restore_checks_stop_even_after_failed_read_and_preserves_transfer_error() {
    let f = Fixture::new();
    let remote = Remote::default();
    let observed = Observed {
        remote: &remote,
        reads: AtomicUsize::new(0),
        fail: true,
    };
    let result = restore_combined_graph_checked(
        &MemoryContentStore::default(),
        &observed,
        &f.query,
        (&f.relations, &f.entities),
        (transfer_limits(), capture_limits().dataset),
        || {
            if observed.reads.load(Ordering::SeqCst) > 0 {
                Err(Failure::Stopped)
            } else {
                Ok(())
            }
        },
    )
    .await;
    assert!(matches!(result, Err(Failure::Stopped)));
    assert_eq!(observed.reads.load(Ordering::SeqCst), 1);
    let result = restore_combined_graph_checked(
        &MemoryContentStore::default(),
        &observed,
        &f.query,
        (&f.relations, &f.entities),
        (transfer_limits(), capture_limits().dataset),
        || Ok::<(), Failure>(()),
    )
    .await;
    assert!(matches!(
        result,
        Err(Failure::Transfer(GraphTransferError::Transfer(
            ContentProtocolError::Remote(RemoteError::Unavailable)
        )))
    ));
}
