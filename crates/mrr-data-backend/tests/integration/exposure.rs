//! Native storage recovery of an opaque Host obligation. No Gateway semantics.
use mrr_data_backend::{Backend, BackendConfig, providers::TursoProvider};
use mrr_data_content::{
    ConditionalCommitPortError, ConditionalContentCommitOutcome, ConditionalContentCommitPort,
    ConditionalContentWrite, ContentBlock, ContentCodec, ContentStore, FilesystemContentStore,
    MemoryContentStore, RemoteContentStore, RemoteError, RemoteFuture, publish_content,
    read_through,
};
use std::sync::atomic::{AtomicUsize, Ordering};
#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct RetainedObligation {
    claim: String,
    reservation: String,
    unknown_exposure: String,
    history: String,
    reserved: u64,
    held: u64,
}

// Acknowledged file storage for this native experiment only. It delegates all
// content addressing, bounded reads and durable writes to MRR's existing store.
struct AcknowledgedFiles(FilesystemContentStore);
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
fn body() -> Vec<u8> {
    let exposure = RetainedObligation {
        claim: "original-claim".into(),
        reservation: "original-reservation".into(),
        unknown_exposure: "original-unknown-exposure".into(),
        history: "original-history-prefix".into(),
        reserved: 4,
        held: 4,
    };
    // Opaque Host data: the backend preserves identity and bytes, not budget arithmetic.
    serde_json::to_vec(&("original-budget/history-scope", true, exposure)).expect("typed body")
}
fn write<'a>(operation: &'a str, bytes: &[u8]) -> ConditionalContentWrite<'a> {
    ConditionalContentWrite {
        scope: "original-budget/history-scope",
        operation_id: operation,
        expected: None,
        replacement: ContentBlock::new(ContentCodec::Raw, bytes).cid(),
    }
}
async fn open(path: &std::path::Path) -> Backend {
    let provider = TursoProvider::new(path.to_path_buf(), tokio::runtime::Handle::current());
    let provider = match std::env::var_os("MRR_BACKEND_TEST_MVCC") {
        Some(_) => provider.with_concurrent_writes(),
        None => provider,
    };
    Backend::open(
        BackendConfig::default(),
        provider,
        tokio::runtime::Handle::current(),
    )
    .await
    .expect("native MRR backend")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_burn_exposure_body_survives_lost_reply_without_revalidation() {
    let temp = tempfile::tempdir().expect("directory");
    let db = temp.path().join("authority.db");
    let remote = AcknowledgedFiles(
        FilesystemContentStore::open(temp.path().join("bodies")).expect("content store"),
    );
    let bytes = body();
    let cache = MemoryContentStore::default();
    let published = publish_content(
        &cache,
        &remote,
        ContentBlock::new(ContentCodec::Raw, &bytes),
    )
    .await
    .expect("actual body acknowledgment");
    let original = write("original-admission", &bytes);
    let backend = open(&db).await;
    let port = backend
        .profile("gateway.admission.v1", "registered-original-home")
        .expect("original home");
    let validations = AtomicUsize::new(0);
    let result = port
        .commit(original, Some(&published), |_| {
            validations.fetch_add(1, Ordering::SeqCst);
            Ok::<(), ()>(())
        })
        .await
        .expect("deciding commit");
    let ConditionalContentCommitOutcome::Committed(receipt) = result else {
        panic!("fresh commit");
    };
    assert_eq!(receipt.committed.revision, 1);
    drop(port);
    backend.shutdown().await.expect("natural backend shutdown");

    let recovered = open(&db).await;
    let port = recovered
        .profile("gateway.admission.v1", "registered-original-home")
        .expect("same original home");
    let outcome = port
        .commit(original, None, |_| -> Result<(), ()> {
            panic!("historical replay must not mint a new validation/permit")
        })
        .await
        .expect("exact lost-reply recovery");
    assert!(matches!(
        outcome,
        ConditionalContentCommitOutcome::Replayed(_)
    ));
    assert_eq!(validations.load(Ordering::SeqCst), 1);
    let fresh_cache = MemoryContentStore::default();
    let content = read_through(&fresh_cache, &remote, &original.replacement, 8192)
        .await
        .expect("verified recoverable lookup")
        .expect("original body exists");
    let (scope, burned, retained): (String, bool, RetainedObligation) =
        serde_json::from_slice(&content.bytes).expect("original Host obligation");
    assert_eq!(scope, original.scope);
    assert!(burned);
    assert_eq!((retained.reserved, retained.held), (4, 4));
    assert_eq!(retained.claim, "original-claim");
    assert_eq!(retained.reservation, "original-reservation");
    assert_eq!(retained.unknown_exposure, "original-unknown-exposure");
    assert_eq!(retained.history, "original-history-prefix");
    let changed = write("original-admission", b"replacement exposure");
    assert!(
        port.commit(changed, None, |_| Ok::<(), ()>(()))
            .await
            .is_err()
    );
    let delivery = port
        .publication_delivery(original.scope, 1)
        .await
        .expect("delivery lookup")
        .expect("retained outbox");
    port.acknowledge_publication(&delivery)
        .await
        .expect("delivery acknowledgment");
    assert_eq!(
        validations.load(Ordering::SeqCst),
        1,
        "outbox acknowledgment does not re-run admission validation"
    );
    drop(port);
    recovered
        .shutdown()
        .await
        .expect("natural recovered shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_recovery_does_not_reconstruct_missing_exposure_body() {
    let temp = tempfile::tempdir().expect("directory");
    let remote = AcknowledgedFiles(
        FilesystemContentStore::open(temp.path().join("bodies")).expect("content store"),
    );
    let cache = MemoryContentStore::default();
    let bytes = body();
    let published = publish_content(
        &cache,
        &remote,
        ContentBlock::new(ContentCodec::Raw, &bytes),
    )
    .await
    .expect("stored body");
    let original = write("original-admission", &bytes);
    let backend = open(&temp.path().join("authority.db")).await;
    let port = backend
        .profile("gateway.admission.v1", "registered-original-home")
        .expect("original home");
    port.commit(original, Some(&published), |_| Ok::<(), ()>(()))
        .await
        .expect("committed metadata");
    std::fs::remove_file(remote.0.root().join(original.replacement.to_string()))
        .expect("restore content store without original body");
    assert!(
        port.recover(original)
            .await
            .expect("metadata remains committed")
            .is_some()
    );
    let empty_projection = MemoryContentStore::default();
    assert!(
        read_through(&empty_projection, &remote, &original.replacement, 8192)
            .await
            .is_err()
    );
    assert!(matches!(
        port.commit(original, None, |_| -> Result<(), ()> {
            panic!("replay validation");
        })
        .await
        .expect("historical metadata"),
        ConditionalContentCommitOutcome::Replayed(_)
    ));
    // No refund/ready decision is supplied by metadata-only recovery.
    drop(port);
    backend.shutdown().await.expect("natural shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validation_callback_side_effect_precedes_refusal_counterexample() {
    let temp = tempfile::tempdir().expect("directory");
    let remote = AcknowledgedFiles(
        FilesystemContentStore::open(temp.path().join("bodies")).expect("content store"),
    );
    let bytes = body();
    let published = publish_content(
        &MemoryContentStore::default(),
        &remote,
        ContentBlock::new(ContentCodec::Raw, &bytes),
    )
    .await
    .expect("stored body");
    let original = write("rejected-admission", &bytes);
    let backend = open(&temp.path().join("authority.db")).await;
    let port = backend
        .profile("gateway.admission.v1", "registered-original-home")
        .expect("original home");
    let externally_observable = AtomicUsize::new(0);
    let result = port
        .commit(original, Some(&published), |_| {
            externally_observable.fetch_add(1, Ordering::SeqCst);
            Err::<(), ()>(())
        })
        .await;
    assert!(matches!(
        result,
        Err(ConditionalCommitPortError::Validation(()))
    ));
    assert_eq!(
        externally_observable.load(Ordering::SeqCst),
        1,
        "FnOnce does not enforce pure validation"
    );
    assert!(
        port.recover(original)
            .await
            .expect("known rejection")
            .is_none()
    );
    drop(port);
    backend.shutdown().await.expect("natural shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caller_selected_home_can_recommit_original_claim_counterexample() {
    let temp = tempfile::tempdir().expect("directory");
    let remote = AcknowledgedFiles(
        FilesystemContentStore::open(temp.path().join("bodies")).expect("content store"),
    );
    let bytes = body();
    let published = publish_content(
        &MemoryContentStore::default(),
        &remote,
        ContentBlock::new(ContentCodec::Raw, &bytes),
    )
    .await
    .expect("stored original body");
    let original = write("same-original-admission", &bytes);
    let backend = open(&temp.path().join("authority.db")).await;
    let validations = AtomicUsize::new(0);
    for namespace in ["registered-original-home", "caller-selected-reconnect-home"] {
        let port = backend
            .profile("gateway.admission.v1", namespace)
            .expect("Host profile construction");
        let outcome = port
            .commit(original, Some(&published), |_| {
                validations.fetch_add(1, Ordering::SeqCst);
                Ok::<(), ()>(())
            })
            .await
            .expect("independent home is valid at physical API");
        assert!(matches!(
            outcome,
            ConditionalContentCommitOutcome::Committed(_)
        ));
    }
    assert_eq!(
        validations.load(Ordering::SeqCst),
        2,
        "original-home admission must be enforced by the Host"
    );
    backend.shutdown().await.expect("natural shutdown");
}
