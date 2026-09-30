use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use cid::Cid;
use meta_relational_reasoning::{
    EntityCatalog, RelationCatalog, RelationField, RelationSchema, ValueSchema,
};
use mrr_data_content::{
    ContentBlock, ContentCodec, ContentStore, FilesystemContentStore, MemoryContentStore,
    RemoteContentStore, RemoteError, RemoteFuture, RemoteTransferLimits, SnapshotTransferLimits,
    TransferSession,
};
use tempfile::tempdir;

use crate::data_protection::{
    CurrentStorageStateV1, EntityRef, PreparedProtectedSnapshot, ProtectedEnvelopeKey,
    ProtectedPublish, ProtectedRestore, ProtectedSnapshotError, ProtectedStage,
    ProtectedStorageMismatch, ProtectionClaimV1, ProtectionIntentV1, RawStorageDestination,
    RawStorageTier, SourceLabel, StorageEffectV1, publish_prepared_snapshot,
    restore_protected_snapshot, stage_protected_snapshot,
};

struct Remote {
    blocks: Mutex<BTreeMap<Cid, Vec<u8>>>,
    gets: AtomicUsize,
    puts: AtomicUsize,
    fail_after_store_at: AtomicUsize,
}

impl Remote {
    fn new() -> Self {
        Self {
            blocks: Mutex::new(BTreeMap::new()),
            gets: AtomicUsize::new(0),
            puts: AtomicUsize::new(0),
            fail_after_store_at: AtomicUsize::new(0),
        }
    }

    fn contains(&self, cid: &Cid) -> bool {
        self.blocks.lock().unwrap().contains_key(cid)
    }
}

impl RemoteContentStore for Remote {
    fn get<'a>(&'a self, cid: &'a Cid, _: usize) -> RemoteFuture<'a, Option<Vec<u8>>> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        let block = self.blocks.lock().unwrap().get(cid).cloned();
        Box::pin(async move { Ok(block) })
    }

    fn put<'a>(&'a self, block: ContentBlock<'a>) -> RemoteFuture<'a, ()> {
        let number = self.puts.fetch_add(1, Ordering::Relaxed) + 1;
        self.blocks
            .lock()
            .unwrap()
            .insert(block.cid(), block.bytes().to_vec());
        let fail = self
            .fail_after_store_at
            .compare_exchange(number, 0, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok();
        Box::pin(async move {
            if fail {
                Err(RemoteError::Unavailable)
            } else {
                Ok(())
            }
        })
    }
}

fn session() -> TransferSession {
    TransferSession::new(
        Duration::from_secs(5),
        RemoteTransferLimits {
            operations: 20,
            bytes: 65_536,
            attempts_per_operation: 1,
            retry_delay: Duration::ZERO,
        },
    )
    .unwrap()
}

fn current(epoch: u64) -> CurrentStorageStateV1<'static> {
    CurrentStorageStateV1 {
        policy_root: "policy-root-1",
        lineage_revision: "lineage-1",
        epoch,
        now: 99,
    }
}

fn relations(snapshot: &mrr_data_core::SnapshotBlock) -> RelationCatalog {
    let relation = RelationSchema::new(
        snapshot.manifest().relations()[0].relation_id(),
        "TokenFixture",
        vec![RelationField::new("value", ValueSchema::String, false).unwrap()],
        vec![],
    )
    .unwrap();
    RelationCatalog::admit(vec![relation]).unwrap()
}

fn limits() -> SnapshotTransferLimits {
    SnapshotTransferLimits::new(4096, 4, 4096, 8192)
}

#[expect(
    clippy::too_many_lines,
    reason = "complete protected stage, publish and restore acceptance"
)]
#[tokio::test]
async fn protected_outbox_rechecks_before_root_and_restores_cold_then_warm() {
    let snapshot = super::contracts::snapshot();
    let source = MemoryContentStore::default();
    source
        .put(ContentBlock::new(ContentCodec::Raw, b"synthetic-arrow"))
        .unwrap();
    source
        .put(ContentBlock::new(ContentCodec::Raw, b"coverage"))
        .unwrap();
    source
        .put(ContentBlock::new(ContentCodec::DagCbor, snapshot.bytes()))
        .unwrap();
    let directory = tempdir().unwrap();
    let outbox = FilesystemContentStore::open(directory.path()).unwrap();
    let relation_catalog = relations(&snapshot);
    let entity_catalog = EntityCatalog::admit(vec![]).unwrap();
    let owner = EntityRef {
        type_name: "Team",
        id: "analytics",
    };
    let labels = [SourceLabel {
        resource: EntityRef {
            type_name: "Dataset",
            id: "orders",
        },
        owner,
        tenant: "tenant-a",
        restricted: true,
    }];
    let owners = [owner];
    let intent = ProtectionIntentV1 {
        storage: StorageEffectV1 {
            operation_id: "op-001",
            subject: EntityRef {
                type_name: "Service",
                id: "publisher",
            },
            purpose: "archive",
            snapshot_root: snapshot.cid(),
            sources: &labels,
            destination: RawStorageDestination {
                resource: EntityRef {
                    type_name: "Bucket",
                    id: "archive",
                },
                tenant: "tenant-a",
                accepted_owners: &owners,
                accepts_restricted: true,
                tier: RawStorageTier::Remote,
            },
            policy_root: "policy-root-1",
            lineage_revision: "lineage-1",
        },
        profile: "aes-256-gcm-v1",
        key_ref: "key-tenant-a",
        key_version: "key-version-7",
        residency: "us-east-1",
    };
    let claim = ProtectionClaimV1 {
        intent,
        epoch: 4,
        expires_at: 100,
        allowed: true,
    };
    let key = ProtectedEnvelopeKey::aes_256_gcm(&[7_u8; 32]).unwrap();
    let prepared: PreparedProtectedSnapshot = stage_protected_snapshot(ProtectedStage {
        intent,
        claim: &claim,
        current: current(4),
        source: &source,
        outbox: &outbox,
        relations: &relation_catalog,
        entities: &entity_catalog,
        inner_limits: limits(),
        max_outer_block_bytes: 4096,
        max_outer_total_bytes: 8192,
        key: &key,
    })
    .await
    .unwrap();
    assert_eq!(prepared.inner_root(), snapshot.cid());
    assert_eq!(prepared.child_roots().len(), 2);
    assert!(outbox.get(snapshot.cid()).is_err());
    drop(outbox);
    let outbox = FilesystemContentStore::open(directory.path()).unwrap();
    assert!(outbox.get(prepared.outer_root()).is_ok());

    let remote = Remote::new();
    let publish_session = session();
    let publish = || ProtectedPublish {
        intent,
        claim: &claim,
        current: current(4),
        prepared: &prepared,
        outbox: &outbox,
        remote: &remote,
        session: &publish_session,
        max_outer_block_bytes: 4096,
    };
    let denied = publish_prepared_snapshot(publish(), || Ok(current(5))).await;
    assert!(matches!(
        denied,
        Err(ProtectedSnapshotError::Admission(
            ProtectedStorageMismatch::Stale
        ))
    ));
    assert!(!remote.contains(prepared.outer_root()));
    assert_eq!(remote.puts.load(Ordering::Relaxed), 3);
    remote.fail_after_store_at.store(7, Ordering::Relaxed);
    let interrupted = publish_prepared_snapshot(publish(), || Ok(current(4))).await;
    assert!(matches!(
        interrupted,
        Err(ProtectedSnapshotError::Remote(RemoteError::Unavailable))
    ));
    assert!(remote.contains(prepared.outer_root()));
    assert_eq!(remote.puts.load(Ordering::Relaxed), 7);
    let receipt = publish_prepared_snapshot(publish(), || Ok(current(4)))
        .await
        .unwrap();
    assert_eq!(receipt.outer_root, *prepared.outer_root());
    assert!(remote.contains(prepared.outer_root()));
    assert_eq!(remote.puts.load(Ordering::Relaxed), 11);
    assert!(!remote.contains(snapshot.cid()));

    let protected_cache = MemoryContentStore::default();
    let restore_session = session();
    let restore = || ProtectedRestore {
        intent,
        prepared: &prepared,
        protected_cache: &protected_cache,
        remote: &remote,
        session: &restore_session,
        key: &key,
        relations: &relation_catalog,
        entities: &entity_catalog,
        inner_limits: limits(),
        max_outer_block_bytes: 4096,
        max_outer_total_bytes: 8192,
    };
    let cold = restore_protected_snapshot(restore()).await.unwrap();
    assert_eq!(cold.snapshot().cid(), snapshot.cid());
    assert_eq!(remote.gets.load(Ordering::Relaxed), 4);
    let warm = restore_protected_snapshot(restore()).await.unwrap();
    assert_eq!(warm.snapshot().cid(), snapshot.cid());
    assert_eq!(remote.gets.load(Ordering::Relaxed), 4);
}
