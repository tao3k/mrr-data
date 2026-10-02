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
use mrr_data_cache::{S3Config, S3ContentStore, http_client_builder};
use mrr_data_content::{
    ContentBlock, ContentCodec, ContentError, ContentStore, FilesystemContentStore,
    MemoryContentStore, RemoteContentStore, RemoteError, RemoteFuture, RemoteTransferLimits,
    SnapshotTransferLimits, TransferSession,
};
use tempfile::tempdir;

use crate::data_protection::{
    CurrentStorageState, EntityRef, PreparedProtectedSnapshot, ProtectedCommitDisposition,
    ProtectedCommitReceipt, ProtectedEnvelopeKey, ProtectedPublish, ProtectedReadClaim,
    ProtectedReadDestination, ProtectedReadIntent, ProtectedReadMismatch, ProtectedRestore,
    ProtectedSnapshotError, ProtectedStage, ProtectedStorageMismatch, ProtectionClaim,
    ProtectionIntent, RawSnapshotPublish, RawSnapshotPublishError, RawStorageDestination,
    RawStorageMismatch, RawStorageTier, SourceLabel, StorageClaim, StorageEffect,
    publish_prepared_snapshot, publish_raw_snapshot, restore_protected_snapshot,
    stage_protected_snapshot,
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

struct CorruptOutbox<'a> {
    inner: &'a FilesystemContentStore,
    corrupted: Cid,
}

impl ContentStore for CorruptOutbox<'_> {
    fn put(&self, block: ContentBlock<'_>) -> Result<Cid, ContentError> {
        self.inner.put(block)
    }

    fn get_bounded(&self, cid: &Cid, max_bytes: usize) -> Result<Vec<u8>, ContentError> {
        if *cid == self.corrupted {
            Ok(b"tampered-manifest".to_vec())
        } else {
            self.inner.get_bounded(cid, max_bytes)
        }
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

fn current(epoch: u64) -> CurrentStorageState<'static> {
    CurrentStorageState {
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
    let intent = ProtectionIntent {
        storage: StorageEffect {
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
    let claim = ProtectionClaim {
        intent,
        epoch: 4,
        expires_at: 100,
        allowed: true,
    };
    let raw_remote = Remote::new();
    assert!(matches!(
        publish_raw_snapshot(
            intent.storage,
            &StorageClaim {
                effect: intent.storage,
                epoch: 4,
                expires_at: 100,
                allowed: true,
            },
            current(4),
            RawSnapshotPublish {
                local: &source,
                remote: &raw_remote,
                session: &session(),
                snapshot: &snapshot,
                relations: &relation_catalog,
                entities: &entity_catalog,
                limits: limits(),
            },
        )
        .await,
        Err(RawSnapshotPublishError::Selection(
            RawStorageMismatch::RestrictedRequiresProtection
        ))
    ));
    assert_eq!(raw_remote.puts.load(Ordering::Relaxed), 0);

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
    let record = prepared.host_record();
    let mut wrong_key = record.clone();
    wrong_key.key_version = "key-version-8".to_owned();
    assert!(matches!(
        PreparedProtectedSnapshot::from_authenticated_record(intent, wrong_key),
        Err(ProtectedSnapshotError::WrongBinding)
    ));
    let mut wrong_total = record.clone();
    wrong_total.total_outer_bytes += 1;
    drop(prepared);
    drop(outbox);
    let outbox = FilesystemContentStore::open(directory.path()).unwrap();
    let prepared = PreparedProtectedSnapshot::from_authenticated_record(intent, record).unwrap();
    assert!(outbox.get(prepared.outer_root()).is_ok());

    let missing_outbox = MemoryContentStore::default();
    let missing_remote = Remote::new();
    assert!(matches!(
        publish_prepared_snapshot(
            ProtectedPublish {
                intent,
                claim: &claim,
                current: current(4),
                prepared: &prepared,
                key: &key,
                outbox: &missing_outbox,
                remote: &missing_remote,
                session: &session(),
                max_outer_block_bytes: 4096,
            },
            || Ok(current(4)),
        )
        .await,
        Err(ProtectedSnapshotError::Local(_))
    ));
    assert_eq!(missing_remote.puts.load(Ordering::Relaxed), 0);
    let corrupt_outbox = CorruptOutbox {
        inner: &outbox,
        corrupted: *prepared.outer_root(),
    };
    let corrupt_remote = Remote::new();
    let refreshes = AtomicUsize::new(0);
    assert!(matches!(
        publish_prepared_snapshot(
            ProtectedPublish {
                intent,
                claim: &claim,
                current: current(4),
                prepared: &prepared,
                key: &key,
                outbox: &corrupt_outbox,
                remote: &corrupt_remote,
                session: &session(),
                max_outer_block_bytes: 4096,
            },
            || {
                refreshes.fetch_add(1, Ordering::Relaxed);
                Ok(current(4))
            },
        )
        .await,
        Err(ProtectedSnapshotError::WrongRoot)
    ));
    assert_eq!(corrupt_remote.puts.load(Ordering::Relaxed), 3);
    assert!(!corrupt_remote.contains(prepared.outer_root()));
    assert_eq!(refreshes.load(Ordering::Relaxed), 0);

    let mut swapped_record = prepared.host_record();
    let outer_values = swapped_record
        .child_roots
        .values()
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(outer_values.len(), 2);
    assert_ne!(outer_values[0], outer_values[1]);
    for (outer, replacement) in swapped_record
        .child_roots
        .values_mut()
        .zip(outer_values.into_iter().rev())
    {
        *outer = replacement;
    }
    let swapped =
        PreparedProtectedSnapshot::from_authenticated_record(intent, swapped_record).unwrap();
    let swapped_remote = Remote::new();
    assert!(matches!(
        publish_prepared_snapshot(
            ProtectedPublish {
                intent,
                claim: &claim,
                current: current(4),
                prepared: &swapped,
                key: &key,
                outbox: &outbox,
                remote: &swapped_remote,
                session: &session(),
                max_outer_block_bytes: 4096,
            },
            || Ok(current(4)),
        )
        .await,
        Err(ProtectedSnapshotError::InvalidManifest)
    ));
    assert_eq!(swapped_remote.puts.load(Ordering::Relaxed), 3);
    assert!(!swapped_remote.contains(prepared.outer_root()));

    let altered =
        PreparedProtectedSnapshot::from_authenticated_record(intent, wrong_total).unwrap();
    let altered_remote = Remote::new();
    let altered_session = session();
    assert!(matches!(
        publish_prepared_snapshot(
            ProtectedPublish {
                intent,
                claim: &claim,
                current: current(4),
                prepared: &altered,
                key: &key,
                outbox: &outbox,
                remote: &altered_remote,
                session: &altered_session,
                max_outer_block_bytes: 4096,
            },
            || Ok(current(4)),
        )
        .await,
        Err(ProtectedSnapshotError::WrongBinding)
    ));
    assert!(!altered_remote.contains(prepared.outer_root()));

    let mut tiny_record = prepared.host_record();
    tiny_record.total_outer_bytes = 1;
    let tiny = PreparedProtectedSnapshot::from_authenticated_record(intent, tiny_record).unwrap();
    let tiny_remote = Remote::new();
    assert!(matches!(
        publish_prepared_snapshot(
            ProtectedPublish {
                intent,
                claim: &claim,
                current: current(4),
                prepared: &tiny,
                key: &key,
                outbox: &outbox,
                remote: &tiny_remote,
                session: &session(),
                max_outer_block_bytes: 4096,
            },
            || Ok(current(4)),
        )
        .await,
        Err(ProtectedSnapshotError::WrongBinding)
    ));
    assert_eq!(tiny_remote.puts.load(Ordering::Relaxed), 0);

    let remote = Remote::new();
    let publish_session = session();
    let publish = || ProtectedPublish {
        intent,
        claim: &claim,
        current: current(4),
        prepared: &prepared,
        key: &key,
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
    let publication = prepared.publication(intent).unwrap();
    assert!(
        publication
            .decide_commit(&claim, current(4), None, None)
            .is_err()
    );
    let receipt = publish_prepared_snapshot(publish(), || Ok(current(4)))
        .await
        .unwrap();
    assert_eq!(receipt.outer_root, *prepared.outer_root());
    assert!(remote.contains(prepared.outer_root()));
    assert_eq!(remote.puts.load(Ordering::Relaxed), 11);
    assert!(!remote.contains(snapshot.cid()));
    assert_eq!(
        publication.decide_commit(&claim, current(4), Some(receipt.as_ack()), None),
        Ok(ProtectedCommitDisposition::Apply)
    );
    let committed = ProtectedCommitReceipt {
        publication,
        child_count: receipt.child_count,
        total_outer_bytes: receipt.total_outer_bytes,
    };
    assert_eq!(
        publication.decide_commit(&claim, current(5), None, Some(&committed)),
        Ok(ProtectedCommitDisposition::Replay)
    );
    let conflicting = ProtectedCommitReceipt {
        publication: crate::data_protection::ProtectedPublication {
            intent: ProtectionIntent {
                storage: StorageEffect {
                    operation_id: "op-conflict",
                    ..intent.storage
                },
                ..intent
            },
            ..publication
        },
        ..committed
    };
    assert!(
        publication
            .decide_commit(&claim, current(4), None, Some(&conflicting))
            .is_err()
    );

    let protected_cache = MemoryContentStore::default();
    let restore_session = session();
    let read = ProtectedReadIntent {
        operation_id: "read-001",
        subject: EntityRef {
            type_name: "Service",
            id: "reader",
        },
        purpose: "analysis",
        receipt: committed,
        reader: ProtectedReadDestination {
            resource: intent.storage.destination.resource,
            tenant: "tenant-a",
            accepted_owners: &owners,
            accepts_restricted: true,
        },
        policy_root: "policy-root-1",
        lineage_revision: "lineage-1",
    };
    let read_claim = ProtectedReadClaim {
        intent: read,
        epoch: 4,
        expires_at: 100,
        allowed: true,
    };
    let restore = |state| ProtectedRestore {
        read,
        claim: &read_claim,
        current: state,
        committed: Some(&committed),
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
    let tiny_committed = ProtectedCommitReceipt {
        total_outer_bytes: 1,
        ..committed
    };
    let tiny_read = ProtectedReadIntent {
        receipt: tiny_committed,
        ..read
    };
    let tiny_read_claim = ProtectedReadClaim {
        intent: tiny_read,
        ..read_claim
    };
    let tiny_read_remote = Remote::new();
    *tiny_read_remote.blocks.lock().unwrap() = remote.blocks.lock().unwrap().clone();
    let tiny_cache = MemoryContentStore::default();
    assert!(matches!(
        restore_protected_snapshot(
            ProtectedRestore {
                read: tiny_read,
                claim: &tiny_read_claim,
                committed: Some(&tiny_committed),
                prepared: &tiny,
                protected_cache: &tiny_cache,
                remote: &tiny_read_remote,
                session: &session(),
                ..restore(current(4))
            },
            || Ok((current(4), Some(&tiny_committed))),
        )
        .await,
        Err(ProtectedSnapshotError::WrongBinding)
    ));
    assert_eq!(tiny_read_remote.gets.load(Ordering::Relaxed), 1);
    assert!(matches!(
        restore_protected_snapshot(
            ProtectedRestore {
                committed: None,
                ..restore(current(4))
            },
            || Ok((current(4), Some(&committed)))
        )
        .await,
        Err(ProtectedSnapshotError::Read(
            ProtectedReadMismatch::MissingCommit
        ))
    ));
    assert_eq!(remote.gets.load(Ordering::Relaxed), 0);
    assert!(matches!(
        restore_protected_snapshot(
            ProtectedRestore {
                prepared: &altered,
                ..restore(current(4))
            },
            || Ok((current(4), Some(&committed)))
        )
        .await,
        Err(ProtectedSnapshotError::WrongBinding)
    ));
    assert_eq!(remote.gets.load(Ordering::Relaxed), 0);
    assert!(matches!(
        restore_protected_snapshot(restore(current(5)), || Ok((current(4), Some(&committed))))
            .await,
        Err(ProtectedSnapshotError::Read(ProtectedReadMismatch::Stale))
    ));
    assert_eq!(remote.gets.load(Ordering::Relaxed), 0);
    assert!(matches!(
        restore_protected_snapshot(restore(current(4)), || Ok((current(5), Some(&committed))))
            .await,
        Err(ProtectedSnapshotError::Read(ProtectedReadMismatch::Stale))
    ));
    assert_eq!(remote.gets.load(Ordering::Relaxed), 4);
    let changed_count = ProtectedCommitReceipt {
        child_count: committed.child_count + 1,
        ..committed
    };
    assert!(matches!(
        restore_protected_snapshot(restore(current(4)), || {
            Ok((current(4), Some(&changed_count)))
        })
        .await,
        Err(ProtectedSnapshotError::Read(
            ProtectedReadMismatch::DifferentCommit
        ))
    ));
    assert_eq!(remote.gets.load(Ordering::Relaxed), 4);
    assert!(matches!(
        restore_protected_snapshot(restore(current(4)), || Ok((current(4), None))).await,
        Err(ProtectedSnapshotError::Read(
            ProtectedReadMismatch::MissingCommit
        ))
    ));
    assert_eq!(remote.gets.load(Ordering::Relaxed), 4);
    assert!(matches!(
        restore_protected_snapshot(restore(current(4)), || {
            Ok((
                CurrentStorageState {
                    now: 98,
                    ..current(4)
                },
                Some(&committed),
            ))
        })
        .await,
        Err(ProtectedSnapshotError::Read(ProtectedReadMismatch::Stale))
    ));
    assert_eq!(remote.gets.load(Ordering::Relaxed), 4);
    let warm =
        restore_protected_snapshot(restore(current(4)), || Ok((current(4), Some(&committed))))
            .await
            .unwrap();
    assert_eq!(warm.snapshot().cid(), snapshot.cid());
    assert_eq!(remote.gets.load(Ordering::Relaxed), 4);
    assert!(matches!(
        restore_protected_snapshot(restore(current(5)), || Ok((current(4), Some(&committed))))
            .await,
        Err(ProtectedSnapshotError::Read(ProtectedReadMismatch::Stale))
    ));
    assert_eq!(remote.gets.load(Ordering::Relaxed), 4);
}

#[tokio::test]
#[ignore = "requires tools/s3-conformance/run.py SigV4/TLS server"]
#[expect(
    clippy::too_many_lines,
    reason = "one TLS/SigV4 flow exercises stage, publish, commit, and gated restore"
)]
async fn protected_s3_tls_conformance() {
    let endpoint = std::env::var("MRR_S3_CONFORMANCE_ENDPOINT").unwrap();
    assert!(endpoint.starts_with("https://127.0.0.1:"));
    let ca = std::fs::read(std::env::var("MRR_S3_CONFORMANCE_CA").unwrap()).unwrap();
    let client = http_client_builder()
        .no_proxy()
        .add_root_certificate(reqwest::Certificate::from_pem(&ca).unwrap())
        .build()
        .unwrap();
    let remote = S3ContentStore::new(
        S3Config::default()
            .bucket("conformance")
            .region("us-east-1")
            .endpoint(&endpoint)
            .root("protected/agent-a")
            .access_key_id("local-conformance-key")
            .secret_access_key("local-conformance-secret")
            .disable_config_load()
            .disable_ec2_metadata(),
        client,
        Duration::from_secs(5),
    )
    .unwrap();
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
    let outbox_dir = tempdir().unwrap();
    let outbox = FilesystemContentStore::open(outbox_dir.path()).unwrap();
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
    let intent = ProtectionIntent {
        storage: StorageEffect {
            operation_id: "op-s3-protected",
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
    let claim = ProtectionClaim {
        intent,
        epoch: 4,
        expires_at: 100,
        allowed: true,
    };
    let key = ProtectedEnvelopeKey::aes_256_gcm(&[7_u8; 32]).unwrap();
    let prepared = stage_protected_snapshot(ProtectedStage {
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
    let publish_session = session();
    let physical = publish_prepared_snapshot(
        ProtectedPublish {
            intent,
            claim: &claim,
            current: current(4),
            prepared: &prepared,
            key: &key,
            outbox: &outbox,
            remote: &remote,
            session: &publish_session,
            max_outer_block_bytes: 4096,
        },
        || Ok(current(4)),
    )
    .await
    .unwrap();
    let publication = prepared.publication(intent).unwrap();
    assert_eq!(
        publication.decide_commit(&claim, current(4), Some(physical.as_ack()), None),
        Ok(ProtectedCommitDisposition::Apply)
    );
    let committed = ProtectedCommitReceipt {
        publication,
        child_count: physical.child_count,
        total_outer_bytes: physical.total_outer_bytes,
    };
    assert!(remote.get(snapshot.cid(), 4096).await.unwrap().is_none());
    let protected_cache = MemoryContentStore::default();
    let restore_session = session();
    let read = ProtectedReadIntent {
        operation_id: "read-s3-protected",
        subject: EntityRef {
            type_name: "Service",
            id: "reader",
        },
        purpose: "analysis",
        receipt: committed,
        reader: ProtectedReadDestination {
            resource: intent.storage.destination.resource,
            tenant: "tenant-a",
            accepted_owners: &owners,
            accepts_restricted: true,
        },
        policy_root: "policy-root-1",
        lineage_revision: "lineage-1",
    };
    let read_claim = ProtectedReadClaim {
        intent: read,
        epoch: 4,
        expires_at: 100,
        allowed: true,
    };
    let restored = restore_protected_snapshot(
        ProtectedRestore {
            read,
            claim: &read_claim,
            current: current(4),
            committed: Some(&committed),
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
        },
        || Ok((current(4), Some(&committed))),
    )
    .await
    .unwrap();
    assert_eq!(restored.snapshot().cid(), snapshot.cid());
}
