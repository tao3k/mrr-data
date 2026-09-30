use meta_relational_reasoning::{
    EntityCatalog, ExternalRevisionIdentity, GenerationId, RelationCatalog, RelationField,
    RelationId, RelationSchema, RevisionBinding, SemanticSnapshot, ValueSchema,
};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, RelationDescriptor, SnapshotBlock,
    SnapshotManifest, SnapshotManifestRequest, SnapshotRowBinding, raw_cid,
};

fn snapshot() -> SnapshotBlock {
    let generation = GenerationId::from_canonical_bytes("generation:token-fixture").unwrap();
    let revision = RevisionBinding::admit(
        ExternalRevisionIdentity::new("test", "source", "revision").unwrap(),
        generation,
    )
    .unwrap();
    let semantic = SemanticSnapshot::admit(generation, vec![revision]).unwrap();
    let relation_id = RelationId::from_canonical_bytes("relation:token-fixture").unwrap();
    let schema = RelationSchema::new(
        relation_id,
        "TokenFixture",
        vec![RelationField::new("value", ValueSchema::String, false).unwrap()],
        vec![],
    )
    .unwrap();
    let relations = RelationCatalog::admit(vec![schema]).unwrap();
    let entities = EntityCatalog::admit(vec![]).unwrap();
    let batch = BatchDescriptor::new(raw_cid(b"synthetic-arrow"), 1, 15).unwrap();
    let descriptor = RelationDescriptor::new(relation_id, 1, vec![batch]).unwrap();
    let coverage = CoverageDescriptor::new(CoverageKind::Unknown, raw_cid(b"coverage")).unwrap();
    let request =
        SnapshotManifestRequest::new(semantic, &relations, &entities, vec![descriptor], coverage);
    SnapshotBlock::encode(SnapshotManifest::admit(request).unwrap()).unwrap()
}

use crate::data_protection::{
    DataProtectionDecisions, DataProtectionMismatch, DataProtectionProfile,
    PseudonymizationInputBinding, ReleaseReceiptClaim,
};

#[test]
fn selected_input_stays_on_immutable_snapshot() {
    let source = snapshot();
    let digest = [9; 32];
    let input = PseudonymizationInputBinding::new(
        &source,
        "customer_id",
        &digest,
        "campaign-a",
        "aes-siv-profile",
    );
    assert_eq!(input.source().root(), source.cid());
    assert_eq!(input.field(), "customer_id");
    assert_eq!(input.value_digest(), &digest);
    assert_eq!(input.context(), "campaign-a");
    assert_eq!(input.profile(), &"aes-siv-profile");
    assert!(input.row().is_none());
    let relation = &source.manifest().relations()[0];
    let row = SnapshotRowBinding::new(
        &source,
        relation.relation_id(),
        relation.batches()[0].cid(),
        0,
    )
    .unwrap();
    let located = PseudonymizationInputBinding::at_row(
        row,
        "customer_id",
        &digest,
        "campaign-a",
        "aes-siv-profile",
    );
    assert_eq!(located.source().root(), source.cid());
    assert_eq!(
        located.row().unwrap().child_cid(),
        relation.batches()[0].cid()
    );
}

#[test]
fn cloud_profile_requires_exact_release_and_two_decisions() {
    let source = snapshot();
    let receipt = ReleaseReceiptClaim {
        artifact_digest: "sha256:candidate",
        source_commit: "commit-a",
        policy_root: "CustomerDataRelease",
        epoch: 7,
    };
    let profile = DataProtectionProfile::new(&source, "customer-campaign", receipt);
    let both = DataProtectionDecisions {
        policy_root: receipt.policy_root,
        dataset: "customer-campaign",
        artifact_digest: receipt.artifact_digest,
        epoch: 7,
        pipeline_release_allowed: true,
        transformation_allowed: true,
    };
    assert_eq!(profile.source().root(), source.cid());
    assert_eq!(profile.check(receipt, 7, both), Ok(()));
    assert_eq!(
        profile.check(receipt, 8, both),
        Err(DataProtectionMismatch::ReleaseReceipt)
    );
    assert_eq!(
        profile.check(
            ReleaseReceiptClaim {
                artifact_digest: "sha256:other",
                ..receipt
            },
            7,
            both
        ),
        Err(DataProtectionMismatch::ReleaseReceipt)
    );
    for wrong in [
        DataProtectionDecisions {
            policy_root: "ReleaseReady",
            ..both
        },
        DataProtectionDecisions {
            dataset: "other-dataset",
            ..both
        },
        DataProtectionDecisions {
            artifact_digest: "sha256:other",
            ..both
        },
        DataProtectionDecisions { epoch: 8, ..both },
    ] {
        assert_eq!(
            profile.check(receipt, 7, wrong),
            Err(DataProtectionMismatch::DecisionScope)
        );
    }
    assert_eq!(
        profile.check(
            receipt,
            7,
            DataProtectionDecisions {
                pipeline_release_allowed: false,
                ..both
            }
        ),
        Err(DataProtectionMismatch::PipelineDenied)
    );
    assert_eq!(
        profile.check(
            receipt,
            7,
            DataProtectionDecisions {
                transformation_allowed: false,
                ..both
            }
        ),
        Err(DataProtectionMismatch::TransformationDenied)
    );
}

#[test]
#[expect(clippy::too_many_lines, reason = "raw storage scope rejection matrix")]
fn raw_storage_requires_exact_current_scope_and_unrestricted_sources() {
    use crate::data_protection::{
        CurrentStorageGovernance, RawStorageClaim, RawStorageDestination, RawStorageIntent,
        RawStorageMismatch, RawStorageTier, SourceLabel,
    };

    let source = snapshot();
    let labels = [SourceLabel {
        resource: "source-a",
        owner: "owner-a",
        tenant: "tenant-a",
        restricted: false,
    }];
    let owners = ["owner-a"];
    let destination = RawStorageDestination {
        name: "tenant-a-remote",
        tenant: "tenant-a",
        accepted_owners: &owners,
        tier: RawStorageTier::Remote,
    };
    let intent = RawStorageIntent {
        subject: "service-a",
        purpose: "snapshot-distribution",
        snapshot: &source,
        sources: &labels,
        destination,
    };
    let policy = [7; 32];
    let current = CurrentStorageGovernance {
        policy_digest: &policy,
        epoch: 4,
        now: 99,
    };
    let claim = RawStorageClaim {
        subject: intent.subject,
        purpose: intent.purpose,
        root: source.cid(),
        sources: &labels,
        destination,
        policy_digest: &policy,
        governance_epoch: 4,
        expires_at: 100,
        allowed: true,
    };
    assert_eq!(intent.check_claim(&claim, current), Ok(()));
    assert_eq!(
        intent.check_claim(
            &RawStorageClaim {
                allowed: false,
                ..claim
            },
            current
        ),
        Err(RawStorageMismatch::Denied)
    );
    assert_eq!(
        intent.check_claim(
            &claim,
            CurrentStorageGovernance {
                epoch: 5,
                ..current
            }
        ),
        Err(RawStorageMismatch::Stale)
    );
    let other_root = raw_cid(b"other-snapshot");
    assert_eq!(
        intent.check_claim(
            &RawStorageClaim {
                root: &other_root,
                ..claim
            },
            current
        ),
        Err(RawStorageMismatch::DifferentEffect)
    );
    let altered_labels = [SourceLabel {
        resource: "source-b",
        ..labels[0]
    }];
    assert_eq!(
        intent.check_claim(
            &RawStorageClaim {
                sources: &altered_labels,
                ..claim
            },
            current
        ),
        Err(RawStorageMismatch::DifferentEffect)
    );
    let other_destination = RawStorageDestination {
        name: "other-remote",
        ..destination
    };
    assert_eq!(
        intent.check_claim(
            &RawStorageClaim {
                destination: other_destination,
                ..claim
            },
            current
        ),
        Err(RawStorageMismatch::DifferentEffect)
    );
    let restricted = [SourceLabel {
        restricted: true,
        ..labels[0]
    }];
    let restricted_intent = RawStorageIntent {
        sources: &restricted,
        ..intent
    };
    assert_eq!(
        restricted_intent.check_claim(
            &RawStorageClaim {
                sources: &restricted,
                ..claim
            },
            current
        ),
        Err(RawStorageMismatch::RestrictedRequiresProtection)
    );
    let wrong_tenant = [SourceLabel {
        tenant: "tenant-b",
        ..labels[0]
    }];
    assert_eq!(
        RawStorageIntent {
            sources: &wrong_tenant,
            ..intent
        }
        .check_claim(
            &RawStorageClaim {
                sources: &wrong_tenant,
                ..claim
            },
            current
        ),
        Err(RawStorageMismatch::Tenant)
    );
    let wrong_owner = [SourceLabel {
        owner: "owner-b",
        ..labels[0]
    }];
    assert_eq!(
        RawStorageIntent {
            sources: &wrong_owner,
            ..intent
        }
        .check_claim(
            &RawStorageClaim {
                sources: &wrong_owner,
                ..claim
            },
            current
        ),
        Err(RawStorageMismatch::Owner)
    );
}

#[cfg(feature = "raw-publish")]
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "deny-before-I/O and allowed snapshot transfer fixture"
)]
async fn raw_snapshot_gate_precedes_remote_io_and_allows_unrestricted() {
    use crate::data_protection::{
        CurrentStorageGovernance, RawSnapshotPublish, RawSnapshotPublishError, RawStorageClaim,
        RawStorageDestination, RawStorageIntent, RawStorageMismatch, RawStorageTier, SourceLabel,
        publish_raw_snapshot,
    };
    use mrr_data_content::{
        ContentBlock, ContentCodec, ContentStore, MemoryContentStore, RemoteContentStore,
        RemoteFuture, RemoteTransferLimits, SnapshotTransferLimits, TransferSession,
    };
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    struct Remote(AtomicUsize);
    impl RemoteContentStore for Remote {
        fn get<'a>(&'a self, _: &'a cid::Cid, _: usize) -> RemoteFuture<'a, Option<Vec<u8>>> {
            Box::pin(async { Ok(None) })
        }
        fn put<'a>(&'a self, _: ContentBlock<'a>) -> RemoteFuture<'a, ()> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(()) })
        }
    }

    let source = snapshot();
    let labels = [SourceLabel {
        resource: "source-a",
        owner: "owner-a",
        tenant: "tenant-a",
        restricted: true,
    }];
    let owners = ["owner-a"];
    let destination = RawStorageDestination {
        name: "tenant-a-remote",
        tenant: "tenant-a",
        accepted_owners: &owners,
        tier: RawStorageTier::Remote,
    };
    let intent = RawStorageIntent {
        subject: "service-a",
        purpose: "snapshot-distribution",
        snapshot: &source,
        sources: &labels,
        destination,
    };
    let policy = [7; 32];
    let claim = RawStorageClaim {
        subject: intent.subject,
        purpose: intent.purpose,
        root: source.cid(),
        sources: &labels,
        destination,
        policy_digest: &policy,
        governance_epoch: 4,
        expires_at: 100,
        allowed: true,
    };
    let current = CurrentStorageGovernance {
        policy_digest: &policy,
        epoch: 4,
        now: 99,
    };
    let local = MemoryContentStore::default();
    let remote = Remote(AtomicUsize::new(0));
    let session = TransferSession::new(
        Duration::from_secs(5),
        RemoteTransferLimits {
            operations: 4,
            bytes: 4096,
            attempts_per_operation: 1,
            retry_delay: Duration::ZERO,
        },
    )
    .unwrap();
    let relation = RelationSchema::new(
        source.manifest().relations()[0].relation_id(),
        "TokenFixture",
        vec![RelationField::new("value", ValueSchema::String, false).unwrap()],
        vec![],
    )
    .unwrap();
    let relations = RelationCatalog::admit(vec![relation]).unwrap();
    let entities = EntityCatalog::admit(vec![]).unwrap();
    let transfer = RawSnapshotPublish {
        local: &local,
        remote: &remote,
        session: &session,
        relations: &relations,
        entities: &entities,
        limits: SnapshotTransferLimits::new(4096, 4, 4096, 8192),
    };
    assert_eq!(
        publish_raw_snapshot(intent, &claim, current, transfer).await,
        Err(RawSnapshotPublishError::Selection(
            RawStorageMismatch::RestrictedRequiresProtection
        ))
    );
    assert_eq!(remote.0.load(Ordering::Relaxed), 0);
    assert_eq!(session.stats().operations, 0);

    local
        .put(ContentBlock::new(ContentCodec::Raw, b"synthetic-arrow"))
        .unwrap();
    local
        .put(ContentBlock::new(ContentCodec::Raw, b"coverage"))
        .unwrap();
    let unrestricted = [SourceLabel {
        restricted: false,
        ..labels[0]
    }];
    let allowed_intent = RawStorageIntent {
        sources: &unrestricted,
        ..intent
    };
    let allowed_claim = RawStorageClaim {
        sources: &unrestricted,
        ..claim
    };
    let transfer = RawSnapshotPublish {
        local: &local,
        remote: &remote,
        session: &session,
        relations: &relations,
        entities: &entities,
        limits: SnapshotTransferLimits::new(4096, 4, 4096, 8192),
    };
    let published = publish_raw_snapshot(allowed_intent, &allowed_claim, current, transfer)
        .await
        .unwrap();
    assert_eq!(published.root(), source.cid());
    assert_eq!(remote.0.load(Ordering::Relaxed), 3);
    assert_eq!(session.stats().operations, 3);
}
