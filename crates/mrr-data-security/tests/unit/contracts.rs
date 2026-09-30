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

use crate::data_protection::{
    CurrentStorageStateV1, EntityRef, RawStorageDestination, RawStorageTier, SourceLabel,
    StorageClaimV1, StorageEffectV1,
};
use serde_json::Value;

fn fixture_str<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key].as_str().unwrap()
}

fn fixture_entity(value: &Value) -> EntityRef<'_> {
    EntityRef {
        type_name: fixture_str(value, "type"),
        id: fixture_str(value, "id"),
    }
}

struct FixtureEffect<'a> {
    value: &'a Value,
    root: cid::Cid,
    sources: Vec<SourceLabel<'a>>,
    owners: Vec<EntityRef<'a>>,
}

impl<'a> FixtureEffect<'a> {
    fn new(value: &'a Value) -> Self {
        assert_eq!(value["version"], 1);
        let text = fixture_str(value, "snapshot_cid");
        let root = cid::Cid::try_from(text).unwrap();
        assert_eq!(root.to_string(), text, "CID wire form must be canonical");
        let sources = value["sources"]
            .as_array()
            .unwrap()
            .iter()
            .map(|source| SourceLabel {
                resource: fixture_entity(&source["resource"]),
                owner: fixture_entity(&source["owner"]),
                tenant: fixture_str(source, "tenant"),
                restricted: source["restricted"].as_bool().unwrap(),
            })
            .collect();
        let owners = value["destination"]["accepted_owners"]
            .as_array()
            .unwrap()
            .iter()
            .map(fixture_entity)
            .collect();
        Self {
            value,
            root,
            sources,
            owners,
        }
    }

    fn projected(&self) -> StorageEffectV1<'_> {
        let value = self.value;
        let destination = &value["destination"];
        StorageEffectV1 {
            operation_id: fixture_str(value, "operation_id"),
            subject: fixture_entity(&value["subject"]),
            purpose: fixture_str(value, "purpose"),
            snapshot_root: &self.root,
            sources: &self.sources,
            destination: RawStorageDestination {
                resource: fixture_entity(&destination["resource"]),
                tenant: fixture_str(destination, "tenant"),
                accepted_owners: &self.owners,
                accepts_restricted: destination["accepts_restricted"].as_bool().unwrap(),
                tier: match fixture_str(destination, "tier") {
                    "remote" => RawStorageTier::Remote,
                    "durable-local" => RawStorageTier::DurableLocal,
                    other => panic!("unknown tier {other}"),
                },
            },
            policy_root: fixture_str(value, "policy_root"),
            lineage_revision: fixture_str(value, "lineage_revision"),
        }
    }
}

#[test]
fn storage_effect_v1_matches_pinned_spec_matrix() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../fixtures/storage-effect-v1.json")).unwrap();
    assert_eq!(fixture["schema"], "cedar-poo-storage-effect-v1");
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 17);
    for case in cases {
        let effect = FixtureEffect::new(&case["effect"]);
        let claim_effect = FixtureEffect::new(&case["claim"]["effect"]);
        let current = &case["current"];
        let claim = StorageClaimV1 {
            effect: claim_effect.projected(),
            epoch: case["claim"]["epoch"].as_u64().unwrap(),
            expires_at: case["claim"]["expires_at"].as_u64().unwrap(),
            allowed: case["claim"]["allowed"].as_bool().unwrap(),
        };
        let current = CurrentStorageStateV1 {
            policy_root: fixture_str(current, "policy_root"),
            lineage_revision: fixture_str(current, "lineage_revision"),
            epoch: current["epoch"].as_u64().unwrap(),
            now: current["now"].as_u64().unwrap(),
        };
        assert_eq!(
            effect.projected().check_raw(&claim, current).is_ok(),
            case["allow"].as_bool().unwrap(),
            "SPEC fixture {}",
            case["name"]
        );
    }
}

#[cfg(feature = "raw-publish")]
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "deny-before-I/O and allowed snapshot transfer fixture"
)]
async fn raw_snapshot_gate_precedes_remote_io_and_allows_unrestricted() {
    use crate::data_protection::{
        RawSnapshotPublish, RawSnapshotPublishError, RawStorageMismatch, publish_raw_snapshot,
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

    let snapshot = snapshot();
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
    let destination = RawStorageDestination {
        resource: EntityRef {
            type_name: "Bucket",
            id: "archive",
        },
        tenant: "tenant-a",
        accepted_owners: &owners,
        accepts_restricted: false,
        tier: RawStorageTier::Remote,
    };
    let effect = StorageEffectV1 {
        operation_id: "op-001",
        subject: EntityRef {
            type_name: "Service",
            id: "publisher",
        },
        purpose: "archive",
        snapshot_root: snapshot.cid(),
        sources: &labels,
        destination,
        policy_root: "policy-root-1",
        lineage_revision: "lineage-1",
    };
    let claim = StorageClaimV1 {
        effect,
        epoch: 4,
        expires_at: 100,
        allowed: true,
    };
    let current = CurrentStorageStateV1 {
        policy_root: "policy-root-1",
        lineage_revision: "lineage-1",
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
        snapshot.manifest().relations()[0].relation_id(),
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
        snapshot: &snapshot,
        relations: &relations,
        entities: &entities,
        limits: SnapshotTransferLimits::new(4096, 4, 4096, 8192),
    };
    assert_eq!(
        publish_raw_snapshot(effect, &claim, current, transfer).await,
        Err(RawSnapshotPublishError::Selection(
            RawStorageMismatch::RestrictedRequiresProtection
        ))
    );
    assert_eq!(remote.0.load(Ordering::Relaxed), 0);
    assert_eq!(session.stats().operations, 0);

    let other_root = raw_cid(b"not-this-snapshot");
    let wrong_root = StorageEffectV1 {
        snapshot_root: &other_root,
        ..effect
    };
    let transfer = RawSnapshotPublish {
        local: &local,
        remote: &remote,
        session: &session,
        snapshot: &snapshot,
        relations: &relations,
        entities: &entities,
        limits: SnapshotTransferLimits::new(4096, 4, 4096, 8192),
    };
    assert_eq!(
        publish_raw_snapshot(
            wrong_root,
            &StorageClaimV1 {
                effect: wrong_root,
                ..claim
            },
            current,
            transfer
        )
        .await,
        Err(RawSnapshotPublishError::Selection(
            RawStorageMismatch::DifferentEffect
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
    let allowed_effect = StorageEffectV1 {
        sources: &unrestricted,
        ..effect
    };
    let allowed_claim = StorageClaimV1 {
        effect: allowed_effect,
        ..claim
    };
    let transfer = RawSnapshotPublish {
        local: &local,
        remote: &remote,
        session: &session,
        snapshot: &snapshot,
        relations: &relations,
        entities: &entities,
        limits: SnapshotTransferLimits::new(4096, 4, 4096, 8192),
    };
    let published = publish_raw_snapshot(allowed_effect, &allowed_claim, current, transfer)
        .await
        .unwrap();
    assert_eq!(published.root(), snapshot.cid());
    assert_eq!(remote.0.load(Ordering::Relaxed), 3);
    assert_eq!(session.stats().operations, 3);
}
