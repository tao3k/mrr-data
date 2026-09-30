use meta_relational_reasoning::{
    EntityCatalog, ExternalRevisionIdentity, GenerationId, RelationCatalog, RelationField,
    RelationId, RelationSchema, RevisionBinding, SemanticSnapshot, ValueSchema,
};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, RelationDescriptor, SnapshotBlock,
    SnapshotManifest, SnapshotManifestRequest, SnapshotRowBinding, raw_cid,
};

pub(super) fn snapshot() -> SnapshotBlock {
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
    CurrentStorageStateV1, EntityRef, ProtectedCommitDispositionV1, ProtectedCommitReceiptV1,
    ProtectedPhysicalAckV1, ProtectedPublicationV1, ProtectedReadClaimV1, ProtectedReadDestination,
    ProtectedReadIntentV1, ProtectionClaimV1, ProtectionIntentV1, RawStorageDestination,
    RawStorageTier, SourceLabel, StorageClaimV1, StorageEffectV1,
};
use serde_json::Value;

fn fixture_str<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key].as_str().unwrap()
}

fn fixture_cid(value: &Value, key: &str) -> cid::Cid {
    let text = fixture_str(value, key);
    let cid = cid::Cid::try_from(text).unwrap();
    assert_eq!(cid.to_string(), text, "CID wire form must be canonical");
    cid
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

struct FixtureIntent<'a> {
    value: &'a Value,
    storage: FixtureEffect<'a>,
}

impl<'a> FixtureIntent<'a> {
    fn new(value: &'a Value) -> Self {
        assert_eq!(value["version"], 1);
        Self {
            value,
            storage: FixtureEffect::new(&value["storage"]),
        }
    }

    fn projected(&self) -> ProtectionIntentV1<'_> {
        ProtectionIntentV1 {
            storage: self.storage.projected(),
            profile: fixture_str(self.value, "profile"),
            key_ref: fixture_str(self.value, "key_ref"),
            key_version: fixture_str(self.value, "key_version"),
            residency: fixture_str(self.value, "residency"),
        }
    }
}

fn fixture_current(value: &Value) -> CurrentStorageStateV1<'_> {
    CurrentStorageStateV1 {
        policy_root: fixture_str(value, "policy_root"),
        lineage_revision: fixture_str(value, "lineage_revision"),
        epoch: value["epoch"].as_u64().unwrap(),
        now: value["now"].as_u64().unwrap(),
    }
}

#[test]
fn protected_storage_v1_matches_pinned_spec_matrix() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../fixtures/protected-storage-v1.json")).unwrap();
    assert_eq!(fixture["schema"], "cedar-poo-protected-storage-v1");
    check_protected_intents(&fixture);
    check_protected_publications(&fixture);
    check_protected_commits(&fixture);
    check_protected_reads(&fixture);
}

struct FixturePublication<'a> {
    value: &'a Value,
    intent: FixtureIntent<'a>,
    outer: cid::Cid,
}

impl<'a> FixturePublication<'a> {
    fn new(value: &'a Value) -> Self {
        Self {
            value,
            intent: FixtureIntent::new(&value["intent"]),
            outer: fixture_cid(value, "outer_root_cid"),
        }
    }

    fn projected(&self) -> ProtectedPublicationV1<'_> {
        ProtectedPublicationV1 {
            intent: self.intent.projected(),
            outer_root: &self.outer,
            envelope_version: self.value["envelope_version"]
                .as_u64()
                .unwrap()
                .try_into()
                .unwrap(),
            key_version: fixture_str(self.value, "key_version"),
        }
    }
}

struct FixtureRead<'a> {
    value: &'a Value,
    publication: FixturePublication<'a>,
    owners: Vec<EntityRef<'a>>,
}

impl<'a> FixtureRead<'a> {
    fn new(value: &'a Value) -> Self {
        assert_eq!(value["version"], 1);
        Self {
            value,
            publication: FixturePublication::new(&value["publication"]),
            owners: value["reader"]["accepted_owners"]
                .as_array()
                .unwrap()
                .iter()
                .map(fixture_entity)
                .collect(),
        }
    }

    fn projected(&self) -> ProtectedReadIntentV1<'_> {
        let reader = &self.value["reader"];
        ProtectedReadIntentV1 {
            operation_id: fixture_str(self.value, "operation_id"),
            subject: fixture_entity(&self.value["subject"]),
            purpose: fixture_str(self.value, "purpose"),
            publication: self.publication.projected(),
            reader: ProtectedReadDestination {
                resource: fixture_entity(&reader["resource"]),
                tenant: fixture_str(reader, "tenant"),
                accepted_owners: &self.owners,
                accepts_restricted: reader["accepts_restricted"].as_bool().unwrap(),
            },
            policy_root: fixture_str(self.value, "policy_root"),
            lineage_revision: fixture_str(self.value, "lineage_revision"),
        }
    }
}

fn check_protected_reads(fixture: &Value) {
    let cases = fixture["read_cases"].as_array().unwrap();
    assert_eq!(cases.len(), 15);
    for case in cases {
        let read = FixtureRead::new(&case["read"]);
        let claimed = FixtureRead::new(&case["claim"]["intent"]);
        let claim = ProtectedReadClaimV1 {
            intent: claimed.projected(),
            epoch: case["claim"]["epoch"].as_u64().unwrap(),
            expires_at: case["claim"]["expires_at"].as_u64().unwrap(),
            allowed: case["claim"]["allowed"].as_bool().unwrap(),
        };
        let committed_value = &case["committed"];
        let committed_publication = (!committed_value.is_null())
            .then(|| FixturePublication::new(&committed_value["publication"]));
        let committed =
            committed_publication
                .as_ref()
                .map(|publication| ProtectedCommitReceiptV1 {
                    publication: publication.projected(),
                    child_count: committed_value["child_count"]
                        .as_u64()
                        .unwrap()
                        .try_into()
                        .unwrap(),
                    total_outer_bytes: committed_value["total_outer_bytes"]
                        .as_u64()
                        .unwrap()
                        .try_into()
                        .unwrap(),
                });
        assert_eq!(
            read.projected()
                .check_read(
                    &claim,
                    fixture_current(&case["current"]),
                    committed.as_ref()
                )
                .is_ok(),
            case["allow"].as_bool().unwrap(),
            "SPEC read fixture {}",
            case["name"]
        );
    }
}

fn check_protected_intents(fixture: &Value) {
    let intents = fixture["intent_cases"].as_array().unwrap();
    assert_eq!(intents.len(), 14);
    for case in intents {
        let intent = FixtureIntent::new(&case["intent"]);
        let claimed = FixtureIntent::new(&case["claim"]["intent"]);
        let claim = ProtectionClaimV1 {
            intent: claimed.projected(),
            epoch: case["claim"]["epoch"].as_u64().unwrap(),
            expires_at: case["claim"]["expires_at"].as_u64().unwrap(),
            allowed: case["claim"]["allowed"].as_bool().unwrap(),
        };
        assert_eq!(
            intent
                .projected()
                .check_intent(&claim, fixture_current(&case["current"]))
                .is_ok(),
            case["allow"].as_bool().unwrap(),
            "SPEC intent fixture {}",
            case["name"]
        );
    }
}

fn check_protected_publications(fixture: &Value) {
    let publications = fixture["publication_cases"].as_array().unwrap();
    assert_eq!(publications.len(), 9);
    for case in publications {
        let value = &case["publication"];
        let intent = FixtureIntent::new(&value["intent"]);
        let claimed = FixtureIntent::new(&case["claim"]["intent"]);
        let text = fixture_str(value, "outer_root_cid");
        let outer = if text.is_empty() {
            None
        } else {
            let cid = cid::Cid::try_from(text).unwrap();
            assert_eq!(cid.to_string(), text, "outer CID must be canonical");
            Some(cid)
        };
        let claim = ProtectionClaimV1 {
            intent: claimed.projected(),
            epoch: case["claim"]["epoch"].as_u64().unwrap(),
            expires_at: case["claim"]["expires_at"].as_u64().unwrap(),
            allowed: case["claim"]["allowed"].as_bool().unwrap(),
        };
        let admitted = outer.is_some_and(|outer| {
            ProtectedPublicationV1 {
                intent: intent.projected(),
                outer_root: &outer,
                envelope_version: value["envelope_version"]
                    .as_u64()
                    .unwrap()
                    .try_into()
                    .unwrap(),
                key_version: fixture_str(value, "key_version"),
            }
            .check_pre_root(&claim, fixture_current(&case["current"]))
            .is_ok()
        });
        assert_eq!(
            admitted,
            case["allow"].as_bool().unwrap(),
            "SPEC publication fixture {}",
            case["name"]
        );
    }
}

fn check_protected_commits(fixture: &Value) {
    let commits = fixture["commit_cases"].as_array().unwrap();
    assert_eq!(commits.len(), 14);
    for case in commits {
        let value = &case["publication"];
        let intent = FixtureIntent::new(&value["intent"]);
        let claimed = FixtureIntent::new(&case["claim"]["intent"]);
        let outer = fixture_cid(value, "outer_root_cid");
        let publication = ProtectedPublicationV1 {
            intent: intent.projected(),
            outer_root: &outer,
            envelope_version: value["envelope_version"]
                .as_u64()
                .unwrap()
                .try_into()
                .unwrap(),
            key_version: fixture_str(value, "key_version"),
        };
        let claim = ProtectionClaimV1 {
            intent: claimed.projected(),
            epoch: case["claim"]["epoch"].as_u64().unwrap(),
            expires_at: case["claim"]["expires_at"].as_u64().unwrap(),
            allowed: case["claim"]["allowed"].as_bool().unwrap(),
        };
        let physical_value = &case["physical"];
        let physical_inner =
            (!physical_value.is_null()).then(|| fixture_cid(physical_value, "inner_root_cid"));
        let physical_outer =
            (!physical_value.is_null()).then(|| fixture_cid(physical_value, "outer_root_cid"));
        let physical =
            physical_inner
                .as_ref()
                .zip(physical_outer.as_ref())
                .map(|(inner_root, outer_root)| ProtectedPhysicalAckV1 {
                    inner_root,
                    outer_root,
                    child_count: physical_value["child_count"]
                        .as_u64()
                        .unwrap()
                        .try_into()
                        .unwrap(),
                    total_outer_bytes: physical_value["total_outer_bytes"]
                        .as_u64()
                        .unwrap()
                        .try_into()
                        .unwrap(),
                });
        let existing_value = &case["existing"];
        let existing_intent = (!existing_value.is_null())
            .then(|| FixtureIntent::new(&existing_value["publication"]["intent"]));
        let existing_outer = (!existing_value.is_null())
            .then(|| fixture_cid(&existing_value["publication"], "outer_root_cid"));
        let existing = existing_intent.as_ref().zip(existing_outer.as_ref()).map(
            |(existing_intent, existing_outer)| ProtectedCommitReceiptV1 {
                publication: ProtectedPublicationV1 {
                    intent: existing_intent.projected(),
                    outer_root: existing_outer,
                    envelope_version: existing_value["publication"]["envelope_version"]
                        .as_u64()
                        .unwrap()
                        .try_into()
                        .unwrap(),
                    key_version: fixture_str(&existing_value["publication"], "key_version"),
                },
                child_count: existing_value["child_count"]
                    .as_u64()
                    .unwrap()
                    .try_into()
                    .unwrap(),
                total_outer_bytes: existing_value["total_outer_bytes"]
                    .as_u64()
                    .unwrap()
                    .try_into()
                    .unwrap(),
            },
        );
        let actual = match publication.decide_commit(
            &claim,
            fixture_current(&case["current"]),
            physical,
            existing.as_ref(),
        ) {
            Ok(ProtectedCommitDispositionV1::Apply) => "apply",
            Ok(ProtectedCommitDispositionV1::Replay) => "replay",
            Err(_) => "reject",
        };
        assert_eq!(
            actual,
            fixture_str(case, "disposition"),
            "SPEC commit fixture {}",
            case["name"]
        );
    }
}

#[test]
fn storage_profile_matrix_matches_pinned_spec_cross_product() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../fixtures/storage-profiles-v1.json")).unwrap();
    assert_eq!(fixture["schema"], "cedar-poo-storage-profiles-v1");
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 16);
    let mut raw_allowed = 0;
    let mut protected_allowed = 0;
    for case in cases {
        assert_eq!(
            case["profile_composed"], true,
            "LeanPoo profile {}",
            case["name"]
        );
        let effect = FixtureEffect::new(&case["effect"]);
        let storage = effect.projected();
        let protection = &case["protection"];
        let intent = ProtectionIntentV1 {
            storage,
            profile: fixture_str(protection, "profile"),
            key_ref: fixture_str(protection, "key_ref"),
            key_version: fixture_str(protection, "key_version"),
            residency: fixture_str(protection, "residency"),
        };
        let current = fixture_current(&case["current"]);
        let raw = storage
            .check_raw(
                &StorageClaimV1 {
                    effect: storage,
                    epoch: 4,
                    expires_at: 100,
                    allowed: true,
                },
                current,
            )
            .is_ok();
        let protected = intent
            .check_intent(
                &ProtectionClaimV1 {
                    intent,
                    epoch: 4,
                    expires_at: 100,
                    allowed: true,
                },
                current,
            )
            .is_ok();
        assert_eq!(
            raw,
            case["raw_allow"].as_bool().unwrap(),
            "{}",
            case["name"]
        );
        assert_eq!(
            protected,
            case["protected_allow"].as_bool().unwrap(),
            "{}",
            case["name"]
        );
        raw_allowed += usize::from(raw);
        protected_allowed += usize::from(protected);
    }
    assert_eq!(raw_allowed, 4);
    assert_eq!(protected_allowed, 6);
}

#[cfg(feature = "protected-envelope")]
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one envelope fixture checks every authenticated binding substitution"
)]
fn protected_envelope_randomizes_outer_identity_and_authenticates_binding() {
    use crate::data_protection::{
        ProtectedBlockBindingV1, ProtectedBlockRole, ProtectedEnvelopeError, ProtectedEnvelopeKey,
        open_block, seal_block,
    };
    use mrr_data_content::{ContentBlock, ContentCodec};

    let fixture: Value =
        serde_json::from_str(include_str!("../../fixtures/protected-storage-v1.json")).unwrap();
    let intent = FixtureIntent::new(&fixture["intent_cases"][0]["intent"]);
    let projected = intent.projected();
    let bytes = b"private child bytes";
    let inner = ContentBlock::new(ContentCodec::Raw, bytes).cid();
    let binding = ProtectedBlockBindingV1 {
        intent: projected,
        inner_cid: &inner,
        role: ProtectedBlockRole::Child,
        key_version: "key-version-7",
    };
    let key = ProtectedEnvelopeKey::aes_256_gcm(&[7_u8; 32]).unwrap();
    let first = seal_block(binding, bytes, &key, 1024).unwrap();
    let second = seal_block(binding, bytes, &key, 1024).unwrap();
    assert_ne!(first.outer_cid(), second.outer_cid());
    assert!(
        !first
            .bytes()
            .windows(bytes.len())
            .any(|window| window == bytes)
    );
    assert!(
        !first
            .bytes()
            .windows(inner.to_string().len())
            .any(|window| window == inner.to_string().as_bytes())
    );
    assert_eq!(
        open_block(binding, first.outer_cid(), first.bytes(), &key, 1024)
            .unwrap()
            .as_slice(),
        bytes
    );
    let changed_residency = ProtectedBlockBindingV1 {
        intent: ProtectionIntentV1 {
            residency: "other-region",
            ..projected
        },
        ..binding
    };
    let changed_tenant = ProtectedBlockBindingV1 {
        intent: ProtectionIntentV1 {
            storage: StorageEffectV1 {
                destination: RawStorageDestination {
                    tenant: "tenant-b",
                    ..projected.storage.destination
                },
                ..projected.storage
            },
            ..projected
        },
        ..binding
    };
    let other_sources = [SourceLabel {
        owner: EntityRef {
            type_name: "Team",
            id: "other",
        },
        ..projected.storage.sources[0]
    }];
    let changed_owner = ProtectedBlockBindingV1 {
        intent: ProtectionIntentV1 {
            storage: StorageEffectV1 {
                sources: &other_sources,
                ..projected.storage
            },
            ..projected
        },
        ..binding
    };
    let changed_key_ref = ProtectedBlockBindingV1 {
        intent: ProtectionIntentV1 {
            key_ref: "other-key",
            ..projected
        },
        ..binding
    };
    let changed_key_version = ProtectedBlockBindingV1 {
        key_version: "key-version-8",
        ..binding
    };
    let changed_role = ProtectedBlockBindingV1 {
        role: ProtectedBlockRole::Root,
        ..binding
    };
    let other_inner = ContentBlock::new(ContentCodec::Raw, b"other").cid();
    let changed_inner = ProtectedBlockBindingV1 {
        inner_cid: &other_inner,
        ..binding
    };
    for changed in [
        changed_residency,
        changed_tenant,
        changed_owner,
        changed_key_ref,
        changed_role,
        changed_inner,
    ] {
        assert_eq!(
            open_block(changed, first.outer_cid(), first.bytes(), &key, 1024),
            Err(ProtectedEnvelopeError::Cryptography)
        );
    }
    assert_eq!(
        open_block(
            changed_key_version,
            first.outer_cid(),
            first.bytes(),
            &key,
            1024
        ),
        Err(ProtectedEnvelopeError::InvalidProfile)
    );
    let other_key = ProtectedEnvelopeKey::aes_256_gcm(&[8_u8; 32]).unwrap();
    assert_eq!(
        open_block(binding, first.outer_cid(), first.bytes(), &other_key, 1024),
        Err(ProtectedEnvelopeError::Cryptography)
    );
    let mut tampered = first.bytes().to_vec();
    *tampered.last_mut().unwrap() ^= 1;
    let tampered_cid = ContentBlock::new(ContentCodec::Raw, &tampered).cid();
    assert_eq!(
        open_block(binding, &tampered_cid, &tampered, &key, 1024),
        Err(ProtectedEnvelopeError::Cryptography)
    );
    assert_eq!(
        seal_block(binding, b"wrong bytes", &key, 1024),
        Err(ProtectedEnvelopeError::InvalidInner)
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
