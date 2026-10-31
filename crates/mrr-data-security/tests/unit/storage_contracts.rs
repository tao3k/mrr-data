use crate::data_protection::{
    CurrentStorageState, EntityRef, ProtectedCommitDisposition, ProtectedCommitReceipt,
    ProtectedPhysicalAck, ProtectedPublication, ProtectedReadClaim, ProtectedReadDestination,
    ProtectedReadIntent, ProtectionClaim, ProtectionIntent, RawStorageDestination, RawStorageTier,
    SourceLabel, StorageClaim, StorageEffect,
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
        assert!(value.get("version").is_none());
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

    fn projected(&self) -> StorageEffect<'_> {
        let value = self.value;
        let destination = &value["destination"];
        StorageEffect {
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
fn storage_effect_matches_pinned_spec_matrix() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../fixtures/storage-effect-v1.json")).unwrap();
    assert_eq!(fixture["schema"], "cedar-poo-storage-effect-v1");
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 17);
    for case in cases {
        let effect = FixtureEffect::new(&case["effect"]);
        let claim_effect = FixtureEffect::new(&case["claim"]["effect"]);
        let current = &case["current"];
        let claim = StorageClaim {
            effect: claim_effect.projected(),
            epoch: case["claim"]["epoch"].as_u64().unwrap(),
            expires_at: case["claim"]["expires_at"].as_u64().unwrap(),
            allowed: case["claim"]["allowed"].as_bool().unwrap(),
        };
        let current = CurrentStorageState {
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
        assert!(value.get("version").is_none());
        Self {
            value,
            storage: FixtureEffect::new(&value["storage"]),
        }
    }

    fn projected(&self) -> ProtectionIntent<'_> {
        ProtectionIntent {
            storage: self.storage.projected(),
            profile: fixture_str(self.value, "profile"),
            key_ref: fixture_str(self.value, "key_ref"),
            key_version: fixture_str(self.value, "key_version"),
            residency: fixture_str(self.value, "residency"),
        }
    }
}

fn fixture_current(value: &Value) -> CurrentStorageState<'_> {
    CurrentStorageState {
        policy_root: fixture_str(value, "policy_root"),
        lineage_revision: fixture_str(value, "lineage_revision"),
        epoch: value["epoch"].as_u64().unwrap(),
        now: value["now"].as_u64().unwrap(),
    }
}

#[test]
fn protected_storage_matches_pinned_spec_matrix() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../fixtures/protected-storage-v1.json")).unwrap();
    assert_eq!(fixture["schema"], "cedar-poo-protected-storage-v1");
    check_protected_intents(&fixture);
    check_protected_publications(&fixture);
    check_protected_commits(&fixture);
    check_protected_reads(&fixture);
    check_protected_read_releases(&fixture);
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

    fn projected(&self) -> ProtectedPublication<'_> {
        ProtectedPublication {
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

struct FixtureReceipt<'a> {
    value: &'a Value,
    publication: FixturePublication<'a>,
}

impl<'a> FixtureReceipt<'a> {
    fn new(value: &'a Value) -> Self {
        Self {
            value,
            publication: FixturePublication::new(&value["publication"]),
        }
    }

    fn projected(&self) -> ProtectedCommitReceipt<'_> {
        ProtectedCommitReceipt {
            publication: self.publication.projected(),
            child_count: self.value["child_count"]
                .as_u64()
                .unwrap()
                .try_into()
                .unwrap(),
            total_outer_bytes: self.value["total_outer_bytes"]
                .as_u64()
                .unwrap()
                .try_into()
                .unwrap(),
        }
    }
}

struct FixtureRead<'a> {
    value: &'a Value,
    receipt: FixtureReceipt<'a>,
    owners: Vec<EntityRef<'a>>,
}

impl<'a> FixtureRead<'a> {
    fn new(value: &'a Value) -> Self {
        assert!(value.get("version").is_none());
        Self {
            value,
            receipt: FixtureReceipt::new(&value["receipt"]),
            owners: value["reader"]["accepted_owners"]
                .as_array()
                .unwrap()
                .iter()
                .map(fixture_entity)
                .collect(),
        }
    }

    fn projected(&self) -> ProtectedReadIntent<'_> {
        let reader = &self.value["reader"];
        ProtectedReadIntent {
            operation_id: fixture_str(self.value, "operation_id"),
            subject: fixture_entity(&self.value["subject"]),
            purpose: fixture_str(self.value, "purpose"),
            receipt: self.receipt.projected(),
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
    assert_eq!(cases.len(), 18);
    for case in cases {
        let read = FixtureRead::new(&case["read"]);
        let claimed = FixtureRead::new(&case["claim"]["intent"]);
        let claim = ProtectedReadClaim {
            intent: claimed.projected(),
            epoch: case["claim"]["epoch"].as_u64().unwrap(),
            expires_at: case["claim"]["expires_at"].as_u64().unwrap(),
            allowed: case["claim"]["allowed"].as_bool().unwrap(),
        };
        let committed_value = &case["committed"];
        let committed_fixture =
            (!committed_value.is_null()).then(|| FixtureReceipt::new(committed_value));
        let committed = committed_fixture.as_ref().map(FixtureReceipt::projected);
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

fn check_protected_read_releases(fixture: &Value) {
    let cases = fixture["read_release_cases"].as_array().unwrap();
    assert_eq!(cases.len(), 11);
    for case in cases {
        let read = FixtureRead::new(&case["read"]);
        let claimed = FixtureRead::new(&case["claim"]["intent"]);
        let claim = ProtectedReadClaim {
            intent: claimed.projected(),
            epoch: case["claim"]["epoch"].as_u64().unwrap(),
            expires_at: case["claim"]["expires_at"].as_u64().unwrap(),
            allowed: case["claim"]["allowed"].as_bool().unwrap(),
        };
        let before_value = &case["committed_before"];
        let before_fixture = (!before_value.is_null()).then(|| FixtureReceipt::new(before_value));
        let before_receipt = before_fixture.as_ref().map(FixtureReceipt::projected);
        let after_value = &case["committed_after"];
        let after_fixture = (!after_value.is_null()).then(|| FixtureReceipt::new(after_value));
        let after_receipt = after_fixture.as_ref().map(FixtureReceipt::projected);
        assert_eq!(
            read.projected()
                .check_release(
                    &claim,
                    (fixture_current(&case["before"]), before_receipt.as_ref()),
                    (fixture_current(&case["after"]), after_receipt.as_ref()),
                )
                .is_ok(),
            case["allow"].as_bool().unwrap(),
            "SPEC read release fixture {}",
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
        let claim = ProtectionClaim {
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
        let claim = ProtectionClaim {
            intent: claimed.projected(),
            epoch: case["claim"]["epoch"].as_u64().unwrap(),
            expires_at: case["claim"]["expires_at"].as_u64().unwrap(),
            allowed: case["claim"]["allowed"].as_bool().unwrap(),
        };
        let admitted = outer.is_some_and(|outer| {
            ProtectedPublication {
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
        let publication = ProtectedPublication {
            intent: intent.projected(),
            outer_root: &outer,
            envelope_version: value["envelope_version"]
                .as_u64()
                .unwrap()
                .try_into()
                .unwrap(),
            key_version: fixture_str(value, "key_version"),
        };
        let claim = ProtectionClaim {
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
                .map(|(inner_root, outer_root)| ProtectedPhysicalAck {
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
            |(existing_intent, existing_outer)| ProtectedCommitReceipt {
                publication: ProtectedPublication {
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
            Ok(ProtectedCommitDisposition::Apply) => "apply",
            Ok(ProtectedCommitDisposition::Replay) => "replay",
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
        let intent = ProtectionIntent {
            storage,
            profile: fixture_str(protection, "profile"),
            key_ref: fixture_str(protection, "key_ref"),
            key_version: fixture_str(protection, "key_version"),
            residency: fixture_str(protection, "residency"),
        };
        let current = fixture_current(&case["current"]);
        let raw = storage
            .check_raw(
                &StorageClaim {
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
                &ProtectionClaim {
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
        ProtectedBlockBinding, ProtectedBlockRole, ProtectedEnvelopeError, ProtectedEnvelopeKey,
        open_block, seal_block,
    };
    use mrr_data_content::{ContentBlock, ContentCodec};

    let fixture: Value =
        serde_json::from_str(include_str!("../../fixtures/protected-storage-v1.json")).unwrap();
    let intent = FixtureIntent::new(&fixture["intent_cases"][0]["intent"]);
    let projected = intent.projected();
    let bytes = b"private child bytes";
    let inner = ContentBlock::new(ContentCodec::Raw, bytes).cid();
    let binding = ProtectedBlockBinding {
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
    let changed_residency = ProtectedBlockBinding {
        intent: ProtectionIntent {
            residency: "other-region",
            ..projected
        },
        ..binding
    };
    let changed_tenant = ProtectedBlockBinding {
        intent: ProtectionIntent {
            storage: StorageEffect {
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
    let changed_owner = ProtectedBlockBinding {
        intent: ProtectionIntent {
            storage: StorageEffect {
                sources: &other_sources,
                ..projected.storage
            },
            ..projected
        },
        ..binding
    };
    let changed_key_ref = ProtectedBlockBinding {
        intent: ProtectionIntent {
            key_ref: "other-key",
            ..projected
        },
        ..binding
    };
    let changed_key_version = ProtectedBlockBinding {
        key_version: "key-version-8",
        ..binding
    };
    let changed_role = ProtectedBlockBinding {
        role: ProtectedBlockRole::Root,
        ..binding
    };
    let other_inner = ContentBlock::new(ContentCodec::Raw, b"other").cid();
    let changed_inner = ProtectedBlockBinding {
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
