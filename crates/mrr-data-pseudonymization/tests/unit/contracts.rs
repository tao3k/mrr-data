use meta_relational_reasoning::{
    EntityCatalog, ExternalRevisionIdentity, GenerationId, RelationCatalog, RelationField,
    RelationId, RelationSchema, RevisionBinding, SemanticSnapshot, ValueSchema,
};
use mrr_data_core::{
    BatchDescriptor, CoverageDescriptor, CoverageKind, RelationDescriptor, SnapshotBlock,
    SnapshotManifest, SnapshotManifestRequest, raw_cid,
};

use crate::{
    ClaimMismatch, Mode, SelectedTokenInput, TokenAction, TokenAuthorizationClaim,
    TokenAuthorizationRequest, TokenLineage, TokenProfile, compatible_inputs,
    hmac_catalog_separated,
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

fn profile<'a>(mode: Mode, scope: &'a str, wrapping: &'a str) -> TokenProfile<'a> {
    TokenProfile {
        mode,
        scope,
        lineage: TokenLineage {
            tenant: "tenant-a",
            key_domain: "research-key",
            token_key_version: "key-1",
            transform_version: "normalization-1",
            wrapping_version: wrapping,
        },
    }
}

#[test]
fn actual_context_and_recipe_control_token_compatibility() {
    let source = snapshot();
    let digest = [23; 32];
    let first = SelectedTokenInput {
        field: "subject_id",
        value_digest: &digest,
        context: "tenant-a:study-1",
        profile: profile(Mode::AesSiv, "study-1", "wrapping-1"),
    }
    .bind_to(&source);
    let rewrapped = SelectedTokenInput {
        field: "subject_id",
        value_digest: &digest,
        context: "tenant-a:study-1",
        profile: profile(Mode::AesSiv, "study-1", "wrapping-2"),
    }
    .bind_to(&source);
    let changed_context = SelectedTokenInput {
        field: "subject_id",
        value_digest: &digest,
        context: "tenant-a:study-2",
        profile: profile(Mode::AesSiv, "study-1", "wrapping-1"),
    }
    .bind_to(&source);
    assert_eq!(first.source().root(), source.cid());
    assert!(compatible_inputs(&first, &rewrapped));
    assert!(!compatible_inputs(&first, &changed_context));
}

#[test]
fn hmac_scope_separation_is_independent_of_wrapper_rotation() {
    let catalog = [
        profile(Mode::HmacSha256, "study-1", "wrapping-1"),
        profile(Mode::HmacSha256, "study-2", "wrapping-2"),
    ];
    assert!(!hmac_catalog_separated(&catalog));
    assert!(hmac_catalog_separated(&catalog[..1]));
    let same_scope = [
        profile(Mode::HmacSha256, "study-1", "wrapping-1"),
        profile(Mode::HmacSha256, "study-1", "wrapping-2"),
    ];
    assert!(hmac_catalog_separated(&same_scope));
}

#[test]
fn claim_must_match_exact_effect_and_current_governance() {
    let source = snapshot();
    let digest = [23; 32];
    let input = SelectedTokenInput {
        field: "patient_id",
        value_digest: &digest,
        context: "study-1",
        profile: profile(Mode::AesSiv, "study-1", "wrapper-1"),
    }
    .bind_to(&source);
    let policy = [31; 32];
    let request = TokenAuthorizationRequest {
        subject: "researcher-1",
        purpose: "approved-study",
        dataset: "cohort-a",
        action: TokenAction::Deidentify,
        input: &input,
    };
    let claim = TokenAuthorizationClaim {
        subject: request.subject,
        purpose: request.purpose,
        dataset: request.dataset,
        action: request.action,
        root: source.cid(),
        field: input.field(),
        value_digest: &digest,
        context: input.context(),
        profile: *input.profile(),
        policy_digest: &policy,
        governance_epoch: 7,
        expires_at: 100,
    };
    assert_eq!(request.check_claim(&claim, &policy, 7, 99), Ok(()));
    assert_eq!(
        request.check_claim(&claim, &policy, 8, 99),
        Err(ClaimMismatch::Stale)
    );
    assert_eq!(
        request.check_claim(&claim, &policy, 7, 100),
        Err(ClaimMismatch::Stale)
    );
    let other_policy = [32; 32];
    assert_eq!(
        request.check_claim(&claim, &other_policy, 7, 99),
        Err(ClaimMismatch::Stale)
    );
    assert_eq!(
        request.check_claim(
            &TokenAuthorizationClaim {
                dataset: "cohort-b",
                ..claim
            },
            &policy,
            7,
            99
        ),
        Err(ClaimMismatch::DifferentOperation)
    );
    assert_eq!(
        request.check_claim(
            &TokenAuthorizationClaim {
                action: TokenAction::Reidentify,
                ..claim
            },
            &policy,
            7,
            99
        ),
        Err(ClaimMismatch::DifferentOperation)
    );
    let other_digest = [24; 32];
    assert_eq!(
        request.check_claim(
            &TokenAuthorizationClaim {
                value_digest: &other_digest,
                ..claim
            },
            &policy,
            7,
            99
        ),
        Err(ClaimMismatch::DifferentOperation)
    );
}

#[cfg(feature = "google-sdp")]
fn google_selected(
    value: &str,
    dataset: &str,
) -> cedar_poo_bridge::google_sdp::SelectedTabularInput {
    cedar_poo_bridge::google_sdp::SelectedTabularInput {
        dataset: dataset.into(),
        value_field: "patient_id".into(),
        context_field: "study_context".into(),
        value: value.into(),
        context: "study-1".into(),
        key_domain: "research-key".into(),
        token_key_version: "key-1".into(),
        transform_version: "normalization-1".into(),
        wrapping_version: "wrapper-1".into(),
        surrogate_info_type: None,
    }
}

#[cfg(feature = "google-sdp")]
fn google_key() -> cedar_poo_bridge::google_sdp::WrappedKeyBinding {
    cedar_poo_bridge::google_sdp::WrappedKeyBinding {
        key_domain: "research-key".into(),
        token_key_version: "key-1".into(),
        wrapping_version: "wrapper-1".into(),
        kms_key_name: "projects/p/locations/us/keyRings/r/cryptoKeys/k".into(),
        wrapped_key_base64: "a2V5".into(),
    }
}

#[cfg(feature = "google-sdp")]
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "complete de-identify and re-identify boundary fixture"
)]
fn google_request_requires_exact_snapshot_selection() {
    use crate::{CurrentGovernance, prepare_google_aes_siv_deidentify};
    use cedar_poo_bridge::google_sdp::GoogleSdpResponse;
    use sha2::{Digest, Sha256};

    let source = snapshot();
    let digest: [u8; 32] = Sha256::digest(b"synthetic-patient-1").into();
    let input = SelectedTokenInput {
        field: "patient_id",
        value_digest: &digest,
        context: "study-1",
        profile: profile(Mode::AesSiv, "study-1", "wrapper-1"),
    }
    .bind_to(&source);
    let policy = [31; 32];
    let request = TokenAuthorizationRequest {
        subject: "researcher-1",
        purpose: "approved-study",
        dataset: "cohort-a",
        action: TokenAction::Deidentify,
        input: &input,
    };
    let claim = TokenAuthorizationClaim {
        subject: request.subject,
        purpose: request.purpose,
        dataset: request.dataset,
        action: request.action,
        root: source.cid(),
        field: input.field(),
        value_digest: &digest,
        context: input.context(),
        profile: *input.profile(),
        policy_digest: &policy,
        governance_epoch: 7,
        expires_at: 100,
    };
    let current = CurrentGovernance {
        policy_digest: &policy,
        epoch: 7,
        now: 99,
    };
    let plan = prepare_google_aes_siv_deidentify(
        &request,
        &claim,
        current,
        google_selected("synthetic-patient-1", "cohort-a"),
        "projects/p/locations/us".to_owned(),
        google_key(),
    )
    .unwrap();
    assert!(
        plan.deidentify_body(CurrentGovernance {
            now: 100,
            ..current
        })
        .is_err()
    );
    assert!(
        plan.deidentify_body(CurrentGovernance { now: 98, ..current })
            .is_err()
    );
    assert!(
        plan.deidentify_body(CurrentGovernance {
            policy_digest: &[99; 32],
            ..current
        })
        .is_err()
    );
    assert!(
        plan.deidentify_body(CurrentGovernance {
            epoch: 8,
            ..current
        })
        .is_err()
    );
    let body = String::from_utf8(
        plan.deidentify_body(current)
            .unwrap()
            .to_json_bytes()
            .unwrap(),
    )
    .unwrap();
    assert!(body.contains("cryptoDeterministicConfig"));
    assert_eq!(plan.identity().root(), source.cid());
    let response = GoogleSdpResponse::from_json_bytes(
        br#"{"item":{"table":{"headers":[{"name":"patient_id"},{"name":"study_context"}],"rows":[{"values":[{"stringValue":"c3ludGhldGljLWNpcGhlcnRleHQ="},{"stringValue":"study-1"}]}]}},"overview":{"transformationSummaries":[{"field":{"name":"patient_id"},"results":[{"count":"1","code":"SUCCESS"}]}]}}"#,
    )
    .unwrap();
    let checked = plan.check_response(&response, current).unwrap();
    let stale_plan = prepare_google_aes_siv_deidentify(
        &request,
        &claim,
        current,
        google_selected("synthetic-patient-1", "cohort-a"),
        "projects/p/locations/us".to_owned(),
        google_key(),
    )
    .unwrap();
    assert!(
        stale_plan
            .check_response(
                &response,
                CurrentGovernance {
                    now: 100,
                    ..current
                }
            )
            .is_err()
    );
    assert_eq!(checked.identity().policy_digest(), &policy);
    assert_eq!(checked.identity().governance_epoch(), 7);
    assert_eq!(checked.identity().dataset(), "cohort-a");
    assert_eq!(checked.identity().input_digest(), &digest);
    assert_eq!(checked.token(), "c3ludGhldGljLWNpcGhlcnRleHQ=");
    let expected_token_digest: [u8; 32] = Sha256::digest(checked.token().as_bytes()).into();
    assert_eq!(checked.token_digest(), expected_token_digest);
    let reidentify_request = TokenAuthorizationRequest {
        action: TokenAction::Reidentify,
        ..request
    };
    let next_policy = [32; 32];
    let reidentify_claim = TokenAuthorizationClaim {
        action: TokenAction::Reidentify,
        policy_digest: &next_policy,
        governance_epoch: 8,
        ..claim
    };
    let next = CurrentGovernance {
        policy_digest: &next_policy,
        epoch: 8,
        now: 99,
    };
    let prepare_reidentify = |request, claim, current, selected, key| {
        crate::prepare_google_aes_siv_reidentify(
            request,
            claim,
            current,
            selected,
            "projects/p/locations/us".to_owned(),
            key,
            &checked,
        )
    };
    let reidentify = prepare_reidentify(
        &reidentify_request,
        &reidentify_claim,
        next,
        google_selected("synthetic-patient-1", "cohort-a"),
        google_key(),
    )
    .unwrap();
    assert!(reidentify.endpoint().unwrap().ends_with(":reidentify"));
    assert!(
        reidentify
            .reidentify_body(CurrentGovernance { now: 100, ..next })
            .is_err()
    );
    assert!(
        reidentify
            .reidentify_body(CurrentGovernance { now: 98, ..next })
            .is_err()
    );
    assert!(
        reidentify
            .reidentify_body(CurrentGovernance {
                policy_digest: &policy,
                ..next
            })
            .is_err()
    );
    let reidentify_body = reidentify
        .reidentify_body(next)
        .unwrap()
        .to_json_bytes()
        .unwrap();
    assert!(
        String::from_utf8(reidentify_body)
            .unwrap()
            .contains(checked.token())
    );
    let reidentify_response = GoogleSdpResponse::from_json_bytes(
        br#"{"item":{"table":{"headers":[{"name":"patient_id"},{"name":"study_context"}],"rows":[{"values":[{"stringValue":"synthetic-patient-1"},{"stringValue":"study-1"}]}]}},"overview":{"transformationSummaries":[{"field":{"name":"patient_id"},"results":[{"count":"1","code":"SUCCESS"}]}]}}"#,
    )
    .unwrap();
    let restored = reidentify
        .check_response(&reidentify_response, next)
        .unwrap();
    let stale_reidentify = prepare_reidentify(
        &reidentify_request,
        &reidentify_claim,
        next,
        google_selected("synthetic-patient-1", "cohort-a"),
        google_key(),
    )
    .unwrap();
    assert!(
        stale_reidentify
            .check_response(&reidentify_response, CurrentGovernance { now: 100, ..next })
            .is_err()
    );
    assert_eq!(restored.value(), "synthetic-patient-1");
    assert_eq!(restored.identity().policy_digest(), &next_policy);
    assert_eq!(restored.token_digest(), &expected_token_digest);
    assert_eq!(
        restored.deidentify_response_sha256(),
        checked.response_sha256()
    );
    let wrong_plaintext = GoogleSdpResponse::from_json_bytes(
        br#"{"item":{"table":{"headers":[{"name":"patient_id"},{"name":"study_context"}],"rows":[{"values":[{"stringValue":"wrong-patient"},{"stringValue":"study-1"}]}]}},"overview":{"transformationSummaries":[{"field":{"name":"patient_id"},"results":[{"count":"1","code":"SUCCESS"}]}]}}"#,
    )
    .unwrap();
    let reidentify_again = prepare_reidentify(
        &reidentify_request,
        &reidentify_claim,
        next,
        google_selected("synthetic-patient-1", "cohort-a"),
        google_key(),
    )
    .unwrap();
    assert!(
        reidentify_again
            .check_response(&wrong_plaintext, next)
            .is_err()
    );
    assert_eq!(
        prepare_reidentify(
            &request,
            &claim,
            current,
            google_selected("synthetic-patient-1", "cohort-a"),
            google_key(),
        )
        .err(),
        Some(crate::GoogleSelectionMismatch::WrongAction)
    );
    assert_eq!(
        prepare_reidentify(
            &reidentify_request,
            &reidentify_claim,
            current,
            google_selected("synthetic-patient-1", "cohort-a"),
            google_key(),
        )
        .err(),
        Some(crate::GoogleSelectionMismatch::Claim(ClaimMismatch::Stale))
    );
    let mut changed_key = google_key();
    changed_key.wrapped_key_base64 = "b3RoZXI=".into();
    assert_eq!(
        prepare_reidentify(
            &reidentify_request,
            &reidentify_claim,
            next,
            google_selected("synthetic-patient-1", "cohort-a"),
            changed_key,
        )
        .err(),
        Some(crate::GoogleSelectionMismatch::PriorOutput)
    );
    assert_google_rejections(&request, &claim, current);
}

#[cfg(feature = "google-sdp")]
fn assert_google_rejections(
    request: &TokenAuthorizationRequest<'_>,
    claim: &TokenAuthorizationClaim<'_>,
    current: crate::CurrentGovernance<'_>,
) {
    use crate::{GoogleSelectionMismatch, prepare_google_aes_siv_deidentify};

    assert_eq!(
        prepare_google_aes_siv_deidentify(
            request,
            claim,
            current,
            google_selected("synthetic-patient-1", "cohort-b"),
            "projects/p/locations/us".to_owned(),
            google_key(),
        )
        .err(),
        Some(GoogleSelectionMismatch::Dataset)
    );
    assert_eq!(
        prepare_google_aes_siv_deidentify(
            request,
            claim,
            current,
            google_selected("substituted-patient", "cohort-a"),
            "projects/p/locations/us".to_owned(),
            google_key(),
        )
        .err(),
        Some(GoogleSelectionMismatch::ValueDigest)
    );
    assert_eq!(
        prepare_google_aes_siv_deidentify(
            request,
            claim,
            crate::CurrentGovernance {
                epoch: 8,
                ..current
            },
            google_selected("synthetic-patient-1", "cohort-a"),
            "projects/p/locations/us".to_owned(),
            google_key(),
        )
        .err(),
        Some(GoogleSelectionMismatch::Claim(ClaimMismatch::Stale))
    );
}

#[cfg(feature = "google-sdp")]
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "end-to-end cloud selection fixture and rejection matrix"
)]
fn cloud_profile_requires_release_and_transformation_decisions() {
    use crate::{
        CloudDataProtectionSelection, CloudGateMismatch, CloudPseudonymizationGate,
        CurrentGovernance, GoogleSelectionMismatch, prepare_cloud_google_aes_siv_deidentify,
    };
    use cedar_poo_bridge::google_sdp::GoogleSdpResponse;
    use mrr_data_core::SnapshotRowBinding;
    use mrr_data_security::data_protection::{
        DataProtectionDecisions, DataProtectionMismatch, DataProtectionProfile, ReleaseReceiptClaim,
    };
    use sha2::{Digest, Sha256};

    let source = snapshot();
    let relation = &source.manifest().relations()[0];
    let row = SnapshotRowBinding::new(
        &source,
        relation.relation_id(),
        relation.batches()[0].cid(),
        0,
    )
    .unwrap();
    let digest: [u8; 32] = Sha256::digest(b"synthetic-customer-1").into();
    let policy = [31; 32];
    let input = SelectedTokenInput {
        field: "customer_id",
        value_digest: &digest,
        context: "campaign-a",
        profile: TokenProfile {
            mode: Mode::AesSiv,
            scope: "campaign-a",
            lineage: TokenLineage {
                tenant: "customer-a",
                key_domain: "campaign-key",
                token_key_version: "dek-a",
                transform_version: "canonical-v1",
                wrapping_version: "kek-a",
            },
        },
    }
    .bind_to_row(row);
    let request = TokenAuthorizationRequest {
        subject: "data-protection-service",
        purpose: "customer-campaign",
        dataset: "customer-campaign",
        action: TokenAction::Deidentify,
        input: &input,
    };
    let claim = TokenAuthorizationClaim {
        subject: request.subject,
        purpose: request.purpose,
        dataset: request.dataset,
        action: request.action,
        root: source.cid(),
        field: input.field(),
        value_digest: &digest,
        context: input.context(),
        profile: *input.profile(),
        policy_digest: &policy,
        governance_epoch: 7,
        expires_at: 100,
    };
    let release = ReleaseReceiptClaim {
        artifact_digest: "sha256:candidate",
        source_commit: "commit-a",
        policy_root: "CustomerDataRelease",
        epoch: 7,
    };
    let profile = DataProtectionProfile::new(&source, request.dataset, release);
    let both = DataProtectionDecisions {
        policy_root: release.policy_root,
        dataset: request.dataset,
        artifact_digest: release.artifact_digest,
        epoch: 7,
        pipeline_release_allowed: true,
        transformation_allowed: true,
    };
    let gate = CloudPseudonymizationGate {
        target_profile: *input.profile(),
        admitted_context: "campaign-a",
        artifact_digest: release.artifact_digest,
        key_authorized: true,
    };
    let cloud = CloudDataProtectionSelection {
        profile: &profile,
        receipt: release,
        current_epoch: 7,
        decisions: both,
        gate,
    };
    let current = CurrentGovernance {
        policy_digest: &policy,
        epoch: 7,
        now: 99,
    };
    let mut selected = google_selected("synthetic-customer-1", request.dataset);
    selected.value_field = "customer_id".into();
    selected.context_field = "campaign".into();
    selected.context = "campaign-a".into();
    selected.key_domain = "campaign-key".into();
    selected.token_key_version = "dek-a".into();
    selected.transform_version = "canonical-v1".into();
    selected.wrapping_version = "kek-a".into();
    let prepare = |cloud| {
        prepare_cloud_google_aes_siv_deidentify(
            cloud,
            &request,
            &claim,
            current,
            selected.clone(),
            "projects/p/locations/us".to_owned(),
            google_key_for_campaign(),
        )
    };
    let plan = prepare(cloud).unwrap();
    let bound_release = plan.identity().cloud_release().unwrap().clone();
    assert_eq!(bound_release.artifact_digest, release.artifact_digest);
    assert_eq!(bound_release.policy_root, release.policy_root);
    assert_eq!(bound_release.epoch, 7);
    let bound_row = plan.identity().row().unwrap().clone();
    assert_eq!(bound_row.relation_id, relation.relation_id());
    assert_eq!(bound_row.child_cid, *relation.batches()[0].cid());
    assert_eq!(bound_row.row_index, 0);
    let response = GoogleSdpResponse::from_json_bytes(
        br#"{"item":{"table":{"headers":[{"name":"customer_id"},{"name":"campaign"}],"rows":[{"values":[{"stringValue":"c3ludGhldGljLWNpcGhlcnRleHQ="},{"stringValue":"campaign-a"}]}]}},"overview":{"transformationSummaries":[{"field":{"name":"customer_id"},"results":[{"count":"1","code":"SUCCESS"}]}]}}"#,
    )
    .unwrap();
    let checked = plan.check_response(&response, current).unwrap();
    assert_eq!(checked.identity().row().unwrap(), &bound_row);
    assert_eq!(checked.identity().cloud_release().unwrap(), &bound_release);
    let unbound = SelectedTokenInput {
        field: input.field(),
        value_digest: &digest,
        context: input.context(),
        profile: *input.profile(),
    }
    .bind_to(&source);
    let unbound_request = TokenAuthorizationRequest {
        input: &unbound,
        ..request
    };
    assert_eq!(
        prepare_cloud_google_aes_siv_deidentify(
            cloud,
            &unbound_request,
            &claim,
            current,
            selected.clone(),
            "projects/p/locations/us".to_owned(),
            google_key_for_campaign(),
        )
        .err(),
        Some(GoogleSelectionMismatch::RowUnbound)
    );
    assert_eq!(
        prepare_cloud_google_aes_siv_deidentify(
            cloud,
            &request,
            &claim,
            CurrentGovernance {
                epoch: 8,
                ..current
            },
            selected.clone(),
            "projects/p/locations/us".to_owned(),
            google_key_for_campaign(),
        )
        .err(),
        Some(GoogleSelectionMismatch::GovernanceEpoch)
    );
    assert_eq!(
        prepare(CloudDataProtectionSelection {
            current_epoch: 8,
            ..cloud
        })
        .err(),
        Some(GoogleSelectionMismatch::GovernanceEpoch)
    );
    assert_eq!(
        prepare(CloudDataProtectionSelection {
            decisions: DataProtectionDecisions {
                policy_root: "ReleaseReady",
                ..both
            },
            ..cloud
        })
        .err(),
        Some(GoogleSelectionMismatch::DataProtection(
            DataProtectionMismatch::DecisionScope
        ))
    );
    assert_eq!(
        prepare(CloudDataProtectionSelection {
            gate: CloudPseudonymizationGate {
                key_authorized: false,
                ..gate
            },
            ..cloud
        })
        .err(),
        Some(GoogleSelectionMismatch::CloudGate(
            CloudGateMismatch::KeyNotAuthorized
        ))
    );
    assert_eq!(
        prepare(CloudDataProtectionSelection {
            decisions: DataProtectionDecisions {
                pipeline_release_allowed: false,
                ..both
            },
            ..cloud
        })
        .err(),
        Some(GoogleSelectionMismatch::DataProtection(
            DataProtectionMismatch::PipelineDenied
        ))
    );
    assert_eq!(
        prepare(CloudDataProtectionSelection {
            decisions: DataProtectionDecisions {
                transformation_allowed: false,
                ..both
            },
            ..cloud
        })
        .err(),
        Some(GoogleSelectionMismatch::DataProtection(
            DataProtectionMismatch::TransformationDenied
        ))
    );
}

#[cfg(feature = "google-sdp")]
#[test]
fn cloud_gate_mirrors_the_lean_recipe_tenant_context_and_artifact_relation() {
    use crate::{CloudGateMismatch, CloudPseudonymizationGate};

    let source = snapshot();
    let digest = [17; 32];
    let selected = SelectedTokenInput {
        field: "customer_id",
        value_digest: &digest,
        context: "campaign-a",
        profile: TokenProfile {
            mode: Mode::AesSiv,
            scope: "campaign-a",
            lineage: TokenLineage {
                tenant: "customer-a",
                key_domain: "campaign-key",
                token_key_version: "dek-a",
                transform_version: "canonical-v1",
                wrapping_version: "kek-a",
            },
        },
    }
    .bind_to(&source);
    let gate = CloudPseudonymizationGate {
        target_profile: *selected.profile(),
        admitted_context: "campaign-a",
        artifact_digest: "sha256:candidate",
        key_authorized: true,
    };
    assert_eq!(gate.check(&selected, "sha256:candidate"), Ok(()));
    assert_eq!(
        CloudPseudonymizationGate {
            target_profile: TokenProfile {
                mode: Mode::HmacSha256,
                ..gate.target_profile
            },
            ..gate
        }
        .check(&selected, "sha256:candidate"),
        Err(CloudGateMismatch::Mode)
    );
    assert_eq!(
        CloudPseudonymizationGate {
            target_profile: TokenProfile {
                lineage: TokenLineage {
                    token_key_version: "dek-other",
                    ..gate.target_profile.lineage
                },
                ..gate.target_profile
            },
            ..gate
        }
        .check(&selected, "sha256:candidate"),
        Err(CloudGateMismatch::Recipe)
    );
    assert_eq!(
        CloudPseudonymizationGate {
            target_profile: TokenProfile {
                lineage: TokenLineage {
                    tenant: "customer-b",
                    ..gate.target_profile.lineage
                },
                ..gate.target_profile
            },
            ..gate
        }
        .check(&selected, "sha256:candidate"),
        Err(CloudGateMismatch::Tenant)
    );
    assert_eq!(
        CloudPseudonymizationGate {
            key_authorized: false,
            ..gate
        }
        .check(&selected, "sha256:candidate"),
        Err(CloudGateMismatch::KeyNotAuthorized)
    );
    assert_eq!(
        CloudPseudonymizationGate {
            admitted_context: "campaign-b",
            ..gate
        }
        .check(&selected, "sha256:candidate"),
        Err(CloudGateMismatch::Context)
    );
    assert_eq!(
        gate.check(&selected, "sha256:other"),
        Err(CloudGateMismatch::ArtifactDigest)
    );
}

#[cfg(feature = "google-sdp")]
fn google_key_for_campaign() -> cedar_poo_bridge::google_sdp::WrappedKeyBinding {
    let mut key = google_key();
    key.key_domain = "campaign-key".into();
    key.token_key_version = "dek-a".into();
    key.wrapping_version = "kek-a".into();
    key
}
