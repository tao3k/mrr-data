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
    let body = String::from_utf8(plan.deidentify_body().unwrap().to_json_bytes().unwrap()).unwrap();
    assert!(body.contains("cryptoDeterministicConfig"));
    assert_eq!(plan.identity().root(), source.cid());
    let response = GoogleSdpResponse::from_json_bytes(
        br#"{"item":{"table":{"headers":[{"name":"patient_id"},{"name":"study_context"}],"rows":[{"values":[{"stringValue":"c3ludGhldGljLWNpcGhlcnRleHQ="},{"stringValue":"study-1"}]}]}},"overview":{"transformationSummaries":[{"field":{"name":"patient_id"},"results":[{"count":"1","code":"SUCCESS"}]}]}}"#,
    )
    .unwrap();
    let checked = plan.check_response(&response).unwrap();
    assert_eq!(checked.identity().policy_digest(), &policy);
    assert_eq!(checked.identity().governance_epoch(), 7);
    assert_eq!(checked.identity().dataset(), "cohort-a");
    assert_eq!(checked.identity().input_digest(), &digest);
    assert_eq!(checked.token(), "c3ludGhldGljLWNpcGhlcnRleHQ=");
    let expected_token_digest: [u8; 32] = Sha256::digest(checked.token().as_bytes()).into();
    assert_eq!(checked.token_digest(), expected_token_digest);
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
