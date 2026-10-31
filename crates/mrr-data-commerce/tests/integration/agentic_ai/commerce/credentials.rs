//! Signed receipt/credential consumers using the Lean reservation template.

use super::transactions::{TestPort, complete};
use super::{Fixture, signer};
use mrr_data_commerce::budget_commit::{
    CurrentCommerceAuthority, SharedBudgetClaims, commit_shared_reservation,
};
use mrr_data_commerce::credential::{
    CredentialAdmissionRequest, CredentialClaims, CredentialError, CredentialIssuerTrust,
    CredentialRecoveryError, VerifiedCredential, admit_committed_credential,
};
use mrr_data_content::{ConditionalCommitPortError, ContentBlock, ContentCodec, ContentRevision};
use p256::ecdsa::{Signature, signature::Signer};

type Result = std::result::Result<VerifiedCredential, CredentialRecoveryError<&'static str>>;

pub(super) fn claims(fixture: &Fixture) -> CredentialClaims {
    let exported = super::projection();
    let mut credential: CredentialClaims =
        serde_json::from_value(exported["credentialTemplate"].clone()).unwrap();
    // Only these Host-resolved byte commitments vary from the Lean template.
    credential.receipt.expected_content_id = ContentBlock::new(
        ContentCodec::Raw,
        &serde_json::to_vec(&fixture.before).unwrap(),
    )
    .cid()
    .to_string()
    .into();
    credential.receipt.committed_content_id = ContentBlock::new(
        ContentCodec::Raw,
        &serde_json::to_vec(&fixture.after).unwrap(),
    )
    .cid()
    .to_string()
    .into();
    assert_eq!(
        credential.receipt.reservation,
        fixture.after.reservations[0]
    );
    credential
}

pub(super) fn trust() -> CredentialIssuerTrust {
    let mut issuers = CredentialIssuerTrust::default();
    issuers.enroll("wallet".into(), *signer(6).verifying_key());
    issuers
}

fn committed_port(fixture: &Fixture) -> TestPort {
    let port = TestPort::new(&fixture.before);
    fixture.with_request(&fixture.before, &fixture.after, true, |request| {
        complete(commit_shared_reservation(
            &port,
            request,
            || {
                Ok(CurrentCommerceAuthority {
                    host: fixture.host(),
                    now: 10,
                })
            },
            |lineage, offer| lineage == fixture.lineage && offer == &fixture.lean_offer,
        ))
        .unwrap();
    });
    port
}

fn check(
    fixture: &Fixture,
    port: &TestPort,
    current: &SharedBudgetClaims,
    credential: &CredentialClaims,
    issuers: &CredentialIssuerTrust,
    authority: CurrentCommerceAuthority,
) -> Result {
    let signature: Signature = signer(6).sign(&credential.signing_bytes().unwrap());
    check_signature(
        fixture, port, current, credential, issuers, authority, &signature,
    )
}

fn check_signature(
    fixture: &Fixture,
    port: &TestPort,
    current: &SharedBudgetClaims,
    credential: &CredentialClaims,
    issuers: &CredentialIssuerTrust,
    authority: CurrentCommerceAuthority,
    signature: &Signature,
) -> Result {
    fixture.with_request(&fixture.before, &fixture.after, true, |request| {
        let bytes = serde_json::to_vec(current).unwrap();
        complete(admit_committed_credential(
            port,
            CredentialAdmissionRequest {
                expected_scope: "buyer-trip-root",
                expected_issuer: &"wallet".into(),
                issuer_trust: issuers,
                credential,
                signature: signature.to_bytes().as_slice(),
                signed: request.signed,
                before_bytes: request.current_bytes,
                committed_bytes: request.proposed_bytes,
                current: ContentRevision {
                    revision: current.revision,
                    root: ContentBlock::new(ContentCodec::Raw, &bytes).cid(),
                },
                current_bytes: &bytes,
            },
            || Ok(authority),
            |lineage, offer| lineage == fixture.lineage && offer == &fixture.lean_offer,
        ))
    })
}

fn authority(fixture: &Fixture) -> CurrentCommerceAuthority {
    CurrentCommerceAuthority {
        host: fixture.host(),
        now: 10,
    }
}

#[test]
fn exact_committed_lean_reservation_admits_signed_credential() {
    let fixture = Fixture::lean();
    let credential = claims(&fixture);
    let validated = check(
        &fixture,
        &committed_port(&fixture),
        &fixture.after,
        &credential,
        &trust(),
        authority(&fixture),
    )
    .unwrap();
    assert_eq!(validated.claims(), &credential);
}

#[test]
fn later_head_retains_exact_receipt_without_charging_budget_again() {
    let fixture = Fixture::lean();
    let port = committed_port(&fixture);
    let mut current = fixture.after.clone();
    current.revision += 1;
    current
        .reservations
        .insert(0, fixture.previous(30_000, true));
    assert!(
        check(
            &fixture,
            &port,
            &current,
            &claims(&fixture),
            &trust(),
            authority(&fixture)
        )
        .is_ok()
    );
    current
        .reservations
        .retain(|entry| entry.purchase.purchase_id != "buy-child");
    assert!(matches!(
        check(
            &fixture,
            &port,
            &current,
            &claims(&fixture),
            &trust(),
            authority(&fixture)
        ),
        Err(CredentialRecoveryError::Validation(
            CredentialError::MissingCurrentReservation
        ))
    ));
}

#[test]
fn proposal_and_unavailable_ledger_never_become_committed_credentials() {
    let fixture = Fixture::lean();
    let port = TestPort::new(&fixture.before);
    assert!(matches!(
        check(
            &fixture,
            &port,
            &fixture.after,
            &claims(&fixture),
            &trust(),
            authority(&fixture)
        ),
        Err(CredentialRecoveryError::Validation(
            CredentialError::MissingCommit
        ))
    ));
    port.set_unavailable();
    assert!(matches!(
        check(
            &fixture,
            &port,
            &fixture.after,
            &claims(&fixture),
            &trust(),
            authority(&fixture)
        ),
        Err(CredentialRecoveryError::Port(
            ConditionalCommitPortError::BeforeCommit("unavailable")
        ))
    ));
}

#[test]
fn signed_receipt_cannot_substitute_scope_operation_revision_or_content() {
    let fixture = Fixture::lean();
    let port = committed_port(&fixture);
    let credential = claims(&fixture);
    let mut cases = vec![];
    let mut altered = credential.clone();
    altered.receipt.scope = "other".into();
    cases.push(altered);
    let mut altered = credential.clone();
    altered.receipt.operation_id = "other".into();
    cases.push(altered);
    let mut altered = credential.clone();
    altered.receipt.expected_content_id = "other".into();
    cases.push(altered);
    let mut altered = credential.clone();
    altered.receipt.committed_content_id = "other".into();
    cases.push(altered);
    let mut altered = credential.clone();
    altered.receipt.committed_revision += 1;
    cases.push(altered);
    for altered in cases {
        assert!(matches!(
            check(
                &fixture,
                &port,
                &fixture.after,
                &altered,
                &trust(),
                authority(&fixture)
            ),
            Err(CredentialRecoveryError::Validation(
                CredentialError::ReceiptMismatch
            ))
        ));
    }
}

#[test]
fn issuer_signature_rotation_revocation_and_identity_are_current() {
    let fixture = Fixture::lean();
    let port = committed_port(&fixture);
    let credential = claims(&fixture);
    let original: Signature = signer(6).sign(&credential.signing_bytes().unwrap());
    let mut changed = credential.clone();
    changed.credential_id = "substituted".into();
    assert!(matches!(
        check_signature(
            &fixture,
            &port,
            &fixture.after,
            &changed,
            &trust(),
            authority(&fixture),
            &original
        ),
        Err(CredentialRecoveryError::Validation(
            CredentialError::Signature
        ))
    ));
    let mut issuers = trust();
    issuers.enroll("wallet".into(), *signer(7).verifying_key());
    assert!(matches!(
        check(
            &fixture,
            &port,
            &fixture.after,
            &credential,
            &issuers,
            authority(&fixture)
        ),
        Err(CredentialRecoveryError::Validation(
            CredentialError::Signature
        ))
    ));
    issuers.revoke(&"wallet".into());
    assert!(matches!(
        check(
            &fixture,
            &port,
            &fixture.after,
            &credential,
            &issuers,
            authority(&fixture)
        ),
        Err(CredentialRecoveryError::Validation(
            CredentialError::UntrustedIssuer
        ))
    ));
    let mut changed = credential;
    changed.issuer_id = "other-wallet".into();
    assert!(matches!(
        check(
            &fixture,
            &port,
            &fixture.after,
            &changed,
            &trust(),
            authority(&fixture)
        ),
        Err(CredentialRecoveryError::Validation(
            CredentialError::WrongIssuer
        ))
    ));
}

#[test]
fn revoked_ancestor_child_expiry_and_changed_root_deny_credential() {
    let fixture = Fixture::lean();
    let port = committed_port(&fixture);
    for mandate_id in [&fixture.root.mandate_id, &fixture.child.mandate_id] {
        let mut current = fixture.after.clone();
        current.revision += 1;
        current.revoked_mandate_ids.push(mandate_id.clone());
        assert!(
            check(
                &fixture,
                &port,
                &current,
                &claims(&fixture),
                &trust(),
                authority(&fixture)
            )
            .is_err()
        );
    }
    let mut current = fixture.after.clone();
    current.revision += 1;
    current.root.policy_epoch += 1;
    assert!(
        check(
            &fixture,
            &port,
            &current,
            &claims(&fixture),
            &trust(),
            authority(&fixture)
        )
        .is_err()
    );
    assert!(matches!(
        check(
            &fixture,
            &port,
            &fixture.after,
            &claims(&fixture),
            &trust(),
            CurrentCommerceAuthority {
                host: fixture.host(),
                now: 18
            }
        ),
        Err(CredentialRecoveryError::Validation(
            CredentialError::Expired
        ))
    ));
}

#[test]
fn fresh_authority_after_recovery_denies_revoked_signer_or_policy() {
    let fixture = Fixture::lean();
    let port = committed_port(&fixture);
    let mut current = authority(&fixture);
    current.host.revoke_mandate(
        fixture.root.principal.clone(),
        fixture.child.mandate_id.clone(),
    );
    assert!(
        check(
            &fixture,
            &port,
            &fixture.after,
            &claims(&fixture),
            &trust(),
            current
        )
        .is_err()
    );
    let mut current = authority(&fixture);
    current
        .host
        .set_policy_epoch(fixture.root.principal.clone(), 5)
        .unwrap();
    assert!(
        check(
            &fixture,
            &port,
            &fixture.after,
            &claims(&fixture),
            &trust(),
            current
        )
        .is_err()
    );
}

#[test]
fn changed_checkout_or_signed_price_cannot_reuse_original_reservation() {
    let fixture = Fixture::lean();
    let port = committed_port(&fixture);
    let credential = claims(&fixture);
    let mut altered = fixture.clone();
    altered.checkout.push(b' ');
    assert!(
        check(
            &altered,
            &port,
            &fixture.after,
            &credential,
            &trust(),
            authority(&fixture)
        )
        .is_err()
    );
    let mut altered = fixture.clone();
    altered.offer.amount_minor += 1;
    altered.lean_offer.terms.amount_minor += 1;
    assert!(matches!(
        check(
            &altered,
            &port,
            &fixture.after,
            &credential,
            &trust(),
            authority(&fixture)
        ),
        Err(CredentialRecoveryError::Validation(
            CredentialError::ReceiptMismatch
        ))
    ));
}

#[test]
fn current_ancestor_cap_is_checked_and_credential_cannot_outlive_checkout() {
    let fixture = Fixture::lean();
    let port = committed_port(&fixture);
    let mut current = fixture.after.clone();
    current.revision += 1;
    current
        .reservations
        .insert(0, fixture.previous(60_000, false));
    assert!(matches!(
        check(
            &fixture,
            &port,
            &current,
            &claims(&fixture),
            &trust(),
            authority(&fixture)
        ),
        Err(CredentialRecoveryError::Validation(
            CredentialError::InvalidCurrentBudget
        ))
    ));
    let mut credential = claims(&fixture);
    credential.expires_at = 21;
    assert!(matches!(
        check(
            &fixture,
            &port,
            &fixture.after,
            &credential,
            &trust(),
            authority(&fixture)
        ),
        Err(CredentialRecoveryError::Validation(
            CredentialError::Expired
        ))
    ));
}

#[test]
fn caller_cannot_supply_issuer_or_receipt_verified_flags() {
    let fixture = Fixture::lean();
    let value = serde_json::to_value(claims(&fixture)).unwrap();
    for receipt_flag in [false, true] {
        let mut value = value.clone();
        if receipt_flag {
            value["receipt"]["verified"] = true.into();
        } else {
            value["verified"] = true.into();
        }
        assert!(serde_json::from_value::<CredentialClaims>(value).is_err());
    }
}

#[test]
fn signed_reservation_substitution_cannot_reuse_commit_evidence() {
    let fixture = Fixture::lean();
    let port = committed_port(&fixture);
    let original = claims(&fixture);
    let mut changed = original.clone();
    changed.receipt.reservation.purchase.terms.amount_minor += 1;
    assert!(
        check(
            &fixture,
            &port,
            &fixture.after,
            &changed,
            &trust(),
            authority(&fixture)
        )
        .is_err()
    );
    let mut changed = original;
    changed.receipt.reservation.lineage[0].agent_id = "other-agent".into();
    assert!(
        check(
            &fixture,
            &port,
            &fixture.after,
            &changed,
            &trust(),
            authority(&fixture)
        )
        .is_err()
    );
}
