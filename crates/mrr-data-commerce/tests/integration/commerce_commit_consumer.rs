use cedar_poo_commerce::admission::{
    AdmissionError, CommerceAdmissionHost, DelegatedAdmissionRequest, SignedDelegation,
};
use cedar_poo_commerce::projection::{LeanMandateClaims, LeanOfferClaims};
use cedar_poo_commerce::signatures::{
    MandatePayload, OfferPayload, PrincipalId, delegation_signing_bytes, sha256_hex,
};
use mrr_data_commerce::budget_commit::{
    BudgetCommitError, SharedBudgetClaims, SharedBudgetCommitRequest, SharedBudgetReservation,
    decide_shared_reservation_commit,
};
use mrr_data_content::{
    CacheAdmission, ConditionalCommitDisposition, ConditionalCommitError, ContentBlock,
    ContentCodec, ContentRevision, PublishReceipt,
};
use p256::ecdsa::{Signature, SigningKey, signature::Signer};

fn projection() -> serde_json::Value {
    serde_json::from_slice(include_bytes!("../fixtures/commerce-projection-v1.json")).unwrap()
}

fn signer(seed: u8) -> SigningKey {
    SigningKey::from_bytes((&[seed; 32]).into()).unwrap()
}

fn key(seed: u8) -> String {
    signer(seed)
        .verifying_key()
        .to_encoded_point(true)
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Clone)]
struct Fixture {
    root: MandatePayload,
    child: MandatePayload,
    offer: OfferPayload,
    checkout: Vec<u8>,
    lineage: Vec<LeanMandateClaims>,
    lean_offer: LeanOfferClaims,
    before: SharedBudgetClaims,
    after: SharedBudgetClaims,
}

impl Fixture {
    fn lean() -> Self {
        let fixture = projection();
        let root = MandatePayload {
            mandate_id: "trip-root".into(),
            principal: PrincipalId {
                entity_type_id: "Account".into(),
                entity_type_path: vec!["finance".into()],
                entity_id: "buyer".into(),
            },
            agent_id: "travel-agent".into(),
            agent_public_key: key(2).into(),
            allowed_merchants: vec!["ride-seller".into()],
            allowed_products: vec!["airport-ride".into()],
            asset: "HKD".into(),
            per_purchase_cap: 50_000,
            total_cap: 80_000,
            policy_epoch: 4,
            expires_at: 30,
        };
        let child = MandatePayload {
            mandate_id: "trip-child".into(),
            agent_id: "booking-agent".into(),
            agent_public_key: key(4).into(),
            per_purchase_cap: 40_000,
            total_cap: 60_000,
            expires_at: 25,
            ..root.clone()
        };
        let checkout = br#"{"ride":"airport","total_minor":30000}"#.to_vec();
        let offer = OfferPayload {
            merchant_id: "ride-seller".into(),
            product_id: "airport-ride".into(),
            offer_id: "quote-42".into(),
            checkout_sha256: sha256_hex(&checkout),
            amount_minor: 30_000,
            asset: "HKD".into(),
            expires_at: 20,
        };
        Self {
            root,
            child,
            offer,
            checkout,
            lineage: serde_json::from_value(fixture["lineage"].clone()).unwrap(),
            lean_offer: serde_json::from_value(fixture["offer"].clone()).unwrap(),
            before: serde_json::from_value(fixture["sharedBefore"].clone()).unwrap(),
            after: serde_json::from_value(fixture["sharedAfter"].clone()).unwrap(),
        }
    }

    fn host(&self) -> CommerceAdmissionHost {
        let mut host = CommerceAdmissionHost::new();
        host.enroll_principal_key(self.root.principal.clone(), *signer(1).verifying_key());
        host.enroll_merchant_key(self.offer.merchant_id.clone(), *signer(3).verifying_key());
        host.set_policy_epoch(self.root.principal.clone(), 4)
            .unwrap();
        host
    }

    fn decide(
        &self,
        host: &CommerceAdmissionHost,
        before: &SharedBudgetClaims,
        after: &SharedBudgetClaims,
        acknowledged: bool,
    ) -> Result<ConditionalCommitDisposition, BudgetCommitError> {
        self.with_request(before, after, acknowledged, |request| {
            decide_shared_reservation_commit(host, request, |claims, offer| {
                claims == self.lineage && offer == &self.lean_offer
            })
        })
    }

    fn with_request<T>(
        &self,
        before: &SharedBudgetClaims,
        after: &SharedBudgetClaims,
        acknowledged: bool,
        execute: impl FnOnce(SharedBudgetCommitRequest<'_>) -> T,
    ) -> T {
        let root_signature: Signature = signer(1).sign(&self.root.signing_bytes());
        let child_signature: Signature =
            signer(2).sign(&delegation_signing_bytes(&self.root, &self.child));
        let offer_signature: Signature = signer(3).sign(&self.offer.signing_bytes());
        let child_signature_bytes = child_signature.to_bytes();
        let delegations = [SignedDelegation {
            child: self.child.clone(),
            signature: child_signature_bytes.as_slice(),
        }];
        let before_bytes = serde_json::to_vec(before).unwrap();
        let after_bytes = serde_json::to_vec(after).unwrap();
        let current = ContentRevision {
            revision: before.revision,
            root: ContentBlock::new(ContentCodec::Raw, &before_bytes).cid(),
        };
        let physical = PublishReceipt {
            cid: ContentBlock::new(
                ContentCodec::Raw,
                if acknowledged {
                    &after_bytes
                } else {
                    b"other state"
                },
            )
            .cid(),
            cache: CacheAdmission::Stored,
        };
        execute(SharedBudgetCommitRequest {
            signed: DelegatedAdmissionRequest {
                root: self.root.clone(),
                root_signature: root_signature.to_bytes().as_slice(),
                delegations: if self.lineage.len() == 1 {
                    &[]
                } else {
                    &delegations
                },
                offer: self.offer.clone(),
                checkout_bytes: &self.checkout,
                offer_signature: offer_signature.to_bytes().as_slice(),
                lean_lineage: &self.lineage,
                lean_offer: &self.lean_offer,
                now: after.now,
            },
            scope: "buyer-trip-root",
            operation_id: &after.reservations[0].purchase.purchase_id,
            current,
            current_bytes: &before_bytes,
            proposed_bytes: &after_bytes,
            physical: &physical,
        })
    }

    fn next(&self, before: &SharedBudgetClaims) -> SharedBudgetClaims {
        let mut after = before.clone();
        after.revision += 1;
        after
            .reservations
            .insert(0, self.after.reservations[0].clone());
        after
    }

    fn previous(&self, amount: u64, root_only: bool) -> SharedBudgetReservation {
        let mut previous = self.after.reservations[0].clone();
        previous.purchase.purchase_id = format!("previous-buy-{amount}");
        previous.purchase.terms.offer_id = format!("previous-quote-{amount}").into();
        previous.purchase.terms.amount_minor = amount;
        if root_only {
            previous.lineage.truncate(1);
            previous.purchase.agent_id = self.lineage[0].agent_id.clone();
            previous.purchase.mandate_id = self.lineage[0].mandate_id.clone();
        }
        previous
    }
}

#[test]
fn exact_lean_transition_plans_one_cid_bound_commit() {
    let fixture = Fixture::lean();
    let next = fixture
        .decide(&fixture.host(), &fixture.before, &fixture.after, true)
        .unwrap();
    let ConditionalCommitDisposition::Apply(committed) = next else {
        panic!("new reservation must Apply")
    };
    assert_eq!(committed.revision, 2);
    assert_eq!(
        committed.root,
        ContentBlock::new(
            ContentCodec::Raw,
            &serde_json::to_vec(&fixture.after).unwrap()
        )
        .cid()
    );
}

#[test]
fn parent_and_root_cumulative_caps_are_independent() {
    let fixture = Fixture::lean();
    let mut before = fixture.before.clone();
    before.reservations.push(fixture.previous(40_000, false));
    before.revision = 2;
    assert_eq!(
        fixture.decide(&fixture.host(), &before, &fixture.next(&before), true),
        Err(BudgetCommitError::BudgetExceeded)
    );
    before.reservations = vec![
        fixture.previous(50_000, true),
        fixture.previous(10_000, true),
    ];
    before.revision = 3;
    assert_eq!(
        fixture.decide(&fixture.host(), &before, &fixture.next(&before), true),
        Err(BudgetCommitError::BudgetExceeded)
    );
}

#[test]
fn reservations_cannot_rewrite_prior_entries_or_reset_identity_caps() {
    let fixture = Fixture::lean();
    let mut before = fixture.before.clone();
    before.reservations.push(fixture.previous(10_000, false));
    let mut after = fixture.next(&before);
    after.reservations[1].purchase.terms.amount_minor = 0;
    assert_eq!(
        fixture.decide(&fixture.host(), &before, &after, true),
        Err(BudgetCommitError::InvalidTransition)
    );
    before.reservations[0].lineage[1].total_cap = 55_000;
    assert_eq!(
        fixture.decide(&fixture.host(), &before, &fixture.next(&before), true),
        Err(BudgetCommitError::ReboundMandate)
    );
}

#[test]
fn duplicate_offer_cannot_acquire_another_operation_id() {
    let fixture = Fixture::lean();
    let mut before = fixture.before.clone();
    let mut previous = fixture.previous(30_000, true);
    previous.purchase.terms = fixture.lean_offer.terms.clone();
    before.reservations.push(previous);
    assert_eq!(
        fixture.decide(&fixture.host(), &before, &fixture.next(&before), true),
        Err(BudgetCommitError::DuplicatePurchaseOrOffer)
    );
}

#[test]
fn current_revocation_and_exact_publication_are_required() {
    let fixture = Fixture::lean();
    assert_eq!(
        fixture.decide(&fixture.host(), &fixture.before, &fixture.after, false),
        Err(BudgetCommitError::Commit(
            ConditionalCommitError::DifferentPublication
        ))
    );
    let mut host = fixture.host();
    host.revoke_mandate(
        fixture.root.principal.clone(),
        fixture.root.mandate_id.clone(),
    );
    assert_eq!(
        fixture.decide(&host, &fixture.before, &fixture.after, true),
        Err(BudgetCommitError::Admission(AdmissionError::RevokedMandate))
    );
    let mut before = fixture.before.clone();
    before
        .revoked_mandate_ids
        .push(fixture.child.mandate_id.clone());
    assert_eq!(
        fixture.decide(&fixture.host(), &before, &fixture.next(&before), true),
        Err(BudgetCommitError::RevokedMandate)
    );
}

#[test]
fn root_purchases_use_the_same_shared_allocator_protocol() {
    let mut fixture = Fixture::lean();
    fixture.lineage.truncate(1);
    fixture.after.reservations[0].lineage = fixture.lineage.clone();
    fixture.after.reservations[0].purchase.agent_id = fixture.root.agent_id.clone();
    fixture.after.reservations[0].purchase.mandate_id = fixture.root.mandate_id.clone();
    assert!(
        fixture
            .decide(&fixture.host(), &fixture.before, &fixture.after, true)
            .is_ok()
    );
}

#[test]
fn mutated_revision_and_time_cannot_be_committed() {
    let fixture = Fixture::lean();
    let mut after = fixture.after.clone();
    after.revision = 3;
    assert_eq!(
        fixture.decide(&fixture.host(), &fixture.before, &after, true),
        Err(BudgetCommitError::InvalidTransition)
    );
    after = fixture.after.clone();
    after.now = 9;
    assert_eq!(
        fixture.decide(&fixture.host(), &fixture.before, &after, true),
        Err(BudgetCommitError::InvalidTransition)
    );
}

#[path = "agentic_ai/commerce/budget_transactions.rs"]
mod transactions;

#[cfg(feature = "credential")]
#[path = "agentic_ai/commerce/credentials.rs"]
mod credentials;

#[cfg(feature = "consumption")]
#[path = "agentic_ai/commerce/consumption.rs"]
mod consumption;

#[cfg(feature = "presentation")]
#[path = "agentic_ai/commerce/presentation.rs"]
mod presentation;
