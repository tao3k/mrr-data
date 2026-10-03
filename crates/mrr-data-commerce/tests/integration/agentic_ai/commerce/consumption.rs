//! Test-only commit/provider models; no production payment or durability evidence.
use super::credentials::{claims, trust};
use super::transactions::{TestPort, complete};
use super::{Fixture, signer};
use mrr_data_commerce::budget_commit::{CurrentCommerceAuthority, commit_shared_reservation};
use mrr_data_commerce::consumption::{
    ConsumptionClaimError, ConsumptionClaimRequest, ConsumptionError, ConsumptionLedgerClaims,
    CurrentConsumptionAuthority, DispatchPermit, PaymentDispatchClaims, ProviderId, claim_dispatch,
};
use mrr_data_commerce::credential::{CredentialAdmissionRequest, CredentialClaims};
use mrr_data_commerce::provider::{
    PaymentOutcome, PaymentProviderPort, ProviderDispatchError, ProviderFailure, ProviderFuture,
    ProviderReceiptClaims, ProviderReceiptTrust, SignedProviderReceipt, recover_payment,
    release_payment,
};
use mrr_data_content::{
    CacheAdmission, ConditionalCommitFuture, ConditionalCommitPortError,
    ConditionalContentCommitOutcome, ConditionalContentCommitPort, ConditionalContentReceipt,
    ConditionalContentWrite, ContentBlock, ContentCodec, ContentRevision, PublishReceipt,
};
use p256::ecdsa::{Signature, signature::Signer};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct DualPort {
    budget: TestPort,
    consumption: TestPort,
}
impl DualPort {
    fn route(&self, scope: &str) -> &TestPort {
        if scope == "buyer-trip-root" {
            &self.budget
        } else {
            &self.consumption
        }
    }
    fn new(fixture: &Fixture, ledger: &ConsumptionLedgerClaims) -> Self {
        let port = Self {
            budget: TestPort::new(&fixture.before),
            consumption: TestPort::new_head(head(ledger)),
        };
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
}
impl ConditionalContentCommitPort for DualPort {
    type Error = &'static str;
    fn commit<'a, V, F>(
        &'a self,
        write: ConditionalContentWrite<'a>,
        physical: Option<&'a PublishReceipt>,
        validate: F,
    ) -> ConditionalCommitFuture<'a, ConditionalContentCommitOutcome<'a>, Self::Error, V>
    where
        V: Send + 'a,
        F: FnOnce(Option<ContentRevision>) -> Result<(), V> + Send + 'a,
    {
        self.route(write.scope).commit(write, physical, validate)
    }
    fn recover<'a>(
        &'a self,
        write: ConditionalContentWrite<'a>,
    ) -> ConditionalCommitFuture<'a, Option<ConditionalContentReceipt<'a>>, Self::Error> {
        self.route(write.scope).recover(write)
    }
}
fn head(ledger: &ConsumptionLedgerClaims) -> ContentRevision {
    ContentRevision {
        revision: ledger.revision,
        root: ContentBlock::new(ContentCodec::Raw, &serde_json::to_vec(ledger).unwrap()).cid(),
    }
}
fn before() -> ConsumptionLedgerClaims {
    let data = super::projection();
    serde_json::from_value(data["consumptionBefore"].clone()).unwrap()
}
fn fresh(fixture: &Fixture) -> CurrentConsumptionAuthority {
    let bytes = serde_json::to_vec(&fixture.after).unwrap();
    CurrentConsumptionAuthority {
        commerce: CurrentCommerceAuthority {
            host: fixture.host(),
            now: 10,
        },
        issuers: trust(),
        budget: ContentRevision {
            revision: fixture.after.revision,
            root: ContentBlock::new(ContentCodec::Raw, &bytes).cid(),
        },
        budget_bytes: bytes,
    }
}
fn claim(
    port: &DualPort,
    fixture: &Fixture,
    credential: &CredentialClaims,
    ledger: &ConsumptionLedgerClaims,
    live: CurrentConsumptionAuthority,
) -> Result<DispatchPermit, ConsumptionClaimError<&'static str>> {
    let provider: ProviderId = "processor".into();
    let dispatch = PaymentDispatchClaims::new(provider.clone(), credential.clone());
    let proposed = ledger
        .prepare(dispatch)
        .map_err(ConsumptionClaimError::Validation)?;
    let current_bytes = serde_json::to_vec(ledger).unwrap();
    let proposed_bytes = serde_json::to_vec(&proposed).unwrap();
    let physical = PublishReceipt {
        cid: head(&proposed).root,
        cache: CacheAdmission::Stored,
    };
    let signature: Signature = signer(6).sign(&credential.signing_bytes().unwrap());
    let issuer_trust = trust();
    let prepared_budget = serde_json::to_vec(&fixture.after).unwrap();
    fixture.with_request(&fixture.before, &fixture.after, true, |request| {
        complete(claim_dispatch(
            port,
            ConsumptionClaimRequest {
                provider: &provider,
                current: head(ledger),
                current_bytes: &current_bytes,
                proposed_bytes: &proposed_bytes,
                physical: &physical,
                credential: CredentialAdmissionRequest {
                    issuer_trust: &issuer_trust,
                    expected_scope: "buyer-trip-root",
                    expected_issuer: &"wallet".into(),
                    credential,
                    signature: signature.to_bytes().as_slice(),
                    signed: request.signed,
                    before_bytes: request.current_bytes,
                    committed_bytes: request.proposed_bytes,
                    current: ContentRevision {
                        revision: fixture.after.revision,
                        root: ContentBlock::new(ContentCodec::Raw, &prepared_budget).cid(),
                    },
                    current_bytes: &prepared_budget,
                },
            },
            || Ok(live),
            |lineage, offer| lineage == fixture.lineage && offer == &fixture.lean_offer,
        ))
    })
}

#[derive(Default)]
struct Provider {
    calls: AtomicUsize,
    mode: AtomicUsize,
    row: Mutex<Option<(PaymentDispatchClaims, SignedProviderReceipt)>>,
}
impl Provider {
    fn receipt(request: &PaymentDispatchClaims, altered: bool) -> SignedProviderReceipt {
        let claims = ProviderReceiptClaims {
            provider_id: request.provider_id.clone(),
            request_commitment: if altered {
                "other-checkout".into()
            } else {
                request.commitment().unwrap()
            },
            outcome: PaymentOutcome::Succeeded,
            reference: "provider-payment-42".into(),
        };
        let signature: Signature = signer(8).sign(&claims.signing_bytes().unwrap());
        SignedProviderReceipt {
            claims,
            signature: signature.to_bytes().to_vec(),
        }
    }
}
impl PaymentProviderPort for Provider {
    type Error = &'static str;
    fn dispatch(
        &self,
        permit: DispatchPermit,
    ) -> ProviderFuture<'_, SignedProviderReceipt, Self::Error> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let request = permit.request().clone();
            let receipt = Self::receipt(&request, self.mode.load(Ordering::SeqCst) == 2);
            *self.row.lock().unwrap() = Some((request, receipt.clone()));
            if self.mode.load(Ordering::SeqCst) == 1 {
                Err(ProviderFailure::Unknown("lost response"))
            } else {
                Ok(receipt)
            }
        })
    }
    fn recover<'a>(
        &'a self,
        request: &'a PaymentDispatchClaims,
    ) -> ProviderFuture<'a, Option<SignedProviderReceipt>, Self::Error> {
        Box::pin(async move {
            if self.mode.load(Ordering::SeqCst) == 3 {
                return Err(ProviderFailure::Unknown("unavailable"));
            }
            Ok(self
                .row
                .lock()
                .unwrap()
                .as_ref()
                .filter(|(old, _)| old.idempotency_key == request.idempotency_key)
                .map(|(_, receipt)| receipt.clone()))
        })
    }
}
fn provider_trust() -> ProviderReceiptTrust {
    let mut trust = ProviderReceiptTrust::default();
    trust.enroll("processor".into(), *signer(8).verifying_key());
    trust
}

#[test]
fn exact_lean_claim_releases_once_and_verifies_provider_receipt() {
    let fixture = Fixture::lean();
    let credential = claims(&fixture);
    let ledger = before();
    let port = DualPort::new(&fixture, &ledger);
    let request = PaymentDispatchClaims::new("processor".into(), credential.clone());
    let proposed = ledger.prepare(request.clone()).unwrap();
    let data = super::projection();
    let mut source: ConsumptionLedgerClaims =
        serde_json::from_value(data["consumptionAfterTemplate"].clone()).unwrap();
    source.requests[0].idempotency_key = request.idempotency_key;
    source.requests[0].credential.receipt.expected_content_id =
        credential.receipt.expected_content_id.clone();
    source.requests[0].credential.receipt.committed_content_id =
        credential.receipt.committed_content_id.clone();
    assert_eq!(source, proposed);
    let permit = claim(&port, &fixture, &credential, &ledger, fresh(&fixture)).unwrap();
    let provider = Provider::default();
    let receipt = complete(release_payment(&provider, permit, &provider_trust())).unwrap();
    assert_eq!(receipt.claims().outcome, PaymentOutcome::Succeeded);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        claim(&port, &fixture, &credential, &ledger, fresh(&fixture)),
        Err(ConsumptionClaimError::Validation(
            ConsumptionError::AlreadyConsumed
        ))
    ));
}

#[test]
fn reissued_credential_or_provider_change_cannot_consume_same_purchase() {
    let fixture = Fixture::lean();
    let credential = claims(&fixture);
    let ledger = before();
    let port = DualPort::new(&fixture, &ledger);
    let permit = claim(&port, &fixture, &credential, &ledger, fresh(&fixture)).unwrap();
    let recorded = ledger.prepare(permit.request().clone()).unwrap();
    let mut reissued = credential.clone();
    reissued.credential_id = "reissued".into();
    assert!(matches!(
        claim(&port, &fixture, &reissued, &recorded, fresh(&fixture)),
        Err(ConsumptionClaimError::Validation(
            ConsumptionError::AlreadyConsumed
        ))
    ));
    assert!(
        recorded
            .prepare(PaymentDispatchClaims::new(
                "other-provider".into(),
                reissued.clone()
            ))
            .is_err()
    );
    assert!(matches!(
        claim(&port, &fixture, &reissued, &ledger, fresh(&fixture)),
        Err(ConsumptionClaimError::Port(
            ConditionalCommitPortError::Protocol(_)
        ))
    ));
    assert_eq!(
        PaymentDispatchClaims::new("processor".into(), credential).idempotency_key,
        PaymentDispatchClaims::new("other-provider".into(), reissued).idempotency_key
    );
}

#[test]
fn lost_consumption_ack_grants_no_permit_even_after_exact_recovery() {
    let fixture = Fixture::lean();
    let credential = claims(&fixture);
    let ledger = before();
    let port = DualPort::new(&fixture, &ledger);
    port.consumption.lose_ack();
    assert!(matches!(
        claim(&port, &fixture, &credential, &ledger, fresh(&fixture)),
        Err(ConsumptionClaimError::Port(
            ConditionalCommitPortError::Unknown(_)
        ))
    ));
    assert!(matches!(
        claim(&port, &fixture, &credential, &ledger, fresh(&fixture)),
        Err(ConsumptionClaimError::Validation(
            ConsumptionError::AlreadyConsumed
        ))
    ));
}

#[test]
fn unknown_payment_recovers_exact_signed_status_without_another_release() {
    let fixture = Fixture::lean();
    let credential = claims(&fixture);
    let ledger = before();
    let port = DualPort::new(&fixture, &ledger);
    let permit = claim(&port, &fixture, &credential, &ledger, fresh(&fixture)).unwrap();
    let original = permit.request().clone();
    let provider = Provider::default();
    provider.mode.store(1, Ordering::SeqCst);
    assert!(matches!(
        complete(release_payment(&provider, permit, &provider_trust())),
        Err(ProviderDispatchError::Provider(ProviderFailure::Unknown(_)))
    ));
    let receipt = complete(recover_payment(&provider, &original, &provider_trust()))
        .unwrap()
        .unwrap();
    assert_eq!(receipt.claims().outcome, PaymentOutcome::Succeeded);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert!(claim(&port, &fixture, &credential, &ledger, fresh(&fixture)).is_err());
    provider.mode.store(3, Ordering::SeqCst);
    assert!(matches!(
        complete(recover_payment(&provider, &original, &provider_trust())),
        Err(ProviderDispatchError::Provider(ProviderFailure::Unknown(_)))
    ));
}

#[test]
fn substituted_receipt_and_current_provider_key_revocation_are_denied() {
    let fixture = Fixture::lean();
    let credential = claims(&fixture);
    let ledger = before();
    let port = DualPort::new(&fixture, &ledger);
    let permit = claim(&port, &fixture, &credential, &ledger, fresh(&fixture)).unwrap();
    let request = permit.request().clone();
    let provider = Provider::default();
    provider.mode.store(2, Ordering::SeqCst);
    assert!(matches!(
        complete(release_payment(&provider, permit, &provider_trust())),
        Err(ProviderDispatchError::Validation(
            ConsumptionError::ReceiptMismatch
        ))
    ));
    *provider.row.lock().unwrap() = Some((request.clone(), Provider::receipt(&request, false)));
    let mut trust = provider_trust();
    trust.enroll("processor".into(), *signer(9).verifying_key());
    assert!(complete(recover_payment(&provider, &request, &trust)).is_err());
    trust.revoke(&"processor".into());
    assert!(complete(recover_payment(&provider, &request, &trust)).is_err());
    let mut changed = request;
    changed
        .credential
        .receipt
        .reservation
        .purchase
        .terms
        .amount_minor += 1;
    assert!(complete(recover_payment(&provider, &changed, &provider_trust())).is_err());
}

#[test]
fn claim_refreshes_budget_and_issuer_authority_before_consumption() {
    let fixture = Fixture::lean();
    let credential = claims(&fixture);
    let ledger = before();
    let port = DualPort::new(&fixture, &ledger);
    let mut live = fresh(&fixture);
    live.issuers.revoke(&"wallet".into());
    assert!(claim(&port, &fixture, &credential, &ledger, live).is_err());
    let mut live = fresh(&fixture);
    let mut budget = fixture.after.clone();
    budget.revision += 1;
    budget
        .revoked_mandate_ids
        .push(fixture.root.mandate_id.clone());
    live.budget_bytes = serde_json::to_vec(&budget).unwrap();
    live.budget = ContentRevision {
        revision: budget.revision,
        root: ContentBlock::new(ContentCodec::Raw, &live.budget_bytes).cid(),
    };
    assert!(claim(&port, &fixture, &credential, &ledger, live).is_err());
    assert!(claim(&port, &fixture, &credential, &ledger, fresh(&fixture)).is_ok());
}

#[test]
fn competing_claims_release_only_one_provider_call() {
    let fixture = Fixture::lean();
    let credential = claims(&fixture);
    let ledger = before();
    let port = DualPort::new(&fixture, &ledger);
    let provider = Provider::default();
    let barrier = std::sync::Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..2)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    match claim(&port, &fixture, &credential, &ledger, fresh(&fixture)) {
                        Ok(permit) => {
                            complete(release_payment(&provider, permit, &provider_trust()))
                                .unwrap();
                            true
                        }
                        Err(_) => false,
                    }
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.into_iter().filter(|released| *released).count(), 1);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
}
