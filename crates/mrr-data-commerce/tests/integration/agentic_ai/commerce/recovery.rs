use super::{
    DualPort, Fixture, Provider, active_fence, before, claim, claims, complete, fresh, head,
    provider_trust,
};
use mrr_data_commerce::consumption::{
    ConsumptionClaimError, ConsumptionLedgerClaims, PaymentDispatchClaims, ProviderId, RootFence,
};
use mrr_data_commerce::credential::CredentialClaims;
use mrr_data_commerce::provider::{
    ProviderDispatchError, ProviderFailure, ProviderFuture, SignedProviderReceipt, recover_payment,
    release_payment,
};
use mrr_data_content::ConditionalCommitPortError;
use std::sync::atomic::Ordering;

use mrr_data_commerce::consumption::{RecoveredDispatch, recover_dispatch};
use mrr_data_commerce::recovery::{
    DispatchOwnership, OwnedDispatchPermit, ResumablePaymentProviderPort, acquire_dispatch,
    release_owned_payment,
};

impl ResumablePaymentProviderPort for Provider {
    fn ownership<'a>(
        &'a self,
        request: &'a PaymentDispatchClaims,
    ) -> ProviderFuture<'a, Option<DispatchOwnership>, Self::Error> {
        Box::pin(async move {
            let row = self.ownership.lock().unwrap();
            if row
                .as_ref()
                .is_some_and(|r| r.request_commitment != request.commitment().unwrap())
            {
                return Err(ProviderFailure::NotSent("body conflict"));
            }
            Ok(row.clone())
        })
    }
    fn acquire<'a>(
        &'a self,
        dispatch: &'a RecoveredDispatch,
        expected: u64,
        worker: &'a str,
    ) -> ProviderFuture<'a, DispatchOwnership, Self::Error> {
        Box::pin(async move {
            let fence = self.fence.lock().unwrap();
            let request = dispatch.request();
            let commitment = request.commitment().unwrap();
            if !fence.accepts(
                &dispatch.ticket().unwrap(),
                "processor",
                &request.idempotency_key,
                &commitment,
            ) {
                return Err(ProviderFailure::NotSent("fenced"));
            }
            let mut row = self.ownership.lock().unwrap();
            let state = row.get_or_insert(DispatchOwnership {
                request_commitment: commitment.clone(),
                generation: 0,
                owner: "fresh".into(),
                accepted: false,
            });
            if state.request_commitment != commitment {
                return Err(ProviderFailure::NotSent("body conflict"));
            }
            let next = state
                .acquire(expected, worker)
                .ok_or(ProviderFailure::NotSent("ownership conflict"))?;
            *state = next.clone();
            if self.mode.load(Ordering::SeqCst) == 4 {
                return Err(ProviderFailure::Unknown("lost ownership ACK"));
            }
            Ok(next)
        })
    }
    fn dispatch_owned(
        &self,
        permit: OwnedDispatchPermit,
    ) -> ProviderFuture<'_, SignedProviderReceipt, Self::Error> {
        Box::pin(async move {
            let fence = self.fence.lock().unwrap();
            let mut owner = self.ownership.lock().unwrap();
            let Some(state) = owner.as_mut() else {
                return Err(ProviderFailure::NotSent("missing ownership"));
            };
            if !permit
                .eligible_at(&fence, &"processor".into(), state)
                .unwrap()
            {
                return Err(ProviderFailure::NotSent("obsolete owner or root"));
            }
            *state = state
                .accept(
                    permit.ownership().generation,
                    &permit.ownership().owner,
                    &permit.request().commitment().unwrap(),
                )
                .unwrap();
            self.calls.fetch_add(1, Ordering::SeqCst);
            let request = permit.request().clone();
            let receipt = Self::receipt(&request, false);
            *self.row.lock().unwrap() = Some((request, receipt.clone()));
            if self.mode.load(Ordering::SeqCst) == 1 {
                Err(ProviderFailure::Unknown("lost response"))
            } else {
                Ok(receipt)
            }
        })
    }
}

fn recover_claim(
    port: &DualPort,
    _fixture: &Fixture,
    credential: &CredentialClaims,
    ledger: &ConsumptionLedgerClaims,
) -> Result<Option<RecoveredDispatch>, ConsumptionClaimError<&'static str>> {
    let provider: ProviderId = "processor".into();
    let proposed = ledger
        .prepare(PaymentDispatchClaims::new(
            provider.clone(),
            credential.clone(),
            active_fence(),
        ))
        .unwrap();
    let current_bytes = serde_json::to_vec(ledger).unwrap();
    let proposed_bytes = serde_json::to_vec(&proposed).unwrap();
    complete(recover_dispatch(
        port,
        mrr_data_commerce::consumption::ConsumptionRecoveryRequest {
            budget_scope: "buyer-trip-root",
            provider: &provider,
            purchase_id: &credential.receipt.reservation.purchase.purchase_id,
            current: head(ledger),
            current_bytes: &current_bytes,
            proposed_bytes: &proposed_bytes,
        },
    ))
}

#[test]
fn crash_after_claim_recovers_and_fences_paused_fresh_sender() {
    let fixture = Fixture::lean();
    let credential = claims(&fixture);
    let ledger = before();
    let port = DualPort::new(&fixture, &ledger);
    let provider = Provider::default();
    let old = claim(&port, &fixture, &credential, &ledger, fresh(&fixture)).unwrap();
    let recovered = recover_claim(&port, &fixture, &credential, &ledger)
        .unwrap()
        .unwrap();
    let owner = complete(acquire_dispatch(
        &provider,
        recovered,
        0,
        "restarted-worker",
    ))
    .unwrap();
    assert!(matches!(
        complete(release_payment(&provider, old, &provider_trust())),
        Err(ProviderDispatchError::Provider(ProviderFailure::NotSent(
            "obsolete owner"
        )))
    ));
    complete(release_owned_payment(&provider, owner, &provider_trust())).unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn ownership_takeover_fences_old_owner_and_accepted_operation_never_reopens() {
    let fixture = Fixture::lean();
    let credential = claims(&fixture);
    let ledger = before();
    let port = DualPort::new(&fixture, &ledger);
    let provider = Provider::default();
    let original = claim(&port, &fixture, &credential, &ledger, fresh(&fixture)).unwrap();
    let old = complete(acquire_dispatch(
        &provider,
        RecoveredDispatch::from_permit(original),
        0,
        "old",
    ))
    .unwrap();
    let recovered = recover_claim(&port, &fixture, &credential, &ledger)
        .unwrap()
        .unwrap();
    let new = complete(acquire_dispatch(&provider, recovered, 1, "new")).unwrap();
    assert!(complete(release_owned_payment(&provider, old, &provider_trust())).is_err());
    provider.mode.store(1, Ordering::SeqCst);
    let request = new.request().clone();
    assert!(matches!(
        complete(release_owned_payment(&provider, new, &provider_trust())),
        Err(ProviderDispatchError::Provider(ProviderFailure::Unknown(_)))
    ));
    let recovered = recover_claim(&port, &fixture, &credential, &ledger)
        .unwrap()
        .unwrap();
    assert!(complete(acquire_dispatch(&provider, recovered, 2, "again")).is_err());
    assert!(
        complete(recover_payment(&provider, &request, &provider_trust()))
            .unwrap()
            .is_some()
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn lost_ownership_ack_requires_read_and_new_cas() {
    let fixture = Fixture::lean();
    let credential = claims(&fixture);
    let ledger = before();
    let port = DualPort::new(&fixture, &ledger);
    let provider = Provider::default();
    let original = claim(&port, &fixture, &credential, &ledger, fresh(&fixture)).unwrap();
    let request = original.request().clone();
    provider.mode.store(4, Ordering::SeqCst);
    assert!(matches!(
        complete(acquire_dispatch(
            &provider,
            RecoveredDispatch::from_permit(original),
            0,
            "lost"
        )),
        Err(ProviderDispatchError::Provider(ProviderFailure::Unknown(_)))
    ));
    let state = complete(provider.ownership(&request)).unwrap().unwrap();
    assert_eq!(state.generation, 1);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    provider.mode.store(0, Ordering::SeqCst);
    let recovered = recover_claim(&port, &fixture, &credential, &ledger)
        .unwrap()
        .unwrap();
    let owner = complete(acquire_dispatch(
        &provider,
        recovered,
        state.generation,
        "replacement",
    ))
    .unwrap();
    complete(release_owned_payment(&provider, owner, &provider_trust())).unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn recovery_does_not_adopt_new_root_generation_or_reopen_revoked_root() {
    for retired in [true, false] {
        let fixture = Fixture::lean();
        let credential = claims(&fixture);
        let ledger = before();
        let port = DualPort::new(&fixture, &ledger);
        let provider = Provider::default();
        drop(claim(&port, &fixture, &credential, &ledger, fresh(&fixture)).unwrap());
        *provider.fence.lock().unwrap() = RootFence {
            generation: 8,
            retired,
            ..active_fence()
        };
        let recovered = recover_claim(&port, &fixture, &credential, &ledger)
            .unwrap()
            .unwrap();
        assert_eq!(recovered.ticket().unwrap().generation, 7);
        assert!(complete(acquire_dispatch(&provider, recovered, 0, "restarted")).is_err());
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn lost_consumption_ack_recovers_descriptor_then_acquires_one_owner() {
    let fixture = Fixture::lean();
    let credential = claims(&fixture);
    let ledger = before();
    let port = DualPort::new(&fixture, &ledger);
    let provider = Provider::default();
    port.consumption.lose_ack();
    assert!(matches!(
        claim(&port, &fixture, &credential, &ledger, fresh(&fixture)),
        Err(ConsumptionClaimError::Port(
            ConditionalCommitPortError::Unknown(_)
        ))
    ));
    let recovered = recover_claim(&port, &fixture, &credential, &ledger)
        .unwrap()
        .unwrap();
    let owner = complete(acquire_dispatch(&provider, recovered, 0, "restart")).unwrap();
    complete(release_owned_payment(&provider, owner, &provider_trust())).unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn missing_or_substituted_consumption_record_cannot_resume() {
    let fixture = Fixture::lean();
    let credential = claims(&fixture);
    let ledger = before();
    let port = DualPort::new(&fixture, &ledger);
    assert!(
        recover_claim(&port, &fixture, &credential, &ledger)
            .unwrap()
            .is_none()
    );
    drop(claim(&port, &fixture, &credential, &ledger, fresh(&fixture)).unwrap());
    let mut changed = credential.clone();
    changed.credential_id = "other-credential".into();
    assert!(recover_claim(&port, &fixture, &changed, &ledger).is_err());
}

#[test]
fn paused_acquired_owner_is_rejected_after_root_retirement() {
    let fixture = Fixture::lean();
    let credential = claims(&fixture);
    let ledger = before();
    let port = DualPort::new(&fixture, &ledger);
    let provider = Provider::default();
    let original = claim(&port, &fixture, &credential, &ledger, fresh(&fixture)).unwrap();
    let owner = complete(acquire_dispatch(
        &provider,
        RecoveredDispatch::from_permit(original),
        0,
        "worker",
    ))
    .unwrap();
    *provider.fence.lock().unwrap() = RootFence {
        generation: 8,
        retired: true,
        ..active_fence()
    };
    assert!(complete(release_owned_payment(&provider, owner, &provider_trust())).is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn competing_takeovers_only_one_owner_can_accept() {
    let fixture = Fixture::lean();
    let credential = claims(&fixture);
    let ledger = before();
    let port = DualPort::new(&fixture, &ledger);
    let provider = Provider::default();
    drop(claim(&port, &fixture, &credential, &ledger, fresh(&fixture)).unwrap());
    let a = recover_claim(&port, &fixture, &credential, &ledger)
        .unwrap()
        .unwrap();
    let b = recover_claim(&port, &fixture, &credential, &ledger)
        .unwrap()
        .unwrap();
    let (a, b) = std::thread::scope(|scope| {
        let a = scope.spawn(|| complete(acquire_dispatch(&provider, a, 0, "a")));
        let b = scope.spawn(|| complete(acquire_dispatch(&provider, b, 0, "b")));
        (a.join().unwrap(), b.join().unwrap())
    });
    assert_ne!(a.is_ok(), b.is_ok());
    let owner = a.or(b).unwrap();
    complete(release_owned_payment(&provider, owner, &provider_trust())).unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn accepted_row_without_local_receipt_survives_restart_without_reopening() {
    let fixture = Fixture::lean();
    let credential = claims(&fixture);
    let ledger = before();
    let port = DualPort::new(&fixture, &ledger);
    let provider = Provider::default();
    let permit = claim(&port, &fixture, &credential, &ledger, fresh(&fixture)).unwrap();
    let request = permit.request().clone();
    complete(release_payment(&provider, permit, &provider_trust())).unwrap();
    let durable_owner = serde_json::to_vec(&*provider.ownership.lock().unwrap()).unwrap();
    let restarted = Provider::default();
    *restarted.ownership.lock().unwrap() = serde_json::from_slice(&durable_owner).unwrap();
    let recovered = recover_claim(&port, &fixture, &credential, &ledger)
        .unwrap()
        .unwrap();
    assert!(complete(acquire_dispatch(&restarted, recovered, 0, "restart")).is_err());
    assert!(
        complete(recover_payment(&restarted, &request, &provider_trust()))
            .unwrap()
            .is_none()
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(restarted.calls.load(Ordering::SeqCst), 0);
}
