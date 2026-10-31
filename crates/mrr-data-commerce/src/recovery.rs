//! Provider-owned recovery protocol for historically committed dispatches.
//! Every takeover is a protected compare-and-swap. The provider checks both
//! ownership and root fences at actual durable acceptance; no timeout proves death.
use super::consumption::{
    ConsumptionError, PaymentDispatchClaims, ProviderId, RecoveredDispatch, RootFence,
};
use super::provider::{
    PaymentProviderPort, ProviderDispatchError, ProviderFuture, ProviderReceiptTrust,
    SignedProviderReceipt, VerifiedProviderReceipt,
};
pub use cedar_poo_commerce::acceptance::DispatchOwnership;

/// Move-only capability for one provider-confirmed ownership generation.
/// It cannot be reconstructed from untrusted worker state or an old receipt.
#[derive(Debug)]
pub struct OwnedDispatchPermit {
    dispatch: RecoveredDispatch,
    ownership: DispatchOwnership,
}
impl OwnedDispatchPermit {
    /// Exact original request; takeover never changes body, provider or operation.
    pub fn request(&self) -> &PaymentDispatchClaims {
        self.dispatch.request()
    }
    /// Confirmed ownership snapshot; compare with current endpoint state at acceptance.
    pub fn ownership(&self) -> &DispatchOwnership {
        &self.ownership
    }
    /// Validate both fences inside the protected endpoint transaction.
    /// # Errors
    /// Returns InvalidClaims when historical request encoding fails.
    pub fn eligible_at(
        &self,
        fence: &RootFence,
        provider: &ProviderId,
        current: &DispatchOwnership,
    ) -> Result<bool, ConsumptionError> {
        let ticket = self.dispatch.ticket()?;
        Ok(self.request().provider_id == *provider
            && *current == self.ownership
            && !current.accepted
            && fence.accepts(
                &ticket,
                provider.as_str(),
                &self.request().idempotency_key,
                &current.request_commitment,
            ))
    }
}

/// Additional deploying-endpoint contract; no storage or payment processor supplied.
/// Initialize an absent ownership row ONLY from an authenticated consumed dispatch,
/// binding `(scope, provider, operation)` to its exact immutable body and ticket.
/// Atomically compare expected generation and install a new owner under refreshed
/// budget, issuer and root authority. Never adopt a new root generation on recovery.
/// The inherited fresh `dispatch` path MUST use this same ownership row at
/// generation zero, so a takeover fences a still-paused original fresh permit.
/// Serialize takeover, root installation and durable protected acceptance. Dispatch
/// checks `eligible_at` and advances ownership to accepted in the SAME transaction
/// that records responsibility for the exact effect. Accepted operations cannot be
/// reacquired, even when outcome/settlement is unknown; only status recovery is allowed.
/// Durably accepted work must be processed or reconciled by the endpoint's own
/// idempotent queue/rail protocol. A mutex then an unprotected bank call is insufficient.
/// Losing ownership/fence rows fails closed. Never reset them from worker snapshots.
pub trait ResumablePaymentProviderPort: PaymentProviderPort {
    /// Read the exact authenticated ownership row after an uncertain takeover.
    /// Absence is not permission to release; acquisition still uses protected CAS.
    /// # Errors
    /// Returns unavailable status or a conflicting request body.
    fn ownership<'a>(
        &'a self,
        request: &'a PaymentDispatchClaims,
    ) -> ProviderFuture<'a, Option<DispatchOwnership>, Self::Error>;
    /// Return authenticated committed ownership after the protected CAS.
    /// # Errors
    /// Returns refused takeover or explicitly uncertain ownership persistence.
    fn acquire<'a>(
        &'a self,
        dispatch: &'a RecoveredDispatch,
        expected: u64,
        worker: &'a str,
    ) -> ProviderFuture<'a, DispatchOwnership, Self::Error>;
    /// Accept once under BOTH current ownership and root authorization fences.
    /// # Errors
    /// Known non-release or unknown outcome never authorizes a second acceptance.
    fn dispatch_owned(
        &self,
        permit: OwnedDispatchPermit,
    ) -> ProviderFuture<'_, SignedProviderReceipt, Self::Error>;
}

/// Acquire a new generation for the exact historical dispatch. A lost ownership
/// ACK gives no permit; a later protected takeover can fence out the lost owner.
/// # Errors
/// Preserves uncertainty or rejects substituted ownership/body/generation.
pub async fn acquire_dispatch<P: ResumablePaymentProviderPort>(
    provider: &P,
    dispatch: RecoveredDispatch,
    expected: u64,
    worker: &str,
) -> Result<OwnedDispatchPermit, ProviderDispatchError<P::Error>> {
    let generation = expected
        .checked_add(1)
        .ok_or(ProviderDispatchError::Validation(
            ConsumptionError::InvalidTransition,
        ))?;
    let commitment = dispatch
        .request()
        .commitment()
        .map_err(ProviderDispatchError::Validation)?;
    if worker.is_empty() {
        return Err(ProviderDispatchError::Validation(
            ConsumptionError::InvalidClaims,
        ));
    }
    let ownership = provider
        .acquire(&dispatch, expected, worker)
        .await
        .map_err(ProviderDispatchError::Provider)?;
    if ownership.generation != generation
        || ownership.owner != worker
        || ownership.accepted
        || ownership.request_commitment != commitment
    {
        return Err(ProviderDispatchError::Validation(
            ConsumptionError::ReceiptMismatch,
        ));
    }
    Ok(OwnedDispatchPermit {
        dispatch,
        ownership,
    })
}

/// Release one acquired owner and verify the exact provider's signed status.
/// # Errors
/// Preserves unknown outcomes and rejects substituted or untrusted receipts.
pub async fn release_owned_payment<P: ResumablePaymentProviderPort>(
    provider: &P,
    permit: OwnedDispatchPermit,
    trust: &ProviderReceiptTrust,
) -> Result<VerifiedProviderReceipt, ProviderDispatchError<P::Error>> {
    let request = permit.request().clone();
    let receipt = provider
        .dispatch_owned(permit)
        .await
        .map_err(ProviderDispatchError::Provider)?;
    trust
        .verify(&request, receipt)
        .map_err(ProviderDispatchError::Validation)
}
