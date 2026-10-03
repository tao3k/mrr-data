//! Provider request/receipt boundary for a move-only consumption release permit.
//!
//! Deploying adapters authenticate transport and synchronize live authority
//! through release. Provider idempotency and settlement semantics are explicit
//! adapter obligations. These interfaces supply no payment network implementation.

use super::consumption::{ConsumptionError, DispatchPermit, PaymentDispatchClaims, ProviderId};
use p256::ecdsa::{Signature, VerifyingKey, signature::Verifier};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, future::Future, pin::Pin};

/// Signed terminal processor status; it is not fulfillment evidence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum PaymentOutcome {
    Succeeded,
    Rejected,
}

/// Provider-signed local receipt DTO without caller-owned verification flags.
/// The commitment covers the entire exact request, including amount, checkout,
/// full lineage, commit receipt, credential, provider and idempotency key.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProviderReceiptClaims {
    pub provider_id: ProviderId,
    pub request_commitment: String,
    pub outcome: PaymentOutcome,
    pub reference: String,
}
impl ProviderReceiptClaims {
    /// Versioned domain plus fixed typed JSON receipt serialization.
    /// # Errors
    /// Returns InvalidClaims on serialization failure.
    pub fn signing_bytes(&self) -> Result<Vec<u8>, ConsumptionError> {
        let mut bytes = b"cedar-poo/commerce/provider-receipt/v1\0".to_vec();
        bytes.extend(serde_json::to_vec(self).map_err(|_| ConsumptionError::InvalidClaims)?);
        Ok(bytes)
    }
}

/// Untrusted signed receipt bytes returned by dispatch or status recovery.
#[derive(Clone, Debug)]
pub struct SignedProviderReceipt {
    pub claims: ProviderReceiptClaims,
    pub signature: Vec<u8>,
}

/// Current provider keys enrolled independently from a presented receipt.
#[derive(Default)]
pub struct ProviderReceiptTrust {
    keys: BTreeMap<ProviderId, VerifyingKey>,
}
impl ProviderReceiptTrust {
    /// Replace a provider's current signing key; stale keys then fail verification.
    pub fn enroll(&mut self, provider: ProviderId, key: VerifyingKey) {
        self.keys.insert(provider, key);
    }
    /// Revoke the provider's authority to attest payment status.
    pub fn revoke(&mut self, provider: &ProviderId) {
        self.keys.remove(provider);
    }
    fn verify(
        &self,
        request: &PaymentDispatchClaims,
        receipt: SignedProviderReceipt,
    ) -> Result<VerifiedProviderReceipt, ConsumptionError> {
        if receipt.claims.provider_id != request.provider_id
            || receipt.claims.reference.is_empty()
            || receipt.claims.request_commitment != request.commitment()?
        {
            return Err(ConsumptionError::ReceiptMismatch);
        }
        let key = self
            .keys
            .get(&request.provider_id)
            .ok_or(ConsumptionError::WrongProvider)?;
        let signature = Signature::from_slice(&receipt.signature)
            .map_err(|_| ConsumptionError::ReceiptMismatch)?;
        key.verify(&receipt.claims.signing_bytes()?, &signature)
            .map_err(|_| ConsumptionError::ReceiptMismatch)?;
        Ok(VerifiedProviderReceipt {
            claims: receipt.claims,
        })
    }
}

/// Only independently trusted signature verification constructs this value.
#[derive(Debug)]
pub struct VerifiedProviderReceipt {
    claims: ProviderReceiptClaims,
}
impl VerifiedProviderReceipt {
    /// Exact signed terminal status; successful transport alone cannot mint it.
    pub fn claims(&self) -> &ProviderReceiptClaims {
        &self.claims
    }
}

/// Known non-release differs from an unknown economic outcome. Both retain the
/// consumed purchase; neither authorizes another permit or a different retry ID.
#[derive(Clone, Debug)]
pub enum ProviderFailure<E> {
    NotSent(E),
    Unknown(E),
}
/// Sendable provider/status future, preserving unknown outcomes.
pub type ProviderFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, ProviderFailure<E>>> + Send + 'a>>;

/// Implementations release only a newly claimed permit, carry its exact request
/// to the provider, enforce stable-key idempotency, and authenticate responses.
/// At the protected provider acceptance event, implementations MUST evaluate
/// `permit.eligible_at` against independently authenticated current fence state,
/// using the endpoint's independently configured provider ID, atomically with
/// recording the accepted request/effect. An earlier client check
/// is insufficient. Persist fences and accepted operation/body bindings together
/// with the deploying endpoint's recovery protocol; stale worker snapshots never
/// replace that authority. Reject missing fences as known non-release.
/// Root retirement blocks new acceptance, while exact status recovery for requests
/// already accepted remains available. Never reset or garbage collect root fences
/// while old carriers can return. No durable storage engine is supplied here.
/// They must synchronize the current budget, issuer and mandate authority through
/// the actual external release. A permit is a validated snapshot, not a lease
/// proving those facts remained current while an adapter suspended or restarted.
/// Recovery performs only a status query; absent/unknown responses never permit
/// a fresh payment. Production adapters need their own conformance/durability tests.
pub trait PaymentProviderPort: Sync {
    type Error: Send;
    /// Consume the unique release permit and return a signed processor status.
    /// # Errors
    /// Returns known non-release or an explicitly uncertain payment outcome.
    fn dispatch(
        &self,
        permit: DispatchPermit,
    ) -> ProviderFuture<'_, SignedProviderReceipt, Self::Error>;
    /// Effect-free query for the exact original request and stable operation key.
    /// # Errors
    /// Unavailable/unknown status is an error, not authenticated absence.
    fn recover<'a>(
        &'a self,
        request: &'a PaymentDispatchClaims,
    ) -> ProviderFuture<'a, Option<SignedProviderReceipt>, Self::Error>;
}

/// Provider failures and untrusted/mismatched receipts remain separate.
#[derive(Debug)]
pub enum ProviderDispatchError<E> {
    Provider(ProviderFailure<E>),
    Validation(ConsumptionError),
}

/// Release a move-only permit once and verify the exact provider's signed result.
/// The supplied trust registry is Host-owned current evidence; the deploying
/// provider adapter fulfills the live-authority/release contract above.
/// # Errors
/// Preserves provider uncertainty or rejects receipt issuer/signature/substitution.
pub async fn release_payment<P: PaymentProviderPort>(
    provider: &P,
    permit: DispatchPermit,
    trust: &ProviderReceiptTrust,
) -> Result<VerifiedProviderReceipt, ProviderDispatchError<P::Error>> {
    let request = permit.request().clone();
    let receipt = provider
        .dispatch(permit)
        .await
        .map_err(ProviderDispatchError::Provider)?;
    trust
        .verify(&request, receipt)
        .map_err(ProviderDispatchError::Validation)
}

/// Recover only the signed status of the original consumed request. No permit
/// is minted and no credential lifetime extension or payment retry is granted.
/// # Errors
/// Preserves unavailable/unknown status and rejects substituted receipts.
pub async fn recover_payment<P: PaymentProviderPort>(
    provider: &P,
    request: &PaymentDispatchClaims,
    trust: &ProviderReceiptTrust,
) -> Result<Option<VerifiedProviderReceipt>, ProviderDispatchError<P::Error>> {
    provider
        .recover(request)
        .await
        .map_err(ProviderDispatchError::Provider)?
        .map(|receipt| {
            trust
                .verify(request, receipt)
                .map_err(ProviderDispatchError::Validation)
        })
        .transpose()
}
