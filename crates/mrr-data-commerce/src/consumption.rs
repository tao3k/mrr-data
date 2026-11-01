//! One release claim per scoped purchase, with provider-bound requests and receipts.
//!
//! The Host/port synchronizes current budget, registry and issuer status through
//! claim and release. Unknown claims produce no permit; replay is status only.
//! A provider implementation and durable backend remain deployment obligations.

use super::budget_commit::CurrentCommerceAuthority;
use super::credential::{
    CredentialAdmissionRequest, CredentialClaims, CredentialError, CredentialIssuerTrust,
    CredentialRecoveryError, recover_committed_credential, validate_authority,
};
pub use cedar_poo_commerce::acceptance::{AcceptanceTicket, AuthorizationRoot, RootFence};
use cedar_poo_commerce::projection::{LeanMandateClaims, LeanOfferClaims};
use cedar_poo_commerce::signatures::sha256_hex;
use mrr_data_content::{
    ConditionalCommitError, ConditionalCommitPortError, ConditionalContentCommitOutcome,
    ConditionalContentCommitPort, ConditionalContentWrite, ContentBlock, ContentCodec,
    ContentRevision, PublishReceipt,
};
use serde::{Deserialize, Serialize};

/// Independently configured provider identity, not inferred from a receipt.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ProviderId(String);
impl From<&str> for ProviderId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}
impl ProviderId {
    /// Read the exact configured provider identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Raw typed dispatch DTO; construction alone grants no release authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PaymentDispatchClaims {
    pub provider_id: ProviderId,
    pub idempotency_key: String,
    pub credential: CredentialClaims,
    /// Root snapshot persisted atomically with consumption, never refreshed on replay.
    pub authority: RootFence,
}
impl PaymentDispatchClaims {
    /// Deterministic provider retry key derived from the scoped purchase, not
    /// credential identity. A different credential cannot create another claim.
    pub fn new(provider: ProviderId, credential: CredentialClaims, authority: RootFence) -> Self {
        let scope = &credential.receipt.scope;
        let purchase = &credential.receipt.reservation.purchase.purchase_id;
        let mut bytes = b"cedar-poo/commerce/payment-operation/v1\0".to_vec();
        for field in [scope, purchase] {
            bytes.extend_from_slice(&(field.len() as u64).to_be_bytes());
            bytes.extend_from_slice(field.as_bytes());
        }
        Self {
            provider_id: provider,
            idempotency_key: sha256_hex(&bytes),
            credential,
            authority,
        }
    }

    /// Commitment to the full exact request, including provider and credential.
    /// # Errors
    /// Returns InvalidClaims if serialization fails.
    pub fn commitment(&self) -> Result<String, ConsumptionError> {
        let mut bytes = b"cedar-poo/commerce/payment-request/v1\0".to_vec();
        bytes.extend(serde_json::to_vec(self).map_err(|_| ConsumptionError::InvalidClaims)?);
        Ok(sha256_hex(&bytes))
    }
}

/// Host-authenticated consumption head for one budget scope. Rows are tombstones:
/// neither a rejected payment nor an unknown response removes a used purchase.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ConsumptionLedgerClaims {
    pub budget_scope: String,
    pub revision: u64,
    pub requests: Vec<PaymentDispatchClaims>,
}
impl ConsumptionLedgerClaims {
    /// Prepare a pure proposal. Publish/commit and fresh authority checks remain
    /// necessary before a DispatchPermit can exist.
    /// # Errors
    /// Returns AlreadyConsumed or invalid scope/revision.
    pub fn prepare(&self, request: PaymentDispatchClaims) -> Result<Self, ConsumptionError> {
        if self.budget_scope.is_empty() || self.budget_scope != request.credential.receipt.scope {
            return Err(ConsumptionError::WrongScope);
        }
        if self.requests.iter().any(|old| {
            old.credential.receipt.reservation.purchase.purchase_id
                == request.credential.receipt.reservation.purchase.purchase_id
        }) {
            return Err(ConsumptionError::AlreadyConsumed);
        }
        let mut next = self.clone();
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or(ConsumptionError::InvalidTransition)?;
        next.requests.insert(0, request);
        Ok(next)
    }
}

/// Refusals from the consumption protocol or refreshed domain authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConsumptionError {
    InvalidClaims,
    WrongScope,
    WrongProvider,
    AlreadyConsumed,
    InvalidTransition,
    ContentMismatch,
    MissingValidation,
    InvalidFence,
    ReceiptMismatch,
    Credential(CredentialError),
    Commit(ConditionalCommitError),
}

/// Current registry, issuer keys and budget head read under the port's protection.
/// The adapter must synchronize changes to all of these through provider release.
pub struct CurrentConsumptionAuthority {
    /// Independently authenticated root generation synchronized with this claim.
    /// Issuer cuts must also be installed at the provider before reporting quiescence.
    pub root_fence: RootFence,
    pub commerce: CurrentCommerceAuthority,
    pub issuers: CredentialIssuerTrust,
    pub budget: ContentRevision,
    pub budget_bytes: Vec<u8>,
}

/// Prepared proposal and separately trusted provider/budget namespace.
pub struct ConsumptionClaimRequest<'a> {
    pub credential: CredentialAdmissionRequest<'a>,
    pub provider: &'a ProviderId,
    pub current: ContentRevision,
    pub current_bytes: &'a [u8],
    pub proposed_bytes: &'a [u8],
    pub physical: &'a PublishReceipt,
}

/// Recovery and consumption errors remain separate; uncertain commits grant no
/// release permit and must be queried under the exact original operation.
#[derive(Debug)]
pub enum ConsumptionClaimError<E> {
    Credential(CredentialRecoveryError<E>),
    Validation(ConsumptionError),
    Port(ConditionalCommitPortError<E, ConsumptionError>),
}

/// Move-only, non-serializable permission from one newly committed claim.
/// Exact replay and recovery can never reconstruct this value. A crash after
/// claim but before dispatch therefore preserves safety and may lose liveness.
#[derive(Debug)]
pub struct DispatchPermit {
    request: PaymentDispatchClaims,
    acceptance: AcceptanceTicket,
}
impl DispatchPermit {
    /// Claim-time root snapshot; the provider checks it at protected acceptance.
    pub fn acceptance(&self) -> &AcceptanceTicket {
        &self.acceptance
    }
    /// Eligibility under independently trusted provider-local fence state.
    /// Provider identity is independently configured by the endpoint.
    /// This must run atomically with acceptance, never before an unprotected await.
    /// # Errors
    /// Returns InvalidClaims if the exact request commitment cannot be encoded.
    pub fn eligible_at(
        &self,
        fence: &RootFence,
        provider: &ProviderId,
    ) -> Result<bool, ConsumptionError> {
        Ok(self.request.provider_id == *provider
            && fence.accepts(
                &self.acceptance,
                provider.as_str(),
                &self.request.idempotency_key,
                &self.request.commitment()?,
            ))
    }
    /// Inspect the exact request; this accessor cannot create another permit.
    pub fn request(&self) -> &PaymentDispatchClaims {
        &self.request
    }
}

fn transition(
    input: &ConsumptionClaimRequest<'_>,
) -> Result<(String, PaymentDispatchClaims), ConsumptionError> {
    if ContentBlock::new(ContentCodec::Raw, input.current_bytes).cid() != input.current.root {
        return Err(ConsumptionError::ContentMismatch);
    }
    let before: ConsumptionLedgerClaims =
        serde_json::from_slice(input.current_bytes).map_err(|_| ConsumptionError::InvalidClaims)?;
    let after: ConsumptionLedgerClaims = serde_json::from_slice(input.proposed_bytes)
        .map_err(|_| ConsumptionError::InvalidClaims)?;
    let authority = after
        .requests
        .first()
        .ok_or(ConsumptionError::InvalidTransition)?
        .authority
        .clone();
    let request = PaymentDispatchClaims::new(
        input.provider.clone(),
        input.credential.credential.clone(),
        authority,
    );
    if before.revision != input.current.revision
        || before.budget_scope != input.credential.expected_scope
        || after != before.prepare(request.clone())?
        || input.provider.as_str().is_empty()
    {
        return Err(ConsumptionError::InvalidTransition);
    }
    // One canonical consumption namespace per trusted budget scope, independent
    // of provider and credential ID. Backend aliases must preserve uniqueness.
    let scope = format!(
        "cedar-poo/commerce/consumption/v1/{}",
        sha256_hex(before.budget_scope.as_bytes())
    );
    Ok((scope, request))
}

fn validate_live<F>(
    input: CredentialAdmissionRequest<'_>,
    live: &CurrentConsumptionAuthority,
    policy: F,
) -> Result<(), ConsumptionError>
where
    F: FnOnce(&[LeanMandateClaims], &LeanOfferClaims) -> bool,
{
    let refreshed = CredentialAdmissionRequest {
        issuer_trust: &live.issuers,
        current: live.budget,
        current_bytes: &live.budget_bytes,
        ..input
    };
    refreshed
        .issuer_trust
        .verify(
            refreshed.expected_issuer,
            refreshed.credential,
            refreshed.signature,
        )
        .map_err(ConsumptionError::Credential)?;
    validate_authority(&live.commerce, refreshed, policy)
        .map(|_| ())
        .map_err(ConsumptionError::Credential)
}

/// Recover the reservation, then claim consumption with fresh authority inside
/// the port transaction. A callback result must remain synchronized through
/// provider release. Replayed/unknown commits never mint another permit.
/// # Errors
/// Returns semantic refusal, recovery failure or uncertain consumption commit.
pub async fn claim_dispatch<'a, P, F, G>(
    port: &'a P,
    input: ConsumptionClaimRequest<'a>,
    refresh: F,
    policy: G,
) -> Result<DispatchPermit, ConsumptionClaimError<P::Error>>
where
    P: ConditionalContentCommitPort,
    F: FnOnce() -> Result<CurrentConsumptionAuthority, ConsumptionError> + Send + 'a,
    G: FnOnce(&[LeanMandateClaims], &LeanOfferClaims) -> bool + Send + 'a,
{
    recover_committed_credential(port, &input.credential)
        .await
        .map_err(ConsumptionClaimError::Credential)?;
    let (scope, request) = transition(&input).map_err(ConsumptionClaimError::Validation)?;
    let write = ConditionalContentWrite {
        scope: &scope,
        operation_id: &request.credential.receipt.reservation.purchase.purchase_id,
        expected: Some(input.current),
        replacement: ContentBlock::new(ContentCodec::Raw, input.proposed_bytes).cid(),
    };
    let physical = input.physical;
    let mut acceptance = None;
    let outcome = port
        .commit(write, Some(physical), |observed| {
            if observed != Some(input.current) {
                return Err(ConsumptionError::InvalidTransition);
            }
            let live = refresh()?;
            let root = input
                .credential
                .credential
                .receipt
                .reservation
                .lineage
                .first()
                .ok_or(ConsumptionError::InvalidFence)?;
            if live.root_fence != request.authority
                || live.root_fence.retired
                || live.root_fence.root.budget_scope != input.credential.expected_scope
                || live.root_fence.root.mandate_id != root.mandate_id.as_str()
            {
                return Err(ConsumptionError::InvalidFence);
            }
            validate_live(input.credential, &live, policy)?;
            acceptance = Some(AcceptanceTicket {
                root: live.root_fence.root,
                generation: live.root_fence.generation,
                provider_id: request.provider_id.as_str().to_owned(),
                operation_id: request.idempotency_key.clone(),
                request_commitment: request.commitment()?,
            });
            Ok(())
        })
        .await
        .map_err(ConsumptionClaimError::Port)?;
    let ConditionalContentCommitOutcome::Committed(receipt) = outcome else {
        return Err(ConsumptionClaimError::Validation(
            ConsumptionError::AlreadyConsumed,
        ));
    };
    write
        .recover_receipt(Some(&receipt))
        .map_err(|error| ConsumptionClaimError::Validation(ConsumptionError::Commit(error)))?;
    let acceptance = acceptance.ok_or(ConsumptionClaimError::Validation(
        ConsumptionError::MissingValidation,
    ))?;
    Ok(DispatchPermit {
        request,
        acceptance,
    })
}

/// Historical dispatch recovered from an authenticated exact consumption commit.
/// It is neither a fresh permit nor permission to charge. Ownership acquisition
/// at the provider must refresh live authority before resuming execution.
#[derive(Debug)]
pub struct RecoveredDispatch {
    request: PaymentDispatchClaims,
}
impl RecoveredDispatch {
    /// Transfer a fresh permit into the same fenced ownership protocol.
    #[must_use]
    pub fn from_permit(permit: DispatchPermit) -> Self {
        Self {
            request: permit.request,
        }
    }
    /// Exact immutable historical request, including its original root generation.
    pub fn request(&self) -> &PaymentDispatchClaims {
        &self.request
    }
    /// Rebuild the historical ticket without adopting a newer root generation.
    /// # Errors
    /// Returns InvalidClaims when the commitment cannot be encoded.
    pub fn ticket(&self) -> Result<AcceptanceTicket, ConsumptionError> {
        Ok(AcceptanceTicket {
            root: self.request.authority.root.clone(),
            generation: self.request.authority.generation,
            provider_id: self.request.provider_id.as_str().into(),
            operation_id: self.request.idempotency_key.clone(),
            request_commitment: self.request.commitment()?,
        })
    }
}

/// Original immutable proposal locator, independently scoped by the Host.
/// A status read needs neither current signing keys nor publication ACK.
pub struct ConsumptionRecoveryRequest<'a> {
    pub budget_scope: &'a str,
    pub provider: &'a ProviderId,
    pub purchase_id: &'a str,
    pub current: ContentRevision,
    pub current_bytes: &'a [u8],
    pub proposed_bytes: &'a [u8],
}

/// Recover one exact consumption operation, even after its head advances.
/// No current-authority check or fresh dispatch permit is implied by this read.
/// A deploying port authenticates the operation ledger and its validation path.
/// # Errors
/// Returns mismatch, absence, or explicit uncertainty; never retries a new ID.
pub async fn recover_dispatch<P: ConditionalContentCommitPort>(
    port: &P,
    input: ConsumptionRecoveryRequest<'_>,
) -> Result<Option<RecoveredDispatch>, ConsumptionClaimError<P::Error>> {
    let parse = |bytes: &[u8]| -> Result<ConsumptionLedgerClaims, ConsumptionClaimError<P::Error>> {
        serde_json::from_slice(bytes)
            .map_err(|_| ConsumptionClaimError::Validation(ConsumptionError::InvalidClaims))
    };
    if ContentBlock::new(ContentCodec::Raw, input.current_bytes).cid() != input.current.root {
        return Err(ConsumptionClaimError::Validation(
            ConsumptionError::ContentMismatch,
        ));
    }
    let before = parse(input.current_bytes)?;
    let after = parse(input.proposed_bytes)?;
    let request = after
        .requests
        .first()
        .ok_or(ConsumptionClaimError::Validation(
            ConsumptionError::InvalidTransition,
        ))?
        .clone();
    let normalized = PaymentDispatchClaims::new(
        request.provider_id.clone(),
        request.credential.clone(),
        request.authority.clone(),
    );
    if before.budget_scope != input.budget_scope
        || before.revision != input.current.revision
        || request.provider_id != *input.provider
        || request.credential.receipt.reservation.purchase.purchase_id != input.purchase_id
        || request != normalized
        || after
            != before
                .prepare(request.clone())
                .map_err(ConsumptionClaimError::Validation)?
    {
        return Err(ConsumptionClaimError::Validation(
            ConsumptionError::InvalidTransition,
        ));
    }
    let scope = format!(
        "cedar-poo/commerce/consumption/v1/{}",
        sha256_hex(before.budget_scope.as_bytes())
    );
    let write = ConditionalContentWrite {
        scope: &scope,
        operation_id: &request.credential.receipt.reservation.purchase.purchase_id,
        expected: Some(input.current),
        replacement: ContentBlock::new(ContentCodec::Raw, input.proposed_bytes).cid(),
    };
    let receipt = port.recover(write).await.map_err(|error| {
        ConsumptionClaimError::Port(match error {
            ConditionalCommitPortError::Protocol(e) => ConditionalCommitPortError::Protocol(e),
            ConditionalCommitPortError::BeforeCommit(e) => {
                ConditionalCommitPortError::BeforeCommit(e)
            }
            ConditionalCommitPortError::Unknown(e) => ConditionalCommitPortError::Unknown(e),
            ConditionalCommitPortError::Validation(never) => match never {},
        })
    })?;
    let Some(receipt) = receipt else {
        return Ok(None);
    };
    write
        .recover_receipt(Some(&receipt))
        .map_err(|e| ConsumptionClaimError::Validation(ConsumptionError::Commit(e)))?;
    Ok(Some(RecoveredDispatch { request }))
}
