//! Signed credentials bound to recovered commits and current commerce authority.
//!
//! The Host authenticates current head observations and serializes authority
//! changes through any later effect release. This module validates correspondence;
//! it issues no token, consumes no credential, and dispatches no payment.

use std::{collections::BTreeMap, convert::Infallible};

use super::budget_commit::{
    BudgetCommitError, CurrentCommerceAuthority, SharedBudgetClaims, SharedBudgetReservation,
    admit, read_transition, validate_reservation,
};
use cedar_poo_commerce::admission::DelegatedAdmissionRequest;
use mrr_data_content::{
    ConditionalCommitError, ConditionalCommitPortError, ConditionalContentCommitPort,
    ConditionalContentWrite, ContentBlock, ContentCodec, ContentRevision,
};
use p256::ecdsa::{Signature, VerifyingKey, signature::Verifier};
use serde::{Deserialize, Serialize};

macro_rules! claim_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);
        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }
        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }
        impl $name {
            /// Read the exact wire identifier without normalizing it.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}
claim_id!(
    CredentialIssuerId,
    "Independently enrolled credential issuer identity."
);
claim_id!(
    CommitOperationId,
    "Exact conditional commit operation identity."
);
claim_id!(
    CommitContentId,
    "Exact serialized content CID carried by a receipt."
);

/// Every Lean receipt field except the Host-derived verification flag.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReservationCommitClaims {
    pub scope: String,
    pub operation_id: CommitOperationId,
    pub expected_revision: u64,
    pub expected_content_id: CommitContentId,
    pub committed_revision: u64,
    pub committed_content_id: CommitContentId,
    pub reservation: SharedBudgetReservation,
}

/// Complete issuer-signed credential claims, with no caller-owned verified bit.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CredentialClaims {
    pub credential_id: String,
    pub issuer_id: CredentialIssuerId,
    pub receipt: ReservationCommitClaims,
    pub expires_at: u64,
}

impl CredentialClaims {
    /// Versioned signing format: domain prefix followed by typed serde JSON.
    /// Struct field order is fixed, lists retain order, and unknown fields are
    /// rejected on input. Changing this encoding requires a new domain version.
    /// # Errors
    /// Returns InvalidClaims if these typed claims cannot be serialized.
    pub fn signing_bytes(&self) -> Result<Vec<u8>, CredentialError> {
        let mut bytes = b"cedar-poo/agentic-ai/commerce/credential/v1\0".to_vec();
        bytes.extend(serde_json::to_vec(self).map_err(|_| CredentialError::InvalidClaims)?);
        Ok(bytes)
    }
}

/// Current issuer keys are enrolled independently from a presented credential.
#[derive(Default)]
pub struct CredentialIssuerTrust {
    keys: BTreeMap<CredentialIssuerId, VerifyingKey>,
}

impl CredentialIssuerTrust {
    /// Set the current key; replacing it invalidates signatures from the old key.
    pub fn enroll(&mut self, issuer_id: CredentialIssuerId, key: VerifyingKey) {
        self.keys.insert(issuer_id, key);
    }

    /// Remove the issuer's current authority to attest credentials.
    pub fn revoke(&mut self, issuer_id: &CredentialIssuerId) {
        self.keys.remove(issuer_id);
    }

    pub(super) fn verify(
        &self,
        expected_issuer: &CredentialIssuerId,
        claims: &CredentialClaims,
        signature: &[u8],
    ) -> Result<(), CredentialError> {
        if expected_issuer.as_str().is_empty() || &claims.issuer_id != expected_issuer {
            return Err(CredentialError::WrongIssuer);
        }
        let key = self
            .keys
            .get(expected_issuer)
            .ok_or(CredentialError::UntrustedIssuer)?;
        let signature = Signature::from_slice(signature).map_err(|_| CredentialError::Signature)?;
        key.verify(&claims.signing_bytes()?, &signature)
            .map_err(|_| CredentialError::Signature)
    }
}

/// Semantic refusals distinct from missing commits and unavailable ledgers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CredentialError {
    InvalidClaims,
    WrongIssuer,
    UntrustedIssuer,
    Signature,
    MissingCommit,
    ReceiptMismatch,
    CurrentContentMismatch,
    MissingCurrentReservation,
    InvalidCurrentBudget,
    Expired,
    Budget(BudgetCommitError),
    Commit(ConditionalCommitError),
}

/// Backend recovery errors preserve uncertain outcomes; they are never absence.
#[derive(Debug)]
pub enum CredentialRecoveryError<E> {
    Validation(CredentialError),
    Port(ConditionalCommitPortError<E, Infallible>),
}

/// Locally validated snapshot. It grants no single-use or payment-effect right.
#[derive(Debug)]
pub struct VerifiedCredential {
    claims: CredentialClaims,
}

impl VerifiedCredential {
    /// Only the validator can construct a value carrying these exact claims.
    pub fn claims(&self) -> &CredentialClaims {
        &self.claims
    }
}

/// Inputs read and authenticated by the Host, separate from signed claims.
/// The current head must come from the same trusted scope as the recovered row.
/// Before an external effect, synchronize fresh authority, head, and single-use
/// state in the provider's transaction; this snapshot alone is not that gate.
pub struct CredentialAdmissionRequest<'a> {
    pub issuer_trust: &'a CredentialIssuerTrust,
    pub expected_scope: &'a str,
    pub expected_issuer: &'a CredentialIssuerId,
    pub credential: &'a CredentialClaims,
    pub signature: &'a [u8],
    pub signed: DelegatedAdmissionRequest<'a>,
    pub before_bytes: &'a [u8],
    pub committed_bytes: &'a [u8],
    pub current: ContentRevision,
    pub current_bytes: &'a [u8],
}

fn exact_write<'a>(
    request: &CredentialAdmissionRequest<'a>,
) -> Result<ConditionalContentWrite<'a>, CredentialError> {
    let receipt = &request.credential.receipt;
    let before = ContentRevision {
        revision: receipt.expected_revision,
        root: ContentBlock::new(ContentCodec::Raw, request.before_bytes).cid(),
    };
    let replacement = ContentBlock::new(ContentCodec::Raw, request.committed_bytes).cid();
    if request.expected_scope.is_empty()
        || receipt.scope != request.expected_scope
        || receipt.expected_content_id.as_str() != before.root.to_string()
        || receipt.committed_content_id.as_str() != replacement.to_string()
        || receipt.expected_content_id == receipt.committed_content_id
        || receipt.expected_revision.checked_add(1) != Some(receipt.committed_revision)
        || receipt.expected_revision == 0
        || receipt.operation_id.as_str().is_empty()
        || receipt.operation_id.as_str() != receipt.reservation.purchase.purchase_id
        || request.credential.credential_id.is_empty()
    {
        return Err(CredentialError::ReceiptMismatch);
    }
    Ok(ConditionalContentWrite {
        scope: request.expected_scope,
        operation_id: receipt.operation_id.as_str(),
        expected: Some(before),
        replacement,
    })
}

fn current_budget(
    request: &CredentialAdmissionRequest<'_>,
) -> Result<SharedBudgetClaims, CredentialError> {
    let receipt = &request.credential.receipt;
    if ContentBlock::new(ContentCodec::Raw, request.current_bytes).cid() != request.current.root {
        return Err(CredentialError::CurrentContentMismatch);
    }
    let current: SharedBudgetClaims = serde_json::from_slice(request.current_bytes)
        .map_err(|_| CredentialError::InvalidClaims)?;
    if current.revision != request.current.revision
        || current.revision < receipt.committed_revision
        || (current.revision == receipt.committed_revision
            && request.current.root.to_string() != receipt.committed_content_id.as_str())
        || current.now > request.signed.now
    {
        return Err(CredentialError::InvalidCurrentBudget);
    }
    if !current.reservations.contains(&receipt.reservation) {
        return Err(CredentialError::MissingCurrentReservation);
    }
    Ok(current)
}

fn validate_current(
    request: &CredentialAdmissionRequest<'_>,
    current: &SharedBudgetClaims,
) -> Result<(), CredentialError> {
    let receipt = &request.credential.receipt;
    let lineage = &receipt.reservation.lineage;
    if lineage.first() != Some(&current.root)
        || current
            .reservations
            .iter()
            .any(|entry| entry.lineage.first() != Some(&current.root))
    {
        return Err(CredentialError::InvalidCurrentBudget);
    }
    let expires = request.credential.expires_at;
    if request.signed.now >= expires || expires > request.signed.lean_offer.expires_at {
        return Err(CredentialError::Expired);
    }
    let total = sum(current.reservations.iter())?;
    if total > current.root.total_cap {
        return Err(CredentialError::InvalidCurrentBudget);
    }
    for mandate in lineage {
        if current.revoked_mandate_ids.contains(&mandate.mandate_id)
            || current
                .reservations
                .iter()
                .flat_map(|entry| &entry.lineage)
                .any(|previous| previous.mandate_id == mandate.mandate_id && previous != mandate)
            || sum(current.reservations.iter().filter(|entry| {
                entry
                    .lineage
                    .iter()
                    .any(|ancestor| ancestor.mandate_id == mandate.mandate_id)
            }))? > mandate.total_cap
        {
            return Err(CredentialError::InvalidCurrentBudget);
        }
        if expires > mandate.expires_at {
            return Err(CredentialError::Expired);
        }
    }
    Ok(())
}

fn sum<'a>(
    mut entries: impl Iterator<Item = &'a SharedBudgetReservation>,
) -> Result<u64, CredentialError> {
    entries.try_fold(0_u64, |total, entry| {
        total
            .checked_add(entry.purchase.terms.amount_minor)
            .ok_or(CredentialError::InvalidCurrentBudget)
    })
}

pub(super) fn validate_authority<F>(
    host: &CurrentCommerceAuthority,
    mut request: CredentialAdmissionRequest<'_>,
    policy: F,
) -> Result<VerifiedCredential, CredentialError>
where
    F: FnOnce(
        &[cedar_poo_commerce::projection::LeanMandateClaims],
        &cedar_poo_commerce::projection::LeanOfferClaims,
    ) -> bool,
{
    request.signed.now = host.now;
    let current = current_budget(&request)?;
    validate_current(&request, &current)?;
    let after: SharedBudgetClaims = serde_json::from_slice(request.committed_bytes)
        .map_err(|_| CredentialError::InvalidClaims)?;
    let before = ContentRevision {
        revision: request.credential.receipt.expected_revision,
        root: ContentBlock::new(ContentCodec::Raw, request.before_bytes).cid(),
    };
    let (original, committed) = read_transition(
        before,
        request.before_bytes,
        request.committed_bytes,
        after.now,
    )
    .map_err(CredentialError::Budget)?;
    let entry = &request.credential.receipt.reservation;
    if committed.reservations.first() != Some(entry)
        || original.root != current.root
        || committed.revision != request.credential.receipt.committed_revision
        || committed.now > host.now
    {
        return Err(CredentialError::ReceiptMismatch);
    }
    let (lineage, offer) =
        admit(&host.host, request.signed, policy).map_err(CredentialError::Budget)?;
    let Some(final_mandate) = lineage.last() else {
        return Err(CredentialError::ReceiptMismatch);
    };
    if entry.lineage != lineage
        || entry.purchase.terms != offer.terms
        || entry.purchase.mandate_id != final_mandate.mandate_id
        || entry.purchase.agent_id != final_mandate.agent_id
    {
        return Err(CredentialError::ReceiptMismatch);
    }
    validate_reservation(&original, entry, &lineage, &offer).map_err(CredentialError::Budget)?;
    Ok(VerifiedCredential {
        claims: request.credential.clone(),
    })
}

/// Recover an exact commit before rechecking current authority and issuer trust.
/// All observations are authenticated by the supplied Host/port contract; there
/// is no bundled DB, token service, AP2 JWT parser or payment provider here.
/// # Errors
/// Returns missing commit, semantic refusal, signature refusal or recovery error.
pub async fn admit_committed_credential<'a, P, F, G>(
    port: &'a P,
    request: CredentialAdmissionRequest<'a>,
    refresh_authority: F,
    policy: G,
) -> Result<VerifiedCredential, CredentialRecoveryError<P::Error>>
where
    P: ConditionalContentCommitPort,
    F: FnOnce() -> Result<CurrentCommerceAuthority, CredentialError>,
    G: FnOnce(
        &[cedar_poo_commerce::projection::LeanMandateClaims],
        &cedar_poo_commerce::projection::LeanOfferClaims,
    ) -> bool,
{
    recover_committed_credential(port, &request).await?;
    let validate = CredentialRecoveryError::Validation;
    let authority = refresh_authority().map_err(validate)?;
    request
        .issuer_trust
        .verify(
            request.expected_issuer,
            request.credential,
            request.signature,
        )
        .map_err(validate)?;
    validate_authority(&authority, request, policy).map_err(validate)
}

pub(super) async fn recover_committed_credential<'a, P: ConditionalContentCommitPort>(
    port: &'a P,
    request: &CredentialAdmissionRequest<'a>,
) -> Result<(), CredentialRecoveryError<P::Error>> {
    let validate = CredentialRecoveryError::Validation;
    let write = exact_write(request).map_err(validate)?;
    let recovered = port
        .recover(write)
        .await
        .map_err(CredentialRecoveryError::Port)?;
    let receipt = write
        .recover_receipt(recovered.as_ref())
        .map_err(|error| validate(CredentialError::Commit(error)))?
        .ok_or_else(|| validate(CredentialError::MissingCommit))?;
    if receipt.committed.revision != request.credential.receipt.committed_revision {
        return Err(validate(CredentialError::ReceiptMismatch));
    }
    Ok(())
}
