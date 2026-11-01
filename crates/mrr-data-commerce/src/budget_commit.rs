//! Cedar POO shared-budget transition bound to MRR Data's commit protocol.
//!
//! The Host authenticates the current content head, runs these checks with fresh
//! authority inside its backend transaction, and atomically persists the head and
//! receipt. This module plans no DB operation and makes no durability claim.

use cedar_poo_commerce::admission::{
    AdmissionError, AdmissionRequest, CommerceAdmissionHost, DelegatedAdmissionRequest,
};
use cedar_poo_commerce::projection::{LeanMandateClaims, LeanOfferClaims, LeanPurchaseTermsClaims};
use cedar_poo_commerce::signatures::{AgentId, MandateId};
use mrr_data_content::{
    ConditionalCommitDisposition, ConditionalCommitError, ConditionalCommitPortError,
    ConditionalContentCommitOutcome, ConditionalContentCommitPort, ConditionalContentWrite,
    ContentBlock, ContentCodec, ContentRevision, PublishReceipt,
};
use serde::{Deserialize, Serialize};

/// Exact purchase identity and merchant terms charged by one reservation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SharedBudgetPurchase {
    pub purchase_id: String,
    pub mandate_id: MandateId,
    pub agent_id: AgentId,
    pub terms: LeanPurchaseTermsClaims,
}

/// Complete mandate lineage retained with its charged purchase.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SharedBudgetReservation {
    pub lineage: Vec<LeanMandateClaims>,
    pub purchase: SharedBudgetPurchase,
}

/// Persisted claims omit derived signature-verification flags. Reading a block
/// does not reestablish authority to reserve a purchase.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SharedBudgetClaims {
    pub root: LeanMandateClaims,
    pub now: u64,
    pub reservations: Vec<SharedBudgetReservation>,
    pub revoked_mandate_ids: Vec<MandateId>,
    pub revision: u64,
}

/// Refusals from claims, live authority, shared accounting, or commit checks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BudgetCommitError {
    InvalidClaims,
    CurrentContentMismatch,
    InvalidTransition,
    OperationMismatch,
    AuthorityUnavailable,
    LineageMismatch,
    RevokedMandate,
    ReboundMandate,
    DuplicatePurchaseOrOffer,
    BudgetExceeded,
    ArithmeticOverflow,
    Admission(AdmissionError),
    Commit(ConditionalCommitError),
}

fn spend<'a>(
    mut entries: impl Iterator<Item = &'a SharedBudgetReservation>,
) -> Result<u64, BudgetCommitError> {
    entries.try_fold(0_u64, |sum, entry| {
        sum.checked_add(entry.purchase.terms.amount_minor)
            .ok_or(BudgetCommitError::ArithmeticOverflow)
    })
}

pub(super) fn admit<F>(
    host: &CommerceAdmissionHost,
    signed: DelegatedAdmissionRequest<'_>,
    policy: F,
) -> Result<(Vec<LeanMandateClaims>, LeanOfferClaims), BudgetCommitError>
where
    F: FnOnce(&[LeanMandateClaims], &LeanOfferClaims) -> bool,
{
    if signed.delegations.is_empty() {
        let [root] = signed.lean_lineage else {
            return Err(BudgetCommitError::LineageMismatch);
        };
        let admitted = host
            .admit(
                AdmissionRequest {
                    mandate: signed.root,
                    mandate_signature: signed.root_signature,
                    offer: signed.offer,
                    checkout_bytes: signed.checkout_bytes,
                    offer_signature: signed.offer_signature,
                    lean_mandate: root,
                    lean_offer: signed.lean_offer,
                    now: signed.now,
                },
                |mandate, offer| policy(std::slice::from_ref(mandate), offer),
            )
            .map_err(BudgetCommitError::Admission)?;
        Ok((vec![admitted.mandate().clone()], admitted.offer().clone()))
    } else {
        let admitted = host
            .admit_delegated(signed, policy)
            .map_err(BudgetCommitError::Admission)?;
        Ok((admitted.lineage().to_vec(), admitted.offer().clone()))
    }
}

/// Check the exact one-purchase Lean proposal against independently read current
/// bytes, fresh signatures and every ancestor's shared spend, then apply the
/// provider-neutral MRR Data conditional-write protocol.
///
/// The caller authenticates `scope` and `current`, supplies a trusted clock in
/// `signed`, and rechecks live authority under the same backend transaction as
/// this head comparison. A returned Apply is a plan, never a commit receipt or
/// payment authority. For status recovery use MRR Data's exact operation receipt
/// replay; that query grants no new reservation.
pub struct SharedBudgetCommitRequest<'a> {
    pub signed: DelegatedAdmissionRequest<'a>,
    pub scope: &'a str,
    pub operation_id: &'a str,
    pub current: ContentRevision,
    pub current_bytes: &'a [u8],
    pub proposed_bytes: &'a [u8],
    pub physical: &'a PublishReceipt,
}

/// Validate one fresh reservation proposal. The Host applies this decision
/// under the atomic backend transaction described on `SharedBudgetCommitRequest`.
pub fn decide_shared_reservation_commit<F>(
    host: &CommerceAdmissionHost,
    request: SharedBudgetCommitRequest<'_>,
    policy: F,
) -> Result<ConditionalCommitDisposition, BudgetCommitError>
where
    F: FnOnce(&[LeanMandateClaims], &LeanOfferClaims) -> bool,
{
    let SharedBudgetCommitRequest {
        signed,
        scope,
        operation_id,
        current,
        current_bytes,
        proposed_bytes,
        physical,
    } = request;
    let (before, after) = read_transition(current, current_bytes, proposed_bytes, signed.now)?;
    let (lineage, offer) = admit(host, signed, policy)?;
    let entry = &after.reservations[0];
    if entry.purchase.purchase_id != operation_id {
        return Err(BudgetCommitError::OperationMismatch);
    }
    validate_reservation(&before, entry, &lineage, &offer)?;
    ConditionalContentWrite {
        scope,
        operation_id,
        expected: Some(current),
        replacement: ContentBlock::new(ContentCodec::Raw, proposed_bytes).cid(),
    }
    .decide_commit(Some(current), Some(physical), None)
    .map_err(BudgetCommitError::Commit)
}

pub(super) fn read_transition(
    current: ContentRevision,
    current_bytes: &[u8],
    proposed_bytes: &[u8],
    now: u64,
) -> Result<(SharedBudgetClaims, SharedBudgetClaims), BudgetCommitError> {
    if ContentBlock::new(ContentCodec::Raw, current_bytes).cid() != current.root {
        return Err(BudgetCommitError::CurrentContentMismatch);
    }
    let before: SharedBudgetClaims =
        serde_json::from_slice(current_bytes).map_err(|_| BudgetCommitError::InvalidClaims)?;
    let after: SharedBudgetClaims =
        serde_json::from_slice(proposed_bytes).map_err(|_| BudgetCommitError::InvalidClaims)?;
    let expected_next = before
        .revision
        .checked_add(1)
        .ok_or(BudgetCommitError::ArithmeticOverflow)?;
    if before.revision != current.revision
        || after.revision != expected_next
        || after.root != before.root
        || after.revoked_mandate_ids != before.revoked_mandate_ids
        || after.now != now
        || after.now < before.now
    {
        return Err(BudgetCommitError::InvalidTransition);
    }
    let Some((_, rest)) = after.reservations.split_first() else {
        return Err(BudgetCommitError::InvalidTransition);
    };
    if rest != before.reservations {
        return Err(BudgetCommitError::InvalidTransition);
    }
    Ok((before, after))
}

pub(super) fn validate_reservation(
    before: &SharedBudgetClaims,
    entry: &SharedBudgetReservation,
    lineage: &[LeanMandateClaims],
    offer: &LeanOfferClaims,
) -> Result<(), BudgetCommitError> {
    let Some(final_mandate) = lineage.last() else {
        return Err(BudgetCommitError::LineageMismatch);
    };
    if lineage.first() != Some(&before.root)
        || entry.lineage != lineage
        || entry.purchase.mandate_id != final_mandate.mandate_id
        || entry.purchase.agent_id != final_mandate.agent_id
        || entry.purchase.terms != offer.terms
        || entry.purchase.purchase_id.is_empty()
    {
        return Err(BudgetCommitError::LineageMismatch);
    }
    let amount = entry.purchase.terms.amount_minor;
    if spend(before.reservations.iter())?
        .checked_add(amount)
        .ok_or(BudgetCommitError::ArithmeticOverflow)?
        > before.root.total_cap
    {
        return Err(BudgetCommitError::BudgetExceeded);
    }
    for previous in &before.reservations {
        if previous.lineage.first() != Some(&before.root) {
            return Err(BudgetCommitError::LineageMismatch);
        }
        if previous.purchase.purchase_id == entry.purchase.purchase_id
            || (previous.purchase.terms.merchant_id == offer.terms.merchant_id
                && previous.purchase.terms.offer_id == offer.terms.offer_id)
        {
            return Err(BudgetCommitError::DuplicatePurchaseOrOffer);
        }
    }
    for mandate in lineage {
        if before.revoked_mandate_ids.contains(&mandate.mandate_id) {
            return Err(BudgetCommitError::RevokedMandate);
        }
        if before
            .reservations
            .iter()
            .flat_map(|previous| &previous.lineage)
            .any(|previous| previous.mandate_id == mandate.mandate_id && previous != mandate)
        {
            return Err(BudgetCommitError::ReboundMandate);
        }
        let spent = spend(before.reservations.iter().filter(|previous| {
            previous
                .lineage
                .iter()
                .any(|ancestor| ancestor.mandate_id == mandate.mandate_id)
        }))?;
        if spent
            .checked_add(amount)
            .ok_or(BudgetCommitError::ArithmeticOverflow)?
            > mandate.total_cap
        {
            return Err(BudgetCommitError::BudgetExceeded);
        }
    }
    Ok(())
}

/// Host-authenticated current registry and clock, refreshed inside commit validation.
/// A deploying adapter must synchronize this authority through the head transaction.
pub struct CurrentCommerceAuthority {
    pub host: CommerceAdmissionHost,
    pub now: u64,
}

/// Commit one exact reservation through a backend's atomic head/receipt port.
/// The fresh authority callback is invoked only for a new write, inside the
/// backend transaction. Replay is a status result and grants no new reservation.
/// A clock change that makes the proposed timestamp stale requires a fresh proposal.
/// # Errors
/// Returns protocol or semantic refusal, known backend refusal, or an Unknown
/// outcome that must be recovered under the original exact operation identity.
pub async fn commit_shared_reservation<'a, P, F, G>(
    port: &'a P,
    mut request: SharedBudgetCommitRequest<'a>,
    refresh_authority: F,
    policy: G,
) -> Result<
    ConditionalContentCommitOutcome<'a>,
    ConditionalCommitPortError<P::Error, BudgetCommitError>,
>
where
    P: ConditionalContentCommitPort,
    F: FnOnce() -> Result<CurrentCommerceAuthority, BudgetCommitError> + Send + 'a,
    G: FnOnce(&[LeanMandateClaims], &LeanOfferClaims) -> bool + Send + 'a,
{
    let write = ConditionalContentWrite {
        scope: request.scope,
        operation_id: request.operation_id,
        expected: Some(request.current),
        replacement: ContentBlock::new(ContentCodec::Raw, request.proposed_bytes).cid(),
    };
    let physical = request.physical;
    port.commit(write, Some(physical), move |observed| {
        if observed != Some(request.current) {
            return Err(BudgetCommitError::Commit(
                ConditionalCommitError::RevisionConflict,
            ));
        }
        let authority = refresh_authority()?;
        request.signed.now = authority.now;
        decide_shared_reservation_commit(&authority.host, request, policy).map(|_| ())
    })
    .await
}
