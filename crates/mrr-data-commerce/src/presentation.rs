//! Atomic presentation lifecycle and signed receipt evidence persistence.
//! Full SD-JWT mandate verification and transport remain Host responsibilities.
pub use cedar_poo_commerce::presentation::{
    AP2_SOURCE_REVISION, CLOSED_CHECKOUT_VCT, CheckoutPresentation, CheckoutReceiptClaims,
    CheckoutReceiptTrust, CheckoutStatus, OPEN_CHECKOUT_VCT, PresentationError, PresentationLedger,
    ReceiptKeyId, VerifiedCheckoutReceipt,
};
use cedar_poo_commerce::signatures::sha256_hex;
use mrr_data_content::{
    ConditionalCommitPortError, ConditionalContentCommitOutcome, ConditionalContentCommitPort,
    ConditionalContentWrite, ContentBlock, ContentCodec, ContentRevision, PublishReceipt,
};
use serde::{Deserialize, Serialize};

/// Append-only evidence committed atomically with the pure lifecycle state.
/// Original mandate chains are retrieved by reference from the Host's evidence store.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PresentationJournal {
    pub ledger: PresentationLedger,
    pub presentations: Vec<CheckoutPresentation>,
    pub receipts: Vec<String>,
}
/// Proposed event for one independently scoped open-mandate/agent ledger.
pub enum PresentationEvent<'a> {
    Present {
        presentation: &'a CheckoutPresentation,
        open_vct: &'a str,
        closed_vct: &'a str,
    },
    Receipt {
        jwt: &'a str,
        now: u64,
    },
}
/// Exact immutable journal proposal and independently authenticated scope/head.
pub struct PresentationCommitRequest<'a> {
    pub scope: &'a str,
    pub current: ContentRevision,
    pub current_bytes: &'a [u8],
    pub proposed_bytes: &'a [u8],
    pub physical: &'a PublishReceipt,
    pub event: PresentationEvent<'a>,
}
/// Semantic refusal, protected validation failure or backend uncertainty.
#[derive(Debug)]
pub enum PresentationCommitError<E> {
    Validation(PresentationError),
    Port(ConditionalCommitPortError<E, PresentationError>),
}
/// Exact replay is status only and grants no new presentation send.
#[derive(Debug, Eq, PartialEq)]
pub enum PresentationCommitDisposition {
    Committed,
    Replayed,
}
struct PreparedPresentation {
    before: PresentationJournal,
    after: PresentationJournal,
    scope: String,
    operation: String,
}
fn prepare(
    input: &PresentationCommitRequest<'_>,
) -> Result<PreparedPresentation, PresentationError> {
    let parse = |bytes: &[u8]| {
        serde_json::from_slice::<PresentationJournal>(bytes)
            .map_err(|_| PresentationError::InvalidClaims)
    };
    if ContentBlock::new(ContentCodec::Raw, input.current_bytes).cid() != input.current.root {
        return Err(PresentationError::InvalidClaims);
    }
    let before = parse(input.current_bytes)?;
    let after = parse(input.proposed_bytes)?;
    if before.ledger.open_mandate_scope != input.scope
        || before.ledger.revision != input.current.revision
    {
        return Err(PresentationError::InvalidTransition);
    }
    let operation = match &input.event {
        PresentationEvent::Present { presentation, .. } => {
            format!("present/{}", presentation.reference)
        }
        PresentationEvent::Receipt { .. } => format!(
            "receipt/{}",
            before
                .ledger
                .pending
                .as_ref()
                .ok_or(PresentationError::InvalidTransition)?
                .reference
        ),
    };
    Ok(PreparedPresentation {
        before,
        after,
        operation,
        scope: format!(
            "cedar-poo/commerce/presentation/v1/{}",
            sha256_hex(input.scope.as_bytes())
        ),
    })
}
fn apply_event(
    before: &PresentationJournal,
    event: &PresentationEvent<'_>,
    trust: &CheckoutReceiptTrust,
) -> Result<PresentationJournal, PresentationError> {
    let mut next = before.clone();
    next.ledger = match event {
        PresentationEvent::Present {
            presentation,
            open_vct,
            closed_vct,
        } => {
            next.presentations.push((*presentation).clone());
            before
                .ledger
                .present(open_vct, closed_vct, (*presentation).clone())
        }
        PresentationEvent::Receipt { jwt, now } => {
            let pending = before
                .ledger
                .pending
                .as_ref()
                .ok_or(PresentationError::InvalidTransition)?;
            let receipt = trust.verify(jwt, pending, *now)?;
            next.receipts.push((*jwt).to_owned());
            before.ledger.complete(&receipt)
        }
    }
    .ok_or(PresentationError::InvalidTransition)?;
    Ok(next)
}
fn confirm<'a, E>(
    write: ConditionalContentWrite<'a>,
    outcome: ConditionalContentCommitOutcome<'a>,
) -> Result<PresentationCommitDisposition, PresentationCommitError<E>> {
    let (receipt, disposition) = match outcome {
        ConditionalContentCommitOutcome::Committed(receipt) => {
            (receipt, PresentationCommitDisposition::Committed)
        }
        ConditionalContentCommitOutcome::Replayed(receipt) => {
            (receipt, PresentationCommitDisposition::Replayed)
        }
    };
    write
        .recover_receipt(Some(&receipt))
        .map_err(|_| PresentationCommitError::Validation(PresentationError::InvalidTransition))?;
    Ok(disposition)
}

/// Persist a slot before sending and preserve exact signed receipt bytes when
/// closing it. Refresh authenticates scope ownership, exact mandate vcts, agent
/// key, expiry and constraints for Present; for Receipt it supplies current merchant
/// keys and terminal-rejection policy. Synchronize all authority changes with this
/// protected commit. Unknown persistence grants no send; exact replay is status only.
/// This supplies no network endpoint, settlement, fulfillment or full AP2 verifier.
/// # Errors
/// Returns conflict, malformed proposal, untrusted receipt or explicit uncertainty.
pub async fn commit_presentation<P, F>(
    port: &P,
    input: PresentationCommitRequest<'_>,
    refresh: F,
) -> Result<PresentationCommitDisposition, PresentationCommitError<P::Error>>
where
    P: ConditionalContentCommitPort,
    F: FnOnce(&PresentationEvent<'_>) -> Result<CheckoutReceiptTrust, PresentationError> + Send,
{
    let prepared = prepare(&input).map_err(PresentationCommitError::Validation)?;
    let write = ConditionalContentWrite {
        scope: &prepared.scope,
        operation_id: &prepared.operation,
        expected: Some(input.current),
        replacement: ContentBlock::new(ContentCodec::Raw, input.proposed_bytes).cid(),
    };
    let mut validated = false;
    let outcome = port
        .commit(write, Some(input.physical), |observed| {
            if observed != Some(input.current) {
                return Err(PresentationError::InvalidTransition);
            }
            let trust = refresh(&input.event)?;
            if apply_event(&prepared.before, &input.event, &trust)? != prepared.after {
                return Err(PresentationError::InvalidTransition);
            }
            validated = true;
            Ok(())
        })
        .await
        .map_err(PresentationCommitError::Port)?;
    let disposition = confirm(write, outcome)?;
    if disposition == PresentationCommitDisposition::Committed && !validated {
        return Err(PresentationCommitError::Validation(
            PresentationError::InvalidTransition,
        ));
    }
    Ok(disposition)
}
