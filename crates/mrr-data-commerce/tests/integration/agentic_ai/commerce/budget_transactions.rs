//! Test-only transaction provider and signed shared-budget consumers.
//! These checks provide no DB, crash recovery, or deployment durability evidence.

use super::{Fixture, key};
use cedar_poo_commerce::admission::AdmissionError;
use mrr_data_commerce::budget_commit::{
    BudgetCommitError, CurrentCommerceAuthority, SharedBudgetClaims, commit_shared_reservation,
};
use mrr_data_content::{
    ConditionalCommitDisposition, ConditionalCommitError, ConditionalCommitFuture,
    ConditionalCommitPortError as PortError, ConditionalContentCommitOutcome as Outcome,
    ConditionalContentCommitPort as Port, ConditionalContentReceipt as Receipt,
    ConditionalContentWrite as Write, ContentBlock, ContentCodec, ContentRevision, PublishReceipt,
};
use std::{
    future::Future,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

struct Row {
    scope: String,
    operation: String,
    expected: Option<ContentRevision>,
    committed: ContentRevision,
}
impl Row {
    fn receipt(&self) -> Receipt<'_> {
        Receipt {
            write: Write {
                scope: &self.scope,
                operation_id: &self.operation,
                expected: self.expected,
                replacement: self.committed.root,
            },
            committed: self.committed,
        }
    }
}
struct State {
    head: Option<ContentRevision>,
    rows: Vec<Row>,
    lose_ack: bool,
    unavailable: bool,
}
pub(super) struct TestPort(Mutex<State>);
impl TestPort {
    pub(super) fn new(before: &SharedBudgetClaims) -> Self {
        Self::new_head(head(before))
    }
    pub(super) fn new_head(current: ContentRevision) -> Self {
        Self(Mutex::new(State {
            head: Some(current),
            rows: vec![],
            lose_ack: false,
            unavailable: false,
        }))
    }
}
impl TestPort {
    #[cfg(feature = "credential")]
    pub(super) fn set_unavailable(&self) {
        self.0.lock().unwrap().unavailable = true;
    }
}
#[cfg(any(feature = "consumption", feature = "presentation"))]
impl TestPort {
    pub(super) fn lose_ack(&self) {
        self.0.lock().unwrap().lose_ack = true;
    }
}
impl Port for TestPort {
    type Error = &'static str;
    fn commit<'a, V, F>(
        &'a self,
        write: Write<'a>,
        physical: Option<&'a PublishReceipt>,
        validate: F,
    ) -> ConditionalCommitFuture<'a, Outcome<'a>, Self::Error, V>
    where
        V: Send + 'a,
        F: FnOnce(Option<ContentRevision>) -> Result<(), V> + Send + 'a,
    {
        Box::pin(async move {
            let mut state = self.0.lock().unwrap();
            if state.unavailable {
                return Err(PortError::BeforeCommit("unavailable"));
            }
            let existing = state
                .rows
                .iter()
                .find(|row| row.scope == write.scope && row.operation == write.operation_id)
                .map(Row::receipt);
            let decision = write
                .decide_commit(state.head, physical, existing.as_ref())
                .map_err(PortError::Protocol)?;
            if decision == ConditionalCommitDisposition::Replay {
                return Ok(Outcome::Replayed(
                    write
                        .recover_receipt(existing.as_ref())
                        .map_err(PortError::Protocol)?
                        .unwrap(),
                ));
            }
            validate(state.head).map_err(PortError::Validation)?;
            let ConditionalCommitDisposition::Apply(next) = decision else {
                unreachable!()
            };
            state.head = Some(next);
            state.rows.push(Row {
                scope: write.scope.into(),
                operation: write.operation_id.into(),
                expected: write.expected,
                committed: next,
            });
            if state.lose_ack {
                return Err(PortError::Unknown("lost commit acknowledgement"));
            }
            Ok(Outcome::Committed(Receipt {
                write,
                committed: next,
            }))
        })
    }
    fn recover<'a>(
        &'a self,
        write: Write<'a>,
    ) -> ConditionalCommitFuture<'a, Option<Receipt<'a>>, Self::Error> {
        Box::pin(async move {
            let state = self.0.lock().unwrap();
            if state.unavailable {
                return Err(PortError::BeforeCommit("unavailable"));
            }
            let existing = state
                .rows
                .iter()
                .find(|row| row.scope == write.scope && row.operation == write.operation_id)
                .map(Row::receipt);
            write
                .recover_receipt(existing.as_ref())
                .map_err(PortError::Protocol)
        })
    }
}

// This provider deliberately completes without suspension. This helper tests
// its synchronous model only; a real adapter must supply its own async runtime.
pub(super) fn complete<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("test transaction provider unexpectedly suspended"),
    }
}
fn head(state: &SharedBudgetClaims) -> ContentRevision {
    ContentRevision {
        revision: state.revision,
        root: ContentBlock::new(ContentCodec::Raw, &serde_json::to_vec(state).unwrap()).cid(),
    }
}
fn authority(fixture: &Fixture) -> Result<CurrentCommerceAuthority, BudgetCommitError> {
    Ok(CurrentCommerceAuthority {
        host: fixture.host(),
        now: 10,
    })
}
fn disposition(outcome: Outcome<'_>) -> ConditionalCommitDisposition {
    match outcome {
        Outcome::Committed(receipt) => ConditionalCommitDisposition::Apply(receipt.committed),
        Outcome::Replayed(_) => ConditionalCommitDisposition::Replay,
    }
}
fn commit<F>(
    fixture: &Fixture,
    port: &TestPort,
    before: &SharedBudgetClaims,
    after: &SharedBudgetClaims,
    refresh: F,
) -> Result<ConditionalCommitDisposition, PortError<&'static str, BudgetCommitError>>
where
    F: FnOnce() -> Result<CurrentCommerceAuthority, BudgetCommitError> + Send,
{
    fixture.with_request(before, after, true, |request| {
        complete(commit_shared_reservation(
            port,
            request,
            refresh,
            |lineage, offer| lineage == fixture.lineage && offer == &fixture.lean_offer,
        ))
        .map(disposition)
    })
}
fn query<'a>(before: &SharedBudgetClaims, after: &'a SharedBudgetClaims) -> Write<'a> {
    Write {
        scope: "buyer-trip-root",
        operation_id: &after.reservations[0].purchase.purchase_id,
        expected: Some(head(before)),
        replacement: head(after).root,
    }
}

#[test]
fn ambiguous_commit_recovers_exact_receipt_and_replay_never_reserves_again() {
    let fixture = Fixture::lean();
    let port = TestPort::new(&fixture.before);
    port.0.lock().unwrap().lose_ack = true;
    let count = AtomicUsize::new(0);
    assert_eq!(
        commit(&fixture, &port, &fixture.before, &fixture.after, || {
            count.fetch_add(1, Ordering::SeqCst);
            authority(&fixture)
        }),
        Err(PortError::Unknown("lost commit acknowledgement"))
    );
    let request = query(&fixture.before, &fixture.after);
    let receipt = complete(port.recover(request)).unwrap().unwrap();
    assert_eq!(receipt.committed, head(&fixture.after));
    assert_eq!(
        commit(&fixture, &port, &fixture.before, &fixture.after, || panic!(
            "replay cannot refresh authority or reserve again"
        )),
        Ok(ConditionalCommitDisposition::Replay)
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(port.0.lock().unwrap().rows.len(), 1);
    port.0.lock().unwrap().head = Some(ContentRevision {
        revision: 9,
        root: head(&fixture.before).root,
    });
    assert_eq!(complete(port.recover(request)).unwrap(), Some(receipt));
    let changed = Write {
        replacement: head(&fixture.before).root,
        ..request
    };
    assert_eq!(
        complete(port.recover(changed)),
        Err(PortError::Protocol(
            ConditionalCommitError::OperationConflict
        ))
    );
}

#[test]
fn a_new_operation_cannot_repeat_the_offer_after_a_lost_commit_ack() {
    let fixture = Fixture::lean();
    let port = TestPort::new(&fixture.before);
    port.0.lock().unwrap().lose_ack = true;
    assert!(matches!(
        commit(&fixture, &port, &fixture.before, &fixture.after, || {
            authority(&fixture)
        }),
        Err(PortError::Unknown(_))
    ));
    let current = fixture.after.clone();
    let mut retry = fixture.next(&current);
    retry.reservations[0].purchase.purchase_id = "new-id-for-uncertain-purchase".into();
    assert_eq!(
        commit(&fixture, &port, &current, &retry, || authority(&fixture)),
        Err(PortError::Validation(
            BudgetCommitError::DuplicatePurchaseOrOffer
        ))
    );
    assert_eq!(port.0.lock().unwrap().rows.len(), 1);
    assert_eq!(port.0.lock().unwrap().head, Some(head(&current)));
}

#[test]
fn revoked_authority_after_preparation_is_refreshed_before_commit() {
    let fixture = Fixture::lean();
    let port = TestPort::new(&fixture.before);
    assert!(
        fixture
            .decide(&fixture.host(), &fixture.before, &fixture.after, true)
            .is_ok()
    );
    let result = commit(&fixture, &port, &fixture.before, &fixture.after, || {
        let mut current = fixture.host();
        current.revoke_mandate(
            fixture.root.principal.clone(),
            fixture.child.mandate_id.clone(),
        );
        Ok(CurrentCommerceAuthority {
            host: current,
            now: 10,
        })
    });
    assert!(matches!(
        result,
        Err(PortError::Validation(BudgetCommitError::Admission(
            AdmissionError::RevokedMandate
        )))
    ));
    assert_eq!(port.0.lock().unwrap().head, Some(head(&fixture.before)));
    assert!(port.0.lock().unwrap().rows.is_empty());
}

#[test]
fn unavailable_authority_and_stale_clock_do_not_commit() {
    let fixture = Fixture::lean();
    let port = TestPort::new(&fixture.before);
    assert_eq!(
        commit(&fixture, &port, &fixture.before, &fixture.after, || Err(
            BudgetCommitError::AuthorityUnavailable
        )),
        Err(PortError::Validation(
            BudgetCommitError::AuthorityUnavailable
        ))
    );
    assert_eq!(
        commit(&fixture, &port, &fixture.before, &fixture.after, || Ok(
            CurrentCommerceAuthority {
                host: fixture.host(),
                now: 11
            }
        )),
        Err(PortError::Validation(BudgetCommitError::InvalidTransition))
    );
    assert!(port.0.lock().unwrap().rows.is_empty());
}

#[test]
fn operation_identity_must_match_the_embedded_purchase() {
    let fixture = Fixture::lean();
    let port = TestPort::new(&fixture.before);
    let result = fixture.with_request(&fixture.before, &fixture.after, true, |mut request| {
        request.operation_id = "changed-operation";
        complete(commit_shared_reservation(
            &port,
            request,
            || authority(&fixture),
            |_, _| true,
        ))
        .map(disposition)
    });
    assert_eq!(
        result,
        Err(PortError::Validation(BudgetCommitError::OperationMismatch))
    );
    assert!(port.0.lock().unwrap().rows.is_empty());
}

#[test]
fn unreadable_operation_ledger_never_becomes_a_missing_receipt() {
    let fixture = Fixture::lean();
    let port = TestPort::new(&fixture.before);
    port.0.lock().unwrap().unavailable = true;
    assert_eq!(
        complete(port.recover(query(&fixture.before, &fixture.after))),
        Err(PortError::BeforeCommit("unavailable"))
    );
}

#[test]
fn competing_signed_children_share_one_head_and_retry_cannot_exceed_root_cap() {
    let first = Fixture::lean();
    let mut second = first.clone();
    second.child.mandate_id = "child-b".into();
    second.child.agent_id = "agent-b".into();
    second.child.agent_public_key = key(5).into();
    second.offer.offer_id = "offer-b".into();
    second.lineage[1].mandate_id = second.child.mandate_id.clone();
    second.lineage[1].agent_id = second.child.agent_id.clone();
    second.lineage[1].agent_public_key = second.child.agent_public_key.clone();
    second.lean_offer.terms.offer_id = second.offer.offer_id.clone();
    second.after.reservations[0].lineage = second.lineage.clone();
    second.after.reservations[0].purchase.mandate_id = second.child.mandate_id.clone();
    second.after.reservations[0].purchase.agent_id = second.child.agent_id.clone();
    second.after.reservations[0].purchase.purchase_id = "purchase-b".into();
    second.after.reservations[0].purchase.terms = second.lean_offer.terms.clone();
    let mut before = first.before.clone();
    before.revision = 2;
    before.reservations.push(first.previous(30_000, true));
    let port = Arc::new(TestPort::new(&before));
    let barrier = Arc::new(Barrier::new(2));
    let fixtures = [first, second];
    let proposals = fixtures.each_ref().map(|fixture| fixture.next(&before));
    let workers: Vec<_> = fixtures
        .iter()
        .cloned()
        .zip(proposals.iter().cloned())
        .map(|(fixture, after)| {
            let port = Arc::clone(&port);
            let barrier = Arc::clone(&barrier);
            let before = before.clone();
            std::thread::spawn(move || {
                barrier.wait();
                commit(&fixture, &port, &before, &after, || authority(&fixture))
            })
        })
        .collect();
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Ok(ConditionalCommitDisposition::Apply(_))))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(
                result,
                Err(PortError::Protocol(
                    ConditionalCommitError::RevisionConflict
                ))
            ))
            .count(),
        1
    );
    let winner = results.iter().position(Result::is_ok).unwrap();
    let loser = 1 - winner;
    let current = &proposals[winner];
    let retry = fixtures[loser].next(current);
    assert_eq!(
        commit(&fixtures[loser], &port, current, &retry, || authority(
            &fixtures[loser]
        )),
        Err(PortError::Validation(BudgetCommitError::BudgetExceeded))
    );
    assert_eq!(port.0.lock().unwrap().head, Some(head(current)));
    assert_eq!(port.0.lock().unwrap().rows.len(), 1);
}
