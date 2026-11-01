use super::signer;
use super::transactions::{TestPort, complete};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use mrr_data_commerce::presentation::{
    CLOSED_CHECKOUT_VCT, CheckoutReceiptTrust, OPEN_CHECKOUT_VCT, PresentationCommitDisposition,
    PresentationCommitError, PresentationCommitRequest, PresentationEvent, PresentationJournal,
    PresentationLedger, commit_presentation,
};
use mrr_data_content::{
    CacheAdmission, ContentBlock, ContentCodec, ContentRevision, PublishReceipt,
};
use p256::ecdsa::{Signature, signature::Signer};
use serde_json::{Value, json};

fn ledger(name: &str) -> PresentationLedger {
    let data: Value = serde_json::from_slice(include_bytes!(
        "../../../fixtures/commerce-presentation-v1.json"
    ))
    .unwrap();
    serde_json::from_value(data[name].clone()).unwrap()
}
fn state(name: &str) -> PresentationJournal {
    PresentationJournal {
        ledger: ledger(name),
        presentations: if name == "fresh" {
            vec![]
        } else {
            vec![ledger("pending").pending.unwrap()]
        },
        receipts: match name {
            "rejected" => vec![receipt("Error", "closed-A", 11)],
            "succeeded" => vec![receipt("Success", "closed-A", 11)],
            _ => vec![],
        },
    }
}
fn head(s: &PresentationJournal) -> ContentRevision {
    ContentRevision {
        revision: s.ledger.revision,
        root: ContentBlock::new(ContentCodec::Raw, &serde_json::to_vec(s).unwrap()).cid(),
    }
}
fn keys() -> CheckoutReceiptTrust {
    let mut keys = CheckoutReceiptTrust::default();
    keys.enroll(
        "merchant".into(),
        "key-7".into(),
        *signer(7).verifying_key(),
    );
    keys.allow_rejection("merchant", "checkout_unavailable".into())
        .unwrap();
    keys
}
fn receipt(status: &str, reference: &str, time: u64) -> String {
    let payload = if status == "Success" {
        json!({"status":status,"iss":"merchant","iat":time,"reference":reference,"order_id":"order-42"})
    } else {
        json!({"status":status,"iss":"merchant","iat":time,"reference":reference,"error":"checkout_unavailable","error_description":"Unavailable"})
    };
    let signing = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(r#"{"alg":"ES256","typ":"JWT"}"#),
        URL_SAFE_NO_PAD.encode(payload.to_string())
    );
    let signature: Signature = signer(7).sign(signing.as_bytes());
    format!(
        "{}.{}",
        signing,
        URL_SAFE_NO_PAD.encode(signature.to_bytes())
    )
}
fn commit(
    port: &TestPort,
    before: &PresentationJournal,
    after: &PresentationJournal,
    event: PresentationEvent<'_>,
    trust: CheckoutReceiptTrust,
) -> Result<PresentationCommitDisposition, PresentationCommitError<&'static str>> {
    let before_bytes = serde_json::to_vec(before).unwrap();
    let after_bytes = serde_json::to_vec(after).unwrap();
    let physical = PublishReceipt {
        cid: head(after).root,
        cache: CacheAdmission::Stored,
    };
    complete(commit_presentation(
        port,
        PresentationCommitRequest {
            scope: &before.ledger.open_mandate_scope,
            current: head(before),
            current_bytes: &before_bytes,
            proposed_bytes: &after_bytes,
            physical: &physical,
            event,
        },
        |_| Ok(trust),
    ))
}
fn present(s: &PresentationJournal) -> PresentationEvent<'_> {
    PresentationEvent::Present {
        presentation: s.ledger.pending.as_ref().unwrap(),
        open_vct: OPEN_CHECKOUT_VCT,
        closed_vct: CLOSED_CHECKOUT_VCT,
    }
}
#[test]
fn persisted_slot_and_exact_rejection_enable_fresh_reference() {
    let fresh = state("fresh");
    let pending = state("pending");
    let rejected = state("rejected");
    let port = TestPort::new_head(head(&fresh));
    assert_eq!(
        commit(&port, &fresh, &pending, present(&pending), keys()).unwrap(),
        PresentationCommitDisposition::Committed
    );
    let jwt = receipt("Error", "closed-A", 11);
    assert_eq!(
        commit(
            &port,
            &pending,
            &rejected,
            PresentationEvent::Receipt { jwt: &jwt, now: 12 },
            keys()
        )
        .unwrap(),
        PresentationCommitDisposition::Committed
    );
    let mut next = pending.ledger.pending.as_ref().unwrap().clone();
    next.reference = "closed-B".into();
    next.presented_at = 12;
    let mut after = rejected.clone();
    after.ledger = rejected
        .ledger
        .present(OPEN_CHECKOUT_VCT, CLOSED_CHECKOUT_VCT, next.clone())
        .unwrap();
    after.presentations.push(next);
    commit(&port, &rejected, &after, present(&after), keys()).unwrap();
    assert!(
        commit(
            &port,
            &after,
            &rejected,
            PresentationEvent::Receipt { jwt: &jwt, now: 12 },
            keys()
        )
        .is_err()
    );
}
#[test]
fn lost_ack_exact_replay_grants_no_new_presentation() {
    let fresh = state("fresh");
    let pending = state("pending");
    let port = TestPort::new_head(head(&fresh));
    port.lose_ack();
    assert!(commit(&port, &fresh, &pending, present(&pending), keys()).is_err());
    assert_eq!(
        commit(
            &port,
            &fresh,
            &pending,
            present(&pending),
            CheckoutReceiptTrust::default()
        )
        .unwrap(),
        PresentationCommitDisposition::Replayed
    );
    assert!(
        pending
            .ledger
            .present(
                OPEN_CHECKOUT_VCT,
                CLOSED_CHECKOUT_VCT,
                pending.ledger.pending.as_ref().unwrap().clone()
            )
            .is_none()
    );
}
#[test]
fn success_is_terminal_for_open_mandate_scope() {
    let pending = state("pending");
    let succeeded = state("succeeded");
    let port = TestPort::new_head(head(&pending));
    let jwt = receipt("Success", "closed-A", 11);
    commit(
        &port,
        &pending,
        &succeeded,
        PresentationEvent::Receipt { jwt: &jwt, now: 12 },
        keys(),
    )
    .unwrap();
    let mut next = pending.ledger.pending.unwrap();
    next.reference = "closed-B".into();
    assert!(
        succeeded
            .ledger
            .present(OPEN_CHECKOUT_VCT, CLOSED_CHECKOUT_VCT, next)
            .is_none()
    );
}
#[test]
fn forged_misbound_and_revoked_receipts_do_not_clear_slot() {
    let pending = state("pending");
    let rejected = state("rejected");
    for jwt in [
        "unsigned".into(),
        receipt("Error", "closed-B", 11),
        receipt("Error", "closed-A", 9),
    ] {
        let port = TestPort::new_head(head(&pending));
        assert!(
            commit(
                &port,
                &pending,
                &rejected,
                PresentationEvent::Receipt { jwt: &jwt, now: 12 },
                keys()
            )
            .is_err()
        );
        let good = receipt("Error", "closed-A", 11);
        commit(
            &port,
            &pending,
            &rejected,
            PresentationEvent::Receipt {
                jwt: &good,
                now: 12,
            },
            keys(),
        )
        .unwrap();
    }
    let port = TestPort::new_head(head(&pending));
    let good = receipt("Error", "closed-A", 11);
    assert!(
        commit(
            &port,
            &pending,
            &rejected,
            PresentationEvent::Receipt {
                jwt: &good,
                now: 12
            },
            CheckoutReceiptTrust::default()
        )
        .is_err()
    );
}
#[test]
fn operation_replay_with_changed_body_is_conflict() {
    let fresh = state("fresh");
    let pending = state("pending");
    let port = TestPort::new_head(head(&fresh));
    commit(&port, &fresh, &pending, present(&pending), keys()).unwrap();
    let mut changed = pending.clone();
    changed.ledger.pending.as_mut().unwrap().merchant_issuer = "substituted".into();
    assert!(commit(&port, &fresh, &changed, present(&changed), keys()).is_err());
}
#[test]
fn competing_presentations_cannot_both_commit() {
    let fresh = state("fresh");
    let pending = state("pending");
    let port = TestPort::new_head(head(&fresh));
    let mut b = pending.clone();
    b.ledger.pending.as_mut().unwrap().reference = "closed-B".into();
    b.ledger.seen[0] = "closed-B".into();
    b.presentations[0].reference = "closed-B".into();
    let (a, b) = std::thread::scope(|scope| {
        let a = scope.spawn(|| commit(&port, &fresh, &pending, present(&pending), keys()));
        let b = scope.spawn(|| commit(&port, &fresh, &b, present(&b), keys()));
        (a.join().unwrap(), b.join().unwrap())
    });
    assert_ne!(a.is_ok(), b.is_ok());
}

#[test]
fn receipt_evidence_cannot_be_erased_in_next_transition() {
    let rejected = state("rejected");
    let port = TestPort::new_head(head(&rejected));
    let mut p = ledger("pending").pending.unwrap();
    p.reference = "closed-B".into();
    p.presented_at = 12;
    let mut next = rejected.clone();
    next.ledger = rejected
        .ledger
        .present(OPEN_CHECKOUT_VCT, CLOSED_CHECKOUT_VCT, p.clone())
        .unwrap();
    next.presentations.push(p);
    next.receipts.clear();
    assert!(commit(&port, &rejected, &next, present(&next), keys()).is_err());
    next.receipts = rejected.receipts.clone();
    commit(&port, &rejected, &next, present(&next), keys()).unwrap();
}
