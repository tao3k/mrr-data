use std::sync::{Arc, Barrier, Mutex};

use crate::{
    CacheAdmission, ConditionalCommitDisposition as Disposition, ConditionalCommitError as Error,
    ConditionalContentReceipt as Receipt, ConditionalContentWrite as Write, ContentBlock,
    ContentCodec, ContentRevision, PublishReceipt,
};

fn revision(number: u64, bytes: &[u8]) -> ContentRevision {
    ContentRevision {
        revision: number,
        root: ContentBlock::new(ContentCodec::Raw, bytes).cid(),
    }
}

fn write(operation_id: &'static str) -> Write<'static> {
    Write {
        scope: "shared-root",
        operation_id,
        expected: Some(revision(1, b"initial")),
        replacement: revision(2, operation_id.as_bytes()).root,
    }
}

fn ack(request: Write<'_>) -> PublishReceipt {
    PublishReceipt {
        cid: request.replacement,
        cache: CacheAdmission::Stored,
    }
}

#[test]
fn absence_creation_and_publication_ack_are_distinct() {
    let request = Write {
        expected: None,
        ..write("create")
    };
    assert_eq!(
        request.decide_commit(None, None, None),
        Err(Error::MissingPublication)
    );
    assert_eq!(
        request.decide_commit(None, Some(&ack(request)), None),
        Ok(Disposition::Apply(ContentRevision {
            revision: 1,
            root: request.replacement
        }))
    );
    assert_eq!(
        request.decide_commit(Some(revision(1, b"existing")), Some(&ack(request)), None),
        Err(Error::RevisionConflict)
    );
    assert_eq!(
        request.decide_commit(None, Some(&ack(write("different"))), None),
        Err(Error::DifferentPublication)
    );
}

#[test]
fn exact_operation_replay_survives_later_head_changes_but_cannot_mutate() {
    let request = write("first");
    let receipt = Receipt {
        write: request,
        committed: ContentRevision {
            revision: 2,
            root: request.replacement,
        },
    };
    assert_eq!(
        request.decide_commit(Some(revision(5, b"later")), None, Some(&receipt)),
        Ok(Disposition::Replay)
    );
    for altered in [
        Write {
            scope: "different-scope",
            ..request
        },
        Write {
            operation_id: "different-operation",
            ..request
        },
        Write {
            expected: Some(revision(2, b"initial")),
            ..request
        },
        Write {
            replacement: revision(2, b"substitution").root,
            ..request
        },
    ] {
        assert_eq!(
            altered.decide_commit(None, None, Some(&receipt)),
            Err(Error::OperationConflict)
        );
    }
    for committed in [revision(3, b"first"), revision(2, b"other")] {
        assert_eq!(
            request.decide_commit(
                None,
                None,
                Some(&Receipt {
                    committed,
                    ..receipt
                })
            ),
            Err(Error::InvalidReceipt)
        );
    }
}

#[test]
fn returning_to_old_content_does_not_reopen_old_revision() {
    let request = write("next");
    assert_eq!(
        request.decide_commit(Some(revision(3, b"initial")), Some(&ack(request)), None),
        Err(Error::RevisionConflict)
    );
    assert_eq!(
        request.decide_commit(Some(revision(1, b"different")), Some(&ack(request)), None),
        Err(Error::RevisionConflict)
    );
}

#[test]
fn revision_and_operation_validation_fail_before_apply() {
    for (request, error) in [
        (
            Write {
                scope: "",
                ..write("first")
            },
            Error::EmptyScope,
        ),
        (
            Write {
                operation_id: "",
                ..write("first")
            },
            Error::EmptyOperation,
        ),
        (
            Write {
                expected: Some(revision(0, b"initial")),
                ..write("first")
            },
            Error::InvalidRevision,
        ),
        (
            Write {
                expected: Some(revision(u64::MAX, b"initial")),
                ..write("first")
            },
            Error::RevisionExhausted,
        ),
    ] {
        assert_eq!(
            request.decide_commit(request.expected, Some(&ack(request)), None),
            Err(error)
        );
    }
}

// A test-only serialized Host transaction. This exercises the protocol's
// linearization obligation and supplies no DB, crash or durability evidence.
type Ledger = Mutex<(Option<ContentRevision>, Vec<Receipt<'static>>)>;

fn apply(ledger: &Ledger, request: Write<'static>) -> Result<Disposition, Error> {
    let mut state = ledger.lock().unwrap();
    let existing = state
        .1
        .iter()
        .find(|row| row.write.operation_id == request.operation_id);
    let disposition = request.decide_commit(state.0, Some(&ack(request)), existing)?;
    if let Disposition::Apply(next) = disposition {
        state.0 = Some(next);
        state.1.push(Receipt {
            write: request,
            committed: next,
        });
    }
    Ok(disposition)
}

#[test]
fn racing_writers_require_one_shared_atomic_comparison() {
    let ledger = Arc::new(Mutex::new((Some(revision(1, b"initial")), Vec::new())));
    let barrier = Arc::new(Barrier::new(2));
    let writers: Vec<_> = ["child-a", "child-b"]
        .into_iter()
        .map(|operation| {
            let ledger = Arc::clone(&ledger);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                apply(&ledger, write(operation))
            })
        })
        .collect();
    let outcomes: Vec<_> = writers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(Disposition::Apply(_))))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == Err(Error::RevisionConflict))
            .count(),
        1
    );
    let committed = ledger.lock().unwrap().1[0];
    // A lost response is recovered by the same operation receipt, without a
    // second write. Reusing its operation ID with altered bytes is rejected.
    assert_eq!(apply(&ledger, committed.write), Ok(Disposition::Replay));
    assert_eq!(
        apply(
            &ledger,
            Write {
                replacement: revision(3, b"changed").root,
                ..committed.write
            }
        ),
        Err(Error::OperationConflict)
    );
    assert_eq!(ledger.lock().unwrap().1.len(), 1);
}
