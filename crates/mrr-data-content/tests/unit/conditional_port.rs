use std::sync::{
    Arc, Barrier, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use crate::{
    CacheAdmission, ConditionalCommitDisposition as Disposition, ConditionalCommitError,
    ConditionalCommitFuture, ConditionalCommitPortError as PortError,
    ConditionalContentCommitOutcome as Outcome, ConditionalContentCommitPort as Port,
    ConditionalContentReceipt as Receipt, ConditionalContentWrite as Write, ContentBlock,
    ContentCodec, ContentRevision, PublishReceipt,
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

#[derive(Default)]
struct State {
    head: Option<ContentRevision>,
    rows: Vec<Row>,
    lose_ack: bool,
    unavailable: bool,
}

// Test-only transaction provider: no DB or crash/durability acceptance is claimed.
#[derive(Default)]
struct TestPort(Mutex<State>);

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
            let disposition = write
                .decide_commit(state.head, physical, existing.as_ref())
                .map_err(PortError::Protocol)?;
            if disposition == Disposition::Replay {
                let receipt = write
                    .recover_receipt(existing.as_ref())
                    .map_err(PortError::Protocol)?
                    .unwrap();
                return Ok(Outcome::Replayed(receipt));
            }
            validate(state.head).map_err(PortError::Validation)?;
            let Disposition::Apply(next) = disposition else {
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

fn write(operation: &'static str) -> Write<'static> {
    Write {
        scope: "shared-root",
        operation_id: operation,
        expected: None,
        replacement: ContentBlock::new(ContentCodec::Raw, operation.as_bytes()).cid(),
    }
}
fn ack(write: Write<'_>) -> PublishReceipt {
    PublishReceipt {
        cid: write.replacement,
        cache: CacheAdmission::Stored,
    }
}

#[tokio::test]
async fn unknown_commit_recovers_exactly_without_repeating_validation_or_write() {
    let port = TestPort::default();
    port.0.lock().unwrap().lose_ack = true;
    let request = write("first");
    let count = AtomicUsize::new(0);
    assert_eq!(
        port.commit(request, Some(&ack(request)), |_| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok::<_, &'static str>(())
        })
        .await,
        Err(PortError::Unknown("lost commit acknowledgement"))
    );
    let receipt = port.recover(request).await.unwrap().unwrap();
    assert_eq!(receipt.write, request);
    assert_eq!(receipt.committed.revision, 1);
    assert_eq!(
        port.commit(request, None, |_| Err("revoked after commit"))
            .await,
        Ok(Outcome::Replayed(receipt))
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(port.0.lock().unwrap().rows.len(), 1);
    let changed = Write {
        replacement: write("other").replacement,
        ..request
    };
    assert_eq!(
        port.recover(changed).await,
        Err(PortError::Protocol(
            ConditionalCommitError::OperationConflict
        ))
    );
    port.0.lock().unwrap().head = Some(ContentRevision {
        revision: 9,
        root: write("later").replacement,
    });
    assert_eq!(port.recover(request).await.unwrap(), Some(receipt));
}

#[tokio::test]
async fn fresh_validation_refusal_does_not_record_a_head_or_receipt() {
    let port = TestPort::default();
    let request = write("denied");
    assert_eq!(
        port.commit(request, Some(&ack(request)), |_| Err(
            "revoked before commit"
        ))
        .await,
        Err(PortError::Validation("revoked before commit"))
    );
    assert_eq!(port.recover(request).await.unwrap(), None);
    assert_eq!(port.0.lock().unwrap().head, None);
}

#[tokio::test]
async fn unavailable_ledger_is_not_authenticated_absence() {
    let port = TestPort::default();
    port.0.lock().unwrap().unavailable = true;
    assert_eq!(
        port.recover(write("missing")).await,
        Err(PortError::BeforeCommit("unavailable"))
    );
}

#[test]
fn two_port_clients_share_one_atomic_head_comparison_and_one_validation() {
    let port = Arc::new(TestPort::default());
    let barrier = Arc::new(Barrier::new(2));
    let count = Arc::new(AtomicUsize::new(0));
    let workers: Vec<_> = ["child-a", "child-b"]
        .into_iter()
        .map(|id| {
            let port = Arc::clone(&port);
            let barrier = Arc::clone(&barrier);
            let count = Arc::clone(&count);
            std::thread::spawn(move || {
                barrier.wait();
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                let request = write(id);
                let physical = ack(request);
                runtime
                    .block_on(port.commit(request, Some(&physical), move |_| {
                        count.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, &'static str>(())
                    }))
                    .map(|outcome| match outcome {
                        Outcome::Committed(receipt) => Disposition::Apply(receipt.committed),
                        Outcome::Replayed(_) => Disposition::Replay,
                    })
            })
        })
        .collect();
    let results: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Ok(Disposition::Apply(_))))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(
                r,
                Err(PortError::Protocol(
                    ConditionalCommitError::RevisionConflict
                ))
            ))
            .count(),
        1
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(port.0.lock().unwrap().rows.len(), 1);
}

#[tokio::test]
async fn recovery_validates_queries_and_receipts_before_reporting_a_status() {
    let port = TestPort::default();
    let malformed = Write {
        operation_id: "",
        ..write("first")
    };
    assert_eq!(
        port.recover(malformed).await,
        Err(PortError::Protocol(ConditionalCommitError::EmptyOperation))
    );
    let request = write("first");
    port.0.lock().unwrap().rows.push(Row {
        scope: request.scope.into(),
        operation: request.operation_id.into(),
        expected: None,
        committed: ContentRevision {
            revision: 2,
            root: request.replacement,
        },
    });
    assert_eq!(
        port.recover(request).await,
        Err(PortError::Protocol(ConditionalCommitError::InvalidReceipt))
    );
}
