//! Native identity, temporal nesting and probe-connection checks.
use super::{COUNT, Record, io::IoKind};
use std::{collections::BTreeMap, sync::atomic::Ordering};
#[derive(Default)]
struct Joined<'a> {
    worker: Option<&'a Record>,
    caller: Option<&'a Record>,
    receipt: Option<&'a Record>,
}
impl Joined<'_> {
    fn check(&self) {
        let (
            Some(Record::Worker {
                total,
                body,
                post,
                bridge,
                commit,
                ..
            }),
            Some(Record::Caller { validation, .. }),
            Some(Record::Receipt { at_ns, .. }),
        ) = (self.worker, self.caller, self.receipt)
        else {
            panic!("one worker/caller/receipt per original operation required")
        };
        let order = [
            total.start,
            body.start,
            bridge.start,
            validation.start,
            validation.end,
            bridge.end,
            body.end,
            post.start,
            commit.total.start,
            commit.total.end,
            post.end,
            total.end,
            *at_ns,
        ];
        assert!(
            order.windows(2).all(|pair| pair[0] <= pair[1]),
            "original causal order"
        );
        assert_eq!(commit.polls, commit.pending + 1);
        for event in &commit.io {
            assert!(event.span.start >= commit.total.start);
            assert!(event.span.end <= commit.total.end);
            assert!(event.span.start <= event.span.end);
            assert!(
                commit
                    .windows
                    .iter()
                    .all(|window| event.span.end <= window.pending_start
                        || event.span.start >= window.next_poll),
                "synchronous IO belongs inside original polls, not Pending gaps"
            );
        }
        for detail in &commit.wake_details {
            assert!(detail.start_ns <= detail.end_ns);
            assert!(commit.wakes.contains(&detail.start_ns));
        }
        assert!(!commit.window_overflow && !commit.wake_overflow);
        assert_eq!(u64::try_from(commit.windows.len()).unwrap(), commit.pending);
        assert_eq!(
            commit
                .windows
                .iter()
                .map(|window| window.next_poll - window.pending_start)
                .sum::<u64>(),
            commit.pending_gap_ns
        );
    }
}
/// # Panics
/// Rejects missing, duplicated or causally inconsistent original-operation phases.
pub fn validate_records(records: &[Record], expected: usize) {
    assert_eq!(records.len(), expected * 3);
    assert_eq!(records.len(), COUNT.load(Ordering::Relaxed));
    let mut joined = BTreeMap::<(&str, &str), Joined<'_>>::new();
    for record in records {
        let (scope, operation) = match record {
            Record::Worker {
                scope, operation, ..
            }
            | Record::Caller {
                scope, operation, ..
            }
            | Record::Receipt {
                scope, operation, ..
            } => (&**scope, &**operation),
        };
        let original = joined.entry((scope, operation)).or_default();
        let slot = match record {
            Record::Worker { .. } => &mut original.worker,
            Record::Caller { .. } => &mut original.caller,
            Record::Receipt { .. } => &mut original.receipt,
        };
        assert!(
            slot.replace(record).is_none(),
            "duplicate original phase record"
        );
    }
    assert_eq!(joined.len(), expected);
    for original in joined.values() {
        original.check();
    }
}

/// # Panics
/// Refuses a disconnected IO probe or observations from a disabled wrapper.
pub fn validate_io(records: &[Record], enabled: bool) {
    let io: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            Record::Worker { commit, .. } => Some(&commit.io),
            _ => None,
        })
        .flatten()
        .collect();
    if enabled {
        assert!(
            io.iter().any(|event| matches!(event.kind, IoKind::Sync)),
            "FULL has actual delegated sync evidence"
        );
    } else {
        assert!(io.is_empty(), "disabled wrapper supplies no IO evidence");
    }
}
