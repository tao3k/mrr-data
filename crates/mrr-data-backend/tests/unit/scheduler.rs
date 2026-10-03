//! Saturation accounting must not consume admission or hide lifecycle refusal.
use crate::{BackendConfig, BackendError, scheduler::Scheduler};

#[test]
fn refusal_counters_distinguish_lanes_and_leave_reserved_recovery_available() {
    let scheduler = Scheduler::new(BackendConfig {
        max_writes: 1,
        max_recoveries: 1,
        ..BackendConfig::default()
    });
    let write = scheduler.admit(false, 1024).unwrap();
    assert!(matches!(
        scheduler.admit(false, 1),
        Err(BackendError::Saturated)
    ));
    let recovery = scheduler.admit(true, 65536).unwrap();
    assert!(matches!(
        scheduler.admit(true, 65536),
        Err(BackendError::Saturated)
    ));
    let status = scheduler.status();
    assert_eq!(
        (status.saturated_writes, status.saturated_recoveries),
        (1, 1)
    );
    assert_eq!((status.active_writes, status.active_recoveries), (1, 1));
    assert_eq!(status.retained_bytes, 1024);
    assert_eq!(status.completed, 0);
    scheduler.drain();
    assert!(matches!(
        scheduler.admit(false, 1),
        Err(BackendError::NotReady)
    ));
    assert_eq!(scheduler.status().saturated_writes, 1);
    drop(write);
    drop(recovery);
    assert_eq!(scheduler.status().completed, 2);
}

#[test]
fn byte_refusal_counts_without_retaining_the_rejected_request() {
    let scheduler = Scheduler::new(BackendConfig::default());
    assert!(matches!(
        scheduler.admit(false, BackendConfig::default().max_retained_bytes + 1),
        Err(BackendError::Saturated)
    ));
    assert_eq!(scheduler.status().saturated_writes, 1);
    assert_eq!(scheduler.status().active_writes, 0);
    assert_eq!(scheduler.status().retained_bytes, 0);
    assert_eq!(scheduler.status().completed, 0);
    let lease = scheduler.admit(false, 1024).unwrap();
    drop(lease);
    assert_eq!(scheduler.status().completed, 1);
}
