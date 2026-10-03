//! Reject invalid worker bounds before creating semaphores or opening storage.
use crate::BackendConfig;

#[test]
fn blocking_limits_refuse_zero_over_admission_and_semaphore_overflow() {
    for config in [
        BackendConfig {
            max_resources: 0,
            ..BackendConfig::default()
        },
        BackendConfig {
            max_resource_workers: 0,
            ..BackendConfig::default()
        },
        BackendConfig {
            max_resource_workers: 9,
            ..BackendConfig::default()
        },
        BackendConfig {
            max_resource_bytes: 0,
            ..BackendConfig::default()
        },
        BackendConfig {
            max_resources: usize::MAX,
            max_resource_workers: tokio::sync::Semaphore::MAX_PERMITS + 1,
            ..BackendConfig::default()
        },
        BackendConfig {
            max_write_workers: 0,
            ..BackendConfig::default()
        },
        BackendConfig {
            max_recovery_workers: 0,
            ..BackendConfig::default()
        },
        BackendConfig {
            max_write_workers: 33,
            ..BackendConfig::default()
        },
        BackendConfig {
            max_recovery_workers: 9,
            ..BackendConfig::default()
        },
        BackendConfig {
            max_writes: usize::MAX,
            max_write_workers: tokio::sync::Semaphore::MAX_PERMITS + 1,
            ..BackendConfig::default()
        },
    ] {
        assert!(config.validate().is_err());
    }
    assert!(
        BackendConfig {
            max_write_workers: 2,
            max_recovery_workers: 2,
            ..BackendConfig::default()
        }
        .validate()
        .is_ok()
    );
}
