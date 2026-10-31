//! Shared admission and capability configuration.
/// Sanitized failures; provider responses and credentials never appear here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendError {
    InvalidConfiguration,
    UnsupportedCapabilities,
    NotReady,
    Saturated,
    Limit,
    Unavailable,
    Corrupt,
    Cancelled,
    WorkerLost,
    AuthorityConflict,
    AuthorityRetired,
}
impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "backend: {self:?}")
    }
}
impl std::error::Error for BackendError {}
/// Whether authority generations participate in the protected transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorityCapability {
    Unsupported,
    Transactional,
}
/// Verified by provider qualification, not vendor naming. No self-attestation
/// authenticates arbitrary external storage or block durability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderCapabilities {
    pub atomic_head_operation: bool,
    pub durable_commit: bool,
    pub historical_lookup: bool,
    pub authority_versions: AuthorityCapability,
}
/// Limits are checked before copying requests or spawning blocking work.
#[derive(Clone, Copy, Debug)]
pub struct BackendConfig {
    pub max_writes: usize,
    pub max_recoveries: usize,
    /// Submitted blocking jobs, including those waiting in the Host pool.
    pub max_write_workers: usize,
    pub max_recovery_workers: usize,
    /// Combined submitted writes, preparations and queries. Recovery is separate.
    /// Host blocking capacity must include this budget plus recovery workers.
    pub max_shared_workers: usize,
    pub max_retained_bytes: usize,
    pub max_identity_bytes: usize,
    /// Accepted preparation jobs plus retained resource handles.
    pub max_resources: usize,
    pub max_resource_workers: usize,
    /// Host-declared reservations; this is not a native RSS measurement.
    pub max_resource_bytes: usize,
}
impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            max_writes: 32,
            max_recoveries: 8,
            max_write_workers: 1,
            max_recovery_workers: 1,
            max_shared_workers: 1,
            max_retained_bytes: 131_072,
            max_identity_bytes: 256,
            max_resources: 8,
            max_resource_workers: 1,
            max_resource_bytes: 1_073_741_824,
        }
    }
}
impl BackendConfig {
    pub(crate) fn validate(self) -> Result<Self, BackendError> {
        if self.max_shared_workers == 0
            || self.max_shared_workers > tokio::sync::Semaphore::MAX_PERMITS
            || self.max_resources == 0
            || self.max_resource_workers == 0
            || self.max_resource_workers > self.max_resources
            || self.max_resource_workers > tokio::sync::Semaphore::MAX_PERMITS
            || self.max_resource_bytes == 0
            || self.max_writes == 0
            || self.max_recoveries == 0
            || self.max_write_workers == 0
            || self.max_recovery_workers == 0
            || self.max_write_workers > self.max_writes
            || self.max_recovery_workers > self.max_recoveries
            || self.max_write_workers > tokio::sync::Semaphore::MAX_PERMITS
            || self.max_recovery_workers > tokio::sync::Semaphore::MAX_PERMITS
            || self.max_retained_bytes < 1024
            || !(1..=256).contains(&self.max_identity_bytes)
        {
            return Err(BackendError::InvalidConfiguration);
        }
        Ok(self)
    }
}
/// Opening/recovery occur before a Backend handle is exposed. Faults never reset
/// a pending operation; a caller must query the same exact operation on reopen.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Lifecycle {
    Ready,
    Draining,
    Closing,
    Closed,
    Faulted,
}
/// Accepted work includes asynchronous queueing. Blocking counts include jobs
/// queued in the Host executor as well as running jobs; they are lane-bounded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackendStatus {
    pub lifecycle: Lifecycle,
    pub active_resources: usize,
    pub blocking_resources: usize,
    pub resource_bytes: usize,
    pub saturated_resources: u64,
    pub active_writes: usize,
    pub active_recoveries: usize,
    pub blocking_writes: usize,
    pub blocking_recoveries: usize,
    pub retained_bytes: usize,
    pub completed: u64,
    /// Lifetime admission refusals caused by write count or retained-byte limits.
    /// Includes administrative writes; excludes validation and lifecycle refusals.
    pub saturated_writes: u64,
    /// Lifetime admission refusals caused by recovery lane limits.
    pub saturated_recoveries: u64,
}

#[cfg(test)]
#[path = "../tests/unit/config.rs"]
mod tests;
