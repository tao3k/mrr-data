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
}
impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "backend: {self:?}")
    }
}
impl std::error::Error for BackendError {}
/// Verified by provider qualification, not vendor naming. No self-attestation
/// authenticates arbitrary external storage or block durability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderCapabilities {
    pub atomic_head_operation: bool,
    pub durable_commit: bool,
    pub historical_lookup: bool,
}
/// Limits are checked before copying requests or spawning blocking work.
#[derive(Clone, Copy, Debug)]
pub struct BackendConfig {
    pub max_writes: usize,
    pub max_recoveries: usize,
    pub max_retained_bytes: usize,
    pub max_identity_bytes: usize,
}
impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            max_writes: 32,
            max_recoveries: 8,
            max_retained_bytes: 131_072,
            max_identity_bytes: 256,
        }
    }
}
impl BackendConfig {
    pub(crate) fn validate(self) -> Result<Self, BackendError> {
        if self.max_writes == 0
            || self.max_recoveries == 0
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
/// Current bounded resource usage and completed worker counts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackendStatus {
    pub lifecycle: Lifecycle,
    pub active_writes: usize,
    pub active_recoveries: usize,
    pub retained_bytes: usize,
    pub completed: u64,
}
