//! Opt-in external snapshot resources. POO Flow retains execution policy.
#![forbid(unsafe_code)]
#[cfg(feature = "runtime")]
mod outbox;
#[cfg(feature = "runtime")]
mod protocol;
#[cfg(feature = "runtime")]
mod runtime;
#[cfg(feature = "runtime")]
mod semantic;
#[cfg(feature = "runtime")]
mod worker;
#[cfg(feature = "runtime")]
pub use runtime::{execute, run_cli, run_sync_pending, run_sync_service};
#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
