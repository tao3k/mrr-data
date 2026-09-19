//! Opt-in external static-edge resource. POO Flow retains execution policy.
#![forbid(unsafe_code)]
#[cfg(feature = "runtime")]
mod protocol;
#[cfg(feature = "runtime")]
mod runtime;
#[cfg(feature = "runtime")]
mod semantic;
#[cfg(feature = "runtime")]
mod worker;
#[cfg(feature = "runtime")]
pub use runtime::{execute, run_cli};
#[cfg(test)]
#[path = "../tests/unit/mod.rs"]
mod tests;
