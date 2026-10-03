//! MRR conditional-commit adapter for admitted commerce claims.
pub mod budget_commit;
#[cfg(feature = "consumption")]
pub mod consumption;
#[cfg(feature = "credential")]
pub mod credential;
#[cfg(feature = "consumption")]
pub mod provider;

#[cfg(test)]
#[path = "../tests/unit/asp_rust_gate.rs"]
mod asp_rust_gate;
