//! Shared `MRR Data` workspace policy dependency for ASP Rust build evidence.
#![forbid(unsafe_code)]

#[doc(hidden)]
pub use asp_rust;
pub use asp_rust::{AspRustConfig, AspRustWorkspacePolicy, default_asp_rust_config};
#[doc(hidden)]
pub use asp_rust_build_support;

mod dev_gate;

pub use dev_gate::{mrr_data_member_policy_config, mrr_data_workspace_policy};
