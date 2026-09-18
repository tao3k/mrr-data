//! Shared MRR Data configuration for ASP Rust test-only policy gates.

use asp_rust::{AspRustConfig, AspRustWorkspacePolicy, default_asp_rust_config};

const ADVICE_ALLOW_EXPLANATION: &str = "scope=mrr-data workspace; owner=mrr-data-asp-rust-build-support; finding_category=agent_advice; why_safe_now=MRR Data keeps advisory findings visible while blocking policy violations; cleanup_trigger=remove when every advisory finding is closed";

/// Return the shared configuration used by package-scoped Dev Gates.
#[must_use]
pub fn mrr_data_member_policy_config() -> AspRustConfig {
    default_asp_rust_config().with_cargo_test_advice_allow_explanation(ADVICE_ALLOW_EXPLANATION)
}

/// Return the single workspace policy used by the facade's workspace gate.
#[must_use]
pub fn mrr_data_workspace_policy() -> AspRustWorkspacePolicy {
    AspRustWorkspacePolicy::new("mrr-data", mrr_data_member_policy_config())
}

/// Mount the shared MRR Data package policy in an existing unit test suite.
#[macro_export]
macro_rules! mrr_data_asp_rust_member_dev_gate {
    () => {
        $crate::asp_rust::asp_rust_cargo_test_gate!(
            advice = allow,
            config = $crate::mrr_data_member_policy_config()
        );
    };
}

/// Mount the MRR Data workspace policy once in the facade's unit test suite.
#[macro_export]
macro_rules! mrr_data_asp_rust_workspace_dev_gate {
    () => {
        $crate::asp_rust::asp_rust_workspace_dev_gate!(
            mode = deny,
            policy = $crate::mrr_data_workspace_policy()
        );
    };
}
