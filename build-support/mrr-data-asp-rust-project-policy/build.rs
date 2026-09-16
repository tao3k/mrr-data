use asp_rust::assert_asp_rust_workspace_policy_from_env;

fn main() {
    asp_rust_build_support::emit_provider_contract_digest();
    let config = asp_rust::default_asp_rust_config();
    let policy = asp_rust::AspRustWorkspacePolicy::new("mrr-data", config);
    assert_asp_rust_workspace_policy_from_env(&policy);
}
