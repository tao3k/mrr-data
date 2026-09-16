fn main() {
    let config = asp_rust::default_asp_rust_config();
    let policy =
        asp_rust::AspRustWorkspacePolicy::new("mrr-data", config).member_crate("mrr-data-arrow");
    asp_rust::assert_asp_rust_downstream_policy_from_env(&policy);
}
