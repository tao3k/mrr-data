set shell := ["bash", "-euo", "pipefail", "-c"]

cargo := env_var_or_default("CARGO", "cargo")
native_env := if os() == "macos" { "env -u SDKROOT -u DEVELOPER_DIR -u CPATH -u LIBRARY_PATH -u C_INCLUDE_PATH -u CPLUS_INCLUDE_PATH -u DYLD_LIBRARY_PATH -u DYLD_FALLBACK_LIBRARY_PATH CC=/usr/bin/clang CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER=/usr/bin/clang MACOSX_DEPLOYMENT_TARGET=$(sw_vers -productVersion | cut -d. -f1).0" } else { "env" }

[group('check')]
format:
    {{ cargo }} fmt --all

[group('test')]
test-data-bindings:
    {{ native_env }} {{ cargo }} test -p mrr-data-core --features content-identity --lib --locked

[group('test')]
test-data-protection:
    {{ native_env }} {{ cargo }} test -p mrr-data-security --lib --locked

[group('test')]
test-pseudonymization-bridge:
    {{ native_env }} {{ cargo }} test -p mrr-data-pseudonymization --features google-sdp --lib --locked

[group('bench')]
bench-data-bindings:
    {{ native_env }} {{ cargo }} bench -p mrr-data-core --features content-identity --bench operation_binding --locked

[group('bench')]
bench-data-protection:
    {{ native_env }} {{ cargo }} bench -p mrr-data-security --bench input_binding --locked

[group('bench')]
bench-token-catalog:
    {{ native_env }} {{ cargo }} bench -p mrr-data-pseudonymization --bench token_catalog --locked

[group('bench')]
bench-google-sdp:
    {{ native_env }} {{ cargo }} bench -p mrr-data-pseudonymization --features google-sdp --bench google_sdp --locked

[group('test')]
test-property-query:
    {{ native_env }} {{ cargo }} test -p mrr-data-content --features property-snapshot --locked
    {{ native_env }} {{ cargo }} test -p mrr-data-datafusion --locked

[group('test')]
clippy-property-query:
    {{ native_env }} {{ cargo }} clippy -p mrr-data-content --features property-snapshot --all-targets --locked -- -D warnings
    {{ native_env }} {{ cargo }} clippy -p mrr-data-datafusion --all-targets --locked -- -D warnings
