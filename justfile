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

[group('bench')]
bench-backend:
    {{ cargo }} test -p mrr-data-backend --no-default-features --features turso --test backend_performance --locked -- --ignored --nocapture --test-threads=1
    {{ cargo }} test -p mrr-data-backend --no-default-features --features duckdb --test backend_performance --locked -- --ignored --nocapture --test-threads=1

[group('bench')]
bench-backend-mixed:
    {{ cargo }} test -p mrr-data-backend --no-default-features --features turso --test backend_mixed_performance --locked -- --ignored --nocapture --test-threads=1
    {{ cargo }} test -p mrr-data-backend --no-default-features --features duckdb --test backend_mixed_performance --locked -- --ignored --nocapture --test-threads=1

[group('bench')]
bench-backend-sustained:
    {{ cargo }} test -p mrr-data-backend --no-default-features --features turso --test backend_sustained_performance --locked -- --ignored --nocapture --test-threads=1
    {{ cargo }} test -p mrr-data-backend --no-default-features --features duckdb --test backend_sustained_performance --locked -- --ignored --nocapture --test-threads=1

[group('test')]
test-property-query:
    {{ native_env }} {{ cargo }} test -p mrr-data-content --features property-snapshot --locked
    {{ native_env }} {{ cargo }} test -p mrr-data-datafusion --locked

[group('test')]
clippy-property-query:
    {{ native_env }} {{ cargo }} clippy -p mrr-data-content --features property-snapshot --all-targets --locked -- -D warnings
    {{ native_env }} {{ cargo }} clippy -p mrr-data-datafusion --all-targets --locked -- -D warnings

[group('test')]
test-backend:
    {{ cargo }} test -p mrr-data-backend --no-default-features --locked
    {{ cargo }} test -p mrr-data-backend --features turso --locked -- --nocapture
    {{ cargo }} test -p mrr-data-backend --features duckdb --locked -- --nocapture
    {{ native_env }} {{ cargo }} test -p mrr-data-commerce --all-features --test commerce_commit_consumer shared_backend_persists --locked -- --nocapture

[group('test')]
clippy-backend:
    {{ cargo }} clippy -p mrr-data-backend --all-features --all-targets --profile test --locked -- -D warnings

[group('test')]
test-backend-query:
    {{ cargo }} test -p mrr-data-backend --no-default-features --features arrow-query --locked
    {{ cargo }} test -p mrr-data-backend --no-default-features --features turso,arrow-query,graph-publish --locked -- --nocapture
    {{ cargo }} test -p mrr-data-backend --no-default-features --features duckdb,graph-publish --locked -- --nocapture
