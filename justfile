set shell := ["bash", "-euo", "pipefail", "-c"]

cargo := env_var_or_default("CARGO", "cargo")
native_env := if os() == "macos" { "env -u SDKROOT -u DEVELOPER_DIR -u CPATH -u LIBRARY_PATH -u C_INCLUDE_PATH -u CPLUS_INCLUDE_PATH -u DYLD_LIBRARY_PATH -u DYLD_FALLBACK_LIBRARY_PATH CC=/usr/bin/clang CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER=/usr/bin/clang MACOSX_DEPLOYMENT_TARGET=$(sw_vers -productVersion | cut -d. -f1).0" } else { "env" }

[group('check')]
format:
    {{ cargo }} fmt --all

[group('test')]
test-property-query:
    #!/usr/bin/env bash
    set -euo pipefail
    gerbil_root="$(gerbil -e '(display (path-expand "~~"))')"
    export GAMBOPT="~~=$gerbil_root,~~bin=$gerbil_root/bin,~~lib=$gerbil_root/lib"
    export GERBIL_GSC="$gerbil_root/bin/gsc"
    export GERBIL_GXPKG="$gerbil_root/bin/gxpkg"
    {{ native_env }} {{ cargo }} test -p mrr-data-poo-flow --features property-query --locked

[group('test')]
clippy-property-query:
    #!/usr/bin/env bash
    set -euo pipefail
    gerbil_root="$(gerbil -e '(display (path-expand "~~"))')"
    export GAMBOPT="~~=$gerbil_root,~~bin=$gerbil_root/bin,~~lib=$gerbil_root/lib"
    export GERBIL_GSC="$gerbil_root/bin/gsc"
    export GERBIL_GXPKG="$gerbil_root/bin/gxpkg"
    {{ native_env }} {{ cargo }} clippy -p mrr-data-poo-flow --features property-query --all-targets --locked -- -D warnings
