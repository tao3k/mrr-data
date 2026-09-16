# IPC fuzzing

The `ipc_import` target drives arbitrary bytes through the bounded Arrow IPC
boundary for a recursively nested MRR relation. Run it with the standard Cargo
fuzz frontend:

```sh
cargo fuzz run ipc_import fuzz/corpus/ipc_import
```

Crash and timeout artifacts are regression inputs. Pull-request CI compiles and
lints the target on the stable toolchain, then runs a bounded 10,000-case ASan
campaign on nightly Rust. The main crate also retains deterministic malformed
corpus tests; none of these replaces longer continuous fuzzing.
