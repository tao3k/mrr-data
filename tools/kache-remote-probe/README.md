# Kache remote consumer verification

This is executable evidence for an **unpublished upstream library extraction**.
The patch moves Kache's existing transport and entry format into `kache-remote`;
it does not copy a cache implementation into MRR Data. The root Kache binary
imports the extracted implementation.

From the MRR Data root, with Git, Python 3 and Cargo on PATH:

```sh
python3 tools/kache-remote-probe/run.py
```

The script clones upstream revision
`7461ca763506c081d6ceb95ea225c04ce2d24df6`, applies `kache-remote.patch`, copies
the current MRR Data root workspace to a temporary directory, and adds the
unpublished dependency to that copy's `[workspace.dependencies]`. All `mrr-*`
dependencies continue to use `workspace = true`. No independent Cargo workspace
is introduced into this repository, and no `/tmp` dependency is left in the
real workspace manifest or lockfile.

For work on an already-patched upstream checkout:

```sh
python3 tools/kache-remote-probe/run.py --kache /absolute/path/to/kache
```

`--output` selects a new directory outside the source workspace. `test.log` and
`clippy.log` are saved there; the temporary checkout and resolved lockfile are
retained for inspection. `CARGO_TARGET_DIR` can reuse an existing build cache.
The tests require permission to bind loopback sockets. Rust 1.95.0 is used to
check the consumer's MSRV; the upstream application's toolchain pin remains
unchanged.

## What runs

The existing six local-cache contracts and ASP package gate run along with four
new consumer tests:

1. Filesystem remote: cache A uploads; empty cache B restores through Kache's
   actual OpenDAL backend, v3 pack decoder and store registration API.
2. S3 wire: Kache's actual signed HTTP client uploads a CAR containing an MRR
   snapshot, then cache B downloads it and MRR re-admits the CAR closure and
   original root. Warm reads and reopen generate no additional GET.
3. Publication failure: rejecting the manifest PUT makes upload fail; the
   consumer does not fetch the orphan pack. An explicit retry succeeds.
4. Wrong content: substituting another valid pack is rejected by the expected
   MRR CID before the target cache registers an entry.

The local HTTP fixture accepts test credentials and checks for a SigV4 header.
It does **not** validate the signature or emulate all S3 operations. It is not
B2/R2 acceptance. It uses no hosted bucket and provisions no credentials.

The probe's coordination helpers are test code. They hold Kache's key/GC locks,
wait for upstream upload acknowledgment, check manifest presence before fetch,
and verify the expected CID before registration. Whole-block reads, a busy-key
error, and existing upstream pack limits are deliberately visible; this is not
yet a bounded, async production adapter or a complete single-flight scheduler.

## Validated extraction boundary

The library exposes `RemoteLayout` entry upload/download, the existing
`RemoteBackend` constructors, remote configuration and deadline types. The
application retains its `CacheRemote` orchestration, compiler policy, prefetch
planning and daemon scheduling. Shared validators come from `kache-format` and
store metadata from `kache-store`.

The patch preserves the existing v3 format and moves its tests. Test fixtures
now use `kache-store::Config` and a test artifact policy instead of the entire
compiler configuration. A test-support feature serves existing binary tests.

## Evidence from this run

- Saved patch applies cleanly to the pinned revision.
- Extracted library: 71 tests passed.
- Kache application: `cargo check -p kache --all-targets --locked` passed;
  affected `remote_` tests: 145 passed, one helper ignored, 2646 filtered out.
- MRR consumer from a clean checkout with the saved patch: 11 passed, one
  subprocess helper ignored (invoked by the lock contract), 0.28 s test time.
- Library and consumer scoped Clippy with `--no-deps -- -D warnings`: passed.
- Actual MRR root workspace after the Tokio update: `cargo test --workspace
  --locked --offline` passed, 72 tests and one subprocess helper ignored.
  Workspace formatting, remote-fixture formatting and diff checks passed.

Dependency-inclusive Clippy under Rust 1.95 initially found a pre-existing
`collapsible_if` warning in `kache-store/src/markers.rs`. Scoped checks exclude
dependency linting; no full-upstream lint or `just check` result is claimed.
Linux, hosted S3 and the upstream mutation gate remain unverified. No upstream
PR or published revision was created.

The next production dependency must reference an available upstream/fork
revision containing this extraction. Do not pin the current upstream revision
as if it already contains `kache-remote`, or turn the temporary checkout into a
permanent product dependency.
