# M4 closure audit — 2026-09-20

Scope: the local S3/Kache distribution slice, including snapshot preflight and
root-last acknowledgement, per-block integrity, resource/retry ledgers,
cancellation and blocking-worker ownership, provider feature isolation, and
independent SigV4/TLS evidence. Kache continues to own cache mechanisms; semantic
admission and native GraphAr qualification are outside this audit.

Two findings were corrected:

| Finding | Correction | Executable evidence |
|---|---|---|
| The public single-file example read an unbounded file and used synchronous Kache operations on its async executor | Require optional `blocking`, cap input at 64 MiB before and during reading, perform file/cache operations through blocking execution | Run the example over trusted local TLS; verify exact round trip and rejection of a 64 MiB + 1 sparse input |
| The conformance receipt identified only the server build and could retain stale success after a failed rerun | Record workspace source fingerprint/compiler/platform; invalidate source changes during acceptance; remove old output before building | Two offline receipt-contract tests, followed by a fresh successful source-bound conformance run |

The independent runner also executes the snapshot example through trusted TLS,
using isolated disposable credentials/configuration. This verifies the documented
entry points as well as the Rust conformance test. No production format, default
feature, retry policy or upstream cache mechanism changed in this audit.

Final local evidence (macOS ARM64, workspace Rust 1.95.0):

- Workspace regression: 123 passed; 3 ordinary ignored entries comprise two
  subprocess helpers invoked by parent tests and one separately executed
  independent S3 conformance test.
- Independent SigV4/TLS conformance: passed; both public examples passed;
  oversized-file rejection passed.
- Receipt freshness contracts: 2 passed.
- Feature isolation: 18 cases passed.
- Full-feature/all-target Clippy with warnings denied, Rust formatting and diff
  whitespace checks: passed.

[Source-bound conformance receipt](s3-conformance-receipt.json) records the
workspace code fingerprint and exact upstream test server revision/build hash.
The fingerprint includes tracked and untracked `.rs`, `.toml`, `.lock`, `.py` and
`.yml` files reported by Git, excluding ignored build outputs. It is a source
identity check, not a reproducible-build attestation or a hash of documentation.

No unresolved finding remains within this inspected local scope. The dedicated
Ubuntu/macOS CI job is configured but remote results have not been observed.
Hosted B2/R2, native server TLS/mTLS and native GraphAr cross-platform acceptance
are not established by these results.


CI follow-through: added manual dispatch, a strict `M4 acceptance` aggregate,
and per-platform source-bound receipt summaries/artifacts (14-day retention).
Actionlint 1.7.12 passed (without optional ShellCheck); a local exercise of the
aggregate accepted success and rejected failure, cancellation and skipping.
The exact S3 job command, both examples and receipt contracts passed again after
the workflow change. The updated JSON receipt matches that workflow source.
GitHub-hosted execution and branch-protection configuration remain unverified.
