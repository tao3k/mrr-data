# Independent local S3/TLS acceptance

Run from the workspace with `git`, `cargo`/`rustup`, Python 3, OpenSSL and a curl
build supporting `--aws-sigv4`:

```sh
rustup toolchain install 1.96.0 --profile minimal
RUSTUP_TOOLCHAIN=1.95.0 python3 tools/s3-conformance/run.py --receipt /tmp/mrr-s3-receipt.json
```

The runner builds upstream [s3s-fs](https://github.com/s3s-project/s3s/tree/8ac644246b5a11664eaed6cbb3a36b3874047586)
at commit `8ac644246b5a11664eaed6cbb3a36b3874047586` with its locked dependencies.
That test service requires Rust 1.96; the production workspace remains Rust 1.95.
No service dependency is added to the workspace Cargo graph. The optional
`--cache-dir` retains the upstream checkout/build between runs. The runner checks
its revision and tracked-file cleanliness before building. The receipt records
the commit, built binary SHA-256, workspace compiler/platform and a workspace
source fingerprint (tracked and untracked Rust/Python/configuration files).
Source changes during the run invalidate the receipt, and a failed rerun removes
the previous output receipt instead of leaving stale success evidence.

Everything binds loopback. The script creates an ephemeral CA and a server leaf
valid only for `127.0.0.1`, starts s3s with disposable test credentials, and runs a
Python standard-library TLS byte relay to it. The relay does not parse HTTP or
S3 and does not implement signing: upstream s3s validates the untouched signed
requests. curl's SigV4 implementation provisions the bucket. The production
OpenDAL/reqwest adapter performs the actual conformance operations.

| Contract | Assertion |
|---|---|
| Correct credentials and trusted CA | PUT and GET return identical bytes |
| Incorrect access key and incorrect secret | Both GET and PUT return PermissionDenied |
| Untrusted CA | GET fails; no false missing-object result |
| Trusted CA, mismatched hostname | GET fails; verification remains enabled |
| Namespace roots | An object under tenant/a is missing under tenant/b |
| Conditional creation | Existing object rejects a different If-None-Match write |
| Idempotent publication | Repeated identical adapter PUT succeeds |
| External corruption | Adapter PUT fails and leaves poisoned bytes unchanged |
| Read budget | Oversized response returns TooLarge |
| Snapshot | Genuine Arrow facts survive publication and cold Kache restore |

The runner also executes both public S3 examples against the same TLS service
and checks that the single-file example rejects input larger than 64 MiB.
`python3 tools/s3-conformance/test_runner.py` verifies source fingerprinting and
stale-receipt removal without network access.

The Rust test is ignored during ordinary workspace tests and explicitly required
by this runner; missing configuration or selecting zero tests is a failure. CI
has a dedicated Ubuntu/macOS job for it. The fault-injection HTTP fixture remains
separate and intentionally does not validate signatures.

Processes are terminated/reaped and temporary credentials, certificates and
objects are removed even on test failures. No trust is installed into the system,
no hosted bucket is touched, and no cloud credentials are needed. This qualifies
one independent local S3 service behind a TLS terminator; it does not establish
B2/R2-specific behavior, every AWS S3 API, native server TLS, or mTLS.


## CI gate and receipts

The CI workflow runs on pull requests, pushes to main and manual dispatch.
`S3 SigV4 and TLS` runs on Ubuntu and macOS. Each successful job writes its
source-bound receipt to the job summary and retains the JSON as an Actions
artifact for 14 days; a missing receipt fails the upload step. Only the receipt
is retained, not temporary credentials, CA keys or object data.

`M4 acceptance` aggregates the MSRV, workspace/feature and independent S3 jobs.
It runs even when a prerequisite fails and rejects failed, cancelled or skipped
prerequisites. This is the stable check name to select if branch protection
requires the M4 gate; this change does not modify repository protection rules.
Existing fuzz and native GraphAr jobs remain separate required work for their
respective scopes. A local pass does not establish that a GitHub run passed.
