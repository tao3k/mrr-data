#!/usr/bin/env python3
"""Compare Rust conformance fixtures with the exact pinned SPEC Git tree.

Use --spec-root to read a local checkout at the pinned commit. In CI, the
script fetches that commit into a temporary bare-sized repository instead.
"""

import argparse
import re
import subprocess
import tempfile
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "crates/mrr-data-pseudonymization/Cargo.toml"
LOCK = ROOT / "Cargo.lock"
FIXTURES = {
    "commerce-acceptance-v1.json": ROOT / "crates/mrr-data-commerce/tests/fixtures/commerce-acceptance-v1.json",
    "commerce-projection-v1.json": ROOT / "crates/mrr-data-commerce/tests/fixtures/commerce-projection-v1.json",
    "storage-effect-v1.json": ROOT / "crates/mrr-data-security/fixtures/storage-effect-v1.json",
    "storage-profiles-v1.json": ROOT / "crates/mrr-data-security/fixtures/storage-profiles-v1.json",
    "protected-storage-v1.json": ROOT / "crates/mrr-data-security/fixtures/protected-storage-v1.json",
    "google-table-batch-v1.json": ROOT / "crates/mrr-data-pseudonymization/tests/fixtures/google-table-batch-v1.json",
}


def command(*args: str, cwd: Path | None = None) -> bytes:
    return subprocess.check_output(args, cwd=cwd, stderr=subprocess.PIPE)


def pinned_spec() -> tuple[str, str]:
    manifest = tomllib.loads(MANIFEST.read_text())
    dependency = manifest["dependencies"]["cedar-poo-pseudonymization"]
    url, revision = dependency["git"], dependency["rev"]
    if url != "https://github.com/tao3k/cedar-poo-spec" or not re.fullmatch(
        r"[0-9a-f]{40}", revision
    ):
        raise ValueError("cedar-poo-pseudonymization must pin a full SPEC Git commit")
    lock = tomllib.loads(LOCK.read_text())
    packages = [p for p in lock["package"] if p["name"] == "cedar-poo-pseudonymization"]
    expected = f"git+{url}?rev={revision}#{revision}"
    if len(packages) != 1 or packages[0].get("source") != expected:
        raise ValueError("Cargo.lock does not pin the manifest SPEC commit")
    commerce = tomllib.loads((ROOT / "crates/mrr-data-commerce/Cargo.toml").read_text())["dependencies"]["cedar-poo-commerce"]
    if (commerce["git"], commerce["rev"]) != (url, revision):
        raise ValueError("Commerce and pseudonymization must use the same SPEC pin")
    return url, revision


def compare(spec_git: Path, revision: str) -> None:
    actual = command("git", "rev-parse", "HEAD", cwd=spec_git).decode().strip()
    if actual != revision:
        raise ValueError(f"SPEC checkout is {actual}, expected {revision}")
    for name, local in FIXTURES.items():
        source = command(
            "git", "show", f"{revision}:Tests/Conformance/{name}", cwd=spec_git
        )
        if local.read_bytes() != source:
            raise ValueError(f"Rust fixture differs from pinned SPEC: {name}")
        print(f"{name}: exact pinned bytes")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec-root", type=Path, help="local SPEC checkout at the pinned commit")
    args = parser.parse_args()
    url, revision = pinned_spec()
    if args.spec_root:
        compare(args.spec_root, revision)
    else:
        with tempfile.TemporaryDirectory(prefix="mrr-spec-fixtures-") as directory:
            repo = Path(directory)
            command("git", "init", "-q", str(repo))
            command("git", "fetch", "-q", "--depth", "1", url, revision, cwd=repo)
            command("git", "checkout", "-q", "--detach", "FETCH_HEAD", cwd=repo)
            compare(repo, revision)
    print(f"SPEC fixture pin verified: {revision}")


if __name__ == "__main__":
    try:
        main()
    except (KeyError, OSError, subprocess.CalledProcessError, ValueError) as error:
        raise SystemExit(f"SPEC fixture check failed: {error}") from error
