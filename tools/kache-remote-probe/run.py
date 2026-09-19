#!/usr/bin/env python3
"""Validate an unpublished upstream extraction using a temporary root workspace.

The user's checkout and its Cargo manifests/lockfile are never rewritten.
Pass --kache to a checkout with the extraction applied, or omit it to clone the
pinned revision and apply the checked-in patch. Cargo uses the normal dependency
cache; this script does not provision cloud credentials or contact a cloud bucket.
"""
import argparse
import os
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

PIN = "7461ca763506c081d6ceb95ea225c04ce2d24df6"
ROOT = Path(__file__).resolve().parents[2]


def run(*args, cwd=None, env=None):
    subprocess.run(args, cwd=cwd, env=env, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kache", type=Path)
    parser.add_argument("--output", type=Path, help="new directory for reproducible receipts")
    args = parser.parse_args()
    output = args.output.resolve() if args.output else Path(tempfile.mkdtemp(prefix="mrr-remote-probe-"))
    if output.is_relative_to(ROOT):
        raise SystemExit("--output must be outside the source workspace")
    if args.output:
        output.mkdir(parents=True, exist_ok=False)
    upstream = args.kache.resolve() if args.kache else output / "kache"
    if not args.kache:
        run("git", "clone", "https://github.com/kunobi-ninja/kache.git", str(upstream))
        run("git", "checkout", "--detach", PIN, cwd=upstream)
        run("git", "apply", str(ROOT / "tools/kache-remote-probe/kache-remote.patch"), cwd=upstream)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=upstream, text=True).strip()
    if revision != PIN or not (upstream / "crates/kache-remote/Cargo.toml").is_file():
        raise SystemExit("expected the pinned Kache revision with the library extraction applied")
    workspace = output / "mrr-data"
    shutil.copytree(ROOT, workspace, ignore=shutil.ignore_patterns(".git", "target", "__pycache__"))
    manifest = workspace / "Cargo.toml"
    text = manifest.read_text().replace(
        "[workspace.dependencies]\n",
        "[workspace.dependencies]\nkache-remote = { path = "
        + json.dumps(str(upstream / "crates/kache-remote")) + " }\n",
        1,
    )
    # One Kache type owner throughout the temporary workspace.
    text += '\n[patch."https://github.com/kunobi-ninja/kache"]\n'
    for name in ("kache-store", "kache-format", "kache-fs"):
        text += name + " = { path = " + json.dumps(str(upstream / "crates" / name)) + " }\n"
    manifest.write_text(text)
    probe = workspace / "crates/mrr-data-kache-probe"
    path = probe / "Cargo.toml"
    path.write_text(path.read_text().replace(
        "[dev-dependencies]\n",
        "[dev-dependencies]\nkache-remote.workspace = true\n"
        'tokio = { workspace = true, features = ["net", "sync"] }\n'
        "axum.workspace = true\nmeta-relational-reasoning.workspace = true\n", 1))
    path.write_text(path.read_text().replace(
        "mrr-data-content.workspace = true",
        'mrr-data-content = { workspace = true, features = ["car"] }'))
    unit = probe / "tests/unit"
    helpers = unit / "contracts.rs"
    text = helpers.read_text().replace("struct DataPolicy;", "pub(super) struct DataPolicy;")
    for name in ("config", "key", "put", "get"):
        text = text.replace(f"fn {name}(", f"pub(super) fn {name}(", 1)
    helpers.write_text(text)
    shutil.copyfile(ROOT / "tools/kache-remote-probe/remote.rs", unit / "remote.rs")
    with (unit / "mod.rs").open("a") as stream:
        stream.write("\nmod remote;\n")
    environment = os.environ.copy()
    environment["KACHE_S3_ACCESS_KEY"] = "probe-access-key"
    environment["KACHE_S3_SECRET_KEY"] = "probe-secret-key"
    environment.setdefault("CARGO_TARGET_DIR", str(output / "target"))
    for command in (
        ["cargo", "+1.95.0", "test", "-p", "mrr-data-kache-probe"],
        ["cargo", "+1.95.0", "clippy", "-p", "mrr-data-kache-probe", "--all-targets", "--no-deps", "--locked", "--", "-D", "warnings"],
    ):
        log = output / (command[2] + ".log")
        print(f"Running {' '.join(command)}; log: {log}", flush=True)
        with log.open("w") as stream:
            result = subprocess.run(command, cwd=workspace, env=environment, stdout=stream, stderr=subprocess.STDOUT)
        if result.returncode:
            print(log.read_text()[-6000:])
            result.check_returncode()
        print("\n".join(line for line in log.read_text().splitlines() if "test result:" in line or "Finished" in line), flush=True)
    print(f"Verified temporary root workspace: {workspace}")
    print("No hosted S3 provider acceptance is claimed.")


if __name__ == "__main__":
    main()
