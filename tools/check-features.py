#!/usr/bin/env python3
"""Compile facade feature slices and enforce normal-dependency isolation.

Run from any directory. Set CARGO_NET_OFFLINE=true to use only cached packages.
Native GraphAr is checked separately by the CI job that provisions its C++ SDK.
"""
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
CASES = [None, "", "arrow", "content-identity", "content", "snapshot", "transfer", "car", "filesystem", "cache", "s3",
         "datafusion", "graphar", "content-identity,graphar", "cache,s3", "snapshot,cache,s3", "transfer,cache,s3",
         "arrow,content-identity,content,snapshot,transfer,car,filesystem,cache,s3,datafusion,graphar"]


def main():
    tree = subprocess.check_output(
        ["cargo", "tree", "-p", "mrr-data-poo-flow", "--no-default-features", "--edges", "normal",
         "--prefix", "none", "--locked"], cwd=ROOT, text=True)
    if {line.split()[0] for line in tree.splitlines() if line.strip()} != {"mrr-data-poo-flow"}:
        raise SystemExit("POO Flow runtime dependencies must remain opt-in")
    for selected in CASES:
        flags = [] if selected is None else ["--no-default-features"]
        if selected:
            flags += ["--features", selected]
        enabled = {"arrow", "filesystem"} if selected is None else set(selected.split(","))
        content = bool(enabled & {"content", "snapshot", "transfer", "car", "filesystem", "cache", "s3", "datafusion"})
        identity = content or "content-identity" in enabled
        expected = {
            "mrr-data-arrow": bool(enabled & {"arrow", "datafusion"}),
            "mrr-data-core": identity or "datafusion" in enabled,
            "mrr-data-content": content,
            "mrr-data-cache": bool(enabled & {"cache", "s3", "transfer"}),
            "tokio": bool(enabled & {"s3", "transfer", "datafusion"}),
            "cid": identity,
            "serde_ipld_dagcbor": identity,
            "fvm_ipld_car": "car" in enabled,
            "kache-store": "cache" in enabled,
            "opendal-service-s3": "s3" in enabled,
            "reqwest": "s3" in enabled,
            "rustls": "s3" in enabled,
            "datafusion": "datafusion" in enabled,
            "mrr-data-graphar": "graphar" in enabled,
            "graphar-rs": False,
        }
        tree = subprocess.check_output(
            ["cargo", "tree", "-p", "mrr-data", "--edges", "normal", "--prefix", "none",
             "--locked", *flags], cwd=ROOT, text=True)
        packages = {line.split()[0] for line in tree.splitlines() if line.strip()}
        for package, present in expected.items():
            if (package in packages) != present:
                raise SystemExit(f"{selected!r}: expected {package} present={present}")
        subprocess.run(["cargo", "check", "-p", "mrr-data", "--quiet", "--locked", *flags],
                       cwd=ROOT, check=True)
        print(f"{selected if selected else ('default' if selected is None else 'none')}: passed", flush=True)


if __name__ == "__main__":
    main()
