#!/usr/bin/env python3
"""Compile facade feature slices and enforce normal-dependency isolation.

Run from any directory. Set CARGO_NET_OFFLINE=true to use only cached packages.
Native GraphAr is checked separately by the CI job that provisions its C++ SDK.
"""
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
CASES = [None, "", "backend", "backend-sqlite", "commerce-cedar", "commerce-credential", "commerce-consumption", "commerce-presentation", "arrow", "content-identity", "content", "snapshot", "transfer", "car", "filesystem", "cache", "s3",
         "data-protection", "raw-publish", "protected-envelope", "protected-publish", "datafusion", "graphar", "content-identity,graphar", "cache,s3", "snapshot,cache,s3", "transfer,cache,s3",
         "arrow,content-identity,content,snapshot,transfer,raw-publish,protected-publish,car,filesystem,cache,s3,datafusion,graphar"]


def main():
    backend_tree = subprocess.check_output(
        ["cargo", "tree", "-p", "mrr-data-backend", "--no-default-features", "--edges", "normal",
         "--prefix", "none", "--locked"], cwd=ROOT, text=True)
    backend_packages = {line.split()[0] for line in backend_tree.splitlines() if line.strip()}
    forbidden = {"cedar-poo-commerce", "cedar-poo-bridge", "mrr-data-commerce", "rusqlite", "kache-store", "opendal-service-s3"}
    if backend_packages & forbidden:
        raise SystemExit(f"generic backend imports domain/provider dependencies: {backend_packages & forbidden}")
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
        if enabled & {"commerce-cedar", "commerce-credential", "commerce-consumption", "commerce-presentation"}:
            enabled |= {"commerce-cedar", "content"}
        if "backend-sqlite" in enabled:
            enabled.add("backend")
        content = bool(enabled & {"backend", "content", "snapshot", "transfer", "raw-publish", "protected-envelope", "protected-publish", "car", "filesystem", "cache", "s3", "datafusion"})
        identity = content or bool(enabled & {"content-identity", "data-protection"})
        expected = {
            "mrr-data-backend": "backend" in enabled,
            "rusqlite": bool(enabled & {"backend-sqlite", "cache"}),
            "mrr-data-commerce": "commerce-cedar" in enabled,
            "cedar-poo-commerce": "commerce-cedar" in enabled,
            "p256": "commerce-cedar" in enabled,
            "mrr-data-arrow": bool(enabled & {"arrow", "datafusion"}),
            "mrr-data-core": identity or "datafusion" in enabled,
            "mrr-data-content": content,
            "mrr-data-cache": bool(enabled & {"cache", "s3", "transfer", "raw-publish", "protected-publish"}),
            "mrr-data-security": bool(enabled & {"data-protection", "raw-publish", "protected-envelope", "protected-publish"}),
            "tokio": bool(enabled & {"backend", "s3", "transfer", "raw-publish", "protected-publish", "datafusion"}),
            "ring": bool(enabled & {"protected-envelope", "protected-publish", "s3"}),
            "zeroize": bool(enabled & {"protected-envelope", "protected-publish", "s3", "commerce-cedar"}),
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
