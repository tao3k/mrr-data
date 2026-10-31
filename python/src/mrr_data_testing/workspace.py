"""Resolve the checkout independently of the caller's working directory."""

import os
from pathlib import Path


def repository_root() -> Path:
    explicit = os.environ.get("MRR_DATA_ROOT")
    candidates = (
        [Path(explicit).resolve()] if explicit else Path(__file__).resolve().parents
    )
    for candidate in candidates:
        if (candidate / "Cargo.toml").is_file() and (
            candidate / "crates/mrr-data"
        ).is_dir():
            return candidate
    raise RuntimeError("MRR Data checkout missing; set MRR_DATA_ROOT to its root")
