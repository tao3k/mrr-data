"""Keep isolated fuzz types on the same immutable MRR source as the workspace."""
from pathlib import Path
import tomllib


def check_fuzz_mrr_pin(root: Path) -> None:
    workspace = tomllib.loads((root / "Cargo.toml").read_text())
    fuzz = tomllib.loads((root / "fuzz/Cargo.toml").read_text())
    expected = workspace["workspace"]["dependencies"]["meta-relational-reasoning"]
    actual = fuzz["dependencies"]["meta-relational-reasoning"]
    if any(actual.get(key) != expected.get(key) for key in ("git", "rev")):
        raise ValueError("fuzz MRR git/rev must match the workspace immutable pin")
    lock = tomllib.loads((root / "fuzz/Cargo.lock").read_text())
    source = f"git+{expected['git']}?rev={expected['rev']}#{expected['rev']}"
    matches = [p for p in lock["package"] if p["name"] == "meta-relational-reasoning"]
    if len(matches) != 1 or matches[0].get("source") != source:
        raise ValueError("fuzz Cargo.lock must contain exactly the workspace MRR source")
