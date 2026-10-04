"""Keep fuzz and optional frontend types on the workspace's immutable MRR pin."""

from pathlib import Path
import tomllib


def check_fuzz_mrr_pin(root: Path) -> None:
    workspace = tomllib.loads((root / "Cargo.toml").read_text())
    fuzz = tomllib.loads((root / "fuzz/Cargo.toml").read_text())
    expected = workspace["workspace"]["dependencies"]["meta-relational-reasoning"]
    frontend = workspace["workspace"]["dependencies"]["mrr-property-source"]
    if any(frontend.get(key) != expected.get(key) for key in ("git", "rev")):
        raise ValueError("source frontend git/rev must match the workspace MRR pin")
    actual = fuzz["dependencies"]["meta-relational-reasoning"]
    if any(actual.get(key) != expected.get(key) for key in ("git", "rev")):
        raise ValueError("fuzz MRR git/rev must match the workspace immutable pin")
    lock = tomllib.loads((root / "fuzz/Cargo.lock").read_text())
    source = f"git+{expected['git']}?rev={expected['rev']}#{expected['rev']}"
    workspace_lock = tomllib.loads((root / "Cargo.lock").read_text())
    for name in ("meta-relational-reasoning", "mrr-property-source"):
        owners = [p for p in workspace_lock["package"] if p["name"] == name]
        if len(owners) != 1 or owners[0].get("source") != source:
            raise ValueError(
                f"workspace Cargo.lock must contain exactly the MRR source for {name}"
            )
    matches = [p for p in lock["package"] if p["name"] == "meta-relational-reasoning"]
    if len(matches) != 1 or matches[0].get("source") != source:
        raise ValueError(
            "fuzz Cargo.lock must contain exactly the workspace MRR source"
        )
