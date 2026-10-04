import json
from pathlib import Path
import sys

import pytest

from mrr_data_testing import execution
from mrr_data_testing.process import TEST_LIMITS


def record(name, executable, *, test=True, kind=None):
    return {
        "reason": "compiler-artifact",
        "target": {"name": name, "kind": kind or ["lib"]},
        "profile": {"test": test},
        "executable": str(executable),
    }


def write_artifacts(path, records):
    path.write_text("\n".join(json.dumps(record) for record in records))
    return path


def test_selects_only_named_cargo_test_executables(tmp_path):
    binary = Path(sys.executable).resolve()
    artifacts = write_artifacts(
        tmp_path / "cargo.jsonl",
        [
            record("owner", tmp_path / "missing", test=False),
            record("unrelated", tmp_path / "missing"),
            record("owner", tmp_path / "missing", kind=["bin"]),
            record("owner", binary),
            {"reason": "build-finished", "success": True},
        ],
    )
    assert execution.compiled_test_binaries(artifacts, ["owner"]) == [binary]


@pytest.mark.parametrize("finished", [[], [False], [1], [True, True]])
def test_failed_or_incomplete_build_cannot_run_previous_artifacts(tmp_path, finished):
    artifacts = write_artifacts(
        tmp_path / "cargo.jsonl",
        [
            record("owner", sys.executable),
            *({"reason": "build-finished", "success": value} for value in finished),
        ],
    )
    with pytest.raises(ValueError, match="one successful Cargo build"):
        execution.compiled_test_binaries(artifacts, ["owner"])


def test_missing_and_ambiguous_named_targets_are_refused(tmp_path):
    other = tmp_path / "other"
    other.write_bytes(b"different artifact")
    artifacts = write_artifacts(
        tmp_path / "cargo.jsonl",
        [
            record("owner", sys.executable),
            record("owner", other),
            {"reason": "build-finished", "success": True},
        ],
    )
    for targets in [["owner"], ["absent"], []]:
        with pytest.raises(ValueError, match="one executable per test target"):
            execution.compiled_test_binaries(artifacts, targets)


def test_artifact_execution_uses_the_unchanged_test_policy(tmp_path, monkeypatch):
    artifacts = write_artifacts(
        tmp_path / "cargo.jsonl",
        [
            record("owner", sys.executable),
            {"reason": "build-finished", "success": True},
        ],
    )
    calls = []
    monkeypatch.setattr(
        execution, "run", lambda command, **kwargs: calls.append((command, kwargs))
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run-process",
            "--phase",
            "test",
            "--cargo-artifacts",
            str(artifacts),
            "--test-target",
            "owner",
            "--",
            "source_handoff",
            "--nocapture",
        ],
    )
    execution.main()
    assert calls == [
        (
            [Path(sys.executable).resolve(), "source_handoff", "--nocapture"],
            {"limits": TEST_LIMITS},
        )
    ]
