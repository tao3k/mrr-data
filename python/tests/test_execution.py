import json
import io
from pathlib import Path
import subprocess
import sys

import pytest

from mrr_data_testing import execution
from mrr_data_testing.process import BUILD_LIMITS, TEST_LIMITS


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


def test_verbose_cargo_capture_keeps_live_bytes_and_only_machine_records():
    output = io.BytesIO()
    capture = execution.CargoArtifactCapture(output)
    payload = (
        b"[owner] cargo:rerun-if-changed=source.ss\n"
        b"[owner] native phase complete\n"
        b'{"reason":"compiler-message","message":{"level":"warning","message":"kept live"}}\n'
        b'{"reason":"compiler-artifact","target":{"name":"owner"}}\n'
        b'{"reason":"build-finished","success":true}\n'
    )
    assert capture(payload[:70]) + capture(payload[70:]) == payload
    assert capture(b"") == b""
    capture.complete()
    records = [json.loads(line) for line in output.getvalue().splitlines()]
    assert [record["reason"] for record in records] == [
        "compiler-artifact",
        "build-finished",
    ]


def test_cargo_capture_groups_real_rust_remarks_without_hiding_errors():
    capture = execution.CargoArtifactCapture(io.BytesIO())
    event = (
        json.dumps(
            {
                "reason": "compiler-message",
                "message": {
                    "level": "note",
                    "message": "<unknown file>:0:0 inline (success): compiler work",
                },
            }
        ).encode()
        + b"\n"
    )
    error = (
        json.dumps(
            {
                "reason": "compiler-message",
                "message": {
                    "level": "error",
                    "message": "unsupported inline (option)",
                },
            }
        ).encode()
        + b"\n"
    )
    assert (
        capture(event * 256 + error)
        == b"rust compiler diagnostic events: 256\n" + error
    )
    assert capture(event) == b""
    assert capture(b"") == b"rust compiler diagnostic events: 1\n"
    assert capture(b"") == b""


@pytest.mark.parametrize(
    "event",
    [
        b"0ms INFO rustc_hir_typeck::coercion return=Ok(real compiler event)\n",
        b" rustc_hir_typeck::coercion::coerce a=usize, b=usize\n",
    ],
)
def test_frontend_capture_groups_real_events_and_keeps_compiler_errors(event):
    output = io.BytesIO()
    capture = execution.CargoArtifactCapture(output)
    error = b'{"reason":"compiler-message","message":{"level":"error","message":"type refusal"}}\n'
    unrelated = b"INFO rustc_hir_typeck::coercion_other kept as raw output\n"
    payload = event * 256 + unrelated + error
    forwarded = capture(payload[:17]) + capture(payload[17:])
    assert forwarded == b"rust frontend diagnostic events: 256\n" + unrelated + error
    assert capture(event) == b""
    assert capture(b"") == b"rust frontend diagnostic events: 1\n"
    assert output.getvalue() == b""
    with pytest.raises(ValueError, match="one successful Cargo build"):
        capture.complete()


@pytest.mark.parametrize("finished", [[], [False], [1], [True, True]])
def test_build_capture_refuses_incomplete_or_failed_cargo(finished):
    capture = execution.CargoArtifactCapture(io.BytesIO())
    for value in finished:
        capture(
            json.dumps({"reason": "build-finished", "success": value}).encode() + b"\n"
        )
    with pytest.raises(ValueError, match="one successful Cargo build"):
        capture.complete()


def test_build_capture_removes_prior_success_on_real_process_failure(
    tmp_path, monkeypatch
):
    artifacts = tmp_path / "cargo.jsonl"
    artifacts.write_text('{"reason":"build-finished","success":true}\n')
    calls = []
    actual_run = execution.run

    def observe(command, **kwargs):
        calls.append((command, kwargs["limits"]))
        return actual_run(command, **kwargs)

    command = [
        sys.executable,
        "-c",
        'print(\'{"reason":"build-finished","success":true}\'); raise SystemExit(7)',
    ]
    monkeypatch.setattr(execution, "run", observe)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run-process",
            "--phase",
            "build",
            "--cargo-artifacts",
            str(artifacts),
            "--",
            *command,
        ],
    )
    with pytest.raises(subprocess.CalledProcessError) as failure:
        execution.main()
    assert failure.value.returncode == 7
    assert calls == [(command, BUILD_LIMITS)]
    assert not artifacts.exists()


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


def test_artifact_protocol_survives_interleaved_raw_stderr(tmp_path):
    artifacts = io.BytesIO()
    capture = execution.CargoArtifactCapture(artifacts)
    diagnostics = execution.CargoArtifactCapture(None, protocol=False)
    script = "import os; os.write(1,b'{\"reason\":\"build-'); os.write(2,b'{not-json LLVM diagnostic\\n'); os.write(1,b'finished\",\"success\":true}\\n')"
    execution.run(
        [sys.executable, "-c", script],
        limits=TEST_LIMITS,
        output_filter=capture,
        stderr_output_filter=diagnostics,
    )
    capture.complete()
    assert json.loads(artifacts.getvalue()) == {
        "reason": "build-finished",
        "success": True,
    }
    assert diagnostics.finished == []


def test_new_pm_pass_events_are_grouped_without_hiding_stderr_errors():
    capture = execution.CargoArtifactCapture(None, protocol=False)
    event = b" Running pass 42 IndVarSimplifyPass on real_function\n"
    error = b"error: actual compiler refusal\n"
    payload = event * 256 + error
    forwarded = capture(payload[:13]) + capture(payload[13:])
    assert forwarded == b"LLVM new-PM pass events: 256\n" + error
    assert capture(event) == b""
    assert capture(b"") == b"LLVM new-PM pass events: 1\n"
    assert capture(b"") == b""


def test_interleaved_new_pm_headers_keep_mixed_errors_raw():
    capture = execution.CargoArtifactCapture(None, protocol=False)
    event = b" Running pass 4 Running pass 8 SROAPass on real_function\n"
    mixed = b' Running pass 9 {"level":"error","message":"backend refusal"}\n'
    assert capture(event * 128 + mixed) == b"LLVM new-PM pass events: 256\n" + mixed
    assert capture(b"") == b""
