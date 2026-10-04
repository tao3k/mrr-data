import sys
import subprocess

import pytest

from mrr_data_testing.duckgql import build
from mrr_data_testing.process import run


def test_failed_source_preparation_removes_previous_success(tmp_path, monkeypatch):
    receipt = tmp_path / "artifact.json"
    receipt.write_text('{"signed": false, "sha256": "previous-success"}')
    monkeypatch.setattr(sys, "argv", ["duckgql-build", "--work-dir", str(tmp_path)])

    def refuse(*args):
        raise RuntimeError("source identity mismatch")

    monkeypatch.setattr(build, "checkout", refuse)
    with pytest.raises(RuntimeError, match="identity mismatch"):
        build.main()
    assert not receipt.exists()


def test_invalid_revision_is_refused_before_subprocess(tmp_path, monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("invalid source revision reached subprocess")

    monkeypatch.setattr(build, "run", unexpected)
    with pytest.raises(SystemExit, match="invalid source revision"):
        build.checkout(tmp_path / "source", "https://example.invalid", "main")


def test_compiler_output_grouping_preserves_split_diagnostics_and_real_failure(capfd):
    output = build.CompilerOutput()
    assert output(b"Running pass: compiler-work\nclang: error: Run") == b""
    assert (
        output(b"ning pass: unsupported setting\n")
        == b"clang: error: Running pass: unsupported setting\n"
    )
    assert b"compiler pass events: 1" in output(b"")
    with pytest.raises(subprocess.CalledProcessError) as failure:
        run(
            [
                sys.executable,
                "-c",
                "print('Running pass: actual child output'); print('clang: error: failed compilation'); raise SystemExit(7)",
            ],
            output_filter=build.CompilerOutput(),
        )
    assert failure.value.returncode == 7
    assert "clang: error: failed compilation" in capfd.readouterr().out


def test_output_grouping_keeps_capture_bytes_unmodified():
    payload = "Running pass: compiler-work\nnormal diagnostic\n"
    result = run(
        [sys.executable, "-c", f"print({payload!r}, end='')"],
        output_filter=build.CompilerOutput(),
        capture_limit=1000,
    )
    assert result.stdout == payload.encode()
