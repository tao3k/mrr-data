import sys

import pytest

from mrr_data_testing.duckgql import build


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
