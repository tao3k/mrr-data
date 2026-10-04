from pathlib import Path

import pytest

from mrr_data_testing.workspace import repository_root


def test_explicit_checkout_is_validated(tmp_path, monkeypatch):
    monkeypatch.setenv("MRR_DATA_ROOT", str(tmp_path))
    with pytest.raises(RuntimeError, match="checkout missing"):
        repository_root()
    (tmp_path / "Cargo.toml").write_text("[workspace]\n")
    (tmp_path / "crates/mrr-data").mkdir(parents=True)
    assert repository_root() == tmp_path


def test_checkout_resolution_ignores_current_directory(tmp_path, monkeypatch):
    monkeypatch.delenv("MRR_DATA_ROOT", raising=False)
    monkeypatch.chdir(tmp_path)
    root = repository_root()
    assert (root / "crates/mrr-data/Cargo.toml").is_file()
    assert root != Path.cwd()
