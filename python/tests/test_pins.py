"""Fail before compilation when independent fuzz/workspace pins drift."""

import pytest
from mrr_data_testing.checks.pins import check_fuzz_mrr_pin


def fixture(
    tmp_path,
    *,
    fuzz_revision="a" * 40,
    lock_revision="a" * 40,
    frontend_revision="a" * 40,
    workspace_revision="a" * 40,
):
    url = "https://github.com/example/mrr.git"
    dep = f'git = "{url}", rev = "{"a" * 40}"'
    (tmp_path / "Cargo.toml").write_text(
        f"[workspace.dependencies]\nmeta-relational-reasoning = {{ {dep} }}\n"
        f'mrr-property-source = {{ git = "{url}", rev = "{frontend_revision}" }}\n'
    )
    (tmp_path / "Cargo.lock").write_text(
        "\n".join(
            f'[[package]]\nname = "{name}"\n'
            f'source = "git+{url}?rev={workspace_revision}#{workspace_revision}"\n'
            for name in ("meta-relational-reasoning", "mrr-property-source")
        )
    )
    (tmp_path / "fuzz").mkdir()
    (tmp_path / "fuzz/Cargo.toml").write_text(
        f'[dependencies]\nmeta-relational-reasoning = {{ git = "{url}", '
        f'rev = "{fuzz_revision}" }}\n'
    )
    (tmp_path / "fuzz/Cargo.lock").write_text(
        '[[package]]\nname = "meta-relational-reasoning"\n'
        f'source = "git+{url}?rev={lock_revision}#{lock_revision}"\n'
    )
    return tmp_path


def test_aligned_pin(tmp_path):
    check_fuzz_mrr_pin(fixture(tmp_path))


@pytest.mark.parametrize("drift", ["fuzz_revision", "lock_revision"])
def test_drift_refuses_before_compile(tmp_path, drift):
    with pytest.raises(ValueError, match="fuzz"):
        check_fuzz_mrr_pin(fixture(tmp_path, **{drift: "b" * 40}))


@pytest.mark.parametrize("drift", ["frontend_revision", "workspace_revision"])
def test_source_owner_drift_refuses_before_compile(tmp_path, drift):
    with pytest.raises(ValueError, match="source"):
        check_fuzz_mrr_pin(fixture(tmp_path, **{drift: "b" * 40}))
