import subprocess

import pytest

from mrr_data_testing import gambit_controls


def sdk(tmp_path, feature="gcc", query="-pipe -foptimize-sibling-calls"):
    compiler = tmp_path / "gsc"
    compiler.write_text("fixture")
    compiler.chmod(0o700)
    driver = compiler.with_name("gambuild-C")
    driver.write_text(
        f'BUILD_FEATURE_C_COMP="{feature}"\n'
        'if test "${CORCXXFLAGS_GAMBUILD+set}" != set; then\n'
        f'  CORCXXFLAGS_GAMBUILD="{query}"\nfi\n'
    )
    return compiler, driver


def test_sdk_query_preserves_all_flags_and_sdk_files(tmp_path, monkeypatch):
    compiler, driver = sdk(tmp_path)
    original = driver.read_bytes()
    flags = "-pipe -fPIC -DABI=1 -foptimize-sibling-calls"
    calls = []

    def query(command, **options):
        calls.append((command, options))
        return subprocess.CompletedProcess(command, 0, flags + "\n", "")

    monkeypatch.setattr(subprocess, "run", query)
    assert (
        gambit_controls.configured_flags(compiler)
        == flags + " " + gambit_controls.TRACKING_OFF
    )
    assert calls[0][0] == [driver, "CORCXXFLAGS_GAMBUILD"]
    assert calls[0][1]["timeout"] == 5
    assert driver.read_bytes() == original


@pytest.mark.parametrize(
    "flags",
    ["-fPIC\nBAD=1", "$(untrusted)", "`untrusted`", "-ftrack-macro-expansion=2"],
)
def test_ambiguous_or_conflicting_options_refuse(flags):
    with pytest.raises(ValueError):
        gambit_controls.diagnostic_flags(flags)


def test_existing_disabled_tracking_is_not_duplicated():
    flags = "-O2 -ftrack-macro-expansion=0 -fPIC"
    assert gambit_controls.diagnostic_flags(flags) == flags


def test_inherited_options_keep_their_abi_flags(tmp_path, monkeypatch):
    compiler, _ = sdk(tmp_path)
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kw: subprocess.CompletedProcess(args, 0, "-pipe\n", ""),
    )
    assert (
        gambit_controls.configured_flags(compiler, "-m64 -fPIC")
        == "-m64 -fPIC -ftrack-macro-expansion=0"
    )


def test_unsupported_sdk_refuses_before_execution(tmp_path, monkeypatch):
    compiler, _ = sdk(tmp_path, feature="clang")
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kw: pytest.fail("wrong compiler must not execute"),
    )
    with pytest.raises(ValueError, match="declare GCC"):
        gambit_controls.configured_flags(compiler)
