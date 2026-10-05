import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from mrr_data_testing import rustc_progress
from mrr_data_testing.duckgql.build import CompilerOutput
from mrr_data_testing.process import Limits, run


def test_frontend_logging_preserves_explicit_host_configuration(monkeypatch):
    calls = []
    monkeypatch.setattr(sys, "argv", ["wrapper", "rustc", "--emit=metadata"])
    monkeypatch.setattr(rustc_progress.os, "execvp", lambda *args: calls.append(args))
    monkeypatch.setenv("RUSTC_LOG", "temporary-test-setting")
    monkeypatch.delenv("RUSTC_LOG")
    rustc_progress.main()
    assert "rustc_hir_typeck::coercion=info" in rustc_progress.os.environ["RUSTC_LOG"]
    assert calls == [("rustc", ["rustc", "--emit=metadata"])]
    monkeypatch.setenv("RUSTC_LOG", "host-selected-logging")
    rustc_progress.main()
    assert rustc_progress.os.environ["RUSTC_LOG"] == "host-selected-logging"


def test_workspace_wrapper_preserves_compiler_output_and_failure(tmp_path, monkeypatch):
    compiler = shutil.which("rustc")
    if compiler is None:
        pytest.skip("native Rust compiler unavailable")
    source = tmp_path / "fixture.rs"
    source.write_text('fn main() { println!("{}", (0..8).sum::<u64>()); }')
    plain, wrapped = tmp_path / "plain", tmp_path / "wrapped"
    arguments = ["--crate-name", "fixture", "--emit=link", "-C", "opt-level=1"]
    subprocess.run([compiler, *arguments, source, "-o", plain], check=True, timeout=10)
    monkeypatch.setenv("MRR_DATA_RUSTC_PHASE_TRACE_CRATES", "fixture")
    wrapper = [sys.executable, Path(rustc_progress.__file__), compiler]
    result = run(
        [*wrapper, *arguments, source, "-o", wrapped],
        limits=Limits(wall_seconds=10, idle_seconds=5),
        capture_limit=2 * 1024 * 1024,
        output_filter=CompilerOutput(),
    )
    assert b"inline (" in result.stdout
    assert b"prologepilog (analysis)" in result.stdout
    assert b"asm-printer (analysis)" in result.stdout
    assert b" Running pass " in result.stdout
    assert subprocess.check_output([plain], timeout=5) == subprocess.check_output(
        [wrapped], timeout=5
    )
    nested = tmp_path / "nested"
    result = run(
        [
            sys.executable,
            Path(rustc_progress.__file__),
            Path(rustc_progress.__file__),
            compiler,
            *arguments,
            source,
            "-o",
            nested,
        ],
        limits=Limits(wall_seconds=10, idle_seconds=5),
        capture_limit=2 * 1024 * 1024,
        output_filter=CompilerOutput(),
    )
    assert b"inline (" in result.stdout
    assert subprocess.check_output([plain], timeout=5) == subprocess.check_output(
        [nested], timeout=5
    )
    source.write_text("fn main() { invalid Rust syntax }")
    refused = subprocess.run(
        [*wrapper, *arguments, source, "-o", tmp_path / "refused"],
        capture_output=True,
        timeout=10,
    )
    assert refused.returncode != 0
    assert b"error" in refused.stderr
    assert not (tmp_path / "refused").exists()


def test_phase_trace_is_owned_and_never_duplicated(monkeypatch):
    calls = []
    observed_calls = []
    monkeypatch.setattr(rustc_progress.os, "execvp", lambda *args: calls.append(args))
    monkeypatch.setattr(
        rustc_progress, "run_observed", lambda args: observed_calls.append(args) or 0
    )
    monkeypatch.setenv(
        "MRR_DATA_RUSTC_PHASE_TRACE_CRATES", "mrr_data_core,mrr_data_content,asp_rust"
    )
    for compiler, crate, enabled in [
        ("rustc", "mrr_data_core", True),
        ("rustc", "mrr_data_content", True),
        ("rustc", "asp_rust", True),
        ("rustc", "foreign", False),
        ("wrapper", "mrr_data_core", False),
    ]:
        monkeypatch.setattr(
            sys, "argv", ["wrapper", compiler, "--crate-name", crate, "--emit=link"]
        )
        previous = len(observed_calls)
        if enabled:
            with pytest.raises(SystemExit) as exit_status:
                rustc_progress.main()
            assert exit_status.value.code == 0
        else:
            rustc_progress.main()
        assert len(observed_calls) == previous + int(enabled)
        if enabled:
            assert "remark=inline prologepilog asm-printer" in observed_calls[-1]
            assert "llvm-args=--print-pass-numbers" in observed_calls[-1]


@pytest.mark.parametrize("kind", ["metadata", "dep-graph"])
def test_metadata_progress_tracks_only_open_owned_files(tmp_path, capsys, kind):
    folder = (
        tmp_path / "rmeta-test"
        if kind == "metadata"
        else tmp_path / "incremental" / "fixture" / "s-test-working"
    )
    folder.mkdir(parents=True)
    metadata = folder / ("full.rmeta" if kind == "metadata" else "dep-graph.part.bin")
    unrelated = folder / "stub.rmeta"
    unrelated.write_bytes(b"foreign" * 1024)
    compiler = tmp_path / "compiler.py"
    compiler.write_text(
        "import sys, time\n"
        "with open(sys.argv[2], 'rb') as foreign, open(sys.argv[1], 'wb', buffering=0) as output:\n"
        " for i in range(4):\n"
        "  output.write(b'x' * 4096)\n"
        "  time.sleep(0.7)\n"
        "sys.exit(7)\n"
    )
    assert (
        rustc_progress.run_observed(
            [sys.executable, str(compiler), str(metadata), str(unrelated)]
        )
        == 7
    )
    output = capsys.readouterr().err
    assert f"{kind}-write" in output
    assert str(metadata) in output
    assert str(unrelated) not in output
    assert metadata.stat().st_size == 16384


def test_observer_preserves_inherited_jobserver_descriptors(tmp_path):
    import os

    read_fd, write_fd = os.pipe()
    try:
        os.set_inheritable(read_fd, True)
        os.set_inheritable(write_fd, True)
        compiler = tmp_path / "jobserver.py"
        compiler.write_text(
            "import os, sys\nos.fstat(int(sys.argv[1]))\nos.fstat(int(sys.argv[2]))\n"
        )
        assert (
            rustc_progress.run_observed(
                [sys.executable, str(compiler), str(read_fd), str(write_fd)]
            )
            == 0
        )
    finally:
        os.close(read_fd)
        os.close(write_fd)


def test_metadata_observer_does_not_turn_a_stall_into_progress(tmp_path):
    from mrr_data_testing.process import ProgressTimeout

    compiler = tmp_path / "stalled.py"
    compiler.write_text("import time\ntime.sleep(10)\n")
    observer = tmp_path / "observer.py"
    observer.write_text(
        "import sys\n"
        "from mrr_data_testing.rustc_progress import run_observed\n"
        "sys.exit(run_observed([sys.executable, sys.argv[1]]))\n"
    )
    with pytest.raises(ProgressTimeout, match="no output progress"):
        run(
            [sys.executable, observer, compiler],
            limits=Limits(wall_seconds=5, idle_seconds=1),
        )


def test_observer_preserves_compiler_signal_exit():
    import signal

    child = "import os, signal; os.kill(os.getpid(), signal.SIGTERM)"
    observer = (
        "import sys; from mrr_data_testing.rustc_progress import run_observed; "
        f"run_observed([sys.executable, '-c', {child!r}])"
    )
    result = subprocess.run([sys.executable, "-c", observer], timeout=5)
    assert result.returncode == -signal.SIGTERM


def test_owned_incremental_directory_reports_closed_outputs_not_stale_or_foreign(
    tmp_path, capsys
):
    root = tmp_path / "incremental" / "fixture"
    working = root / "s-owned-working"
    working.mkdir(parents=True)
    stale = working / "stale.o"
    stale.write_bytes(b"existing" * 1024)
    foreign = root / "s-foreign-working"
    foreign.mkdir()
    (foreign / "other.o").write_bytes(b"adjacent" * 1024)
    active = working / "active.pre-lto.bc"
    compiler = tmp_path / "closed_outputs.py"
    compiler.write_text(
        "import fcntl, sys, time\nfrom pathlib import Path\n"
        "with open(sys.argv[1], 'wb') as lock:\n"
        " fcntl.flock(lock, fcntl.LOCK_EX)\n"
        " time.sleep(1)\n"
        " for i in range(4):\n"
        "  Path(sys.argv[2]).write_bytes(b'x' * (4096 * (i + 1)))\n"
        "  time.sleep(0.7)\n"
    )
    assert (
        rustc_progress.run_observed(
            [sys.executable, str(compiler), str(root / "s-owned.lock"), str(active)]
        )
        == 0
    )
    output = capsys.readouterr().err
    assert "codegen-write" in output
    assert str(active) in output
    assert str(stale) not in output
    assert str(foreign) not in output
