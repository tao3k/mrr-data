import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from mrr_data_testing import rustc_progress


def test_workspace_wrapper_preserves_compiler_output_and_failure(tmp_path):
    compiler = shutil.which("rustc")
    if compiler is None:
        pytest.skip("native Rust compiler unavailable")
    source = tmp_path / "fixture.rs"
    source.write_text('fn main() { println!("{}", (0..8).sum::<u64>()); }')
    plain, wrapped = tmp_path / "plain", tmp_path / "wrapped"
    arguments = ["--crate-name", "fixture", "--emit=link", "-C", "opt-level=1"]
    subprocess.run([compiler, *arguments, source, "-o", plain], check=True, timeout=10)
    wrapper = [sys.executable, Path(rustc_progress.__file__), compiler]
    result = subprocess.run(
        [*wrapper, *arguments, source, "-o", wrapped],
        capture_output=True,
        check=True,
        timeout=10,
    )
    assert b"Executing Pass" in result.stderr
    assert subprocess.check_output([plain], timeout=5) == subprocess.check_output(
        [wrapped], timeout=5
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
