import subprocess
import sys

import pytest

from mrr_data_testing.process import Limits, ProgressTimeout, run


def test_silent_process_is_refused_before_wall_deadline():
    with pytest.raises(ProgressTimeout, match="no output progress"):
        run(
            [sys.executable, "-c", "import time; time.sleep(10)"],
            limits=Limits(wall_seconds=2, idle_seconds=0.1),
        )


def test_continuous_output_does_not_extend_wall_deadline():
    with pytest.raises(ProgressTimeout, match="wall deadline"):
        run(
            [
                sys.executable,
                "-u",
                "-c",
                "import time\nwhile True:\n print('progress'); time.sleep(0.02)",
            ],
            limits=Limits(wall_seconds=0.3, idle_seconds=1),
        )


def test_closing_output_pipe_does_not_bypass_idle_deadline():
    with pytest.raises(ProgressTimeout, match="no output progress"):
        run(
            [
                sys.executable,
                "-c",
                "import os,time; os.close(1); os.close(2); time.sleep(10)",
            ],
            limits=Limits(wall_seconds=2, idle_seconds=0.1),
        )


def test_failed_process_preserves_failure():
    with pytest.raises(subprocess.CalledProcessError) as error:
        run(
            [sys.executable, "-c", "raise SystemExit(7)"],
            limits=Limits(wall_seconds=2, idle_seconds=1),
        )
    assert error.value.returncode == 7


def test_successful_process_completes():
    result = run(
        [sys.executable, "-c", "print('completed')"],
        limits=Limits(wall_seconds=2, idle_seconds=1),
    )
    assert result.returncode == 0
