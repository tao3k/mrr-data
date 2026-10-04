import subprocess
import sys
import time

import pytest

from mrr_data_testing.process import Limits, OutputLimit, ProgressTimeout, run


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


def test_receipt_capture_preserves_exact_output_and_refuses_excess_bytes():
    result = run([sys.executable, "-c", "print('receipt')"], capture_limit=100)
    assert result.stdout == b"receipt\n"
    with pytest.raises(OutputLimit, match="output byte limit"):
        run([sys.executable, "-c", "print('x' * 2000)"], capture_limit=100)


@pytest.mark.parametrize("parent_exits", [False, True])
def test_refusal_stops_descendant_even_after_parent_exit(tmp_path, parent_exits):
    heartbeat = tmp_path / "descendant-progress"
    descendant = (
        "import pathlib,sys,time\n"
        "path = pathlib.Path(sys.argv[1])\n"
        "print('descendant started', flush=True)\n"
        "for counter in range(1000):\n"
        " path.write_text(str(counter)); time.sleep(0.01)\n"
    )
    parent = (
        "import subprocess,sys,time\n"
        "subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2]])\n"
        "if sys.argv[3] == 'wait': time.sleep(10)\n"
    )
    with pytest.raises(ProgressTimeout, match="no output progress"):
        run(
            [
                sys.executable,
                "-c",
                parent,
                descendant,
                heartbeat,
                "exit" if parent_exits else "wait",
            ],
            limits=Limits(wall_seconds=3, idle_seconds=1),
        )
    # Observe actual descendant work, not PID existence: an orphaned zombie may
    # still have a PID while its descriptors and execution have already ended.
    assert heartbeat.exists(), "the descendant must enter its workload"
    observed = heartbeat.read_text()
    time.sleep(0.1)
    assert heartbeat.read_text() == observed, "descendant survived refusal"
