"""Bound subprocesses by elapsed time and actual output progress."""

import os
import selectors
import signal
import subprocess
import sys
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class Limits:
    wall_seconds: float
    idle_seconds: float

    def __post_init__(self):
        if self.wall_seconds <= 0 or self.idle_seconds <= 0:
            raise ValueError("process limits must be positive")


TEST_LIMITS = Limits(wall_seconds=120, idle_seconds=5)
BUILD_LIMITS = Limits(wall_seconds=1200, idle_seconds=30)
CONTROL_LIMITS = Limits(wall_seconds=120, idle_seconds=30)


class ProgressTimeout(TimeoutError):
    """A phase exceeded its wall limit or stopped producing output."""


def run(argv, *, limits=TEST_LIMITS, cwd=None, env=None):
    """Forward real output; terminate the entire process group on refusal."""
    command = list(map(str, argv))
    started = last_output = time.monotonic()
    with subprocess.Popen(
        command,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    ) as child:
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(child.stdout, selectors.EVENT_READ)
                while selector.get_map() or child.poll() is None:
                    now = time.monotonic()
                    if now - started >= limits.wall_seconds:
                        raise ProgressTimeout(f"wall deadline: {command[0]}")
                    if now - last_output >= limits.idle_seconds:
                        raise ProgressTimeout(f"no output progress: {command[0]}")
                    for key, _ in selector.select(
                        timeout=min(
                            0.1,
                            limits.wall_seconds - (now - started),
                            limits.idle_seconds - (now - last_output),
                        )
                    ):
                        block = os.read(key.fileobj.fileno(), 65536)
                        if not block:
                            selector.unregister(key.fileobj)
                            continue
                        last_output = time.monotonic()
                        sys.stdout.buffer.write(block)
                        sys.stdout.buffer.flush()
                remaining = limits.wall_seconds - (time.monotonic() - started)
                try:
                    status = child.wait(timeout=max(0, remaining))
                except subprocess.TimeoutExpired as error:
                    raise ProgressTimeout(f"wall deadline: {command[0]}") from error
                if status:
                    raise subprocess.CalledProcessError(status, command)
                return subprocess.CompletedProcess(command, status)
        finally:
            # Descendants can outlive the direct child or retain its output pipe.
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait()
