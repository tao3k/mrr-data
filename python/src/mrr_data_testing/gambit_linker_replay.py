"""Diagnostic-only frozen linker compilation outside the rejected Cargo gate."""

import hashlib
import json
import shlex
import subprocess
import time
from pathlib import Path

from mrr_data_testing.gambit_controls import diagnostic_flags, sdk_driver, sdk_option
from mrr_data_testing.process import BUILD_LIMITS, run


def input_digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def frozen_linker_input(target):
    root = Path(target).resolve(strict=True)
    candidates = [
        path.resolve(strict=True)
        for path in root.glob(
            "debug/build/mrr-gerbil-*/out/gerbil-program/program_link.i"
        )
    ]
    if len(candidates) != 1 or not candidates[0].is_relative_to(root):
        raise ValueError("one confined preprocessed linker input is required")
    if not candidates[0].is_file() or not candidates[0].stat().st_size:
        raise ValueError("the rejected build must retain a nonempty linker input")
    return candidates[0]


def replay_linker(gsc, target, receipt):
    source = frozen_linker_input(target)
    driver = sdk_driver(gsc)
    compiler = sdk_option(driver, "C_COMPILER")
    options = sdk_option(driver, "FLAGS_OBJ")
    diagnostic_flags(compiler)
    diagnostic_flags(options)
    compiler_args = shlex.split(compiler)
    if not compiler_args:
        raise ValueError("the selected SDK must declare its C compiler")
    version = subprocess.run(
        [*compiler_args, "--version"],
        capture_output=True,
        check=True,
        timeout=5,
        text=True,
    )
    command = [
        *compiler_args,
        *shlex.split(options),
        "-O2",
        "-fno-gcse-lm",
        "-save-temps=obj",
        "-Q",
        "-fopt-info-all",
        f"-fdump-rtl-ira-details={receipt.with_suffix('.ira')}",
        f"-fdump-rtl-reload-details={receipt.with_suffix('.reload')}",
        "-c",
        "-o",
        str(receipt.with_suffix(".o")),
        str(source),
    ]
    before = input_digest(source)
    result = {
        "purpose": "diagnostic only; never replaces the rejected source gate",
        "input": str(source),
        "input_sha256": before,
        "compiler": compiler,
        "compiler_version": version.stdout,
        "command": command,
        "wall_limit": BUILD_LIMITS.wall_seconds,
        "idle_limit": BUILD_LIMITS.idle_seconds,
        "outcome": "running",
    }
    started = time.monotonic_ns()
    try:
        with (
            receipt.with_suffix(".chunks.jsonl").open("w") as chunks,
            receipt.with_suffix(".output").open("wb") as raw,
        ):

            def received(block):
                if block:
                    chunks.write(
                        json.dumps(
                            {
                                "at_ns": time.monotonic_ns() - started,
                                "bytes": len(block),
                                "newlines": block.count(b"\n"),
                            }
                        )
                        + "\n"
                    )
                    chunks.flush()
                    raw.write(block)
                    raw.flush()
                return block

            run(command, limits=BUILD_LIMITS, cwd=source.parent, output_filter=received)
        result["outcome"] = "completed"
    except BaseException as error:
        result["outcome"] = type(error).__name__
        raise
    finally:
        result["unchanged_input"] = input_digest(source) == before
        if result["outcome"] == "completed" and not result["unchanged_input"]:
            result["outcome"] = "input_changed"
        receipt.write_text(json.dumps(result, indent=2) + "\n")
    if not result["unchanged_input"]:
        raise ValueError("diagnostic replay changed its frozen input")
