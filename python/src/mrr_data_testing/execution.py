"""Run an existing test or build command under a declared phase policy."""

import argparse
import json
from pathlib import Path

from mrr_data_testing.process import BUILD_LIMITS, CONTROL_LIMITS, TEST_LIMITS, run


def compiled_test_binaries(artifacts, targets):
    """Select exact named test executables from a successful Cargo build record."""
    selected = {name: set() for name in targets}
    finished = []
    for line in artifacts.read_text().splitlines():
        artifact = json.loads(line)
        if artifact.get("reason") == "build-finished":
            finished.append(artifact.get("success") is True)
        name = artifact.get("target", {}).get("name")
        if (
            artifact.get("reason") == "compiler-artifact"
            and name in selected
            and artifact.get("profile", {}).get("test") is True
            and artifact.get("target", {}).get("kind") in (["lib"], ["test"])
            and artifact.get("executable")
        ):
            binary = Path(artifact["executable"]).resolve(strict=True)
            if not binary.is_file():
                raise ValueError("Cargo test executable is not a file")
            selected[name].add(binary)
    if (
        finished != [True]
        or not selected
        or any(len(paths) != 1 for paths in selected.values())
    ):
        raise ValueError(
            "one successful Cargo build and one executable per test target required"
        )
    return [next(iter(paths)) for paths in selected.values()]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=["test", "build", "control"], required=True)
    parser.add_argument("--cargo-artifacts", type=Path)
    parser.add_argument("--test-target", action="append", default=[])
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.arguments[1:] if args.arguments[:1] == ["--"] else args.arguments
    if args.cargo_artifacts:
        if args.phase != "test" or not args.test_target:
            parser.error(
                "Cargo artifacts require the test phase and named test targets"
            )
        for binary in compiled_test_binaries(args.cargo_artifacts, args.test_target):
            run([binary, *command], limits=TEST_LIMITS)
        return
    if args.test_target:
        parser.error("test targets require Cargo artifacts")
    if not command:
        parser.error("a subprocess command is required after --")
    policies = {"test": TEST_LIMITS, "build": BUILD_LIMITS, "control": CONTROL_LIMITS}
    run(command, limits=policies[args.phase])
