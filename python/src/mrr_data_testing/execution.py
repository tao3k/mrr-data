"""Run an existing test or build command under a declared phase policy."""

import argparse
import json
import re
from pathlib import Path

from mrr_data_testing.process import BUILD_LIMITS, CONTROL_LIMITS, TEST_LIMITS, run


class CargoArtifactCapture:
    """Keep Cargo records separate from live, very-verbose build diagnostics."""

    def __init__(self, output):
        self.output = output
        self.pending = b""
        self.finished = []
        self.events = 0
        self.frontend_events = 0

    def __call__(self, block):
        lines = (self.pending + block).splitlines(keepends=True)
        self.pending = b""
        if block and lines and not lines[-1].endswith(b"\n"):
            self.pending = lines.pop()
        forwarded = bytearray()
        for line in lines:
            if not line.startswith(b"{"):
                if re.match(
                    rb"^\s*(?:\d+(?:\.\d+)?(?:ns|us|ms|s)\s+)?INFO rustc_(?:hir_typeck::coercion|borrowck::region_infer|interface::passes)\b",
                    line,
                ):
                    self.frontend_events += 1
                    if self.frontend_events >= 256:
                        forwarded.extend(self.frontend_summary())
                    continue
                forwarded.extend(line)
                continue
            record = json.loads(line)
            if not isinstance(record, dict) or "reason" not in record:
                forwarded.extend(line)
                continue
            if record["reason"] in ("compiler-artifact", "build-finished"):
                self.output.write(line if line.endswith(b"\n") else line + b"\n")
            if record["reason"] == "build-finished":
                self.finished.append(record.get("success") is True)
            message = record.get("message", {})
            if (
                record["reason"] == "compiler-message"
                and message.get("level") == "note"
                and any(
                    event in message.get("message", "")
                    for event in (" inline (", " prologepilog (")
                )
            ):
                self.events += 1
                if self.events >= 256:
                    forwarded.extend(self.summary())
            else:
                forwarded.extend(line)
        if not block and self.events:
            forwarded.extend(self.summary())
        if not block and self.frontend_events:
            forwarded.extend(self.frontend_summary())
        self.output.flush()
        return bytes(forwarded)

    def summary(self):
        output = f"rust compiler diagnostic events: {self.events}\n".encode()
        self.events = 0
        return output

    def frontend_summary(self):
        output = f"rust frontend diagnostic events: {self.frontend_events}\n".encode()
        self.frontend_events = 0
        return output

    def complete(self):
        if self.finished != [True]:
            raise ValueError("one successful Cargo build required")


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
        if args.phase == "build":
            if args.test_target or not command:
                parser.error("Cargo artifact capture requires a build command")
            try:
                with args.cargo_artifacts.open("wb") as output:
                    capture = CargoArtifactCapture(output)
                    run(command, limits=BUILD_LIMITS, output_filter=capture)
                    capture.complete()
            except BaseException:
                args.cargo_artifacts.unlink(missing_ok=True)
                raise
            return
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
