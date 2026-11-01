"""Run an existing test or build command under a declared phase policy."""

import argparse
from contextlib import nullcontext
import json
import re
from pathlib import Path

from mrr_data_testing.process import BUILD_LIMITS, CONTROL_LIMITS, TEST_LIMITS, run


class CargoArtifactCapture:
    """Keep Cargo records separate from live, very-verbose build diagnostics."""

    def __init__(self, output, *, protocol=True, native_output=None):
        self.output = output
        self.protocol = protocol
        self.pending = b""
        self.finished = []
        self.events = 0
        self.frontend_events = 0
        self.pass_events = 0
        self.native_output = native_output
        self.native_note = False
        self.native_events = 0
        self.native_bytes = 0

    def __call__(self, block):
        lines = (self.pending + block).splitlines(keepends=True)
        self.pending = b""
        if block and lines and not lines[-1].endswith(b"\n"):
            self.pending = lines.pop()
        forwarded = bytearray()
        for line in lines:
            if self.native_diagnostic(line):
                if self.native_events >= 256:
                    forwarded.extend(self.native_summary())
                continue
            if not self.protocol or not line.startswith(b"{"):
                if (
                    re.match(rb"^\s*Running pass\b", line)
                    and b'{"' not in line
                    and b"error:" not in line
                    and b"warning:" not in line
                ):
                    # LLVM worker threads can interleave pass headers. Count
                    # actual header occurrences; retain mixed JSON/errors raw.
                    self.pass_events += line.count(b"Running pass")
                    if self.pass_events >= 256:
                        forwarded.extend(self.pass_summary())
                    continue
                if re.match(
                    rb"^\s*(?:\d+(?:\.\d+)?(?:ns|us|ms|s)\s+)?(?:INFO )?rustc_(?:hir_typeck::coercion|borrowck::region_infer|interface::passes|codegen_ssa::base)\b",
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
                    for event in (" inline (", " prologepilog (", " asm-printer (")
                )
            ):
                self.events += 1
                if self.events >= 256:
                    forwarded.extend(self.summary())
            else:
                forwarded.extend(line)
        if not block and self.pass_events:
            forwarded.extend(self.pass_summary())
        if not block and self.events:
            forwarded.extend(self.summary())
        if not block and self.frontend_events:
            forwarded.extend(self.frontend_summary())
        if not block and self.native_events:
            forwarded.extend(self.native_summary())
        if self.native_output is not None:
            self.native_output.flush()
        if self.output is not None:
            self.output.flush()
        return bytes(forwarded)

    def native_diagnostic(self, line):
        if self.native_output is None:
            return False
        match = re.match(rb"^\[mrr-gerbil [^\]]+\] (.*)", line, re.DOTALL)
        if match is None:
            return False
        self.native_output.write(line)
        payload = match[1].strip().lower()
        if payload.startswith(
            (
                b"cargo:",
                b"[mrr-gerbil-native]",
                b"native compiler artifact=",
                b"phase ",
                b"total ",
                b"time variable",
                b"***",
            )
        ) or any(
            marker in payload
            for marker in (
                b"error",
                b"warning",
                b"failed",
                b"panic",
                b"fatal",
                b"exception",
                b"abort",
                b"sorry",
                b"assertion",
                b"segmentation",
            )
        ):
            self.native_note = False
            return False
        if re.search(rb": (?:note|missed|optimized):", payload):
            self.native_note = True
        if not self.native_note:
            return False
        self.native_events += 1
        self.native_bytes += len(line)
        return True

    def native_summary(self):
        summary = f"native compiler diagnostics: events={self.native_events} bytes={self.native_bytes}; raw retained\n".encode()
        self.native_events = self.native_bytes = 0
        return summary

    def retain_native_tail(self):
        if self.native_output is not None and self.pending.startswith(b"[mrr-gerbil "):
            self.native_output.write(self.pending)
            self.native_output.flush()

    def pass_summary(self):
        output = f"LLVM new-PM pass events: {self.pass_events}\n".encode()
        self.pass_events = 0
        return output

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


def capture_cargo_build(command, artifacts, *, compact_native=False):
    try:
        with (
            artifacts.open("wb") as output,
            artifacts.with_suffix(".native-stdout.log").open("ab")
            if compact_native
            else nullcontext(None) as native_stdout,
            artifacts.with_suffix(".native-stderr.log").open("ab")
            if compact_native
            else nullcontext(None) as native_stderr,
        ):
            capture = CargoArtifactCapture(output, native_output=native_stdout)
            errors = CargoArtifactCapture(
                None, protocol=False, native_output=native_stderr
            )
            try:
                run(
                    command,
                    limits=BUILD_LIMITS,
                    output_filter=capture,
                    stderr_output_filter=errors,
                )
                capture.complete()
            finally:
                capture.retain_native_tail()
                errors.retain_native_tail()
    except BaseException:
        artifacts.unlink(missing_ok=True)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=["test", "build", "control"], required=True)
    parser.add_argument("--cargo-artifacts", type=Path)
    parser.add_argument("--compact-native-diagnostics", action="store_true")
    parser.add_argument("--test-target", action="append", default=[])
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.compact_native_diagnostics and (
        args.phase != "build" or not args.cargo_artifacts
    ):
        parser.error("native diagnostic compaction requires a Cargo artifact build")
    command = args.arguments[1:] if args.arguments[:1] == ["--"] else args.arguments
    if args.cargo_artifacts:
        if args.phase == "build":
            if args.test_target or not command:
                parser.error("Cargo artifact capture requires a build command")
            capture_cargo_build(
                command,
                args.cargo_artifacts,
                compact_native=args.compact_native_diagnostics,
            )
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
