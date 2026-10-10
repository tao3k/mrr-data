#!/usr/bin/env python3
"""Expose real Rust front-end and LLVM work, preserving compiler flags."""

import os
import sys
import shutil
import subprocess
from pathlib import Path


def compiler_artifacts(pid):
    """Only writable compiler artifact descriptors, never adjacent builds."""
    descriptors = Path(f"/proc/{pid}/fd")
    if descriptors.is_dir():
        paths = []
        for descriptor in descriptors.iterdir():
            try:
                info = (Path(f"/proc/{pid}/fdinfo") / descriptor.name).read_text()
                flags = next(
                    line.split()[1]
                    for line in info.splitlines()
                    if line.startswith("flags:")
                )
                if int(flags, 8) & os.O_ACCMODE:
                    paths.append(Path(os.readlink(descriptor)))
            except (OSError, StopIteration, ValueError):
                pass
    elif sys.platform == "darwin" and shutil.which("lsof"):
        try:
            result = subprocess.run(
                ["lsof", "-a", "-p", str(pid), "-Ffan"],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=2,
                check=False,
            )
            paths = []
            writable = False
            for line in result.stdout.decode().splitlines():
                if line.startswith("f"):
                    writable = False
                elif line.startswith("a"):
                    writable = line[1:] in {"w", "u"}
                elif line.startswith("n/") and writable:
                    paths.append(Path(line[1:]))
        except (OSError, subprocess.TimeoutExpired):
            return []
    else:
        return []
    artifacts = [
        path
        for path in paths
        if (
            path.name in {"full.rmeta", "stub.rmeta"}
            and path.parent.name.startswith("rmeta")
        )
        or (
            path.name == "dep-graph.part.bin"
            and path.parent.name.endswith("-working")
            and path.parent.parent.parent.name == "incremental"
        )
    ]
    # rustc's writable generation lock identifies one private working directory.
    # Observe closed CGU/LTO outputs too; never scan the shared target directory.
    artifacts.extend(
        path.with_name(path.stem + "-working")
        for path in paths
        if path.suffix == ".lock" and path.parent.parent.name == "incremental"
    )
    return artifacts


def incremental_outputs(directory):
    try:
        return [
            path for path in directory.iterdir() if path.suffix in {".o", ".bc", ".bin"}
        ]
    except OSError:
        return []


def run_observed(command):
    child = subprocess.Popen(command, close_fds=False)
    observed = {}
    directories = set()
    while child.poll() is None:
        paths = []
        for artifact in compiler_artifacts(child.pid):
            if artifact.name.endswith("-working"):
                outputs = incremental_outputs(artifact)
                if artifact not in directories:
                    # Existing cache inputs are not a fresh progress event.
                    for path in outputs:
                        try:
                            stat = path.stat()
                            observed.setdefault(
                                (stat.st_dev, stat.st_ino), stat.st_size
                            )
                        except OSError:
                            pass
                    directories.add(artifact)
                paths.extend(outputs)
            else:
                paths.append(artifact)
        for path in paths:
            try:
                stat = path.stat()
            except OSError:
                continue
            key = (stat.st_dev, stat.st_ino)
            previous = observed.get(key, 0)
            if stat.st_size > previous:
                kind = (
                    "dep-graph"
                    if path.name == "dep-graph.part.bin"
                    else "metadata"
                    if path.suffix == ".rmeta"
                    else "codegen"
                )
                print(
                    f"rustc {kind}-write pid={child.pid} bytes={stat.st_size} file={path}",
                    file=sys.stderr,
                    flush=True,
                )
                observed[key] = stat.st_size
        try:
            child.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            pass
    code = child.returncode
    if code < 0:
        import signal

        signal.signal(-code, signal.SIG_DFL)
        os.kill(os.getpid(), -code)
    return code


def main():
    compiler, *arguments = sys.argv[1:]
    observed = False
    nested = os.path.realpath(compiler) == os.path.realpath(sys.argv[0])
    if not nested:
        os.environ.setdefault(
            "RUSTC_LOG",
            "rustc_hir_typeck::coercion=info,rustc_borrowck::region_infer=info,rustc_interface::passes=info,rustc_codegen_ssa::base=info",
        )
    if not nested and any(
        arg.startswith("--emit=") and "link" in arg for arg in arguments
    ):
        if os.environ.get("MRR_DATA_RUSTC_PHASE_TRACE_CRATES"):
            crate_index = (
                arguments.index("--crate-name") if "--crate-name" in arguments else -1
            )
            crate = (
                arguments[crate_index + 1]
                if 0 <= crate_index < len(arguments) - 1
                else ""
            )
            if crate in os.environ["MRR_DATA_RUSTC_PHASE_TRACE_CRATES"].split(","):
                observed = True
        remarks = (
            "inline prologepilog asm-printer" if observed else "inline prologepilog"
        )
        arguments.extend(["-C", f"remark={remarks}"])
    if observed:
        # The sampled quiet tail is the new-PM ThinLTO optimizer. Pass ordinals
        # expose its actual work without dumping IR or legacy machine passes.
        arguments.extend(["-C", "llvm-args=--print-pass-numbers"])
        raise SystemExit(run_observed([compiler, *arguments]))
    os.execvp(compiler, [compiler, *arguments])


if __name__ == "__main__":
    main()
