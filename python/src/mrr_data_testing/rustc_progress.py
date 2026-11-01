#!/usr/bin/env python3
"""Expose real Rust front-end and LLVM work, preserving compiler flags."""

import os
import sys
import shutil
import subprocess
import stat
from pathlib import Path


def host_tool(name):
    """An explicit environment path is authoritative; otherwise discover PATH."""
    variable = f"MRR_DATA_{name.upper()}"
    selected = os.environ.get(variable)
    if selected is None:
        return shutil.which(name)
    if not selected or not Path(selected).is_file() or not os.access(selected, os.X_OK):
        raise ValueError(f"{variable} must name an executable file")
    return selected


def compiler_processes(pid):
    """Only the live compiler's descendant tree, never adjacent Cargo jobs."""
    found = {pid}
    pending = [pid]
    while pending:
        parent = pending.pop()
        tasks = Path(f"/proc/{parent}/task")
        if tasks.is_dir():
            children = []
            for child_list in tasks.glob("*/children"):
                try:
                    children.extend(map(int, child_list.read_text().split()))
                except (OSError, ValueError):
                    pass
        elif sys.platform == "darwin" and (pgrep := host_tool("pgrep")):
            try:
                result = subprocess.run(
                    [pgrep, "-P", str(parent)],
                    capture_output=True,
                    timeout=2,
                    check=False,
                )
                children = [int(child) for child in result.stdout.split()]
            except (OSError, ValueError, subprocess.TimeoutExpired):
                children = []
        else:
            children = []
        for child in children:
            if child not in found:
                found.add(child)
                pending.append(child)
    return found


def compiler_artifacts(pid):
    """Only writable compiler artifact descriptors, never adjacent builds."""
    descriptors = Path(f"/proc/{pid}/fd")
    if descriptors.is_dir():
        paths = []
        try:
            entries = list(descriptors.iterdir())
        except OSError:
            # A descendant can exit or lose inspectability after discovery.
            # Missing observation is not progress; preserve the compiler result.
            return []
        for descriptor in entries:
            if descriptor.name.isdigit() and int(descriptor.name) < 3:
                continue
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
    elif sys.platform == "darwin" and (lsof := host_tool("lsof")):
        try:
            result = subprocess.run(
                [lsof, "-a", "-p", str(pid), "-Ffan"],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=2,
                check=False,
            )
            paths = []
            writable = False
            standard = True
            for line in result.stdout.decode().splitlines():
                if line.startswith("f"):
                    writable = False
                    standard = line[1:].isdigit() and int(line[1:]) < 3
                elif line.startswith("a"):
                    writable = line[1:] in {"w", "u"}
                elif line.startswith("n/") and writable and not standard:
                    paths.append(Path(line[1:]))
        except (OSError, subprocess.TimeoutExpired):
            return []
    else:
        return []
    artifacts = []
    for path in paths:
        try:
            if stat.S_ISREG(path.stat().st_mode):
                artifacts.append(path)
        except OSError:
            pass
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
    inherited = set()
    for path in compiler_artifacts(os.getpid()):
        try:
            info = path.stat()
            inherited.add((info.st_dev, info.st_ino))
        except OSError:
            pass
    child = subprocess.Popen(command, close_fds=False)
    observed = {}
    directories = set()
    while child.poll() is None:
        paths = []
        artifacts = (
            artifact
            for owner in compiler_processes(child.pid)
            for artifact in compiler_artifacts(owner)
        )
        for artifact in artifacts:
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
            if key in inherited:
                continue
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
    emits_link = any(arg.startswith("--emit=") and "link" in arg for arg in arguments)
    if not nested:
        os.environ.setdefault(
            "RUSTC_LOG",
            "rustc_hir_typeck::coercion=info,rustc_borrowck::region_infer=info,rustc_interface::passes=info,rustc_codegen_ssa::base=info",
        )
    if not nested and emits_link:
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
    if not nested and emits_link:
        # Every linker/archive tail needs owned-output observation; verbose LLVM
        # tracing remains restricted to the explicit crate selection above.
        raise SystemExit(run_observed([compiler, *arguments]))
    os.execvp(compiler, [compiler, *arguments])


if __name__ == "__main__":
    main()
