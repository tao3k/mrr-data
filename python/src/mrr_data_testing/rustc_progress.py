#!/usr/bin/env python3
"""Expose real Rust front-end and LLVM work, preserving compiler flags."""

import os
import sys


def main():
    compiler, *arguments = sys.argv[1:]
    nested = os.path.realpath(compiler) == os.path.realpath(sys.argv[0])
    if not nested:
        os.environ.setdefault(
            "RUSTC_LOG",
            "rustc_hir_typeck::coercion=info,rustc_borrowck::region_infer=info,rustc_interface::passes=info",
        )
    if not nested and any(
        arg.startswith("--emit=") and "link" in arg for arg in arguments
    ):
        arguments.extend(["-C", "remark=inline prologepilog"])
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
                arguments.extend(["-C", "llvm-args=--print-pass-numbers"])
    os.execvp(compiler, [compiler, *arguments])


if __name__ == "__main__":
    main()
