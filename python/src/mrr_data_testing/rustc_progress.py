#!/usr/bin/env python3
"""Cargo workspace compiler wrapper exposing real LLVM work, preserving flags."""

import os
import sys


def main():
    compiler, *arguments = sys.argv[1:]
    if any(arg.startswith("--emit=") and "link" in arg for arg in arguments):
        arguments.extend(
            [
                "-C",
                "llvm-args=-print-before=instcombine",
                "-C",
                "llvm-args=-debug-pass=Executions",
            ]
        )
    os.execvp(compiler, [compiler, *arguments])


if __name__ == "__main__":
    main()
