"""Run an existing test or build command under a declared phase policy."""

import argparse

from mrr_data_testing.process import BUILD_LIMITS, CONTROL_LIMITS, TEST_LIMITS, run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=["test", "build", "control"], required=True)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.arguments[1:] if args.arguments[:1] == ["--"] else args.arguments
    if not command:
        parser.error("a subprocess command is required after --")
    policies = {"test": TEST_LIMITS, "build": BUILD_LIMITS, "control": CONTROL_LIMITS}
    run(command, limits=policies[args.phase])
