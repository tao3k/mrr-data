"""One installed entry point for qualification commands."""

import argparse
import importlib
import sys

COMMANDS = {
    "run-process": "execution",
    "check-docs": "checks.docs",
    "check-features": "checks.features",
    "check-spec-fixtures": "checks.spec_fixtures",
    "duckgql-build": "duckgql.build",
    "duckgql-qualify": "duckgql.qualify",
    "query-resources": "duckgql.resources",
    "source-resources": "source_resources",
    "s3-conformance": "s3.run",
    "kache-probe": "kache.run",
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=COMMANDS)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    sys.argv = [f"mrr-data-test {args.command}", *args.arguments]
    importlib.import_module(f"mrr_data_testing.{COMMANDS[args.command]}").main()
