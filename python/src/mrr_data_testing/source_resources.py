"""Supervise Rust's original-source process matrix and collect its receipts."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

from mrr_data_testing.process import TEST_LIMITS, run
from mrr_data_testing.execution import compiled_test_binaries
from mrr_data_testing.workspace import repository_root

SCHEMA_PATH = repository_root() / "crates/mrr-data-graphar/source-resources-schema.json"
SCHEMA = json.loads(SCHEMA_PATH.read_text())["properties"]
MATRIX_TEST = "tests::entity_properties::combined::source_handoff::backend::selective::resources::matrix::original_source_resource_matrix"


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def matrix_from_output(output):
    receipts = [
        json.loads(line.removeprefix("SOURCE-RESOURCE-MATRIX "))
        for line in output.decode().splitlines()
        if line.startswith("SOURCE-RESOURCE-MATRIX ")
    ]
    if len(receipts) != 1:
        raise ValueError("one Rust Source matrix protocol receipt required")
    matrix = receipts[0]
    if (
        matrix.get("schema_namespace") != SCHEMA["schema_namespace"]["const"]
        or matrix.get("schema_version") != SCHEMA["schema_version"]["const"]
        or not isinstance(matrix.get("cases"), list)
    ):
        raise ValueError("Rust Source matrix protocol Schema mismatch")
    return matrix["cases"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--test-binary", type=Path)
    source.add_argument("--cargo-artifacts", type=Path)
    parser.add_argument(
        "--scales",
        nargs="+",
        type=int,
        default=[1],
        help="bounded Rust fixture scales; each matrix has its own unchanged watchdog",
    )
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    binary = (
        args.test_binary.resolve(strict=True)
        if args.test_binary
        else compiled_test_binaries(args.cargo_artifacts, ["mrr_data_graphar"])[0]
    )
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.unlink(missing_ok=True)
    root = repository_root()
    metadata = {
        "schema_namespace": SCHEMA["schema_namespace"]["const"],
        "schema_version": SCHEMA["schema_version"]["const"],
        "schema_sha256": digest(SCHEMA_PATH),
        "test_binary_sha256": digest(binary),
        "cargo_artifacts_sha256": digest(args.cargo_artifacts)
        if args.cargo_artifacts
        else None,
        "head_scope": "collection checkout; compiled source identity requires a matching build receipt",
        "base_source_head": subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
        ).strip(),
        "working_tree_clean": not subprocess.check_output(
            ["git", "-C", str(root), "status", "--porcelain"]
        ),
        "rust_matrix": MATRIX_TEST,
        "requested_scales": args.scales,
        "cache_scope": "cold is an empty verified content cache; OS page-cache state is uncontrolled",
        "rss_scope": "isolated process high-water mark including fixture, Rust and native engine",
        "physical_scope": "physical_backend_ns includes execution and candidate projection; engine_first_nonempty_batch_ns excludes planning and precedes MRR admission; decoded_utf8_copy_bytes is only output decoding; observed spill counts reporting operators only",
    }
    cases = []
    for scale in args.scales:
        result = run(
            [
                binary,
                MATRIX_TEST,
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ],
            limits=TEST_LIMITS,
            env=dict(
                os.environ, MRR_NATIVE_PROGRESS="1", MRR_DATA_SOURCE_SCALE=str(scale)
            ),
            capture_limit=2 << 20,
        )
        cases.extend(matrix_from_output(result.stdout))
    with output.open("w") as stream:
        stream.write(json.dumps({"metadata": metadata}, sort_keys=True) + "\n")
        for case in cases:
            stream.write(json.dumps(case, sort_keys=True) + "\n")
    print(
        f"collected {len(cases)} Rust original-source process receipts at {output}",
        flush=True,
    )
