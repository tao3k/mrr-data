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


def matrix_from_output(output, scale_rows=None, workload_scale=None):
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
        or (scale_rows is not None and matrix.get("scale_rows") != scale_rows)
        or (workload_scale is not None and matrix.get("scale") != workload_scale)
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
        default=SCHEMA["workload_scales"]["const"],
        help="bounded Scenario workload multipliers",
    )
    parser.add_argument(
        "--extra-rows",
        nargs="+",
        type=int,
        default=SCHEMA["scales"]["const"],
        help="additional relation rows; large scopes use workload multiplier 1",
    )
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    scopes = [(scale, 4) for scale in args.scales if 4 in args.extra_rows]
    scopes.extend(
        (1, rows) for rows in args.extra_rows if rows != 4 and 1 in args.scales
    )
    if any(rows != 4 for rows in args.extra_rows) and 1 not in args.scales:
        parser.error("large additional-row scopes require workload multiplier 1")
    if not scopes:
        parser.error("no supported requested scale scope")
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
        "requested_extra_rows": args.extra_rows,
        "case_scopes": [{"scale": scale, "scale_rows": rows} for scale, rows in scopes],
        "scale_unit": "scale multiplies four nonmatching Scenario edges; scale_rows selects four-row baseline or added unique Scenario/Case/Profile rows at base workload",
        "unsupported_measurements": SCHEMA["unsupported_measurements"]["const"],
        "progress_limits_scope": "each complete Rust scale matrix retains idle5/wall120; every mode shares one immutable shape/scale closure",
        "cache_scope": "cold is an empty verified content cache; OS page-cache state is uncontrolled",
        "rss_scope": "isolated process high-water mark including fixture, Rust and native engine",
        "physical_scope": "physical_backend_ns includes execution and candidate projection; engine_first_nonempty_batch_ns excludes planning and precedes MRR admission; decoded_utf8_copy_bytes is output decoding only; observed spill covers reporting operators only",
    }
    cases = []
    for workload_scale, scale_rows in scopes:
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
                os.environ,
                MRR_NATIVE_PROGRESS="1",
                MRR_DATA_SOURCE_SCALE=str(workload_scale),
                MRR_DATA_SOURCE_EXTRA_ROWS=str(scale_rows),
            ),
            capture_limit=2 << 20,
        )
        cases.extend(matrix_from_output(result.stdout, scale_rows, workload_scale))
    with output.open("w") as stream:
        stream.write(json.dumps({"metadata": metadata}, sort_keys=True) + "\n")
        for case in cases:
            stream.write(json.dumps(case, sort_keys=True) + "\n")
    print(
        f"collected {len(cases)} Rust original-source process receipts at {output}",
        flush=True,
    )
