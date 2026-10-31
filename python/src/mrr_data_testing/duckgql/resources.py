"""Launch the Rust resource matrix and collect its checked protocol receipt."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
from mrr_data_testing.process import TEST_LIMITS, run
from mrr_data_testing.workspace import repository_root

SCHEMA_PATH = (
    repository_root() / "crates/mrr-data-duckgql-query/query-resources-schema.json"
)
SCHEMA = json.loads(SCHEMA_PATH.read_text())["properties"]


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def compiled_test_binary(artifacts):
    binaries = set()
    for line in artifacts.read_text().splitlines():
        artifact = json.loads(line)
        if (
            artifact.get("reason") == "compiler-artifact"
            and artifact.get("target", {}).get("name") == "duckgql_graphar"
            and artifact.get("target", {}).get("kind") == ["test"]
            and artifact.get("executable")
        ):
            binaries.add(Path(artifact["executable"]).resolve(strict=True))
    if len(binaries) != 1:
        raise ValueError("one compiled DuckGQL integration-test binary required")
    return binaries.pop()


def matrix_from_output(output):
    matrices = [
        json.loads(line.removeprefix("QUERY-RESOURCE-MATRIX "))
        for line in output.decode().splitlines()
        if line.startswith("QUERY-RESOURCE-MATRIX ")
    ]
    if len(matrices) != 1:
        raise ValueError("one Rust matrix protocol receipt required")
    matrix = matrices[0]
    if (
        matrix.get("schema_namespace") != SCHEMA["schema_namespace"]["const"]
        or matrix.get("schema_version") != SCHEMA["schema_version"]["const"]
        or not isinstance(matrix.get("cases"), list)
    ):
        raise ValueError("Rust matrix protocol Schema mismatch")
    # Rust owns dispatch, immutable source parity, expected rows and admission.
    return matrix["cases"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--test-binary", type=Path)
    source.add_argument("--cargo-artifacts", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.unlink(missing_ok=True)
    binary = (
        args.test_binary.resolve(strict=True)
        if args.test_binary
        else compiled_test_binary(args.cargo_artifacts)
    )
    root = repository_root()
    metadata = {
        "schema_namespace": SCHEMA["schema_namespace"]["const"],
        "schema_version": SCHEMA["schema_version"]["const"],
        "schema_sha256": digest(SCHEMA_PATH),
        "program_schema_sha256": digest(
            root / "crates/mrr-data-duckgql-query/schema.json"
        ),
        "test_binary_sha256": digest(binary),
        "base_source_head": subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
        ).strip(),
        "working_tree_clean": not subprocess.check_output(
            ["git", "-C", str(root), "status", "--porcelain"]
        ),
        "matrix_verification": "Rust resources::matrix::isolated_engine_scale_matrix",
        "rss_scope": "isolated process high-water mark including fixture, Rust and active native engine; not an operator-only RSS ceiling",
    }
    if path := os.environ.get("MRR_DUCKGQL_EXTENSION"):
        metadata["extension_sha256"] = digest(path)
        metadata["extension_allow_unsigned"] = (
            os.environ.get("MRR_DUCKGQL_ALLOW_UNSIGNED") == "1"
        )
        if metadata["extension_sha256"] != os.environ.get("MRR_DUCKGQL_SHA256"):
            raise ValueError("Host extension digest mismatch")
    result = run(
        [
            binary,
            "resources::matrix::isolated_engine_scale_matrix",
            "--exact",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ],
        limits=TEST_LIMITS,
        cwd=root,
        capture_limit=1024 * 1024,
    )
    receipts = matrix_from_output(result.stdout)
    output.write_text(
        "\n".join(
            json.dumps(receipt, sort_keys=True) for receipt in [metadata, *receipts]
        )
        + "\n"
    )
    print(
        f"collected {len(receipts)} Rust-verified isolated cases: {output}", flush=True
    )


if __name__ == "__main__":
    main()
