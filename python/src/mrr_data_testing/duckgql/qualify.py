#!/usr/bin/env python3
"""Qualify a Host-installed DuckGQL typed interface; never install extensions.

This native fixture is not an MRR BoundDataQuery bridge or a GraphAr benchmark.
The Host provides an exact CLI and signed/pinned extension installation.
"""

import argparse
import csv
import hashlib
import io
import json
from pathlib import Path
from mrr_data_testing.workspace import repository_root
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", required=True)
    artifacts = parser.add_mutually_exclusive_group(required=True)
    artifacts.add_argument("--extension-directory")
    artifacts.add_argument("--extension-path")
    parser.add_argument("--allow-unsigned", action="store_true")
    args = parser.parse_args()
    if args.extension_directory:
        directory = str(Path(args.extension_directory).resolve()).replace("'", "''")
        prefix = f"SET extension_directory='{directory}'; LOAD duckgql;\n"
    else:
        artifact = str(Path(args.extension_path).resolve()).replace("'", "''")
        prefix = f"LOAD '{artifact}';\n"
    schema_path = repository_root() / "crates/mrr-data-duckgql-query/schema.json"
    schema = json.loads(schema_path.read_text())
    program_version = schema["properties"]["program_version"]["const"]
    template = Path(__file__).with_name("typed-match.sql").read_text()
    fixture = template.replace("__PROGRAM_VERSION__", str(program_version))
    carrier_path = Path(__file__).with_name("parameter-transport.sql")
    carrier = carrier_path.read_text()

    def run(source):
        command = [args.duckdb, "-bail", "-noheader", "-csv"]
        if args.allow_unsigned:
            command.append("-unsigned")
        return subprocess.run(
            command,
            input=source,
            text=True,
            capture_output=True,
            timeout=20,
            check=False,
        )

    version = run(
        prefix
        + "SELECT version(), extension_version, install_path FROM duckdb_extensions() WHERE extension_name='duckgql';"
    )
    if version.returncode:
        raise SystemExit(version.stderr)
    identities = list(csv.reader(io.StringIO(version.stdout)))
    if len(identities) != 1 or len(identities[0]) != 3:
        raise SystemExit("missing loaded extension identity")
    duckdb_version, extension_version, extension_path = identities[0]
    if not extension_path and args.extension_path:
        extension_path = args.extension_path
    if not extension_path:
        raise SystemExit("loaded extension did not expose an artifact path")

    def digest(path):
        with Path(path).open("rb") as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest()

    print(
        json.dumps(
            {
                "stage": "artifact-identity",
                "duckdb_version": duckdb_version,
                "extension_version": extension_version,
                "duckdb_sha256": digest(args.duckdb),
                "extension_sha256": digest(extension_path),
                "fixture_sha256": digest(Path(__file__).with_name("typed-match.sql")),
                "carrier_sha256": digest(carrier_path),
                "schema_sha256": digest(schema_path),
            }
        ),
        flush=True,
    )
    started = time.monotonic()
    expected = [
        ["zoe", "yan", "edge-a"],
        ["alice", "bob", "edge-b"],
        ["alice", "bob", "edge-c"],
    ]

    def read_rows(result):
        if result.returncode:
            raise SystemExit(result.stderr)
        rows = list(csv.reader(io.StringIO(result.stdout)))
        # CREATE GRAPH and SESSION SET GRAPH each return a setup receipt.
        if rows[:2] != [["true", "mrr_audit"], ["true", "mrr_audit"]]:
            raise SystemExit(f"unexpected registration receipts: {rows[:2]!r}")
        return rows[2:]

    # This is an independent native interface probe, not a weakened substitute
    # for the labelled query. MRR integration must pass the labelled gate below.
    unlabelled = fixture.replace(
        "['node', 'knows', 'node']::VARCHAR[]", "['', 'knows', '']::VARCHAR[]"
    )
    rows = read_rows(run(prefix + unlabelled))
    if rows != expected:
        raise SystemExit(f"unexpected native probe rows: {rows!r}")
    print(
        json.dumps(
            {
                "stage": "native-program-probe",
                "program_version": program_version,
                "rows": len(rows),
                "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
            }
        ),
        flush=True,
    )
    wrong_version = template.replace("__PROGRAM_VERSION__", "255")
    refused = run(prefix + wrong_version)
    if (
        refused.returncode == 0
        or "Invalid GQL relational MATCH input" not in refused.stderr
    ):
        raise SystemExit(
            "unsupported program version was not refused with the expected diagnostic"
        )
    print(json.dumps({"stage": "version-refusal", "passed": True}), flush=True)
    labelled = read_rows(run(prefix + fixture))
    setup = fixture.split("SELECT * FROM gql_match_relational(", 1)[0]
    control = "MATCH (a:Node)-[e:knows]->(b:Node) RETURN a.mrr_entity, b.mrr_entity, e.mrr_fact ORDER BY e.mrr_fact;"
    # The text query is an audit control for source/label setup, never the MRR
    # adapter or an execution fallback. Both paths must retain the constraints.
    control_rows = read_rows(run(prefix + setup + control))
    if control_rows != expected:
        raise SystemExit(f"registration control rows differ: {control_rows!r}")
    passed = labelled == expected
    print(
        json.dumps(
            {
                "stage": "labelled-program-parity",
                "passed": passed,
                "typed_rows": len(labelled),
                "control_rows": len(control_rows),
            }
        ),
        flush=True,
    )
    if not passed:
        raise SystemExit(
            "labelled typed-program gate failed; no MRR DuckGQL integration is qualified"
        )

    projections = []
    for index, result_type, property_name in [
        (0, 8, "mrr_entity"),
        (2, 8, "mrr_entity"),
        (1, 9, "mrr_fact"),
    ]:
        projections.append(
            {
                "node_types": [2, 1],
                "result_types": [7, result_type],
                "binding_indices": [index, index],
                "operators": [0, 0],
                "values": ["", ""],
                "properties": [property_name, ""],
                "child_counts": [0, 0],
                "aggregate": [False, False],
                "distinct": [False, False],
            }
        )

    def carrier_query(version, projection_json):
        # EXECUTE is a CLI qualification of prepared scalar parameters. A Rust
        # adapter must use Statement bindings, not generate this EXECUTE text.
        # These aliases also exercise quoted/Unicode/injection-shaped data.
        payloads = [
            json.dumps(["node", "knows", "node"]),
            projection_json,
            json.dumps(["source'", "目标", "fact'); DROP TABLE audit_edges; --"]),
            json.dumps([2]),
        ]
        quoted = ["'" + value.replace("'", "''") + "'" for value in payloads]
        return (
            setup
            + carrier
            + "\nEXECUTE typed_carrier("
            + str(version)
            + ", "
            + ", ".join(quoted)
            + ");\n"
        )

    transported = read_rows(
        run(prefix + carrier_query(program_version, json.dumps(projections)))
    )
    if transported != expected:
        raise SystemExit(f"prepared scalar transport rows differ: {transported!r}")
    print(
        json.dumps(
            {
                "stage": "prepared-scalar-transport",
                "passed": True,
                "rows": len(transported),
                "scalar_parameters": 5,
            }
        ),
        flush=True,
    )
    for name, version, payload, diagnostic in [
        ("version", 255, json.dumps(projections), "Invalid GQL relational MATCH input"),
        ("malformed-json", program_version, "{broken", "Conversion Error"),
        (
            "wrong-type",
            program_version,
            '[{"node_types":["not-an-integer"]}]',
            "Conversion Error",
        ),
    ]:
        refused = run(prefix + carrier_query(version, payload))
        if refused.returncode == 0 or diagnostic not in refused.stderr:
            raise SystemExit(
                f"prepared carrier {name} refusal failed: {refused.stderr}"
            )
        if list(csv.reader(io.StringIO(refused.stdout)))[2:]:
            raise SystemExit(f"prepared carrier {name} published partial rows")
        print(
            json.dumps(
                {"stage": "prepared-carrier-refusal", "case": name, "passed": True}
            ),
            flush=True,
        )


if __name__ == "__main__":
    main()
