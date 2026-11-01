import json
import pytest
from mrr_data_testing.duckgql.resources import (
    SCHEMA,
    compiled_test_binary,
    matrix_from_output,
)


def test_matrix_transport_requires_one_matching_schema_envelope():
    envelope = {
        "schema_namespace": SCHEMA["schema_namespace"]["const"],
        "schema_version": SCHEMA["schema_version"]["const"],
        "cases": [{"opaque_native_case": True}],
    }
    output = f"native progress\nQUERY-RESOURCE-MATRIX {json.dumps(envelope)}\n".encode()
    assert matrix_from_output(output) == envelope["cases"]
    for invalid in (b"test result: ok", output * 2):
        with pytest.raises(ValueError, match="one Rust matrix"):
            matrix_from_output(invalid)
    envelope["schema_version"] = 255
    with pytest.raises(ValueError, match="Schema mismatch"):
        matrix_from_output(f"QUERY-RESOURCE-MATRIX {json.dumps(envelope)}\n".encode())


def test_cargo_artifacts_require_one_matching_compiled_test(tmp_path):
    binary = tmp_path / "native-test"
    binary.write_bytes(b"native executable fixture")
    artifact = {
        "reason": "compiler-artifact",
        "target": {"name": "duckgql_graphar", "kind": ["test"]},
        "executable": str(binary),
    }
    log = tmp_path / "cargo-artifacts.jsonl"
    log.write_text(
        json.dumps(artifact)
        + "\n"
        + json.dumps({"reason": "build-finished", "success": True})
        + "\n"
    )
    assert compiled_test_binary(log) == binary
    log.write_text(
        json.dumps(artifact | {"target": {"name": "different-test", "kind": ["test"]}})
    )
    with pytest.raises(ValueError, match="one compiled"):
        compiled_test_binary(log)
