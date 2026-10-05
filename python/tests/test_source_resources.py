import json
import sys
from types import SimpleNamespace

import pytest

from mrr_data_testing.source_resources import SCHEMA, matrix_from_output
from mrr_data_testing import source_resources


def test_source_transport_requires_one_matching_schema_envelope():
    envelope = {
        "schema_namespace": SCHEMA["schema_namespace"]["const"],
        "schema_version": SCHEMA["schema_version"]["const"],
        "cases": [{"opaque_native_case": True}],
    }
    output = (
        f"native progress\nSOURCE-RESOURCE-MATRIX {json.dumps(envelope)}\n".encode()
    )
    assert matrix_from_output(output) == envelope["cases"]
    for invalid in (b"test result: ok", output * 2):
        with pytest.raises(ValueError, match="one Rust Source matrix"):
            matrix_from_output(invalid)
    envelope["schema_version"] = 255
    with pytest.raises(ValueError, match="Schema mismatch"):
        matrix_from_output(f"SOURCE-RESOURCE-MATRIX {json.dumps(envelope)}\n".encode())


def test_scale_processes_preserve_native_payloads_and_phase_limits(
    tmp_path, monkeypatch
):
    binary = tmp_path / "native-owner"
    binary.write_bytes(b"inert test binary identity")
    output = tmp_path / "receipts.jsonl"
    calls = []

    def run(command, *, limits, env, capture_limit):
        calls.append((command, limits, env, capture_limit))
        envelope = {
            "schema_namespace": SCHEMA["schema_namespace"]["const"],
            "schema_version": SCHEMA["schema_version"]["const"],
            "scale_rows": int(env["MRR_DATA_SOURCE_SCALE"]),
            "cases": [{"opaque_native_payload": env["MRR_DATA_SOURCE_SCALE"]}],
        }
        return SimpleNamespace(
            stdout=f"SOURCE-RESOURCE-MATRIX {json.dumps(envelope)}\n".encode()
        )

    monkeypatch.setattr(source_resources, "run", run)
    monkeypatch.setattr(
        source_resources.subprocess,
        "check_output",
        lambda arguments, **kwargs: (
            "collection-head\n" if arguments[-1] == "HEAD" else b""
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["source-resources", "--test-binary", str(binary), "--output", str(output)],
    )
    source_resources.main()
    assert [call[2]["MRR_DATA_SOURCE_SCALE"] for call in calls] == [
        str(rows) for rows in SCHEMA["scales"]["const"]
    ]
    assert all(call[1] is source_resources.TEST_LIMITS for call in calls)
    records = [json.loads(line) for line in output.read_text().splitlines()]
    assert records[1:] == [
        {"opaque_native_payload": str(rows)} for rows in SCHEMA["scales"]["const"]
    ]
