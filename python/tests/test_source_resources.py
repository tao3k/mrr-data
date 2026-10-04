import json

import pytest

from mrr_data_testing.source_resources import SCHEMA, matrix_from_output


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
