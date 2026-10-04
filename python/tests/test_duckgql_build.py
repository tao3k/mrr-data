import sys
import subprocess
import shutil

import pytest

from mrr_data_testing.duckgql import build
from mrr_data_testing.process import run


def test_failed_source_preparation_removes_previous_success(tmp_path, monkeypatch):
    receipt = tmp_path / "artifact.json"
    receipt.write_text('{"signed": false, "sha256": "previous-success"}')
    monkeypatch.setattr(sys, "argv", ["duckgql-build", "--work-dir", str(tmp_path)])

    def refuse(*args):
        raise RuntimeError("source identity mismatch")

    monkeypatch.setattr(build, "checkout", refuse)
    with pytest.raises(RuntimeError, match="identity mismatch"):
        build.main()
    assert not receipt.exists()


def test_invalid_revision_is_refused_before_subprocess(tmp_path, monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("invalid source revision reached subprocess")

    monkeypatch.setattr(build, "run", unexpected)
    with pytest.raises(SystemExit, match="invalid source revision"):
        build.checkout(tmp_path / "source", "https://example.invalid", "main")


def test_compiler_output_grouping_preserves_split_diagnostics_and_real_failure(capfd):
    output = build.CompilerOutput()
    assert output(b"Running pass: compiler-work\nclang: error: Run") == b""
    assert (
        output(b"ning pass: unsupported setting\n")
        == b"clang: error: Running pass: unsupported setting\n"
    )
    assert output(b" Running pass 1 InstCombinePass on function\n") == b""
    assert b"compiler pass events: 2" in output(b"")
    with pytest.raises(subprocess.CalledProcessError) as failure:
        run(
            [
                sys.executable,
                "-c",
                "print('Running pass: actual child output'); print('clang: error: failed compilation'); raise SystemExit(7)",
            ],
            output_filter=build.CompilerOutput(),
        )
    assert failure.value.returncode == 7
    assert "clang: error: failed compilation" in capfd.readouterr().out


def test_output_grouping_keeps_capture_bytes_unmodified():
    payload = "Running pass: compiler-work\nnormal diagnostic\n"
    result = run(
        [sys.executable, "-c", f"print({payload!r}, end='')"],
        output_filter=build.CompilerOutput(),
        capture_limit=1000,
    )
    assert result.stdout == payload.encode()


def test_rule_partition_preserves_native_linkage_and_results(tmp_path):
    compiler = shutil.which("c++")
    if compiler is None:
        pytest.skip("native C++ compiler unavailable")
    (tmp_path / "GQLParser.h").write_text(
        "#pragma once\nclass GQLParser { public: virtual ~GQLParser(); "
        "virtual int first() const; virtual int second() const; "
        "int value() const; static void initialize(); };\n"
    )
    source = tmp_path / "GQLParser.cpp"
    source.write_text(
        '#include "GQLParser.h"\n'
        "\nnamespace { int private_state() { return 21; } }\n"
        "int GQLParser::value() const { return private_state(); }\n"
        "GQLParser::~GQLParser() = default;\n"
        "//----------------- FirstContext -----------------\n"
        "int GQLParser::first() const { return value(); }\n"
        "//----------------- SecondContext -----------------\n"
        "int GQLParser::second() const { return value() * 2; }\n"
        "\nvoid GQLParser::initialize() {}\n"
    )
    main = tmp_path / "main.cpp"
    main.write_text(
        '#include "GQLParser.h"\n#include <iostream>\n'
        "int main() { GQLParser::initialize(); GQLParser p; "
        "std::cout << p.first() << ' ' << p.second(); }\n"
    )
    parts = build.parser_units(source, tmp_path / "parts", 2)
    for name, units in (("original", [source]), ("partitioned", parts)):
        binary = tmp_path / name
        subprocess.run(
            [
                compiler,
                "-std=c++17",
                "-O2",
                "-I",
                str(tmp_path),
                *map(str, units),
                str(main),
                "-o",
                str(binary),
            ],
            check=True,
            capture_output=True,
            timeout=20,
        )
        assert subprocess.check_output([binary], timeout=5) == b"21 42"
    source.write_text(
        source.read_text().replace("return value();", "return gqlParserStaticData;")
    )
    with pytest.raises(ValueError, match="private state"):
        build.parser_units(source, tmp_path / "refused", 2)
    assert not (tmp_path / "refused").exists()
