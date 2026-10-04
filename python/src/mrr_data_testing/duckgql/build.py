#!/usr/bin/env python3
"""Build a pinned local SDK artifact through upstream CMake, never install it."""

import argparse
import hashlib
import json
from pathlib import Path
from mrr_data_testing.workspace import repository_root
import re
import subprocess
import urllib.request
import zipfile
from mrr_data_testing.process import BUILD_LIMITS, CONTROL_LIMITS, run as run_process


class CompilerOutput:
    """Group real verbose compiler events; preserve other output and diagnostics."""

    def __init__(self):
        self.pending = b""
        self.events = 0
        self.latest = b""

    def __call__(self, block):
        lines = (self.pending + block).splitlines(keepends=True)
        self.pending = b""
        if block and lines and not lines[-1].endswith(b"\n"):
            self.pending = lines.pop()
        output = bytearray()
        for line in lines:
            event = (
                line.startswith(
                    (b"Running pass:", b"Running analysis:", b"Invalidating analysis:")
                )
                or bool(re.match(rb"\s+Running pass \d+ ", line))
                or bool(
                    re.match(
                        rb"\[\d{4}-[^]]+\] 0x[0-9a-f]+\s+(Executing Pass|Freeing Pass|Made Modification) '",
                        line,
                    )
                )
            )
            if event:
                self.events += 1
                self.latest = line.strip()[-200:]
                if self.events >= 256:
                    output.extend(self.summary())
            else:
                output.extend(line)
        if not block and self.events:
            output.extend(self.summary())
        return bytes(output)

    def summary(self):
        output = (
            b"compiler pass events: "
            + str(self.events).encode()
            + b"; latest: "
            + self.latest
            + b"\n"
        )
        self.events = 0
        return output


def parser_units(source, directory, count):
    """Partition pinned ANTLR rule blocks without changing their definitions."""
    original = source.read_bytes()
    markers = list(re.finditer(rb"(?m)^//----------------- ", original))
    footer = original.rfind(b"\nvoid GQLParser::initialize()")
    namespace = original.find(b"\nnamespace {")
    if len(markers) < count or not 0 < namespace < markers[0].start() < footer:
        raise ValueError("unexpected pinned ANTLR parser framing")
    start = markers[0].start()
    prefix, body, suffix = original[:start], original[start:footer], original[footer:]
    for private in (
        b"gqlParserStaticData",
        b"gqlParserOnceFlag",
        b"gqlParserInitialize",
        b"GQLParserStaticData",
    ):
        if private in body:
            raise ValueError("rule block references translation-unit private state")
    offsets = [marker.start() for marker in markers] + [footer]
    blocks = [original[left:right] for left, right in zip(offsets, offsets[1:])]
    if prefix + b"".join(blocks) + suffix != original:
        raise ValueError("ANTLR rule partition changed source bytes")
    directory.mkdir(parents=True, exist_ok=True)
    primary = directory / "parser-initializer.cpp"
    primary.write_bytes(prefix + suffix)
    files = [primary]
    prelude = original[:namespace]
    target_bytes = (len(body) + count - 1) // count
    groups = [[]]
    size = 0
    for index, block in enumerate(blocks):
        if size >= target_bytes and len(groups) < count:
            groups.append([])
            size = 0
        groups[-1].append((offsets[index], block))
        size += len(block)
    filename = json.dumps(source.as_posix()).encode()
    for index, group in enumerate(groups):
        path = directory / f"parser-rules-{index}.cpp"
        contents = bytearray(prelude)
        for offset, block in group:
            line = original[:offset].count(b"\n") + 1
            contents.extend(b"\n#line " + str(line).encode() + b" " + filename + b"\n")
            contents.extend(block)
        path.write_bytes(contents)
        files.append(path)
    return files


def run(argv, *, build=False):
    print("build:", " ".join(map(str, argv)), flush=True)
    run_process(
        argv,
        limits=BUILD_LIMITS if build else CONTROL_LIMITS,
        output_filter=CompilerOutput() if build else None,
    )


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def checkout(path, repository, revision):
    if not re.fullmatch("[0-9a-f]{40}", revision):
        raise SystemExit("Schema contains an invalid source revision")
    if not path.exists():
        run(["git", "init", path])
        run(["git", "-C", path, "remote", "add", "origin", repository])
        run(["git", "-C", path, "fetch", "--depth", "1", "origin", revision])
        run(["git", "-C", path, "checkout", "--detach", "FETCH_HEAD"])
    subprocess.run(
        ["git", "-C", str(path), "diff", "--quiet", "HEAD", "--"], check=True
    )
    observed = subprocess.check_output(
        ["git", "-C", str(path), "rev-parse", "HEAD"], text=True
    ).strip()
    if observed != revision:
        raise SystemExit(f"source identity mismatch at {path}: {observed}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--jobs", type=int, default=12)
    args = parser.parse_args()
    if not 1 <= args.jobs <= 64:
        raise SystemExit("jobs must be in 1..64")
    directory = args.work_dir.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "artifact.json").unlink(missing_ok=True)
    schema_path = repository_root() / "crates/mrr-data-duckgql-query/schema.json"
    schema = json.loads(schema_path.read_text())

    def const(name):
        return schema["properties"][name]["const"]

    duckgql = directory / "duckgql-source"
    duckdb = directory / "duckdb-source"
    checkout(
        duckgql,
        "https://github.com/rahul-iyer/duckdb-gql.git",
        const("extension_revision"),
    )
    checkout(
        duckdb, "https://github.com/duckdb/duckdb", const("duckdb_source_revision")
    )
    archive = directory / "antlr-source.zip"
    if not archive.exists():
        url = f"https://www.antlr.org/download/antlr4-cpp-runtime-{const('antlr_version')}-source.zip"
        with (
            urllib.request.urlopen(url, timeout=60) as response,
            archive.open("wb") as output,
        ):
            while block := response.read(1024 * 1024):
                output.write(block)
    if digest(archive) != const("antlr_source_sha256"):
        raise SystemExit("ANTLR source digest mismatch")
    source = directory / "antlr-source"
    if not source.exists():
        with zipfile.ZipFile(archive) as zipped:
            for item in zipped.infolist():
                target = (source / item.filename).resolve()
                if not target.is_relative_to(source.resolve()):
                    raise SystemExit("archive path escaped source directory")
            zipped.extractall(source)
    prefix = directory / "antlr-install"
    antlr_build = directory / "antlr-build"
    run(
        [
            "cmake",
            "-S",
            source,
            "-B",
            antlr_build,
            "-DCMAKE_BUILD_TYPE=Release",
            "-DCMAKE_POSITION_INDEPENDENT_CODE=ON",
            f"-DCMAKE_INSTALL_PREFIX={prefix}",
            "-DANTLR_BUILD_CPP_TESTS=OFF",
            "-DANTLR_BUILD_SHARED=OFF",
            "-DANTLR4_INSTALL=ON",
        ]
    )
    run(
        [
            "cmake",
            "--build",
            antlr_build,
            "--target",
            "install",
            "--parallel",
            args.jobs,
        ],
        build=True,
    )
    parser = duckgql / "src/parser/generated/GQLParser.cpp"
    units = parser_units(
        parser, directory / "generated-parser", const("generated_parser_parts")
    )
    config = directory / "extension-config.cmake"
    # All values are fixed Schema fields or resolved build paths, never query input.
    config.write_text(
        f'duckdb_extension_load(duckgql SOURCE_DIR "{duckgql.as_posix()}" DONT_LINK EXTENSION_VERSION {const("extension_version")})\n'
        # The extension has independent translation units; only DuckDB's core
        # batches need replacing. Disable automatic grouping for this target.
        "cmake_language(DEFER CALL set_target_properties duckgql_extension duckgql_loadable_extension PROPERTIES UNITY_BUILD OFF)\n"
        # Third-party sources assume independent translation units. In
        # particular zstd has conflicting file-local macros/types in a batch.
        "cmake_language(DEFER CALL set_target_properties duckdb_zstd duckdb_re2 duckdb_mbedtls duckdb_pg_query duckdb_utf8proc duckdb_fsst duckdb_hyperloglog duckdb_fmt duckdb_miniz duckdb_skiplistlib duckdb_fastpforlib duckdb_yyjson PROPERTIES UNITY_BUILD OFF)\n"
        # Original definitions are compiled as bounded rule-block units.
        f'cmake_language(DEFER CALL set_source_files_properties "{parser.as_posix()}" TARGET_DIRECTORY duckgql_loadable_extension PROPERTIES HEADER_FILE_ONLY TRUE)\n'
        "cmake_language(DEFER CALL target_sources duckgql_loadable_extension PRIVATE "
        + " ".join(f'"{path.as_posix()}"' for path in units)
        + ")\n"
        # Expose actual compiler optimization decisions during large units.
        # These diagnostics report work performed by the compiler, rather
        # than manufacturing progress from an elapsed-time heartbeat.
        'cmake_language(DEFER CALL target_compile_options duckgql_loadable_extension PRIVATE "$<$<CXX_COMPILER_ID:Clang,AppleClang>:-Xclang;-fdebug-pass-manager;-mllvm;-debug-pass=Executions>" "$<$<CXX_COMPILER_ID:GNU>:-fopt-info-all>")\n'
    )
    build = directory / "extension-build"
    run(
        [
            "cmake",
            "-S",
            duckdb,
            "-B",
            build,
            "-DCMAKE_BUILD_TYPE=Release",
            # DuckGQL requires C++17. Compile the linked DuckDB archive with
            # the same constexpr linkage rules instead of its C++11 default.
            "-DCMAKE_CXX_STANDARD=17",
            "-DCMAKE_POSITION_INDEPENDENT_CODE=ON",
            # Upstream groups an entire directory into a single translation
            # unit. Use CMake's bounded batches to avoid a silent long tail.
            "-DDISABLE_UNITY=ON",
            "-DCMAKE_UNITY_BUILD=ON",
            "-DCMAKE_UNITY_BUILD_BATCH_SIZE=4",
            f"-DCMAKE_PREFIX_PATH={prefix}",
            f"-DDUCKDB_EXTENSION_CONFIGS={config}",
            f"-DOVERRIDE_GIT_DESCRIBE={const('rust_engine_version')}",
            "-DBUILD_UNITTESTS=OFF",
            "-DBUILD_SHELL=OFF",
            "-DENABLE_SANITIZER=OFF",
            "-DENABLE_UBSAN=OFF",
            "-DEXTENSION_STATIC_BUILD=ON",
        ]
    )
    run(
        [
            "cmake",
            "--build",
            build,
            "--target",
            "duckgql_loadable_extension",
            "--parallel",
            args.jobs,
        ],
        build=True,
    )
    artifact = build / "extension/duckgql/duckgql.duckdb_extension"
    receipt = {
        "schema_sha256": digest(schema_path),
        "duckdb_source_revision": const("duckdb_source_revision"),
        "extension_revision": const("extension_revision"),
        "antlr_source_sha256": digest(archive),
        "artifact": str(artifact),
        "sha256": digest(artifact),
        "signed": False,
        "build_configuration": {
            "cxx_standard": 17,
            "generated_parser_units": len(units),
            "generated_parser_source_sha256": digest(parser),
            "generated_parser_unit_sha256": [digest(path) for path in units],
            "core_unity_batch_size": 4,
            "third_party_unity": False,
            "native_progress_diagnostics": "compiler-pass-execution",
            "jobs": args.jobs,
        },
    }
    (directory / "artifact.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt), flush=True)


if __name__ == "__main__":
    main()
