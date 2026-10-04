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


def run(argv, *, build=False):
    print("build:", " ".join(map(str, argv)), flush=True)
    run_process(argv, limits=BUILD_LIMITS if build else CONTROL_LIMITS)


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
    config = directory / "extension-config.cmake"
    # All values are fixed Schema fields or resolved build paths, never query input.
    config.write_text(
        f'duckdb_extension_load(duckgql SOURCE_DIR "{duckgql.as_posix()}" DONT_LINK EXTENSION_VERSION {const("extension_version")})\n'
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
    }
    (directory / "artifact.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt), flush=True)


if __name__ == "__main__":
    main()
