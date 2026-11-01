import json
from pathlib import Path
import subprocess

import pytest

from mrr_data_testing import gambit_linker_replay as replay
from mrr_data_testing.process import BUILD_LIMITS, ProgressTimeout


def source_input(tmp_path):
    source = (
        tmp_path
        / "target/debug/build/mrr-gerbil-owned/out/gerbil-program/program_link.i"
    )
    source.parent.mkdir(parents=True)
    source.write_text("int ready;\n")
    return source


def compiler_options(monkeypatch):
    monkeypatch.setattr(replay, "sdk_driver", lambda gsc: Path("owned-driver"))
    monkeypatch.setattr(
        replay,
        "sdk_option",
        lambda driver, name: {"C_COMPILER": "owned-gcc", "FLAGS_OBJ": "-fPIC -DABI=1"}[
            name
        ],
    )
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda command, **options: subprocess.CompletedProcess(
            command, 0, "declared GCC\n", ""
        ),
    )


def test_direct_replay_keeps_abi_limits_input_and_raw_fragment_identity(
    tmp_path, monkeypatch
):
    source = source_input(tmp_path)
    compiler_options(monkeypatch)
    commands = []

    def run(command, **options):
        commands.append(command)
        assert options["limits"] is BUILD_LIMITS
        assert options["cwd"] == source.parent
        assert options["output_filter"](b"fragment") == b"fragment"
        assert options["output_filter"](b" tail\n") == b" tail\n"
        assert options["output_filter"](b"") == b""

    monkeypatch.setattr(replay, "run", run)
    receipt = tmp_path / "replay.json"
    replay.replay_linker("selected-gsc", tmp_path / "target", receipt)
    metadata = json.loads(receipt.read_text())
    assert metadata["outcome"] == "completed" and metadata["unchanged_input"]
    assert metadata["wall_limit"] == 1200 and metadata["idle_limit"] == 30
    assert commands[0][:3] == ["owned-gcc", "-fPIC", "-DABI=1"]
    assert "-O2" in commands[0] and commands[0][-1] == str(source)
    assert receipt.with_suffix(".output").read_bytes() == b"fragment tail\n"
    assert [
        json.loads(line)["newlines"]
        for line in receipt.with_suffix(".chunks.jsonl").read_text().splitlines()
    ] == [0, 1]


def test_direct_replay_retains_original_timeout_without_qualifying_gate(
    tmp_path, monkeypatch
):
    source_input(tmp_path)
    compiler_options(monkeypatch)

    def refuse(*args, **options):
        raise ProgressTimeout("original idle refusal")

    monkeypatch.setattr(replay, "run", refuse)
    receipt = tmp_path / "refused.json"
    with pytest.raises(ProgressTimeout, match="original idle refusal"):
        replay.replay_linker("selected-gsc", tmp_path / "target", receipt)
    metadata = json.loads(receipt.read_text())
    assert metadata["outcome"] == "ProgressTimeout" and metadata["unchanged_input"]
    assert "never replaces" in metadata["purpose"]


def test_frozen_input_refuses_alias_escape_and_multiple_candidates(tmp_path):
    source = source_input(tmp_path)
    extra = (
        tmp_path
        / "target/debug/build/mrr-gerbil-other/out/gerbil-program/program_link.i"
    )
    extra.parent.mkdir(parents=True)
    extra.write_text("other")
    with pytest.raises(ValueError, match="one confined"):
        replay.frozen_linker_input(tmp_path / "target")
    extra.unlink()
    outside = tmp_path / "outside.i"
    outside.write_text("outside")
    source.unlink()
    source.symlink_to(outside)
    with pytest.raises(ValueError, match="one confined"):
        replay.frozen_linker_input(tmp_path / "target")


def test_successful_compiler_cannot_qualify_a_changed_input(tmp_path, monkeypatch):
    source = source_input(tmp_path)
    compiler_options(monkeypatch)
    monkeypatch.setattr(
        replay, "run", lambda *args, **kwargs: source.write_text("changed")
    )
    receipt = tmp_path / "changed.json"
    with pytest.raises(ValueError, match="changed its frozen input"):
        replay.replay_linker("selected-gsc", tmp_path / "target", receipt)
    metadata = json.loads(receipt.read_text())
    assert not metadata["unchanged_input"]
    assert metadata["outcome"] == "input_changed"
