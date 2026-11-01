"""Configure diagnostic-only GCC controls through the selected Gambit SDK."""

import argparse
import os
from pathlib import Path
import re
import shlex
import subprocess

TRACKING_OFF = "-ftrack-macro-expansion=0"
OPTIMIZER_NOTES_OFF = f"-fopt-info-all={os.devnull}"


def diagnostic_flags(flags):
    """Preserve all declared options; refuse expansions and conflicting overrides."""
    if any(character in flags for character in "\n\r$`"):
        raise ValueError("Gambit compiler options must be one literal line")
    tracking = [
        option
        for option in shlex.split(flags)
        if option.startswith("-ftrack-macro-expansion")
    ]
    if tracking and tracking != [TRACKING_OFF]:
        raise ValueError("explicit macro-tracking option conflicts with diagnostics")
    return flags if tracking else f"{flags.rstrip()} {TRACKING_OFF}".lstrip()


def quiet_optimizer_flags(flags):
    """Suppress optional optimizer notes, never errors or actual phase events."""
    diagnostic_flags(flags)
    destinations = [
        option
        for option in shlex.split(flags)
        if option.startswith("-fopt-info") and "=" in option
    ]
    if destinations and destinations != [OPTIMIZER_NOTES_OFF]:
        raise ValueError("explicit optimizer report destination cannot be suppressed")
    return flags if destinations else f"{flags.rstrip()} {OPTIMIZER_NOTES_OFF}".lstrip()


def sdk_driver(gsc):
    """Resolve the selected SDK's declared GCC driver, never ambient CC."""
    gsc = Path(gsc).resolve(strict=True)
    if not gsc.is_file() or not os.access(gsc, os.X_OK):
        raise ValueError("GERBIL_GSC must select an executable SDK compiler")
    driver = gsc.with_name("gambuild-C")
    source = driver.read_text()
    features = re.findall(r'^BUILD_FEATURE_C_COMP=["\']?([a-z]+)', source, re.M)
    if features != ["gcc"] or "CORCXXFLAGS_GAMBUILD+set" not in source:
        raise ValueError("selected SDK must declare GCC and the public flag override")
    return driver


def sdk_option(driver, name):
    result = subprocess.run(
        [driver, name],
        capture_output=True,
        check=True,
        timeout=5,
        text=True,
    )
    return result.stdout.removesuffix("\n")


def configured_flags(gsc, inherited=None, optimizer_notes=True):
    """Read SDK defaults through its documented override/query, never ambient CC."""
    defaults = sdk_option(sdk_driver(gsc), "CORCXXFLAGS_GAMBUILD")
    # Even an inherited override cannot bypass validating the selected SDK.
    diagnostic_flags(defaults)
    flags = diagnostic_flags(defaults if inherited is None else inherited)
    return flags if optimizer_notes else quiet_optimizer_flags(flags)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--github-env", type=Path)
    mode.add_argument("--replay-linker", type=Path)
    parser.add_argument("--replay-receipt", type=Path)
    parser.add_argument(
        "--suppress-optimizer-notes",
        action="store_true",
        help="Diagnostic experiment only; suppression can hide Cargo-visible progress",
    )
    args = parser.parse_args()
    gsc = os.environ.get("GERBIL_GSC")
    if not gsc:
        parser.error("GERBIL_GSC must identify the selected immutable SDK")
    if args.replay_linker:
        if not args.replay_receipt or args.suppress_optimizer_notes:
            parser.error(
                "linker replay requires its receipt and original optimizer output"
            )
        from mrr_data_testing.gambit_linker_replay import replay_linker

        replay_linker(gsc, args.replay_linker, args.replay_receipt)
        return
    flags = configured_flags(
        gsc, os.environ.get("CORCXXFLAGS_GAMBUILD"), not args.suppress_optimizer_notes
    )
    with args.github_env.open("a") as output:
        output.write(f"CORCXXFLAGS_GAMBUILD={flags}\n")
    print(
        "Gambit GCC diagnostics: macro tracking off; code generation defaults retained"
    )
