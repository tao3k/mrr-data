"""Configure diagnostic-only GCC controls through the selected Gambit SDK."""

import argparse
import os
from pathlib import Path
import re
import shlex
import subprocess

TRACKING_OFF = "-ftrack-macro-expansion=0"


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


def configured_flags(gsc, inherited=None):
    """Read SDK defaults through its documented override/query, never ambient CC."""
    gsc = Path(gsc).resolve(strict=True)
    if not gsc.is_file() or not os.access(gsc, os.X_OK):
        raise ValueError("GERBIL_GSC must select an executable SDK compiler")
    driver = gsc.with_name("gambuild-C")
    source = driver.read_text()
    features = re.findall(r'^BUILD_FEATURE_C_COMP=["\']?([a-z]+)', source, re.M)
    if features != ["gcc"] or "CORCXXFLAGS_GAMBUILD+set" not in source:
        raise ValueError("selected SDK must declare GCC and the public flag override")
    result = subprocess.run(
        [driver, "CORCXXFLAGS_GAMBUILD"],
        capture_output=True,
        check=True,
        timeout=5,
        text=True,
    )
    defaults = result.stdout.removesuffix("\n")
    # Even an inherited override cannot bypass validating the selected SDK.
    diagnostic_flags(defaults)
    return diagnostic_flags(defaults if inherited is None else inherited)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--github-env", type=Path, required=True)
    args = parser.parse_args()
    gsc = os.environ.get("GERBIL_GSC")
    if not gsc:
        parser.error("GERBIL_GSC must identify the selected immutable SDK")
    flags = configured_flags(gsc, os.environ.get("CORCXXFLAGS_GAMBUILD"))
    with args.github_env.open("a") as output:
        output.write(f"CORCXXFLAGS_GAMBUILD={flags}\n")
    print("Gambit GCC diagnostics: macro tracking off; all SDK defaults retained")
