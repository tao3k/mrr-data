"""Keep the human documentation Org-only and its local entry links live."""

from pathlib import Path
from mrr_data_testing.workspace import repository_root
import re

ROOT = repository_root()
DOCS = ROOT / "docs"
ORG_LINK = re.compile(r"\[\[file:([^\]]+)")
MAINTAINED_READMES = (
    ROOT / "README.org",
    ROOT / "fuzz/README.org",
    ROOT / "python/README.org",
    ROOT / "python/src/mrr_data_testing/duckgql/README.org",
    ROOT / "crates/mrr-data-duckgql-query/README.org",
    ROOT / "python/src/mrr_data_testing/kache/README.org",
    ROOT / "python/src/mrr_data_testing/s3/README.org",
)


def check_link(source: Path, target: str) -> None:
    path = target.split("::", 1)[0]
    if path.startswith(("https://", "http://")):
        return
    if not (source.parent / path).exists():
        raise SystemExit(f"broken local link: {source.relative_to(ROOT)} -> {target}")


def main() -> None:
    files = sorted(path for path in DOCS.rglob("*") if path.is_file())
    wrong = [path.relative_to(ROOT) for path in files if path.suffix != ".org"]
    if wrong:
        raise SystemExit(f"docs must contain only Org documents: {wrong}")
    for source in files:
        for target in ORG_LINK.findall(source.read_text()):
            check_link(source, target)
    for readme in MAINTAINED_READMES:
        if readme.with_suffix(".md").exists() or not readme.is_file():
            raise SystemExit(
                f"maintained README must be Org: {readme.relative_to(ROOT)}"
            )
        for target in ORG_LINK.findall(readme.read_text()):
            check_link(readme, target)
    print(
        f"checked {len(files)} Org documents and {len(MAINTAINED_READMES)} Org READMEs"
    )


if __name__ == "__main__":
    main()
