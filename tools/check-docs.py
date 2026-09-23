"""Keep the human documentation Org-only and its local entry links live."""

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
ORG_LINK = re.compile(r"\[\[file:([^\]]+)")


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
    readme = ROOT / "README.org"
    if (ROOT / "README.md").exists():
        raise SystemExit("root README must be Org: remove README.md")
    for target in ORG_LINK.findall(readme.read_text()):
        check_link(readme, target)
    print(f"checked {len(files)} Org documents and README links")


if __name__ == "__main__":
    main()
