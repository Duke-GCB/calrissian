"""Normalize pdocs output for MkDocs links and inline regex literals."""

from pathlib import Path
import re


def main():
    root = Path(__file__).resolve().parents[1] / "docs/api/latest"
    for page in root.rglob("*.md"):
        def replace(match):
            label, target = match.groups()
            if ":" in target or target.startswith(("/", "#")):
                return match.group(0)
            path, separator, fragment = target.partition("#")
            candidate = page.parent / (path.rstrip("/") + ".md")
            if path.endswith("/") and candidate.is_file():
                return f"[{label}]({path.rstrip('/')}.md{separator}{fragment})"
            return match.group(0)

        text = page.read_text()
        # pdocs renders reStructuredText inline literals as single-quoted text.
        # Markdown otherwise interprets the brackets and parentheses as a link.
        text = text.replace(
            "regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?'",
            "regex used for validation is `[a-z0-9]([-a-z0-9]*[a-z0-9])?`",
        )
        page.write_text(re.sub(r"\[([^\]\n]+)\]\(([^)\s]+)\)", replace, text))


if __name__ == "__main__":
    main()
