from __future__ import annotations

import sys
from pathlib import Path

from scripts.custom_checks.docstr_examples import format_docstring_examples
from scripts.custom_checks.docstrings import format_docstrings
from scripts.custom_checks.version import pyproj_version_matches


def run_checks(files: list[Path]) -> list[str | None]:
    return [
        pyproj_version_matches(),
        format_docstrings(files),
        format_docstring_examples(files),
    ]


if __name__ == "__main__":
    files = tuple(map(Path, sys.argv[1:]))
    if failed := list(filter(None, run_checks(files))):
        print(f"\nCustom repo checks failures:\n{'#' * 80}\n" + "\n\n".join(failed))
        raise SystemExit(1)
