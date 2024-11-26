from __future__ import annotations

import sys
from pathlib import Path

from scripts.custom_checks.docstrings import format_docstrings
from scripts.custom_checks.version import (
    changelog_entry_date,
    changelog_entry_version_matches,
    pyproj_version_matches,
    version_number_and_date_is_increasing,
)


def run_checks(files: list[Path]) -> list[str | None]:
    return [
        pyproj_version_matches(),
        changelog_entry_version_matches(),
        changelog_entry_date(),
        version_number_and_date_is_increasing(),
        format_docstrings(files),
    ]


if __name__ == "__main__":
    files = tuple(map(Path, sys.argv[1:]))
    if failed := list(filter(None, run_checks(files))):
        print(f"\nCustom repo checks failures:\n{'#' * 80}\n" + "\n\n".join(failed))
        raise SystemExit(1)
