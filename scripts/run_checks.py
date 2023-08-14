from __future__ import annotations

from scripts.custom_checks.docstrings import format_docstrings
from scripts.custom_checks.version import (
    changelog_entry_date,
    changelog_entry_version_matches,
    pyproj_version_matches,
    version_number_is_increasing,
)


def run_checks() -> list[str | None]:
    return [
        pyproj_version_matches(),
        changelog_entry_version_matches(),
        changelog_entry_date(),
        version_number_is_increasing(),
        format_docstrings(),
    ]


if __name__ == "__main__":
    if failed := list(filter(None, run_checks())):
        print("\n\n".join(failed))
        raise SystemExit(1)
