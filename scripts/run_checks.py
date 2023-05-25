from __future__ import annotations

from scripts.custom_checks.version import changelog_entry_date, changelog_entry_version, pyproj_version_matches


def run_checks() -> list[str | None]:
    return [
        pyproj_version_matches(),
        changelog_entry_version(),
        changelog_entry_date(),
    ]


if __name__ == "__main__":
    if failed := list(filter(None, run_checks())):
        print("\n\n".join(failed))
        raise SystemExit(1)
