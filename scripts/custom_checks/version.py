import re
from collections.abc import Iterator
from datetime import datetime
from itertools import pairwise
from pathlib import Path
from re import Match

import toml
from packaging.version import Version

from cognite.client import __version__

CWD = Path.cwd()


def pyproj_version_matches() -> str | None:
    with (CWD / "pyproject.toml").open() as fh:
        version_in_pyproject = toml.load(fh)["tool"]["poetry"]["version"]

    if __version__ != version_in_pyproject:
        return (
            f"Version in 'pyproject.toml' ({version_in_pyproject}) does not match the version in "
            f"cognite/client/_version.py: ({__version__})"
        )
    return None


def _parse_changelog() -> Iterator[Match[str]]:
    changelog = (CWD / "CHANGELOG.md").read_text(encoding="utf-8")
    return re.finditer(r"##\s\[(\d+\.\d+\.\d+)\]\s-\s(\d+-\d+-\d+)", changelog)


def changelog_entry_version_matches() -> str | None:
    match = next(_parse_changelog())
    version = match.group(1)
    if version != __version__:
        return (
            f"The latest entry in 'CHANGELOG.md' has a different version ({version}) than "
            f"cognite/client/_version.py: ({__version__}). Did you forgot to add a new entry? "
            "Or maybe you haven't followed the required format?"
        )
    return None


def changelog_entry_date() -> str | None:
    match = next(_parse_changelog())
    try:
        datetime.strptime(date := match.group(2), "%Y-%m-%d")
        return None
    except Exception:
        return f"Date given in the newest entry in 'CHANGELOG.md', {date!r}, is not valid/parsable (YYYY-MM-DD)"


def version_number_and_date_is_increasing() -> str | None:
    versions_and_dates = [(Version(m.group(1)), datetime.strptime(m.group(2), "%Y-%m-%d")) for m in _parse_changelog()]
    for (new_v, new_d), (old_v, old_d) in pairwise(versions_and_dates):
        if new_v < old_v:
            return f"Versions must be strictly increasing: {new_v} is not higher than the previous, {old_v}."
        if new_d < old_d:
            return f"Dates must be strictly increasing: {new_d.date()} is not after than the previous, {old_d.date()}."
    return None
