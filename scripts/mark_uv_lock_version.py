"""Restore the release-please version marker on the root package in ``uv.lock``.

Workaround that keeps the uv pre-commit hook compatible with the Google
release-please action.

``uv.lock`` stores this project's own version, and ``uv sync --locked`` fails
when that version disagrees with ``pyproject.toml``. release-please bumps
``pyproject.toml`` on a release, and for an extra file such as ``uv.lock`` it
only updates a version on a line tagged with ``x-release-please-version``.
The uv pre-commit hook runs ``uv lock``, which rewrites the lockfile and drops
that comment. This script puts the marker back on the ``cognite-sdk`` version
line so the next release-please run can bump the lockfile version too.
"""

import re
import sys
from pathlib import Path

_VERSION_LINE = re.compile(
    r'(^name = "cognite-sdk"\nversion = "[^"]+")(?:  # x-release-please-version)?$',
    re.MULTILINE,
)
_MARKER = "  # x-release-please-version"


def main() -> None:
    path = Path("uv.lock")
    text = path.read_text(encoding="utf-8")
    updated, count = _VERSION_LINE.subn(rf"\1{_MARKER}", text, count=1)
    if count != 1:
        sys.exit("Could not find the cognite-sdk version in uv.lock")
    if updated != text:
        path.write_text(updated, encoding="utf-8")


if __name__ == "__main__":
    main()
