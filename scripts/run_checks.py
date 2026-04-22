from __future__ import annotations

import sys
from pathlib import Path

from cognite.client import global_config
from scripts.custom_checks.docstr_examples import format_docstring_examples
from scripts.custom_checks.docstrings import format_docstrings
from scripts.custom_checks.version import pyproj_version_matches

global_config.silence_feature_preview_warnings = True


def run_checks(files: tuple[Path, ...]) -> list[str | None]:
    return [
        pyproj_version_matches(),
        format_docstrings(files),
        format_docstring_examples(files),
    ]


def _get_files_from_args_or_discover() -> tuple[Path, ...]:
    """Get file paths from command line arguments or discover them.

    When pre-commit runs with pass_filenames: false (used to avoid Windows
    command line length limits), we need to discover files ourselves.
    """
    args = sys.argv[1:]
    if args:
        return tuple(map(Path, args))
    else:
        # Discover all Python files in cognite/ directory
        return tuple(p for p in Path("cognite").rglob("*") if p.is_file() and p.suffix in (".py", ".pyi"))


if __name__ == "__main__":
    files = _get_files_from_args_or_discover()
    if failed := list(filter(None, run_checks(files))):
        print(f"\nCustom repo checks failures:\n{'#' * 80}\n" + "\n\n".join(failed))
        raise SystemExit(1)
