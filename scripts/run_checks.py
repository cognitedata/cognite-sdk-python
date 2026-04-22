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


def _read_files_from_stdin() -> tuple[Path, ...]:
    """Read file paths from stdin (newline-separated)."""
    return tuple(Path(line.strip()) for line in sys.stdin if line.strip())


def _get_files_from_args_or_discover() -> tuple[Path, ...]:
    """Get file paths from command line arguments, stdin, or discover them.

    When pre-commit runs with pass_filenames: true on Windows, the command line
    can exceed the character limit (~8191 chars). To work around this, we support
    reading file paths from stdin using the --stdin flag.
    """
    # Check for --stdin flag
    if "--stdin" in sys.argv:
        return _read_files_from_stdin()

    args = [arg for arg in sys.argv[1:] if not arg.startswith("-")]
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
