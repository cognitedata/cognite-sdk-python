"""Wrapper script for pre-commit hooks to handle Windows command line length limits.

On Windows, the command line has a character limit (~8191 characters). When pre-commit
passes many filenames to hooks, this limit can be exceeded.

This wrapper accepts filenames as arguments (from pre-commit) and pipes them to the
target script via stdin, allowing the target script to process them without hitting
command line limits on subsequent calls.

Usage in .pre-commit-config.yaml:
    entry: poetry run python scripts/precommit_wrapper.py <target_script> [script_args...] ---

The --- marker separates script arguments from filenames that pre-commit will append.
The target script must support reading file paths from stdin (newline-separated).

Note: This script itself may receive a long argument list from pre-commit. On Windows
with Git Bash/MINGW, this typically works. If you still hit limits, you may need to
use pass_filenames: false and have the wrapper discover files via git.
"""

import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path


def get_all_tracked_files(pattern: str | None = None) -> list[str]:
    """Get all tracked files from git, optionally filtered by pattern."""
    result = subprocess.run(
        ["git", "ls-files"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return []
    files = [f.strip() for f in result.stdout.splitlines() if f.strip()]
    if pattern:
        regex = re.compile(pattern)
        files = [f for f in files if regex.search(f)]
    return files


def main() -> int:
    args = sys.argv[1:]

    # Check for special mode where we discover files ourselves (fallback for Windows limits)
    # This is triggered when --discover-files is passed
    discover_mode = "--discover-files" in args
    if discover_mode:
        args = [a for a in args if a != "--discover-files"]

    # Parse arguments - find the --- separator
    pattern: str | None = None
    script_args: list[str] = []
    filenames: list[str] = []

    i = 0
    while i < len(args):
        if args[i] == "--pattern" and i + 1 < len(args):
            pattern = args[i + 1]
            i += 2
        elif args[i] == "---":
            # Everything after --- is filenames from pre-commit
            filenames = args[i + 1 :]
            break
        else:
            script_args.append(args[i])
            i += 1

    if not script_args:
        print("Error: No target script specified.", file=sys.stderr)
        return 1

    # If discover mode or no filenames provided, discover them ourselves
    if discover_mode or not filenames:
        filenames = get_all_tracked_files(pattern)

    # Only process files that exist
    filenames = [f for f in filenames if Path(f).exists()]

    if not filenames:
        # No files to process, exit successfully
        return 0

    # Write filenames to a temp file to avoid command line length issues
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("\n".join(filenames))
        temp_path = f.name

    try:
        # Build the command using -- to separate poetry args from script args
        cmd = ["poetry", "run", "--", "python", *script_args]

        # Run the target script with filenames read from temp file via stdin
        with open(temp_path) as stdin_file:
            process = subprocess.run(cmd, stdin=stdin_file)
        return process.returncode
    finally:
        # Clean up temp file
        os.unlink(temp_path)


if __name__ == "__main__":
    raise SystemExit(main())
