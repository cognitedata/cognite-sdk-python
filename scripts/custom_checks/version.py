from pathlib import Path

import toml

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
