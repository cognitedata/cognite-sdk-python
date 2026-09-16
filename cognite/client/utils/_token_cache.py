from __future__ import annotations

import os
import sys
import tempfile
import warnings
from pathlib import Path

_CACHE_DIR_NAME = "cognite-sdk-python"


def default_token_cache_dir() -> Path:
    try:
        home = Path.home()
    except RuntimeError:
        # No home directory could be resolved (e.g. minimal/serverless containers without $HOME).
        # write_securely()/read_securely() harden the cache regardless of directory, so this is safe.
        return Path(tempfile.gettempdir()) / _CACHE_DIR_NAME

    if sys.platform == "win32":
        base = Path(os.environ.get("LOCALAPPDATA") or (home / "AppData" / "Local"))
    elif sys.platform == "darwin":
        base = home / "Library" / "Caches"
    else:
        base = Path(os.environ.get("XDG_CACHE_HOME") or (home / ".cache"))
    return base / _CACHE_DIR_NAME


def write_securely(path: Path, content: str) -> None:
    """Atomically write `content` to `path`, creating an owner-only-permission file that never follows
    (and replaces, rather than dereferences) a pre-existing symlink at the destination."""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)

    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        # mkstemp() already creates the file at 0600; fchmod() just makes that guarantee explicit
        # rather than relying on stdlib internals. Not available on Windows, which can't express
        # POSIX-style owner/group/other permissions anyway.
        if hasattr(os, "fchmod"):
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as fh:
            fh.write(content)
        os.replace(tmp_name, path)
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise


def read_securely(path: Path) -> str | None:
    """Read `path`, refusing to follow a symlink (treated as a cache miss, not an error)."""
    if not path.exists():
        return None
    if path.is_symlink():
        warnings.warn(
            f"Ignoring token cache at '{path}': refusing to follow a symlink for security reasons.",
            UserWarning,
            stacklevel=2,
        )
        return None
    return path.read_text()
