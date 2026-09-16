from __future__ import annotations

import os
import stat
import sys
import tempfile
from pathlib import Path

import pytest

from cognite.client.utils._token_cache import default_token_cache_dir, read_securely, write_securely

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX permission/symlink semantics not applicable")


class TestDefaultTokenCacheDir:
    def test_not_under_system_tempdir(self) -> None:
        cache_dir = default_token_cache_dir()
        assert not str(cache_dir).startswith(str(Path(tempfile.gettempdir())))
        assert cache_dir.name == "cognite-sdk-python"

    def test_respects_xdg_cache_home(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
        assert default_token_cache_dir() == tmp_path / "cognite-sdk-python"

    def test_falls_back_to_dot_cache_when_xdg_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
        assert default_token_cache_dir() == Path.home() / ".cache" / "cognite-sdk-python"

    def test_falls_back_to_tempdir_when_home_unresolvable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_runtime_error() -> Path:
            raise RuntimeError("no home directory")

        monkeypatch.setattr(Path, "home", staticmethod(raise_runtime_error))
        suffix = f"-{os.getuid()}" if hasattr(os, "getuid") else ""
        assert default_token_cache_dir() == Path(tempfile.gettempdir()) / f"cognite-sdk-python{suffix}"


class TestWriteSecurely:
    def test_creates_parent_dir_with_owner_only_mode(self, tmp_path: Path) -> None:
        target = tmp_path / "sub" / "cache.bin"
        write_securely(target, "content")
        assert stat.S_IMODE(target.parent.stat().st_mode) == 0o700

    def test_written_file_has_owner_only_mode(self, tmp_path: Path) -> None:
        target = tmp_path / "cache.bin"
        write_securely(target, "content")
        assert stat.S_IMODE(target.stat().st_mode) == 0o600

    def test_content_roundtrips(self, tmp_path: Path) -> None:
        target = tmp_path / "cache.bin"
        write_securely(target, "hello world")
        assert target.read_text() == "hello world"

    def test_no_temp_file_left_behind_after_success(self, tmp_path: Path) -> None:
        write_securely(tmp_path / "cache.bin", "content")
        assert [p.name for p in tmp_path.iterdir()] == ["cache.bin"]

    def test_no_temp_file_left_behind_after_failure(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_oserror(*args: object, **kwargs: object) -> None:
            raise OSError("boom")

        monkeypatch.setattr(os, "replace", raise_oserror)
        with pytest.raises(OSError, match="boom"):
            write_securely(tmp_path / "cache.bin", "content")
        assert list(tmp_path.iterdir()) == []

    def test_replaces_preexisting_symlink_without_following_it(self, tmp_path: Path) -> None:
        victim = tmp_path / "victim.txt"
        victim.write_text("original content")
        target = tmp_path / "cache.bin"
        target.symlink_to(victim)

        write_securely(target, "REFRESH-TOKEN-PLANTED-FOR-THIS-TEST")

        assert victim.read_text() == "original content"
        assert not target.is_symlink()
        assert target.read_text() == "REFRESH-TOKEN-PLANTED-FOR-THIS-TEST"

    def test_existing_dir_mode_left_untouched(self, tmp_path: Path) -> None:
        sub = tmp_path / "sub"
        sub.mkdir(mode=0o755)
        write_securely(sub / "cache.bin", "content")
        assert stat.S_IMODE(sub.stat().st_mode) == 0o755


class TestReadSecurely:
    def test_returns_none_when_missing(self, tmp_path: Path) -> None:
        assert read_securely(tmp_path / "missing.bin") is None

    def test_returns_content_for_regular_file(self, tmp_path: Path) -> None:
        target = tmp_path / "cache.bin"
        target.write_text("hello")
        assert read_securely(target) == "hello"

    def test_refuses_symlink_and_warns(self, tmp_path: Path) -> None:
        victim = tmp_path / "victim.txt"
        victim.write_text("arbitrary file content, not a token cache")
        link = tmp_path / "cache.bin"
        link.symlink_to(victim)

        with pytest.warns(UserWarning, match="refusing to follow a symlink"):
            result = read_securely(link)
        assert result is None
