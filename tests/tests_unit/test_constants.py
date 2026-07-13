from __future__ import annotations

import importlib
import sys
import types
from collections.abc import Iterator

import pytest

import cognite.client._constants as constants


@pytest.fixture
def restore_constants() -> Iterator[None]:
    # Reload after each test so the real (pyodide-absent) module state is
    # restored and other tests are not affected by the simulated environment.
    yield
    importlib.reload(constants)


def _reload_with_fake_pyodide_ffi(monkeypatch: pytest.MonkeyPatch, ffi_attrs: dict[str, bool]) -> None:
    """Reload ``cognite.client._constants`` with a fake ``pyodide.ffi`` module."""
    pyodide_mod = types.ModuleType("pyodide")
    ffi_mod = types.ModuleType("pyodide.ffi")
    for name, value in ffi_attrs.items():
        setattr(ffi_mod, name, value)
    setattr(pyodide_mod, "ffi", ffi_mod)
    monkeypatch.setitem(sys.modules, "pyodide", pyodide_mod)
    monkeypatch.setitem(sys.modules, "pyodide.ffi", ffi_mod)
    importlib.reload(constants)


class TestBrowserDetection:
    @pytest.mark.parametrize(
        ("ffi_attrs", "expected"),
        [
            # Pyodide >= 314 renamed IN_BROWSER to IN_PYODIDE (pyodide#5916).
            pytest.param({"IN_PYODIDE": True}, True, id="pyodide-314-in_pyodide-true"),
            pytest.param({"IN_PYODIDE": False}, False, id="pyodide-314-in_pyodide-false"),
            # Pyodide < 314 still exposes the old IN_BROWSER name.
            pytest.param({"IN_BROWSER": True}, True, id="old-pyodide-in_browser-true"),
            pytest.param({"IN_BROWSER": False}, False, id="old-pyodide-in_browser-false"),
            # If both are present (transitional), the new name wins.
            pytest.param({"IN_PYODIDE": True, "IN_BROWSER": False}, True, id="prefers-in_pyodide"),
            # Defensive: pyodide.ffi present but neither flag exposed.
            pytest.param({}, False, id="ffi-present-neither-name"),
        ],
    )
    def test_detects_browser_across_pyodide_versions(
        self,
        monkeypatch: pytest.MonkeyPatch,
        restore_constants: None,
        ffi_attrs: dict[str, bool],
        expected: bool,
    ) -> None:
        _reload_with_fake_pyodide_ffi(monkeypatch, ffi_attrs)
        assert constants.IN_BROWSER is expected
        assert constants._RUNNING_IN_BROWSER is expected

    def test_not_in_browser_when_pyodide_absent(self, monkeypatch: pytest.MonkeyPatch, restore_constants: None) -> None:
        # A plain (non-package) pyodide module makes `import pyodide.ffi` raise
        # ModuleNotFoundError, mirroring a regular (non-pyodide) CPython runtime.
        monkeypatch.setitem(sys.modules, "pyodide", types.ModuleType("pyodide"))
        monkeypatch.delitem(sys.modules, "pyodide.ffi", raising=False)
        importlib.reload(constants)
        assert constants.IN_BROWSER is False
        assert constants._RUNNING_IN_BROWSER is False
