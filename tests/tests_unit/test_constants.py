from __future__ import annotations

import types

import pytest

from cognite.client._constants import _detect_in_pyodide


class TestDetectInPyodide:
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
    def test_detects_pyodide_across_versions(self, ffi_attrs: dict[str, bool], expected: bool) -> None:
        ffi_mod = types.ModuleType("pyodide.ffi")
        for name, value in ffi_attrs.items():
            setattr(ffi_mod, name, value)

        assert _detect_in_pyodide(ffi_mod) is expected

    def test_not_in_pyodide_when_absent(self) -> None:
        assert _detect_in_pyodide(None) is False
