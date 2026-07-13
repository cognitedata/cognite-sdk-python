from __future__ import annotations

from types import ModuleType


# This helper fn should ideally live in utils/_pyodide_helpers.py, but since that module imports
# ClientConfig/CredentialProvider/cognite.client, putting it there would create a circular import.
def _detect_in_pyodide(pyodide_ffi: ModuleType | None) -> bool:
    # Pyodide 314 renamed `pyodide.ffi.IN_BROWSER` to `IN_PYODIDE` (pyodide#5916). We must support both names.
    if pyodide_ffi is None:
        return False
    try:
        return pyodide_ffi.IN_PYODIDE
    except AttributeError:
        try:
            return pyodide_ffi.IN_BROWSER
        except AttributeError:
            return False


try:
    import pyodide.ffi as _pyo_ffi  # type: ignore [import-not-found]
except ImportError:
    _pyo_ffi = None


_RUNNING_IN_BROWSER = _detect_in_pyodide(_pyo_ffi)
DEFAULT_LIMIT_READ = 25
# Max JavaScript-safe integer 2^53 - 1
MAX_VALID_INTERNAL_ID = 9007199254740991
DATA_MODELING_DEFAULT_LIMIT_READ = 10

try:
    import numpy as np  # noqa F401

    NUMPY_IS_AVAILABLE = True

except ImportError:  # pragma no cover
    NUMPY_IS_AVAILABLE = False
