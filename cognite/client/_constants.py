from __future__ import annotations

from types import ModuleType
from typing import Literal


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


try:
    import numpy as np  # noqa F401

    NUMPY_IS_AVAILABLE = True

except ImportError:  # pragma no cover
    NUMPY_IS_AVAILABLE = False


class Omitted:
    """Sentinel value for parameters that are not given or should be treated as not given."""

    def __repr__(self) -> str:
        return "<Omitted parameter>"

    def __bool__(self) -> Literal[False]:
        return False


OMITTED = Omitted()
DEFAULT_LIMIT_READ = 25
# Max JavaScript-safe integer 2^53 - 1
MAX_VALID_INTERNAL_ID = 9007199254740991
DATA_MODELING_DEFAULT_LIMIT_READ = 10
DEFAULT_DATAPOINTS_CHUNK_SIZE = 100_000
_RUNNING_IN_PYODIDE = _detect_in_pyodide(_pyo_ffi)
_RUNNING_IN_BROWSER = _RUNNING_IN_PYODIDE  # backwards compatibility

# Files API constants
FILE_MIN_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MiB
FILE_MAX_MULTIPART_SIZE = 4000 * 1024 * 1024  # 4000 MiB
FILE_DEFAULT_MULTIPART_SIZE = 50 * 1024 * 1024  # 50 MiB
FILE_MAX_MULTIPART_COUNT = 250
