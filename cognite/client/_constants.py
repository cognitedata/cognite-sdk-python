from __future__ import annotations

try:
    import pyodide.ffi  # type: ignore [import-not-found]

    # Pyodide 314 renamed `pyodide.ffi.IN_BROWSER` to `IN_PYODIDE` (pyodide#5916).
    # Support both names, and use getattr so a missing attribute raises no error.
    IN_BROWSER = bool(getattr(pyodide.ffi, "IN_PYODIDE", getattr(pyodide.ffi, "IN_BROWSER", False)))
except ImportError:
    IN_BROWSER = False

_RUNNING_IN_BROWSER = IN_BROWSER
DEFAULT_LIMIT_READ = 25
# Max JavaScript-safe integer 2^53 - 1
MAX_VALID_INTERNAL_ID = 9007199254740991
DATA_MODELING_DEFAULT_LIMIT_READ = 10

try:
    import numpy as np  # noqa F401

    NUMPY_IS_AVAILABLE = True

except ImportError:  # pragma no cover
    NUMPY_IS_AVAILABLE = False
