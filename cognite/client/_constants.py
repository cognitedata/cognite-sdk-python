from __future__ import annotations

try:
    from pyodide.ffi import IN_BROWSER  # type: ignore [import-not-found]
except ModuleNotFoundError:
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
