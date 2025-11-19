from __future__ import annotations

from typing import Literal

try:
    from pyodide.ffi import IN_BROWSER as _RUNNING_IN_BROWSER  # type: ignore [import-not-found]
except ModuleNotFoundError:
    _RUNNING_IN_BROWSER = False

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
