try:
    from pyodide.ffi import IN_BROWSER  # type: ignore [import-not-found]
except ModuleNotFoundError:
    IN_BROWSER = False

try:
    import numpy as np  # noqa F401

    NUMPY_IS_AVAILABLE = True

except ImportError:  # pragma no cover
    NUMPY_IS_AVAILABLE = False


# Max JavaScript-safe integer 2^53 - 1
MAX_VALID_INTERNAL_ID = 9007199254740991
_RUNNING_IN_BROWSER = IN_BROWSER

# Files API constants
FILE_MIN_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MiB
FILE_MAX_MULTIPART_SIZE = 4000 * 1024 * 1024  # 4000 MiB
FILE_DEFAULT_MULTIPART_SIZE = 50 * 1024 * 1024  # 50 MiB
FILE_MAX_MULTIPART_COUNT = 250
