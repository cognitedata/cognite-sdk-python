from __future__ import annotations

import sys

_RUNNING_IN_BROWSER = sys.platform == "emscripten" and "pyodide" in sys.modules

DEFAULT_LIMIT_READ = 25
# Max JavaScript-safe integer 2^53 - 1
MAX_VALID_INTERNAL_ID = 9007199254740991
DATA_MODELING_DEFAULT_LIMIT_READ = 10
