from __future__ import annotations

import contextlib
import sys

from cognite.client.utils._time import (
    MAX_TIMESTAMP_MS,
    MIN_TIMESTAMP_MS,
    datetime_to_ms,
    ms_to_datetime,
    timestamp_to_ms,
)

# Needed for doctest to pass.
with contextlib.suppress(ImportError):
    if sys.version_info >= (3, 9):
        from zoneinfo import ZoneInfo
    else:
        from backports.zoneinfo import ZoneInfo

__all__ = ["ZoneInfo", "MAX_TIMESTAMP_MS", "MIN_TIMESTAMP_MS", "datetime_to_ms", "ms_to_datetime", "timestamp_to_ms"]
