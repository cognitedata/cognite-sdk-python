from __future__ import annotations

import contextlib

from cognite.client.utils._importing import import_zoneinfo
from cognite.client.utils._time import (
    MAX_TIMESTAMP_MS,
    MIN_TIMESTAMP_MS,
    datetime_to_ms,
    datetime_to_ms_iso_timestamp,
    ms_to_datetime,
    timestamp_to_ms,
)

# Needed for doctest to pass.
with contextlib.suppress(ImportError):
    ZoneInfo = import_zoneinfo()

__all__ = [
    "ZoneInfo",
    "MAX_TIMESTAMP_MS",
    "MIN_TIMESTAMP_MS",
    "datetime_to_ms",
    "ms_to_datetime",
    "timestamp_to_ms",
    "datetime_to_ms_iso_timestamp",
]
