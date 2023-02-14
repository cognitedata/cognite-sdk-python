from __future__ import annotations

LIST_LIMIT_DEFAULT = 25
LIST_LIMIT_CEILING = 10_000  # variable used to guarantee all items are returned when list(limit) is None, inf or -1.
# Max JavaScript-safe integer 2^53 - 1
MAX_VALID_INTERNAL_ID = 9007199254740991
