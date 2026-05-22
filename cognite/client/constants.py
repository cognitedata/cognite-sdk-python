from __future__ import annotations

from typing import Literal


class Omitted:
    """Sentinel value for parameters that are not given or should be treated as not given."""

    def __repr__(self) -> str:
        return "<Omitted parameter>"

    def __bool__(self) -> Literal[False]:
        return False


OMITTED = Omitted()
DEFAULT_LIMIT_READ = 25

DATA_MODELING_DEFAULT_LIMIT_READ = 10
DEFAULT_DATAPOINTS_CHUNK_SIZE = 100_000
