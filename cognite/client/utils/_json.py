from __future__ import annotations

import json
import math
import numbers
from decimal import Decimal
from types import MappingProxyType
from typing import Any

__all__ = ["dumps", "loads", "to_float_translation", "to_str_translation"]


def _default_json_encoder(obj: Any) -> Any:
    if isinstance(obj, numbers.Integral):
        return int(obj)
    if isinstance(obj, (Decimal, numbers.Real)):
        return float(obj)
    from cognite.client.data_classes._base import CogniteObject

    if isinstance(obj, CogniteObject):
        return obj.dump(camel_case=True)

    from cognite.client.data_classes._base import CogniteFilter

    if isinstance(obj, CogniteFilter):
        return obj.dump(camel_case=True)
    raise TypeError(f"Object {obj} of type {obj.__class__} can't be serialized by the JSON encoder")


def dumps(
    obj: Any,
    indent: int | None = None,
    allow_nan: bool = True,
    sort_keys: bool = False,
) -> str:
    return json.dumps(
        obj,
        default=_default_json_encoder,
        indent=indent,
        allow_nan=allow_nan,
        sort_keys=sort_keys,
    )


loads = json.loads

_FLOAT_API_MAPPING = MappingProxyType({"Infinity": math.inf, "-Infinity": -math.inf, "NaN": math.nan})
_FLOAT_API_MAPPING_REVERSE = MappingProxyType({math.inf: "Infinity", -math.inf: "-Infinity", math.nan: "NaN"})


def to_float_translation(value: float | str | None) -> float | None:
    # As opposed to protobuf, retrieve_latest uses JSON and it returns out-of-range float values as strings:
    return _FLOAT_API_MAPPING.get(value, value)  # type: ignore [arg-type]


def to_str_translation(value: float | None) -> float | str | None:
    return _FLOAT_API_MAPPING_REVERSE.get(value, value)  # type: ignore [arg-type]
