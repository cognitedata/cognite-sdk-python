from __future__ import annotations

import math
import numbers
from decimal import Decimal
from types import MappingProxyType
from typing import Any

# Users are seeing JSONDecodeError coming from a block that intercepts JSONDecodeError
# (i.e. shouldn't be possible). It seems Python runtimes like e.g. Databricks patches the
# built-in json library with simplejson and thus simplejson.JSONDecodeError != json.JSONDecodeError
try:
    import simplejson as json
    from simplejson import JSONDecodeError
except ImportError:
    import json  # type: ignore [no-redef]
    from json import JSONDecodeError  # type: ignore [assignment]

__all__ = ["dumps", "loads", "JSONDecodeError", "convert_to_float", "convert_nonfinite_float_to_str"]


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

    from cognite.client.data_classes._base import CogniteResourceList

    if isinstance(obj, CogniteResourceList):
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

# As opposed to protobuf, datapoints in JSON returns out-of-range float values as strings. This means
# we're forced to do a lookup for every single datapoint we try to insert (and read in retrieve_latest
# when not ignoring bad)... so we allow some ugly optimizations in the translation code:
_FLOAT_API_MAPPING = MappingProxyType({"Infinity": math.inf, "-Infinity": -math.inf, "NaN": math.nan})
_FLOAT_API_MAPPING_REVERSE = MappingProxyType({math.inf: "Infinity", -math.inf: "-Infinity", math.nan: "NaN"})


def convert_to_float(value: float | str | None) -> float | None:
    if value.__class__ is str:  # like this abomination; faster than float(value)
        return _FLOAT_API_MAPPING[value]  # type: ignore [index]
    return value  # type: ignore [return-value]


def convert_nonfinite_float_to_str(value: float | str | None) -> float | str | None:
    # We accept str because when a user is trying to insert datapoints - we have no idea if the
    # time series to insert into is string or numeric
    try:
        return value if math.isfinite(value) else _FLOAT_API_MAPPING_REVERSE[value]  # type: ignore [arg-type, index]
    except TypeError:
        if value.__class__ is str or value is None:
            return value
        raise
    except KeyError:
        # Depending on numpy and python version, dict lookup may fail for NaN.. thanks IEEE :wink:
        if math.isnan(value):  # type: ignore [arg-type]
            return "NaN"
        raise
