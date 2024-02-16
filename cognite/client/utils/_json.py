from __future__ import annotations

import json
import numbers
from decimal import Decimal
from typing import Any

__all__ = ["dumps", "loads"]


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
