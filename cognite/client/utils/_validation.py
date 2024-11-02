from __future__ import annotations

import functools
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal, TypeAlias

from cognite.client.data_classes._base import T_CogniteSort
from cognite.client.utils._auxiliary import is_unlimited
from cognite.client.utils._identifier import Identifier, IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

SortSpec: TypeAlias = (
    T_CogniteSort
    | str
    | tuple[str, Literal["asc", "desc"]]
    | tuple[str, Literal["asc", "desc"], Literal["auto", "first", "last"]]
)


def assert_type(var: Any, var_name: str, types: list[type], allow_none: bool = False) -> None:
    if var is None:
        if not allow_none:
            raise TypeError(f"{var_name} cannot be None")
    elif not isinstance(var, tuple(types)):
        raise TypeError(f"{var_name!r} must be one of types {types}, not {type(var)}")


def validate_user_input_dict_with_identifier(dct: Mapping, required_keys: set[str]) -> Identifier:
    if not isinstance(dct, Mapping):
        raise TypeError(f"Expected dict-like object, got {type(dct)}")

    # Verify that we have gotten exactly one identifier:
    if (xid := dct.get("external_id")) is None:  # "" is valid ext.id
        xid = dct.get("externalId")
    instance_id = dct.get("instance_id") or dct.get("instanceId")
    id_dct = Identifier.of_either(dct.get("id"), xid, instance_id)
    missing_keys = required_keys.difference(dct)
    invalid_keys = set(dct) - required_keys - {"id", "externalId", "external_id", "instance_id", "instanceId"}
    if missing_keys or invalid_keys:
        raise ValueError(
            f"Given dictionary failed validation. Invalid key(s): {sorted(invalid_keys)}, "
            f"required key(s) missing: {sorted(missing_keys)}."
        )
    return id_dct


def _process_identifiers(
    ids: int | Sequence[int] | None,
    external_ids: str | SequenceNotStr[str] | None,
    *,
    id_name: str,
) -> list[dict[str, int | str]] | None:
    if ids is None and external_ids is None:
        return None
    return IdentifierSequence.load(ids, external_ids, id_name=id_name).as_dicts()


process_data_set_ids: Callable[
    [int | Sequence[int] | None, str | SequenceNotStr[str] | None], list[dict[str, int | str]] | None
] = functools.partial(_process_identifiers, id_name="data_set")
process_asset_subtree_ids: Callable[
    [int | Sequence[int] | None, str | SequenceNotStr[str] | None], list[dict[str, int | str]] | None
] = functools.partial(_process_identifiers, id_name="asset_subtree")


def prepare_filter_sort(
    sort: SortSpec | list[SortSpec] | None, sort_type: type[T_CogniteSort]
) -> list[dict[str, Any]] | None:
    if sort is not None:
        if not isinstance(sort, list):
            sort = [sort]
        return [sort_type.load(item).dump(camel_case=True) for item in sort]
    return None


def verify_limit(limit: Any) -> None:
    if is_unlimited(limit):
        return

    if isinstance(limit, int):
        if limit <= 0:
            raise ValueError("limit must be strictly positive")
    else:
        raise TypeError(
            "A finite 'limit' must be given as a strictly positive integer. "
            "To indicate 'no limit' use one of: [None, -1, math.inf]."
        )
