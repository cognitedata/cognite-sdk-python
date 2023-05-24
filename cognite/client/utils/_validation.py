from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Callable, Mapping, Sequence

from cognite.client.utils._identifier import Identifier, IdentifierSequence

if TYPE_CHECKING:
    from cognite.client.utils._identifier import T_ID

RESERVED_EXTERNAL_IDS = frozenset(
    {
        "Query",
        "Mutation",
        "Subscription",
        "String",
        "Int32",
        "Int64",
        "Int",
        "Float32",
        "Float64",
        "Float",
        "Timestamp",
        "JSONObject",
        "Date",
        "Numeric",
        "Boolean",
        "PageInfo",
        "File",
        "Sequence",
        "TimeSerie",
    }
)
RESERVED_SPACE_IDS = frozenset({"space", "cdf", "dms", "pg3", "shared", "system", "node", "edge"})

RESERVED_PROPERTIES = frozenset(
    {
        "space",
        "externalId",
        "createdTime",
        "lastUpdatedTime",
        "deletedTime",
        "edge_id",
        "node_id",
        "project_id",
        "property_group",
        "seq",
        "tg_table_name",
        "extensions",
    }
)


def validate_user_input_dict_with_identifier(dct: Mapping, required_keys: set[str]) -> dict[str, T_ID]:
    if not isinstance(dct, Mapping):
        raise TypeError(f"Expected dict-like object, got {type(dct)}")

    # Verify that we have gotten exactly one identifier:
    if (xid := dct.get("external_id")) is None:  # "" is valid ext.id
        xid = dct.get("externalId")
    id_dct = Identifier.of_either(dct.get("id"), xid).as_dict(camel_case=True)

    missing_keys = required_keys.difference(dct)
    invalid_keys = set(dct) - required_keys - {"id", "externalId", "external_id"}
    if missing_keys or invalid_keys:
        raise ValueError(
            f"Given dictionary failed validation. Invalid key(s): {sorted(invalid_keys)}, "
            f"required key(s) missing: {sorted(missing_keys)}."
        )
    return id_dct


def _process_identifiers(
    ids: int | Sequence[int] | None,
    external_ids: str | Sequence[str] | None,
    *,
    id_name: str,
) -> list[dict[str, int | str]] | None:
    if ids is None and external_ids is None:
        return None
    return IdentifierSequence.load(ids, external_ids, id_name=id_name).as_dicts()


process_data_set_ids: Callable[
    [int | Sequence[int] | None, str | Sequence[str] | None], list[dict[str, int | str]] | None
] = functools.partial(_process_identifiers, id_name="data_set")
process_asset_subtree_ids: Callable[
    [int | Sequence[int] | None, str | Sequence[str] | None], list[dict[str, int | str]] | None
] = functools.partial(_process_identifiers, id_name="asset_subtree")


def validate_data_modeling_identifier(space: str | None, external_id: str | None = None) -> None:
    if space and space in RESERVED_SPACE_IDS:
        raise ValueError(f"The space ID: {space} is reserved. Please use another ID.")
    if external_id and external_id in RESERVED_EXTERNAL_IDS:
        raise ValueError(f"The external ID: {external_id} is reserved. Please use another ID.")
