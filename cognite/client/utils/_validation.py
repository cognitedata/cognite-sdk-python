from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Callable, Mapping, Sequence

from cognite.client.utils._identifier import Identifier, IdentifierSequence

if TYPE_CHECKING:
    from cognite.client.utils._identifier import T_ID


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
