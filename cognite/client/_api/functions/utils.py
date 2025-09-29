from __future__ import annotations

from typing import TYPE_CHECKING, NoReturn, overload

from cognite.client.utils._auxiliary import at_most_one_is_not_none
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.utils._identifier import Identifier


def _get_function_internal_id(cognite_client: AsyncCogniteClient, identifier: Identifier) -> int:
    primitive = identifier.as_primitive()
    if identifier.is_id:
        return primitive

    if identifier.is_external_id:
        function = cognite_client.functions.retrieve(external_id=primitive)
        if function:
            return function.id

    raise ValueError(f'Function with external ID "{primitive}" is not found')


def _get_function_identifier(function_id: int | None, function_external_id: str | None) -> Identifier:
    identifier = IdentifierSequence.load(function_id, function_external_id, id_name="function")
    if identifier.is_singleton():
        return identifier[0]
    raise ValueError("Exactly one of function_id and function_external_id must be specified")


@overload
def _ensure_at_most_one_id_given(function_id: int, function_external_id: str) -> NoReturn: ...


@overload
def _ensure_at_most_one_id_given(function_id: int | None, function_external_id: str | None) -> None: ...


def _ensure_at_most_one_id_given(function_id: int | None, function_external_id: str | None) -> None:
    if at_most_one_is_not_none(function_id, function_external_id):
        return
    raise ValueError("Both 'function_id' and 'function_external_id' were supplied, pass exactly one or neither.")
