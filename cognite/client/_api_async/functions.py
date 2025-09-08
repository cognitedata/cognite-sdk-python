from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    Function,
    FunctionList,
    FunctionWrite,
    FunctionUpdate,
    TimestampRange,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncFunctionsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/functions"

    async def list(
        self,
        name: str | None = None,
        owner: str | None = None,
        status: str | None = None,
        external_id_prefix: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> FunctionList:
        """`List functions <https://developer.cognite.com/api#tag/Functions/operation/listFunctions>`_"""
        filter = {}
        if name is not None:
            filter["name"] = name
        if owner is not None:
            filter["owner"] = owner
        if status is not None:
            filter["status"] = status
        if external_id_prefix is not None:
            filter["externalIdPrefix"] = external_id_prefix
        if created_time is not None:
            filter["createdTime"] = created_time

        return await self._list(
            list_cls=FunctionList,
            resource_cls=Function,
            method="POST",
            limit=limit,
            filter=filter,
        )

    async def retrieve(self, id: int | None = None, external_id: str | None = None) -> Function | None:
        """`Retrieve a single function by id. <https://developer.cognite.com/api#tag/Functions/operation/getFunctionsByIds>`_"""
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return await self._retrieve_multiple(
            list_cls=FunctionList,
            resource_cls=Function,
            identifiers=identifiers,
        )

    async def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> FunctionList:
        """`Retrieve multiple functions by id. <https://developer.cognite.com/api#tag/Functions/operation/getFunctionsByIds>`_"""
        identifiers = IdentifierSequence.load(ids, external_ids)
        return await self._retrieve_multiple(
            list_cls=FunctionList,
            resource_cls=Function,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    @overload
    async def create(self, function: Sequence[Function] | Sequence[FunctionWrite]) -> FunctionList: ...

    @overload
    async def create(self, function: Function | FunctionWrite) -> Function: ...

    async def create(self, function: Function | FunctionWrite | Sequence[Function] | Sequence[FunctionWrite]) -> Function | FunctionList:
        """`Create one or more functions. <https://developer.cognite.com/api#tag/Functions/operation/createFunctions>`_"""
        return await self._create_multiple(
            list_cls=FunctionList,
            resource_cls=Function,
            items=function,
        )

    async def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more functions <https://developer.cognite.com/api#tag/Functions/operation/deleteFunctions>`_"""
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(id, external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )