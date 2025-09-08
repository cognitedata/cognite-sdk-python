from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    Label,
    LabelDefinition,
    LabelDefinitionFilter,
    LabelDefinitionList,
    LabelDefinitionWrite,
    TimestampRange,
)
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncLabelsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/labels"

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        name: str | None = None,
        external_id_prefix: str | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[LabelDefinition]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        name: str | None = None,
        external_id_prefix: str | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[LabelDefinitionList]: ...

    def __call__(self, chunk_size: int | None = None, **kwargs) -> AsyncIterator[LabelDefinition] | AsyncIterator[LabelDefinitionList]:
        """Async iterator over label definitions."""
        return self._list_generator(
            list_cls=LabelDefinitionList,
            resource_cls=LabelDefinition,
            method="POST",
            chunk_size=chunk_size,
            **kwargs
        )

    def __aiter__(self) -> AsyncIterator[LabelDefinition]:
        """Async iterate over all label definitions."""
        return self.__call__()

    async def list(
        self,
        name: str | None = None,
        external_id_prefix: str | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> LabelDefinitionList:
        """`List label definitions <https://developer.cognite.com/api#tag/Labels/operation/listLabels>`_"""
        filter = LabelDefinitionFilter(
            name=name,
            external_id_prefix=external_id_prefix,
            data_set_ids=data_set_ids,
            data_set_external_ids=data_set_external_ids,
            created_time=created_time,
        ).dump(camel_case=True)

        return await self._list(
            list_cls=LabelDefinitionList,
            resource_cls=LabelDefinition,
            method="POST",
            limit=limit,
            filter=filter,
        )

    async def retrieve(self, external_id: str) -> LabelDefinition | None:
        """`Retrieve a single label definition by external id. <https://developer.cognite.com/api#tag/Labels/operation/byExternalIdsLabels>`_"""
        identifiers = IdentifierSequence.load(external_ids=external_id).as_singleton()
        return await self._retrieve_multiple(
            list_cls=LabelDefinitionList,
            resource_cls=LabelDefinition,
            identifiers=identifiers,
        )

    async def retrieve_multiple(
        self,
        external_ids: SequenceNotStr[str],
        ignore_unknown_ids: bool = False,
    ) -> LabelDefinitionList:
        """`Retrieve multiple label definitions by external id. <https://developer.cognite.com/api#tag/Labels/operation/byExternalIdsLabels>`_"""
        identifiers = IdentifierSequence.load(external_ids=external_ids)
        return await self._retrieve_multiple(
            list_cls=LabelDefinitionList,
            resource_cls=LabelDefinition,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    @overload
    async def create(self, label: Sequence[LabelDefinition] | Sequence[LabelDefinitionWrite]) -> LabelDefinitionList: ...

    @overload
    async def create(self, label: LabelDefinition | LabelDefinitionWrite) -> LabelDefinition: ...

    async def create(self, label: LabelDefinition | LabelDefinitionWrite | Sequence[LabelDefinition] | Sequence[LabelDefinitionWrite]) -> LabelDefinition | LabelDefinitionList:
        """`Create one or more label definitions. <https://developer.cognite.com/api#tag/Labels/operation/createLabels>`_"""
        return await self._create_multiple(
            list_cls=LabelDefinitionList,
            resource_cls=LabelDefinition,
            items=label,
        )

    async def delete(
        self,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more label definitions <https://developer.cognite.com/api#tag/Labels/operation/deleteLabels>`_"""
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )