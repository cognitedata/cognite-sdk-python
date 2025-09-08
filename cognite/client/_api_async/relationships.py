from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    CountAggregate,
    LabelFilter,
    Relationship,
    RelationshipFilter,
    RelationshipList,
    RelationshipUpdate,
    RelationshipWrite,
    TimestampRange,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncRelationshipsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/relationships"

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        source_external_ids: SequenceNotStr[str] | None = None,
        source_types: SequenceNotStr[str] | None = None,
        target_external_ids: SequenceNotStr[str] | None = None,
        target_types: SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        start_time: dict[str, Any] | TimestampRange | None = None,
        end_time: dict[str, Any] | TimestampRange | None = None,
        confidence: dict[str, Any] | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        active_at_time: dict[str, int] | None = None,
        labels: LabelFilter | None = None,
        external_id_prefix: str | None = None,
        limit: int | None = None,
        partitions: int | None = None,
    ) -> AsyncIterator[Relationship]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        source_external_ids: SequenceNotStr[str] | None = None,
        source_types: SequenceNotStr[str] | None = None,
        target_external_ids: SequenceNotStr[str] | None = None,
        target_types: SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        start_time: dict[str, Any] | TimestampRange | None = None,
        end_time: dict[str, Any] | TimestampRange | None = None,
        confidence: dict[str, Any] | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        active_at_time: dict[str, int] | None = None,
        labels: LabelFilter | None = None,
        external_id_prefix: str | None = None,
        limit: int | None = None,
        partitions: int | None = None,
    ) -> AsyncIterator[RelationshipList]: ...

    def __call__(self, chunk_size: int | None = None, **kwargs) -> AsyncIterator[Relationship] | AsyncIterator[RelationshipList]:
        """Async iterator over relationships."""
        return self._list_generator(
            list_cls=RelationshipList,
            resource_cls=Relationship,
            method="POST",
            chunk_size=chunk_size,
            **kwargs
        )

    def __aiter__(self) -> AsyncIterator[Relationship]:
        """Async iterate over all relationships."""
        return self.__call__()

    async def retrieve(self, external_id: str) -> Relationship | None:
        """`Retrieve a single relationship by external id. <https://developer.cognite.com/api#tag/Relationships/operation/byExternalIdsRelationships>`_"""
        identifiers = IdentifierSequence.load(external_ids=external_id).as_singleton()
        return await self._retrieve_multiple(
            list_cls=RelationshipList,
            resource_cls=Relationship,
            identifiers=identifiers,
        )

    async def retrieve_multiple(
        self,
        external_ids: SequenceNotStr[str],
        ignore_unknown_ids: bool = False,
    ) -> RelationshipList:
        """`Retrieve multiple relationships by external id. <https://developer.cognite.com/api#tag/Relationships/operation/byExternalIdsRelationships>`_"""
        identifiers = IdentifierSequence.load(external_ids=external_ids)
        return await self._retrieve_multiple(
            list_cls=RelationshipList,
            resource_cls=Relationship,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    @overload
    async def create(self, relationship: Sequence[Relationship] | Sequence[RelationshipWrite]) -> RelationshipList: ...

    @overload
    async def create(self, relationship: Relationship | RelationshipWrite) -> Relationship: ...

    async def create(self, relationship: Relationship | RelationshipWrite | Sequence[Relationship] | Sequence[RelationshipWrite]) -> Relationship | RelationshipList:
        """`Create one or more relationships. <https://developer.cognite.com/api#tag/Relationships/operation/createRelationships>`_"""
        return await self._create_multiple(
            list_cls=RelationshipList,
            resource_cls=Relationship,
            items=relationship,
        )

    async def delete(
        self,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more relationships <https://developer.cognite.com/api#tag/Relationships/operation/deleteRelationships>`_"""
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    async def update(self, item: Sequence[Relationship | RelationshipUpdate]) -> RelationshipList: ...

    @overload
    async def update(self, item: Relationship | RelationshipUpdate) -> Relationship: ...

    async def update(self, item: Relationship | RelationshipUpdate | Sequence[Relationship | RelationshipUpdate]) -> Relationship | RelationshipList:
        """`Update one or more relationships <https://developer.cognite.com/api#tag/Relationships/operation/updateRelationships>`_"""
        return await self._update_multiple(
            list_cls=RelationshipList,
            resource_cls=Relationship,
            update_cls=RelationshipUpdate,
            items=item,
        )

    @overload
    async def upsert(self, item: Sequence[Relationship | RelationshipWrite], mode: Literal["patch", "replace"] = "patch") -> RelationshipList: ...

    @overload 
    async def upsert(self, item: Relationship | RelationshipWrite, mode: Literal["patch", "replace"] = "patch") -> Relationship: ...

    async def upsert(
        self,
        item: Relationship | RelationshipWrite | Sequence[Relationship | RelationshipWrite],
        mode: Literal["patch", "replace"] = "patch",
    ) -> Relationship | RelationshipList:
        """`Upsert relationships <https://developer.cognite.com/api#tag/Relationships/operation/createRelationships>`_"""
        return await self._upsert_multiple(
            items=item,
            list_cls=RelationshipList,
            resource_cls=Relationship,
            update_cls=RelationshipUpdate,
            mode=mode,
        )

    async def list(
        self,
        source_external_ids: SequenceNotStr[str] | None = None,
        source_types: SequenceNotStr[str] | None = None,
        target_external_ids: SequenceNotStr[str] | None = None,
        target_types: SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        start_time: dict[str, Any] | TimestampRange | None = None,
        end_time: dict[str, Any] | TimestampRange | None = None,
        confidence: dict[str, Any] | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        active_at_time: dict[str, int] | None = None,
        labels: LabelFilter | None = None,
        external_id_prefix: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        partitions: int | None = None,
    ) -> RelationshipList:
        """`List relationships <https://developer.cognite.com/api#tag/Relationships/operation/listRelationships>`_"""
        filter = RelationshipFilter(
            source_external_ids=source_external_ids,
            source_types=source_types,
            target_external_ids=target_external_ids,
            target_types=target_types,
            data_set_ids=data_set_ids,
            data_set_external_ids=data_set_external_ids,
            start_time=start_time,
            end_time=end_time,
            confidence=confidence,
            last_updated_time=last_updated_time,
            created_time=created_time,
            active_at_time=active_at_time,
            labels=labels,
            external_id_prefix=external_id_prefix,
        ).dump(camel_case=True)

        return await self._list(
            list_cls=RelationshipList,
            resource_cls=Relationship,
            method="POST",
            limit=limit,
            filter=filter,
            partitions=partitions,
        )