from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    CountAggregate,
    Sequence as CogniteSequence,
    SequenceFilter,
    SequenceList,
    SequenceUpdate,
    SequenceWrite,
    TimestampRange,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import process_asset_subtree_ids, process_data_set_ids
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncSequencesAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/sequences"

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        name: str | None = None,
        external_id_prefix: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CogniteSequence]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        name: str | None = None,
        external_id_prefix: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[SequenceList]: ...

    def __call__(self, chunk_size: int | None = None, **kwargs) -> AsyncIterator[CogniteSequence] | AsyncIterator[SequenceList]:
        """Async iterator over sequences."""
        return self._list_generator(
            list_cls=SequenceList,
            resource_cls=CogniteSequence,
            method="POST",
            chunk_size=chunk_size,
            **kwargs
        )

    def __aiter__(self) -> AsyncIterator[CogniteSequence]:
        """Async iterate over all sequences."""
        return self.__call__()

    async def retrieve(self, id: int | None = None, external_id: str | None = None) -> CogniteSequence | None:
        """`Retrieve a single sequence by id. <https://developer.cognite.com/api#tag/Sequences/operation/getSequenceById>`_"""
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return await self._retrieve_multiple(
            list_cls=SequenceList,
            resource_cls=CogniteSequence,
            identifiers=identifiers,
        )

    async def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> SequenceList:
        """`Retrieve multiple sequences by id. <https://developer.cognite.com/api#tag/Sequences/operation/getSequenceById>`_"""
        identifiers = IdentifierSequence.load(ids, external_ids)
        return await self._retrieve_multiple(
            list_cls=SequenceList,
            resource_cls=CogniteSequence,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    @overload
    async def create(self, sequence: Sequence[CogniteSequence] | Sequence[SequenceWrite]) -> SequenceList: ...

    @overload
    async def create(self, sequence: CogniteSequence | SequenceWrite) -> CogniteSequence: ...

    async def create(self, sequence: CogniteSequence | SequenceWrite | Sequence[CogniteSequence] | Sequence[SequenceWrite]) -> CogniteSequence | SequenceList:
        """`Create one or more sequences. <https://developer.cognite.com/api#tag/Sequences/operation/createSequence>`_"""
        return await self._create_multiple(
            list_cls=SequenceList,
            resource_cls=CogniteSequence,
            items=sequence,
        )

    async def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more sequences <https://developer.cognite.com/api#tag/Sequences/operation/deleteSequence>`_"""
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(id, external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    async def update(self, item: Sequence[CogniteSequence | SequenceUpdate]) -> SequenceList: ...

    @overload
    async def update(self, item: CogniteSequence | SequenceUpdate) -> CogniteSequence: ...

    async def update(self, item: CogniteSequence | SequenceUpdate | Sequence[CogniteSequence | SequenceUpdate]) -> CogniteSequence | SequenceList:
        """`Update one or more sequences <https://developer.cognite.com/api#tag/Sequences/operation/updateSequence>`_"""
        return await self._update_multiple(
            list_cls=SequenceList,
            resource_cls=CogniteSequence,
            update_cls=SequenceUpdate,
            items=item,
        )

    async def list(
        self,
        name: str | None = None,
        external_id_prefix: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> SequenceList:
        """`List sequences <https://developer.cognite.com/api#tag/Sequences/operation/listSequences>`_"""
        asset_subtree_ids_processed = process_asset_subtree_ids(asset_subtree_ids, asset_subtree_external_ids)
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = SequenceFilter(
            name=name,
            external_id_prefix=external_id_prefix,
            metadata=metadata,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            data_set_ids=data_set_ids_processed,
            created_time=created_time,
            last_updated_time=last_updated_time,
        ).dump(camel_case=True)

        return await self._list(
            list_cls=SequenceList,
            resource_cls=CogniteSequence,
            method="POST",
            limit=limit,
            filter=filter,
        )

    async def aggregate(self, filter: SequenceFilter | dict[str, Any] | None = None) -> list[CountAggregate]:
        """`Aggregate sequences <https://developer.cognite.com/api#tag/Sequences/operation/aggregateSequences>`_"""
        return await self._aggregate(
            cls=CountAggregate,
            resource_path=self._RESOURCE_PATH,
            filter=filter,
        )

    async def search(
        self,
        name: str | None = None,
        description: str | None = None,
        query: str | None = None,
        filter: SequenceFilter | dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> SequenceList:
        """`Search for sequences <https://developer.cognite.com/api#tag/Sequences/operation/searchSequences>`_"""
        return await self._search(
            list_cls=SequenceList,
            search={
                "name": name,
                "description": description,
                "query": query,
            },
            filter=filter or {},
            limit=limit,
        )

    @overload
    async def upsert(self, item: Sequence[CogniteSequence | SequenceWrite], mode: Literal["patch", "replace"] = "patch") -> SequenceList: ...

    @overload 
    async def upsert(self, item: CogniteSequence | SequenceWrite, mode: Literal["patch", "replace"] = "patch") -> CogniteSequence: ...

    async def upsert(
        self,
        item: CogniteSequence | SequenceWrite | Sequence[CogniteSequence | SequenceWrite],
        mode: Literal["patch", "replace"] = "patch",
    ) -> CogniteSequence | SequenceList:
        """`Upsert sequences <https://developer.cognite.com/api#tag/Sequences/operation/createSequence>`_"""
        return await self._upsert_multiple(
            items=item,
            list_cls=SequenceList,
            resource_cls=CogniteSequence,
            update_cls=SequenceUpdate,
            mode=mode,
        )