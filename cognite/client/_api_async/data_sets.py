from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    CountAggregate,
    DataSet,
    DataSetFilter,
    DataSetList,
    DataSetUpdate,
    DataSetWrite,
    TimestampRange,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncDataSetsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/datasets"

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        name: str | None = None,
        external_id_prefix: str | None = None,
        write_protected: bool | None = None,
        metadata: dict[str, str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[DataSet]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        name: str | None = None,
        external_id_prefix: str | None = None,
        write_protected: bool | None = None,
        metadata: dict[str, str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[DataSetList]: ...

    def __call__(self, chunk_size: int | None = None, **kwargs) -> AsyncIterator[DataSet] | AsyncIterator[DataSetList]:
        """Async iterator over data sets."""
        filter = DataSetFilter(
            name=kwargs.get('name'),
            external_id_prefix=kwargs.get('external_id_prefix'),
            write_protected=kwargs.get('write_protected'),
            metadata=kwargs.get('metadata'),
            created_time=kwargs.get('created_time'),
            last_updated_time=kwargs.get('last_updated_time'),
        ).dump(camel_case=True)

        return self._list_generator(
            list_cls=DataSetList,
            resource_cls=DataSet,
            method="POST",
            chunk_size=chunk_size,
            filter=filter,
            limit=kwargs.get('limit'),
        )

    def __aiter__(self) -> AsyncIterator[DataSet]:
        """Async iterate over all data sets."""
        return self.__call__()

    async def retrieve(self, id: int | None = None, external_id: str | None = None) -> DataSet | None:
        """`Retrieve a single data set by id. <https://developer.cognite.com/api#tag/Data-sets/operation/getDataSets>`_"""
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return await self._retrieve_multiple(
            list_cls=DataSetList,
            resource_cls=DataSet,
            identifiers=identifiers,
        )

    async def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> DataSetList:
        """`Retrieve multiple data sets by id. <https://developer.cognite.com/api#tag/Data-sets/operation/getDataSets>`_"""
        identifiers = IdentifierSequence.load(ids, external_ids)
        return await self._retrieve_multiple(
            list_cls=DataSetList,
            resource_cls=DataSet,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    @overload
    async def create(self, data_set: DataSet | DataSetWrite) -> DataSet: ...

    @overload
    async def create(self, data_set: Sequence[DataSet] | Sequence[DataSetWrite]) -> DataSetList: ...

    async def create(self, data_set: DataSet | DataSetWrite | Sequence[DataSet] | Sequence[DataSetWrite]) -> DataSet | DataSetList:
        """`Create one or more data sets. <https://developer.cognite.com/api#tag/Data-sets/operation/createDataSets>`_"""
        return await self._create_multiple(
            list_cls=DataSetList,
            resource_cls=DataSet,
            items=data_set,
        )

    async def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more data sets <https://developer.cognite.com/api#tag/Data-sets/operation/deleteDataSets>`_"""
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(id, external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    async def update(self, item: DataSet | DataSetUpdate) -> DataSet: ...

    @overload
    async def update(self, item: Sequence[DataSet] | Sequence[DataSetUpdate]) -> DataSetList: ...

    async def update(self, item: DataSet | DataSetUpdate | Sequence[DataSet] | Sequence[DataSetUpdate]) -> DataSet | DataSetList:
        """`Update one or more data sets <https://developer.cognite.com/api#tag/Data-sets/operation/updateDataSets>`_"""
        return await self._update_multiple(
            list_cls=DataSetList,
            resource_cls=DataSet,
            update_cls=DataSetUpdate,
            items=item,
        )

    async def list(
        self,
        name: str | None = None,
        external_id_prefix: str | None = None,
        write_protected: bool | None = None,
        metadata: dict[str, str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> DataSetList:
        """`List data sets <https://developer.cognite.com/api#tag/Data-sets/operation/listDataSets>`_"""
        filter = DataSetFilter(
            name=name,
            external_id_prefix=external_id_prefix,
            write_protected=write_protected,
            metadata=metadata,
            created_time=created_time,
            last_updated_time=last_updated_time,
        ).dump(camel_case=True)

        return await self._list(
            list_cls=DataSetList,
            resource_cls=DataSet,
            method="POST",
            limit=limit,
            filter=filter,
        )

    async def aggregate(self, filter: DataSetFilter | dict[str, Any] | None = None) -> list[CountAggregate]:
        """`Aggregate data sets <https://developer.cognite.com/api#tag/Data-sets/operation/aggregateDataSets>`_"""
        return await self._aggregate(
            cls=CountAggregate,
            resource_path=self._RESOURCE_PATH,
            filter=filter,
        )

    @overload
    async def upsert(self, item: Sequence[DataSet | DataSetWrite], mode: Literal["patch", "replace"] = "patch") -> DataSetList: ...

    @overload 
    async def upsert(self, item: DataSet | DataSetWrite, mode: Literal["patch", "replace"] = "patch") -> DataSet: ...

    async def upsert(
        self,
        item: DataSet | DataSetWrite | Sequence[DataSet | DataSetWrite],
        mode: Literal["patch", "replace"] = "patch",
    ) -> DataSet | DataSetList:
        """`Upsert data sets <https://developer.cognite.com/api#tag/Data-sets/operation/createDataSets>`_"""
        return await self._upsert_multiple(
            items=item,
            list_cls=DataSetList,
            resource_cls=DataSet,
            update_cls=DataSetUpdate,
            mode=mode,
        )