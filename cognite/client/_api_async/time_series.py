from __future__ import annotations

import warnings
from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    CountAggregate,
    TimeSeries,
    TimeSeriesFilter,
    TimeSeriesList,
    TimeSeriesUpdate,
    TimeSeriesWrite,
    TimestampRange,
    filters,
)
from cognite.client.data_classes.aggregations import AggregationFilter, UniqueResultList
from cognite.client.data_classes.time_series import TimeSeriesPropertyLike, TimeSeriesSort, SortableTimeSeriesProperty
from cognite.client.data_classes.filters import _BASIC_FILTERS, Filter, _validate_filter
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import prepare_filter_sort, process_asset_subtree_ids, process_data_set_ids
from cognite.client.utils.useful_types import SequenceNotStr

_FILTERS_SUPPORTED: frozenset[type[Filter]] = _BASIC_FILTERS | {filters.Search}


class AsyncTimeSeriesAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/timeseries"

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        name: str | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        unit_quantity: str | None = None,
        is_string: bool | None = None,
        is_step: bool | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        limit: int | None = None,
        partitions: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
    ) -> AsyncIterator[TimeSeries]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        name: str | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        unit_quantity: str | None = None,
        is_string: bool | None = None,
        is_step: bool | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        limit: int | None = None,
        partitions: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
    ) -> AsyncIterator[TimeSeriesList]: ...

    def __call__(self, chunk_size: int | None = None, **kwargs) -> AsyncIterator[TimeSeries] | AsyncIterator[TimeSeriesList]:
        """Async iterator over time series."""
        return self._list_generator(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            method="POST",
            chunk_size=chunk_size,
            **kwargs
        )

    def __aiter__(self) -> AsyncIterator[TimeSeries]:
        """Async iterate over all time series."""
        return self.__call__()

    async def retrieve(self, id: int | None = None, external_id: str | None = None) -> TimeSeries | None:
        """`Retrieve a single time series by id. <https://developer.cognite.com/api#tag/Time-series/operation/getTimeSeriesByIds>`_"""
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return await self._retrieve_multiple(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            identifiers=identifiers,
        )

    async def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> TimeSeriesList:
        """`Retrieve multiple time series by id. <https://developer.cognite.com/api#tag/Time-series/operation/getTimeSeriesByIds>`_"""
        identifiers = IdentifierSequence.load(ids, external_ids)
        return await self._retrieve_multiple(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    async def aggregate(self, filter: TimeSeriesFilter | dict[str, Any] | None = None) -> list[CountAggregate]:
        """`Aggregate time series <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_"""
        return await self._aggregate(
            cls=CountAggregate,
            resource_path=self._RESOURCE_PATH,
            filter=filter,
        )

    async def aggregate_count(self, advanced_filter: Filter | dict[str, Any] | None = None) -> int:
        """`Count time series matching the specified filters. <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_"""
        return await self._advanced_aggregate(
            aggregate="count",
            advanced_filter=advanced_filter,
        )

    async def aggregate_cardinality_values(
        self,
        property: TimeSeriesPropertyLike,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
    ) -> int:
        """`Find approximate property cardinality for time series <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_"""
        return await self._advanced_aggregate(
            aggregate="cardinalityValues",
            properties=property,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    async def aggregate_cardinality_properties(
        self,
        path: TimeSeriesPropertyLike | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
    ) -> int:
        """`Find approximate paths cardinality for time series <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_"""
        return await self._advanced_aggregate(
            aggregate="cardinalityProperties",
            path=path,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    async def aggregate_unique_values(
        self,
        property: TimeSeriesPropertyLike,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> UniqueResultList:
        """`Get unique properties with counts for time series <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_"""
        return await self._advanced_aggregate(
            aggregate="uniqueValues",
            properties=property,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
            limit=limit,
        )

    async def aggregate_unique_properties(
        self,
        path: TimeSeriesPropertyLike | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> UniqueResultList:
        """`Get unique paths with counts for time series <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_"""
        return await self._advanced_aggregate(
            aggregate="uniqueProperties",
            path=path,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
            limit=limit,
        )

    @overload
    async def create(self, time_series: Sequence[TimeSeries] | Sequence[TimeSeriesWrite]) -> TimeSeriesList: ...

    @overload
    async def create(self, time_series: TimeSeries | TimeSeriesWrite) -> TimeSeries: ...

    async def create(self, time_series: TimeSeries | TimeSeriesWrite | Sequence[TimeSeries] | Sequence[TimeSeriesWrite]) -> TimeSeries | TimeSeriesList:
        """`Create one or more time series. <https://developer.cognite.com/api#tag/Time-series/operation/createTimeSeries>`_"""
        return await self._create_multiple(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            items=time_series,
        )

    async def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more time series <https://developer.cognite.com/api#tag/Time-series/operation/deleteTimeSeries>`_"""
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(id, external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    async def update(self, item: Sequence[TimeSeries | TimeSeriesUpdate]) -> TimeSeriesList: ...

    @overload
    async def update(self, item: TimeSeries | TimeSeriesUpdate) -> TimeSeries: ...

    async def update(self, item: TimeSeries | TimeSeriesUpdate | Sequence[TimeSeries | TimeSeriesUpdate]) -> TimeSeries | TimeSeriesList:
        """`Update one or more time series <https://developer.cognite.com/api#tag/Time-series/operation/updateTimeSeries>`_"""
        return await self._update_multiple(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            update_cls=TimeSeriesUpdate,
            items=item,
        )

    @overload
    async def upsert(self, item: Sequence[TimeSeries | TimeSeriesWrite], mode: Literal["patch", "replace"] = "patch") -> TimeSeriesList: ...

    @overload 
    async def upsert(self, item: TimeSeries | TimeSeriesWrite, mode: Literal["patch", "replace"] = "patch") -> TimeSeries: ...

    async def upsert(
        self,
        item: TimeSeries | TimeSeriesWrite | Sequence[TimeSeries | TimeSeriesWrite],
        mode: Literal["patch", "replace"] = "patch",
    ) -> TimeSeries | TimeSeriesList:
        """`Upsert time series <https://developer.cognite.com/api#tag/Time-series/operation/createTimeSeries>`_"""
        return await self._upsert_multiple(
            items=item,
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            update_cls=TimeSeriesUpdate,
            mode=mode,
        )

    async def list(
        self,
        name: str | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        unit_quantity: str | None = None,
        is_string: bool | None = None,
        is_step: bool | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        partitions: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
    ) -> TimeSeriesList:
        """`List time series <https://developer.cognite.com/api#tag/Time-series/operation/advancedListTimeSeries>`_"""
        asset_subtree_ids_processed = process_asset_subtree_ids(asset_subtree_ids, asset_subtree_external_ids)
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = TimeSeriesFilter(
            name=name,
            unit=unit,
            unit_external_id=unit_external_id,
            unit_quantity=unit_quantity,
            is_string=is_string,
            is_step=is_step,
            metadata=metadata,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            data_set_ids=data_set_ids_processed,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
        ).dump(camel_case=True)

        self._validate_filter(advanced_filter)

        return await self._list(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            method="POST",
            limit=limit,
            filter=filter,
            advanced_filter=advanced_filter,
            partitions=partitions,
        )

    async def search(
        self,
        name: str | None = None,
        description: str | None = None,
        query: str | None = None,
        filter: TimeSeriesFilter | dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> TimeSeriesList:
        """`Search for time series <https://developer.cognite.com/api#tag/Time-series/operation/searchTimeSeries>`_"""
        return await self._search(
            list_cls=TimeSeriesList,
            search={
                "name": name,
                "description": description,
                "query": query,
            },
            filter=filter or {},
            limit=limit,
        )

    async def filter(
        self,
        filter: Filter | dict,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> TimeSeriesList:
        """`Advanced filter time series <https://developer.cognite.com/api#tag/Time-series/operation/advancedListTimeSeries>`_"""
        warnings.warn(
            f"{self.__class__.__name__}.filter() method is deprecated and will be removed in the next major version of the SDK. Please use the {self.__class__.__name__}.list() method with advanced_filter parameter instead.",
            DeprecationWarning,
        )
        self._validate_filter(filter)
        return await self._list(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            method="POST",
            limit=limit,
            advanced_filter=filter.dump(camel_case_property=True) if isinstance(filter, Filter) else filter,
        )

    def _validate_filter(self, filter: Filter | dict[str, Any] | None) -> None:
        _validate_filter(filter, _FILTERS_SUPPORTED, type(self).__name__)