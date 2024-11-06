from __future__ import annotations

import warnings
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, overload

from cognite.client._api.datapoints import DatapointsAPI
from cognite.client._api.datapoints_subscriptions import DatapointsSubscriptionAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    TimeSeries,
    TimeSeriesFilter,
    TimeSeriesList,
    TimeSeriesUpdate,
    filters,
)
from cognite.client.data_classes.aggregations import AggregationFilter, CountAggregate, UniqueResultList
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.filters import _BASIC_FILTERS, Filter, _validate_filter
from cognite.client.data_classes.time_series import (
    SortableTimeSeriesProperty,
    TimeSeriesProperty,
    TimeSeriesSort,
    TimeSeriesWrite,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import prepare_filter_sort, process_asset_subtree_ids, process_data_set_ids
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig

SortSpec: TypeAlias = (
    TimeSeriesSort
    | str
    | SortableTimeSeriesProperty
    | tuple[str, Literal["asc", "desc"]]
    | tuple[str, Literal["asc", "desc"], Literal["auto", "first", "last"]]
)

_FILTERS_SUPPORTED: frozenset[type[Filter]] = _BASIC_FILTERS | {filters.Search}


class TimeSeriesAPI(APIClient):
    _RESOURCE_PATH = "/timeseries"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.data = DatapointsAPI(config, api_version, cognite_client)
        self.subscriptions = DatapointsSubscriptionAPI(config, api_version, cognite_client)

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
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        metadata: dict[str, Any] | None = None,
        external_id_prefix: str | None = None,
        created_time: dict[str, Any] | None = None,
        last_updated_time: dict[str, Any] | None = None,
        limit: int | None = None,
        partitions: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
    ) -> Iterator[TimeSeries]: ...
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
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        metadata: dict[str, Any] | None = None,
        external_id_prefix: str | None = None,
        created_time: dict[str, Any] | None = None,
        last_updated_time: dict[str, Any] | None = None,
        limit: int | None = None,
        partitions: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
    ) -> Iterator[TimeSeriesList]: ...
    def __call__(
        self,
        chunk_size: int | None = None,
        name: str | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        unit_quantity: str | None = None,
        is_string: bool | None = None,
        is_step: bool | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        metadata: dict[str, Any] | None = None,
        external_id_prefix: str | None = None,
        created_time: dict[str, Any] | None = None,
        last_updated_time: dict[str, Any] | None = None,
        limit: int | None = None,
        partitions: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
    ) -> Iterator[TimeSeries] | Iterator[TimeSeriesList]:
        """Iterate over time series

        Fetches time series as they are iterated over, so you keep a limited number of objects in memory.

        Args:
            chunk_size (int | None): Number of time series to return in each chunk. Defaults to yielding one time series a time.
            name (str | None): Name of the time series. Often referred to as tag.
            unit (str | None): Unit of the time series.
            unit_external_id (str | None): Filter on unit external ID.
            unit_quantity (str | None): Filter on unit quantity.
            is_string (bool | None): Whether the time series is an string time series.
            is_step (bool | None): Whether the time series is a step (piecewise constant) time series.
            asset_ids (Sequence[int] | None): List time series related to these assets.
            asset_external_ids (SequenceNotStr[str] | None): List time series related to these assets.
            asset_subtree_ids (int | Sequence[int] | None): Only include time series that are related to an asset in a subtree rooted at any of these assetIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            asset_subtree_external_ids (str | SequenceNotStr[str] | None): Only include time series that are related to an asset in a subtree rooted at any of these assetExternalIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            data_set_ids (int | Sequence[int] | None): Return only time series in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | SequenceNotStr[str] | None): Return only time series in the specified data set(s) with this external id / these external ids.
            metadata (dict[str, Any] | None): Custom, application specific metadata. String key -> String value
            external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
            created_time (dict[str, Any] | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (dict[str, Any] | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            limit (int | None): Maximum number of time series to return. Defaults to return all items.
            partitions (int | None): Retrieve resources in parallel using this number of workers (values up to 10 allowed), limit must be set to `None` (or `-1`).
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL (Domain Specific Language). It allows defining complex filtering expressions that combine simple operations, such as equals, prefix, exists, etc., using boolean operators and, or, and not.
            sort (SortSpec | list[SortSpec] | None): The criteria to sort by. Defaults to desc for `_score_` and asc for all other properties. Sort is not allowed if `partitions` is used.

        Returns:
            Iterator[TimeSeries] | Iterator[TimeSeriesList]: yields TimeSeries one by one if chunk_size is not specified, else TimeSeriesList objects.
        """
        asset_subtree_ids_processed = process_asset_subtree_ids(asset_subtree_ids, asset_subtree_external_ids)
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = TimeSeriesFilter(
            name=name,
            unit=unit,
            unit_external_id=unit_external_id,
            unit_quantity=unit_quantity,
            is_step=is_step,
            is_string=is_string,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            metadata=metadata,
            created_time=created_time,
            data_set_ids=data_set_ids_processed,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
        )

        prep_sort = prepare_filter_sort(sort, TimeSeriesSort)
        self._validate_filter(advanced_filter)

        return self._list_generator(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            method="POST",
            chunk_size=chunk_size,
            filter=filter.dump(camel_case=True),
            advanced_filter=advanced_filter,
            limit=limit,
            partitions=partitions,
            sort=prep_sort,
        )

    def __iter__(self) -> Iterator[TimeSeries]:
        """Iterate over time series

        Fetches time series as they are iterated over, so you keep a limited number of metadata objects in memory.

        Returns:
            Iterator[TimeSeries]: yields TimeSeries one by one.
        """
        return self()

    def retrieve(
        self, id: int | None = None, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> TimeSeries | None:
        """`Retrieve a single time series by id. <https://developer.cognite.com/api#tag/Time-series/operation/getTimeSeriesByIds>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID
            instance_id (NodeId | None): Instance ID

        Returns:
            TimeSeries | None: Requested time series or None if it does not exist.

        Examples:

            Get time series by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.time_series.retrieve(id=1)

            Get time series by external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.time_series.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id, instance_ids=instance_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            identifiers=identifiers,
        )

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        instance_ids: Sequence[NodeId] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> TimeSeriesList:
        """`Retrieve multiple time series by id. <https://developer.cognite.com/api#tag/Time-series/operation/getTimeSeriesByIds>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (SequenceNotStr[str] | None): External IDs
            instance_ids (Sequence[NodeId] | None): Instance IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            TimeSeriesList: The requested time series.

        Examples:

            Get time series by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.time_series.retrieve_multiple(ids=[1, 2, 3])

            Get time series by external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.time_series.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids, instance_ids=instance_ids)
        return self._retrieve_multiple(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def aggregate(self, filter: TimeSeriesFilter | dict[str, Any] | None = None) -> list[CountAggregate]:
        """`Aggregate time series <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_

        Args:
            filter (TimeSeriesFilter | dict[str, Any] | None): Filter on time series filter with exact match

        Returns:
            list[CountAggregate]: List of sequence aggregates

        Examples:

            List time series::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.time_series.aggregate(filter={"unit": "kpa"})
        """
        warnings.warn(
            "This method will be deprecated in the next major release. Use aggregate_count instead.", DeprecationWarning
        )
        return self._aggregate(filter=filter, cls=CountAggregate)

    def aggregate_count(
        self,
        advanced_filter: Filter | dict[str, Any] | None = None,
        filter: TimeSeriesFilter | dict[str, Any] | None = None,
    ) -> int:
        """`Count of time series matching the specified filters and search. <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_

        Args:
            advanced_filter (Filter | dict[str, Any] | None): The filter to narrow down the time series to count.
            filter (TimeSeriesFilter | dict[str, Any] | None): The filter to narrow down time series to count requiring exact match.

        Returns:
            int: The number of time series matching the specified filters and search.

        Examples:

        Count the number of time series in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> client = CogniteClient()
            >>> count = client.time_series.aggregate_count()

        Count the number of numeric time series in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import filters
            >>> from cognite.client.data_classes.time_series import TimeSeriesProperty
            >>> client = CogniteClient()
            >>> is_numeric = filters.Equals(TimeSeriesProperty.is_string, False)
            >>> count = client.time_series.aggregate_count(advanced_filter=is_numeric)

        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            "count",
            filter=filter,
            advanced_filter=advanced_filter,
        )

    def aggregate_cardinality_values(
        self,
        property: TimeSeriesProperty | str | list[str],
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        filter: TimeSeriesFilter | dict[str, Any] | None = None,
    ) -> int:
        """`Find approximate property count for time series. <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_

        Args:
            property (TimeSeriesProperty | str | list[str]): The property to count the cardinality of.
            advanced_filter (Filter | dict[str, Any] | None): The filter to narrow down the time series to count cardinality.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): The filter to apply to the resulting buckets.
            filter (TimeSeriesFilter | dict[str, Any] | None): The filter to narrow down the time series to count requiring exact match.
        Returns:
            int: The number of properties matching the specified filters and search.

        Examples:

        Count the number of different units used for time series in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes.time_series import TimeSeriesProperty
            >>> client = CogniteClient()
            >>> unit_count = client.time_series.aggregate_cardinality_values(TimeSeriesProperty.unit)

        Count the number of timezones (metadata key) for time series with the word "critical" in the description
        in your CDF project, but exclude timezones from america:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import filters, aggregations as aggs
            >>> from cognite.client.data_classes.time_series import TimeSeriesProperty
            >>> client = CogniteClient()
            >>> not_america = aggs.Not(aggs.Prefix("america"))
            >>> is_critical = filters.Search(TimeSeriesProperty.description, "critical")
            >>> timezone_count = client.time_series.aggregate_cardinality_values(
            ...     TimeSeriesProperty.metadata_key("timezone"),
            ...     advanced_filter=is_critical,
            ...     aggregate_filter=not_america)

        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            "cardinalityValues",
            properties=property,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    def aggregate_cardinality_properties(
        self,
        path: TimeSeriesProperty | str | list[str],
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        filter: TimeSeriesFilter | dict[str, Any] | None = None,
    ) -> int:
        """`Find approximate paths count for time series.  <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_

        Args:
            path (TimeSeriesProperty | str | list[str]): The scope in every document to aggregate properties. The only value allowed now is ["metadata"]. It means to aggregate only metadata properties (aka keys).
            advanced_filter (Filter | dict[str, Any] | None): The filter to narrow down the time series to count cardinality.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): The filter to apply to the resulting buckets.
            filter (TimeSeriesFilter | dict[str, Any] | None): The filter to narrow down the time series to count requiring exact match.
        Returns:
            int: The number of properties matching the specified filters and search.

        Examples:

            Count the number of metadata keys in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.time_series import TimeSeriesProperty
                >>> client = CogniteClient()
                >>> key_count = client.time_series.aggregate_cardinality_properties(TimeSeriesProperty.metadata)
        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            "cardinalityProperties",
            path=path,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    def aggregate_unique_values(
        self,
        property: TimeSeriesProperty | str | list[str],
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        filter: TimeSeriesFilter | dict[str, Any] | None = None,
    ) -> UniqueResultList:
        """`Get unique properties with counts for time series. <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_

        Args:
            property (TimeSeriesProperty | str | list[str]): The property to group by.
            advanced_filter (Filter | dict[str, Any] | None): The filter to narrow down the time series to count cardinality.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): The filter to apply to the resulting buckets.
            filter (TimeSeriesFilter | dict[str, Any] | None): The filter to narrow down the time series to count requiring exact match.

        Returns:
            UniqueResultList: List of unique values of time series matching the specified filters and search.

        Examples:

            Get the timezones (metadata key) with count for your time series in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.time_series import TimeSeriesProperty
                >>> client = CogniteClient()
                >>> result = client.time_series.aggregate_unique_values(TimeSeriesProperty.metadata_key("timezone"))
                >>> print(result.unique)

            Get the different units with count used for time series created after 2020-01-01 in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.time_series import TimeSeriesProperty
                >>> from cognite.client.utils import timestamp_to_ms
                >>> from datetime import datetime
                >>> client = CogniteClient()
                >>> created_after_2020 = filters.Range(TimeSeriesProperty.created_time, gte=timestamp_to_ms(datetime(2020, 1, 1)))
                >>> result = client.time_series.aggregate_unique_values(TimeSeriesProperty.unit, advanced_filter=created_after_2020)
                >>> print(result.unique)

            Get the different units with count for time series updated after 2020-01-01 in your CDF project, but exclude all units that
            start with "test":

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.time_series import TimeSeriesProperty
                >>> from cognite.client.data_classes import aggregations as aggs, filters
                >>> client = CogniteClient()
                >>> not_test = aggs.Not(aggs.Prefix("test"))
                >>> created_after_2020 = filters.Range(TimeSeriesProperty.last_updated_time, gte=timestamp_to_ms(datetime(2020, 1, 1)))
                >>> result = client.time_series.aggregate_unique_values(TimeSeriesProperty.unit, advanced_filter=created_after_2020, aggregate_filter=not_test)
                >>> print(result.unique)
        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            aggregate="uniqueValues",
            properties=property,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    def aggregate_unique_properties(
        self,
        path: TimeSeriesProperty | str | list[str],
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        filter: TimeSeriesFilter | dict[str, Any] | None = None,
    ) -> UniqueResultList:
        """`Get unique paths with counts for time series. <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_

        Args:
            path (TimeSeriesProperty | str | list[str]): The scope in every document to aggregate properties. The only value allowed now is ["metadata"]. It means to aggregate only metadata properties (aka keys).
            advanced_filter (Filter | dict[str, Any] | None): The filter to narrow down the time series to count cardinality.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): The filter to apply to the resulting buckets.
            filter (TimeSeriesFilter | dict[str, Any] | None): The filter to narrow down the time series to count requiring exact match.

        Returns:
            UniqueResultList: List of unique values of time series matching the specified filters and search.

        Examples:

            Get the metadata keys with count for your time series in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.time_series import TimeSeriesProperty
                >>> client = CogniteClient()
                >>> result = client.time_series.aggregate_unique_values(TimeSeriesProperty.metadata)
        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            aggregate="uniqueProperties",
            path=path,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    @overload
    def create(self, time_series: Sequence[TimeSeries] | Sequence[TimeSeriesWrite]) -> TimeSeriesList: ...

    @overload
    def create(self, time_series: TimeSeries | TimeSeriesWrite) -> TimeSeries: ...

    def create(
        self, time_series: TimeSeries | TimeSeriesWrite | Sequence[TimeSeries] | Sequence[TimeSeriesWrite]
    ) -> TimeSeries | TimeSeriesList:
        """`Create one or more time series. <https://developer.cognite.com/api#tag/Time-series/operation/postTimeSeries>`_

        Args:
            time_series (TimeSeries | TimeSeriesWrite | Sequence[TimeSeries] | Sequence[TimeSeriesWrite]): TimeSeries or list of TimeSeries to create.

        Returns:
            TimeSeries | TimeSeriesList: The created time series.

        Examples:

            Create a new time series::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TimeSeriesWrite
                >>> client = CogniteClient()
                >>> ts = client.time_series.create(TimeSeriesWrite(name="my_ts", data_set_id=123, external_id="foo"))
        """
        return self._create_multiple(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            items=time_series,
            input_resource_cls=TimeSeriesWrite,
        )

    def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more time series. <https://developer.cognite.com/api#tag/Time-series/operation/deleteTimeSeries>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | SequenceNotStr[str] | None): External ID or list of external ids
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Examples:

            Delete time series by id or external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.time_series.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    def update(
        self,
        item: Sequence[TimeSeries | TimeSeriesWrite | TimeSeriesUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> TimeSeriesList: ...

    @overload
    def update(
        self,
        item: TimeSeries | TimeSeriesWrite | TimeSeriesUpdate,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> TimeSeries: ...

    def update(
        self,
        item: TimeSeries
        | TimeSeriesWrite
        | TimeSeriesUpdate
        | Sequence[TimeSeries | TimeSeriesWrite | TimeSeriesUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> TimeSeries | TimeSeriesList:
        """`Update one or more time series. <https://developer.cognite.com/api#tag/Time-series/operation/alterTimeSeries>`_

        Args:
            item (TimeSeries | TimeSeriesWrite | TimeSeriesUpdate | Sequence[TimeSeries | TimeSeriesWrite | TimeSeriesUpdate]): Time series to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (TimeSeries or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            TimeSeries | TimeSeriesList: Updated time series.

        Examples:

            Update a time series that you have fetched. This will perform a full update of the time series::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.time_series.retrieve(id=1)
                >>> res.description = "New description"
                >>> res = client.time_series.update(res)

            Perform a partial update on a time series, updating the description and adding a new field to metadata::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TimeSeriesUpdate
                >>> client = CogniteClient()
                >>> my_update = TimeSeriesUpdate(id=1).description.set("New description").metadata.add({"key": "value"})
                >>> res = client.time_series.update(my_update)

            Perform a partial update on a time series by instance id::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TimeSeriesUpdate
                >>> from cognite.client.data_classes.data_modeling import NodeId

                >>> client = CogniteClient()
                >>> my_update = (
                ...     TimeSeriesUpdate(instance_id=NodeId("test", "hello"))
                ...     .external_id.set("test:hello")
                ...     .metadata.add({"test": "hello"})
                ... )
                >>> client.time_series.update(my_update)
        """
        return self._update_multiple(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            update_cls=TimeSeriesUpdate,
            items=item,
            mode=mode,
        )

    @overload
    def upsert(
        self, item: Sequence[TimeSeries | TimeSeriesWrite], mode: Literal["patch", "replace"] = "patch"
    ) -> TimeSeriesList: ...

    @overload
    def upsert(self, item: TimeSeries | TimeSeriesWrite, mode: Literal["patch", "replace"] = "patch") -> TimeSeries: ...

    def upsert(
        self,
        item: TimeSeries | TimeSeriesWrite | Sequence[TimeSeries | TimeSeriesWrite],
        mode: Literal["patch", "replace"] = "patch",
    ) -> TimeSeries | TimeSeriesList:
        """Upsert time series, i.e., update if it exists, and create if it does not exist.
            Note this is a convenience method that handles the upserting for you by first calling update on all items,
            and if any of them fail because they do not exist, it will create them instead.

            For more details, see :ref:`appendix-upsert`.

        Args:
            item (TimeSeries | TimeSeriesWrite | Sequence[TimeSeries | TimeSeriesWrite]): TimeSeries or list of TimeSeries to upsert.
            mode (Literal['patch', 'replace']): Whether to patch or replace in the case the time series are existing. If you set 'patch', the call will only update fields with non-null values (default). Setting 'replace' will unset any fields that are not specified.

        Returns:
            TimeSeries | TimeSeriesList: The upserted time series(s).

        Examples:

            Upsert for TimeSeries::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TimeSeries
                >>> client = CogniteClient()
                >>> existing_time_series = client.time_series.retrieve(id=1)
                >>> existing_time_series.description = "New description"
                >>> new_time_series = TimeSeries(external_id="new_timeSeries", description="New timeSeries")
                >>> res = client.time_series.upsert([existing_time_series, new_time_series], mode="replace")
        """

        return self._upsert_multiple(
            item,
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            update_cls=TimeSeriesUpdate,
            input_resource_cls=TimeSeries,
            mode=mode,
        )

    def search(
        self,
        name: str | None = None,
        description: str | None = None,
        query: str | None = None,
        filter: TimeSeriesFilter | dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> TimeSeriesList:
        """`Search for time series. <https://developer.cognite.com/api#tag/Time-series/operation/searchTimeSeries>`_
        Primarily meant for human-centric use-cases and data exploration, not for programs, since matching and ordering may change over time. Use the `list` function if stable or exact matches are required.

        Args:
            name (str | None): Prefix and fuzzy search on name.
            description (str | None): Prefix and fuzzy search on description.
            query (str | None): Search on name and description using wildcard search on each of the words (separated by spaces). Retrieves results where at least one word must match. Example: 'some other'
            filter (TimeSeriesFilter | dict[str, Any] | None): Filter to apply. Performs exact match on these fields.
            limit (int): Max number of results to return.

        Returns:
            TimeSeriesList: List of requested time series.

        Examples:

            Search for a time series::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.time_series.search(name="some name")

            Search for all time series connected to asset with id 123::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.time_series.search(filter={"asset_ids":[123]})
        """

        return self._search(
            list_cls=TimeSeriesList,
            search={"name": name, "description": description, "query": query},
            filter=filter or {},
            limit=limit,
        )

    def filter(
        self,
        filter: Filter | dict,
        sort: SortSpec | list[SortSpec] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> TimeSeriesList:
        """`Advanced filter time series <https://developer.cognite.com/api#tag/Time-series/operation/listTimeSeries>`_

        Advanced filter lets you create complex filtering expressions that combine simple operations,
        such as equals, prefix, exists, etc., using boolean operators and, or, and not.
        It applies to basic fields as well as metadata.

        Args:
            filter (Filter | dict): Filter to apply.
            sort (SortSpec | list[SortSpec] | None): The criteria to sort by. Can be up to two properties to sort by default to ascending order.
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            TimeSeriesList: List of time series that match the filter criteria.

        Examples:

            Find all numeric time series and return them sorted by external id:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.filters import Equals
                >>> client = CogniteClient()
                >>> is_numeric = Equals("is_string", False)
                >>> res = client.time_series.filter(filter=is_numeric, sort="external_id")

            Note that you can check the API documentation above to see which properties you can filter on
            with which filters.

            To make it easier to avoid spelling mistakes and easier to look up available properties
            for filtering and sorting, you can also use the `TimeSeriesProperty` and `SortableTimeSeriesProperty` enums.

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.filters import Equals
                >>> from cognite.client.data_classes.time_series import TimeSeriesProperty, SortableTimeSeriesProperty
                >>> client = CogniteClient()
                >>> is_numeric = Equals(TimeSeriesProperty.is_string, False)
                >>> res = client.time_series.filter(filter=is_numeric, sort=SortableTimeSeriesProperty.external_id)
        """
        warnings.warn(
            f"{self.__class__.__name__}.filter() method is deprecated and will be removed in the next major version of the SDK. Use the {self.__class__.__name__}.list() method with advanced_filter parameter instead.",
            DeprecationWarning,
        )
        self._validate_filter(filter)

        return self._list(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            method="POST",
            limit=limit,
            advanced_filter=filter.dump(camel_case_property=True) if isinstance(filter, Filter) else filter,
            sort=prepare_filter_sort(sort, TimeSeriesSort),
        )

    def _validate_filter(self, filter: Filter | dict[str, Any] | None) -> None:
        _validate_filter(filter, _FILTERS_SUPPORTED, type(self).__name__)

    def list(
        self,
        name: str | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        unit_quantity: str | None = None,
        is_string: bool | None = None,
        is_step: bool | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        metadata: dict[str, Any] | None = None,
        external_id_prefix: str | None = None,
        created_time: dict[str, Any] | None = None,
        last_updated_time: dict[str, Any] | None = None,
        partitions: int | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        advanced_filter: Filter | dict[str, Any] | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
    ) -> TimeSeriesList:
        """`List time series <https://developer.cognite.com/api#tag/Time-series/operation/listTimeSeries>`_

        Args:
            name (str | None): Name of the time series. Often referred to as tag.
            unit (str | None): Unit of the time series.
            unit_external_id (str | None): Filter on unit external ID.
            unit_quantity (str | None): Filter on unit quantity.
            is_string (bool | None): Whether the time series is an string time series.
            is_step (bool | None): Whether the time series is a step (piecewise constant) time series.
            asset_ids (Sequence[int] | None): List time series related to these assets.
            asset_external_ids (SequenceNotStr[str] | None): List time series related to these assets.
            asset_subtree_ids (int | Sequence[int] | None): Only include time series that are related to an asset in a subtree rooted at any of these assetIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            asset_subtree_external_ids (str | SequenceNotStr[str] | None): Only include time series that are related to an asset in a subtree rooted at any of these assetExternalIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            data_set_ids (int | Sequence[int] | None): Return only time series in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | SequenceNotStr[str] | None): Return only time series in the specified data set(s) with this external id / these external ids.
            metadata (dict[str, Any] | None): Custom, application specific metadata. String key -> String value
            external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
            created_time (dict[str, Any] | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (dict[str, Any] | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            partitions (int | None): Retrieve resources in parallel using this number of workers (values up to 10 allowed), limit must be set to `None` (or `-1`).
            limit (int | None): Maximum number of time series to return.  Defaults to 25. Set to -1, float("inf") or None to return all items.
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL (Domain Specific Language). It allows defining complex filtering expressions that combine simple operations, such as equals, prefix, exists, etc., using boolean operators and, or, and not. See examples below for usage.
            sort (SortSpec | list[SortSpec] | None): The criteria to sort by. Defaults to desc for `_score_` and asc for all other properties. Sort is not allowed if `partitions` is used.

        Returns:
            TimeSeriesList: The requested time series.

        .. note::
            When using `partitions`, there are few considerations to keep in mind:
                * `limit` has to be set to `None` (or `-1`).
                * API may reject requests if you specify more than 10 partitions. When Cognite enforces this behavior, the requests result in a 400 Bad Request status.
                * Partitions are done independently of sorting: there's no guarantee of the sort order between elements from different partitions. For this reason providing a `sort` parameter when using `partitions` is not allowed.

        Examples:

            List time series::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.time_series.list(limit=5)

            Iterate over time series::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for ts in client.time_series:
                ...     ts # do something with the time series

            Iterate over chunks of time series to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for ts_list in client.time_series(chunk_size=2500):
                ...     ts_list # do something with the time series

            Using advanced filter, find all time series that have a metadata key 'timezone' starting with 'Europe',
            and sort by external id ascending:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> client = CogniteClient()
                >>> in_timezone = filters.Prefix(["metadata", "timezone"], "Europe")
                >>> res = client.time_series.list(advanced_filter=in_timezone, sort=("external_id", "asc"))

            Note that you can check the API documentation above to see which properties you can filter on
            with which filters.

            To make it easier to avoid spelling mistakes and easier to look up available properties
            for filtering and sorting, you can also use the `TimeSeriesProperty` and `SortableTimeSeriesProperty` Enums.

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.time_series import TimeSeriesProperty, SortableTimeSeriesProperty
                >>> client = CogniteClient()
                >>> in_timezone = filters.Prefix(TimeSeriesProperty.metadata_key("timezone"), "Europe")
                >>> res = client.time_series.list(
                ...     advanced_filter=in_timezone,
                ...     sort=(SortableTimeSeriesProperty.external_id, "asc"))

            Combine filter and advanced filter:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> client = CogniteClient()
                >>> not_instrument_lvl5 = filters.And(
                ...    filters.ContainsAny("labels", ["Level5"]),
                ...    filters.Not(filters.ContainsAny("labels", ["Instrument"]))
                ... )
                >>> res = client.time_series.list(asset_subtree_ids=[123456], advanced_filter=not_instrument_lvl5)
        """
        asset_subtree_ids_processed = process_asset_subtree_ids(asset_subtree_ids, asset_subtree_external_ids)
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = TimeSeriesFilter(
            name=name,
            unit=unit,
            unit_external_id=unit_external_id,
            unit_quantity=unit_quantity,
            is_step=is_step,
            is_string=is_string,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            metadata=metadata,
            data_set_ids=data_set_ids_processed,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
        ).dump(camel_case=True)

        prep_sort = prepare_filter_sort(sort, TimeSeriesSort)
        self._validate_filter(advanced_filter)

        return self._list(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            method="POST",
            filter=filter,
            advanced_filter=advanced_filter,
            sort=prep_sort,
            limit=limit,
            partitions=partitions,
        )
