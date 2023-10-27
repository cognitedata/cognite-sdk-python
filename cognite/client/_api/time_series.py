from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterator, Literal, Sequence, Tuple, Union, cast, overload

from typing_extensions import TypeAlias

from cognite.client._api.datapoints import DatapointsAPI
from cognite.client._api.datapoints_subscriptions import DatapointsSubscriptionAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    TimeSeries,
    TimeSeriesAggregate,
    TimeSeriesFilter,
    TimeSeriesList,
    TimeSeriesUpdate,
    filters,
)
from cognite.client.data_classes.aggregations import AggregationFilter, UniqueResultList
from cognite.client.data_classes.filters import Filter, _validate_filter
from cognite.client.data_classes.time_series import SortableTimeSeriesProperty, TimeSeriesProperty, TimeSeriesSort
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import prepare_filter_sort, process_asset_subtree_ids, process_data_set_ids

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig

SortSpec: TypeAlias = Union[
    TimeSeriesSort,
    str,
    SortableTimeSeriesProperty,
    Tuple[str, Literal["asc", "desc"]],
    Tuple[str, Literal["asc", "desc"], Literal["auto", "first", "last"]],
]

_FILTERS_SUPPORTED: frozenset[type[Filter]] = frozenset(
    {
        filters.And,
        filters.Or,
        filters.Not,
        filters.In,
        filters.Equals,
        filters.Exists,
        filters.Range,
        filters.Prefix,
        filters.ContainsAny,
        filters.ContainsAll,
        filters.InAssetSubtree,
        filters.Search,
    }
)


class TimeSeriesAPI(APIClient):
    _RESOURCE_PATH = "/timeseries"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.data = DatapointsAPI(config, api_version, cognite_client)
        self.subscriptions = DatapointsSubscriptionAPI(config, api_version, cognite_client)
        self._unit_warning = FeaturePreviewWarning("beta", "alpha", "Timeseries Unit External ID")

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
        asset_external_ids: Sequence[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        metadata: dict[str, Any] | None = None,
        external_id_prefix: str | None = None,
        created_time: dict[str, Any] | None = None,
        last_updated_time: dict[str, Any] | None = None,
        limit: int | None = None,
        partitions: int | None = None,
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
            asset_external_ids (Sequence[str] | None): List time series related to these assets.
            asset_subtree_ids (int | Sequence[int] | None): Asset subtree id or list of asset subtree ids to filter on.
            asset_subtree_external_ids (str | Sequence[str] | None): Asset external id or list of asset subtree external ids to filter on.
            data_set_ids (int | Sequence[int] | None): Return only time series in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | Sequence[str] | None): Return only time series in the specified data set(s) with this external id / these external ids.
            metadata (dict[str, Any] | None): Custom, application specific metadata. String key -> String value
            external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
            created_time (dict[str, Any] | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (dict[str, Any] | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            limit (int | None): Maximum number of time series to return. Defaults to return all items.
            partitions (int | None): Retrieve assets in parallel using this number of workers. Also requires `limit=None` to be passed.

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

        api_subversion = self._get_api_subversion(filter.unit_external_id, filter.unit_quantity)

        return self._list_generator(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            method="POST",
            chunk_size=chunk_size,
            filter=filter.dump(camel_case=True),
            limit=limit,
            partitions=partitions,
            api_subversion=api_subversion,
        )

    def __iter__(self) -> Iterator[TimeSeries]:
        """Iterate over time series

        Fetches time series as they are iterated over, so you keep a limited number of metadata objects in memory.

        Returns:
            Iterator[TimeSeries]: yields TimeSeries one by one.
        """
        return cast(Iterator[TimeSeries], self())

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> TimeSeries | None:
        """`Retrieve a single time series by id. <https://developer.cognite.com/api#tag/Time-series/operation/getTimeSeriesByIds>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID

        Returns:
            TimeSeries | None: Requested time series or None if it does not exist.

        Examples:

            Get time series by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.retrieve(id=1)

            Get time series by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()

        return self._retrieve_multiple(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            identifiers=identifiers,
            # Unit external id is only supported in beta, this can be removed when it is released in GA.
            api_subversion="beta",
        )

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: Sequence[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> TimeSeriesList:
        """`Retrieve multiple time series by id. <https://developer.cognite.com/api#tag/Time-series/operation/getTimeSeriesByIds>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (Sequence[str] | None): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            TimeSeriesList: The requested time series.

        Examples:

            Get time series by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.retrieve_multiple(ids=[1, 2, 3])

            Get time series by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
            # Unit external id is only supported in beta, this can be removed when it is released in GA.
            api_subversion="beta",
        )

    def aggregate(self, filter: TimeSeriesFilter | dict | None = None) -> list[TimeSeriesAggregate]:
        """`Aggregate time series <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_

        Args:
            filter (TimeSeriesFilter | dict | None): Filter on time series filter with exact match

        Returns:
            list[TimeSeriesAggregate]: List of sequence aggregates

        Examples:

            List time series::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.aggregate(filter={"unit": "kpa"})
        """

        return self._aggregate(filter=filter, cls=TimeSeriesAggregate)

    def aggregate_count(
        self,
        advanced_filter: Filter | dict | None = None,
        filter: TimeSeriesFilter | dict | None = None,
    ) -> int:
        """`Count of time series matching the specified filters and search. <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_

        Args:
            advanced_filter (Filter | dict | None): The filter to narrow down the time series to count.
            filter (TimeSeriesFilter | dict | None): The filter to narrow down time series to count requiring exact match.

        Returns:
            int: The number of time series matching the specified filters and search.

        Examples:

        Count the number of time series in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> c = CogniteClient()
            >>> count = c.time_series.aggregate_count()

        Count the number of numeric time series in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import filters
            >>> from cognite.client.data_classes.time_series import TimeSeriesProperty
            >>> c = CogniteClient()
            >>> is_numeric = filters.Equals(TimeSeriesProperty.is_string, False)
            >>> count = c.time_series.aggregate_count(advanced_filter=is_numeric)

        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            "count",
            filter=filter,
            advanced_filter=advanced_filter,
            api_subversion="beta",
        )

    def aggregate_cardinality_values(
        self,
        property: TimeSeriesProperty | str | list[str],
        advanced_filter: Filter | dict | None = None,
        aggregate_filter: AggregationFilter | dict | None = None,
        filter: TimeSeriesFilter | dict | None = None,
    ) -> int:
        """`Find approximate property count for time series. <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_

        Args:
            property (TimeSeriesProperty | str | list[str]): The property to count the cardinality of.
            advanced_filter (Filter | dict | None): The filter to narrow down the time series to count cardinality.
            aggregate_filter (AggregationFilter | dict | None): The filter to apply to the resulting buckets.
            filter (TimeSeriesFilter | dict | None): The filter to narrow down the time series to count requiring exact match.
        Returns:
            int: The number of properties matching the specified filters and search.

        Examples:

        Count the number of different units used for time series in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes.time_series import TimeSeriesProperty
            >>> c = CogniteClient()
            >>> unit_count = c.time_series.aggregate_cardinality_values(TimeSeriesProperty.unit)

        Count the number of timezones (metadata key) for time series with the word "critical" in the description
        in your CDF project, but exclude timezones from america:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import filters, aggregations as aggs
            >>> from cognite.client.data_classes.time_series import TimeSeriesProperty
            >>> c = CogniteClient()
            >>> not_america = aggs.Not(aggs.Prefix("america"))
            >>> is_critical = filters.Search(TimeSeriesProperty.description, "critical")
            >>> timezone_count = c.time_series.aggregate_cardinality_values(
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
            api_subversion="beta",
        )

    def aggregate_cardinality_properties(
        self,
        path: TimeSeriesProperty | str | list[str],
        advanced_filter: Filter | dict | None = None,
        aggregate_filter: AggregationFilter | dict | None = None,
        filter: TimeSeriesFilter | dict | None = None,
    ) -> int:
        """`Find approximate paths count for time series.  <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_

        Args:
            path (TimeSeriesProperty | str | list[str]): The scope in every document to aggregate properties. The only value allowed now is ["metadata"]. It means to aggregate only metadata properties (aka keys).
            advanced_filter (Filter | dict | None): The filter to narrow down the time series to count cardinality.
            aggregate_filter (AggregationFilter | dict | None): The filter to apply to the resulting buckets.
            filter (TimeSeriesFilter | dict | None): The filter to narrow down the time series to count requiring exact match.
        Returns:
            int: The number of properties matching the specified filters and search.

        Examples:

            Count the number of metadata keys in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.time_series import TimeSeriesProperty
                >>> c = CogniteClient()
                >>> key_count = c.time_series.aggregate_cardinality_properties(TimeSeriesProperty.metadata)
        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            "cardinalityProperties",
            path=path,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
            api_subversion="beta",
        )

    def aggregate_unique_values(
        self,
        property: TimeSeriesProperty | str | list[str],
        advanced_filter: Filter | dict | None = None,
        aggregate_filter: AggregationFilter | dict | None = None,
        filter: TimeSeriesFilter | dict | None = None,
    ) -> UniqueResultList:
        """`Get unique properties with counts for time series. <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_

        Args:
            property (TimeSeriesProperty | str | list[str]): The property to group by.
            advanced_filter (Filter | dict | None): The filter to narrow down the time series to count cardinality.
            aggregate_filter (AggregationFilter | dict | None): The filter to apply to the resulting buckets.
            filter (TimeSeriesFilter | dict | None): The filter to narrow down the time series to count requiring exact match.

        Returns:
            UniqueResultList: List of unique values of time series matching the specified filters and search.

        Examples:

            Get the timezones (metadata key) with count for your time series in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.time_series import TimeSeriesProperty
                >>> c = CogniteClient()
                >>> result = c.time_series.aggregate_unique_values(TimeSeriesProperty.metadata_key("timezone"))
                >>> print(result.unique)

            Get the different units with count used for time series created after 2020-01-01 in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.time_series import TimeSeriesProperty
                >>> from cognite.client.utils import timestamp_to_ms
                >>> from datetime import datetime
                >>> c = CogniteClient()
                >>> created_after_2020 = filters.Range(TimeSeriesProperty.created_time, gte=timestamp_to_ms(datetime(2020, 1, 1)))
                >>> result = c.time_series.aggregate_unique_values(TimeSeriesProperty.unit, advanced_filter=created_after_2020)
                >>> print(result.unique)

            Get the different units with count for time series updated after 2020-01-01 in your CDF project, but exclude all units that
            start with "test":

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.time_series import TimeSeriesProperty
                >>> from cognite.client.data_classes import aggregations as aggs, filters
                >>> c = CogniteClient()
                >>> not_test = aggs.Not(aggs.Prefix("test"))
                >>> created_after_2020 = filters.Range(TimeSeriesProperty.last_updated_time, gte=timestamp_to_ms(datetime(2020, 1, 1)))
                >>> result = c.time_series.aggregate_unique_values(TimeSeriesProperty.unit, advanced_filter=created_after_2020, aggregate_filter=not_test)
                >>> print(result.unique)
        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            aggregate="uniqueValues",
            properties=property,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
            api_subversion="beta",
        )

    def aggregate_unique_properties(
        self,
        path: TimeSeriesProperty | str | list[str],
        advanced_filter: Filter | dict | None = None,
        aggregate_filter: AggregationFilter | dict | None = None,
        filter: TimeSeriesFilter | dict | None = None,
    ) -> UniqueResultList:
        """`Get unique paths with counts for time series. <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_

        Args:
            path (TimeSeriesProperty | str | list[str]): The scope in every document to aggregate properties. The only value allowed now is ["metadata"]. It means to aggregate only metadata properties (aka keys).
            advanced_filter (Filter | dict | None): The filter to narrow down the time series to count cardinality.
            aggregate_filter (AggregationFilter | dict | None): The filter to apply to the resulting buckets.
            filter (TimeSeriesFilter | dict | None): The filter to narrow down the time series to count requiring exact match.

        Returns:
            UniqueResultList: List of unique values of time series matching the specified filters and search.

        Examples:

            Get the metadata keys with count for your time series in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.time_series import TimeSeriesProperty
                >>> c = CogniteClient()
                >>> result = c.time_series.aggregate_unique_values(TimeSeriesProperty.metadata)
        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            aggregate="uniqueProperties",
            path=path,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
            api_subversion="beta",
        )

    @overload
    def create(self, time_series: Sequence[TimeSeries]) -> TimeSeriesList:
        ...

    @overload
    def create(self, time_series: TimeSeries) -> TimeSeries:
        ...

    def create(self, time_series: TimeSeries | Sequence[TimeSeries]) -> TimeSeries | TimeSeriesList:
        """`Create one or more time series. <https://developer.cognite.com/api#tag/Time-series/operation/postTimeSeries>`_

        Args:
            time_series (TimeSeries | Sequence[TimeSeries]): TimeSeries or list of TimeSeries to create.

        Returns:
            TimeSeries | TimeSeriesList: The created time series.

        Examples:

            Create a new time series::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TimeSeries
                >>> c = CogniteClient()
                >>> ts = c.time_series.create(TimeSeries(name="my ts"))
        """
        api_subversion = self._get_subapiversion_item(time_series)

        return self._create_multiple(
            list_cls=TimeSeriesList, resource_cls=TimeSeries, items=time_series, api_subversion=api_subversion
        )

    def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | Sequence[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more time series. <https://developer.cognite.com/api#tag/Time-series/operation/deleteTimeSeries>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | Sequence[str] | None): External ID or list of external ids
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Examples:

            Delete time series by id or external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.time_series.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    def update(self, item: Sequence[TimeSeries | TimeSeriesUpdate]) -> TimeSeriesList:
        ...

    @overload
    def update(self, item: TimeSeries | TimeSeriesUpdate) -> TimeSeries:
        ...

    def update(
        self, item: TimeSeries | TimeSeriesUpdate | Sequence[TimeSeries | TimeSeriesUpdate]
    ) -> TimeSeries | TimeSeriesList:
        """`Update one or more time series. <https://developer.cognite.com/api#tag/Time-series/operation/alterTimeSeries>`_

        Args:
            item (TimeSeries | TimeSeriesUpdate | Sequence[TimeSeries | TimeSeriesUpdate]): Time series to update

        Returns:
            TimeSeries | TimeSeriesList: Updated time series.

        Examples:

            Update a time series that you have fetched. This will perform a full update of the time series::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.retrieve(id=1)
                >>> res.description = "New description"
                >>> res = c.time_series.update(res)

            Perform a partial update on a time series, updating the description and adding a new field to metadata::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TimeSeriesUpdate
                >>> c = CogniteClient()
                >>> my_update = TimeSeriesUpdate(id=1).description.set("New description").metadata.add({"key": "value"})
                >>> res = c.time_series.update(my_update)
        """
        api_subversion = self._get_subapiversion_item(item)

        return self._update_multiple(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            update_cls=TimeSeriesUpdate,
            items=item,
            api_subversion=api_subversion,
        )

    def _get_subapiversion_item(
        self, item: TimeSeries | TimeSeriesUpdate | Sequence[TimeSeries | TimeSeriesUpdate]
    ) -> str | None:
        api_subversion: str | None = None
        if isinstance(item, TimeSeries) and item.unit_external_id:
            api_subversion = "beta"
        elif isinstance(item, TimeSeriesUpdate) and "unitExternalId" in item.dump(camel_case=True).get("update", {}):
            api_subversion = "beta"
        elif isinstance(item, Sequence) and any(
            ts.unit_external_id
            if isinstance(ts, TimeSeries)
            else "unitExternalId" in ts.dump(camel_case=True).get("update", {})
            for ts in item
        ):
            api_subversion = "beta"

        if api_subversion == "beta":
            self._unit_warning.warn()
        return api_subversion

    @overload
    def upsert(self, item: Sequence[TimeSeries], mode: Literal["patch", "replace"] = "patch") -> TimeSeriesList:
        ...

    @overload
    def upsert(self, item: TimeSeries, mode: Literal["patch", "replace"] = "patch") -> TimeSeries:
        ...

    def upsert(
        self, item: TimeSeries | Sequence[TimeSeries], mode: Literal["patch", "replace"] = "patch"
    ) -> TimeSeries | TimeSeriesList:
        """Upsert time series, i.e., update if it exists, and create if it does not exist.
            Note this is a convenience method that handles the upserting for you by first calling update on all items,
            and if any of them fail because they do not exist, it will create them instead.

            For more details, see :ref:`appendix-upsert`.

        Args:
            item (TimeSeries | Sequence[TimeSeries]): TimeSeries or list of TimeSeries to upsert.
            mode (Literal["patch", "replace"]): Whether to patch or replace in the case the time series are existing. If you set 'patch', the call will only update fields with non-null values (default). Setting 'replace' will unset any fields that are not specified.
                Note replace will skip beta properties (unit_external_id), if you want to replace the beta properties you have to use the update method.

        Returns:
            TimeSeries | TimeSeriesList: The upserted time series(s).

        Examples:

            Upsert for TimeSeries::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TimeSeries
                >>> c = CogniteClient()
                >>> existing_time_series = c.time_series.retrieve(id=1)
                >>> existing_time_series.description = "New description"
                >>> new_time_series = TimeSeries(external_id="new_timeSeries", description="New timeSeries")
                >>> res = c.time_series.upsert([existing_time_series, new_time_series], mode="replace")
        """
        api_subversion = self._get_subapiversion_item(item)

        return self._upsert_multiple(
            item,
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            update_cls=TimeSeriesUpdate,
            input_resource_cls=TimeSeries,
            mode=mode,
            api_subversion=api_subversion,
        )

    def search(
        self,
        name: str | None = None,
        description: str | None = None,
        query: str | None = None,
        filter: TimeSeriesFilter | dict | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> TimeSeriesList:
        """`Search for time series. <https://developer.cognite.com/api#tag/Time-series/operation/searchTimeSeries>`_
        Primarily meant for human-centric use-cases and data exploration, not for programs, since matching and ordering may change over time. Use the `list` function if stable or exact matches are required.

        Args:
            name (str | None): Prefix and fuzzy search on name.
            description (str | None): Prefix and fuzzy search on description.
            query (str | None): Search on name and description using wildcard search on each of the words (separated by spaces). Retrieves results where at least one word must match. Example: 'some other'
            filter (TimeSeriesFilter | dict | None): Filter to apply. Performs exact match on these fields.
            limit (int): Max number of results to return.

        Returns:
            TimeSeriesList: List of requested time series.

        Examples:

            Search for a time series::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.search(name="some name")

            Search for all time series connected to asset with id 123::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.search(filter={"asset_ids":[123]})
        """

        if isinstance(filter, TimeSeriesFilter):
            api_subversion: str | None = self._get_api_subversion(filter.unit_external_id, filter.unit_quantity)
        elif isinstance(filter, dict):
            api_subversion = self._get_api_subversion(filter.get("unitExternalId"), filter.get("unitQuantity"))
        else:
            api_subversion = None

        return self._search(
            list_cls=TimeSeriesList,
            search={"name": name, "description": description, "query": query},
            filter=filter or {},
            limit=limit,
            api_subversion=api_subversion,
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
                >>> from cognite.client.data_classes import filters
                >>> c = CogniteClient()
                >>> f = filters
                >>> is_numeric = f.Equals("is_string", False)
                >>> res = c.time_series.filter(filter=is_numeric, sort="external_id")

            Note that you can check the API documentation above to see which properties you can filter on
            with which filters.

            To make it easier to avoid spelling mistakes and easier to look up available properties
            for filtering and sorting, you can also use the `TimeSeriesProperty` and `SortableTimeSeriesProperty` enums.

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.time_series import TimeSeriesProperty, SortableTimeSeriesProperty
                >>> c = CogniteClient()
                >>> f = filters
                >>> is_numeric = f.Equals(TimeSeriesProperty.is_string, False)
                >>> res = c.time_series.filter(filter=is_numeric, sort=SortableTimeSeriesProperty.external_id)
        """
        self._validate_filter(filter)

        return self._list(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            method="POST",
            limit=limit,
            advanced_filter=filter.dump(camel_case=True) if isinstance(filter, Filter) else filter,
            sort=prepare_filter_sort(sort, TimeSeriesSort),
        )

    def _validate_filter(self, filter: Filter | dict | None) -> None:
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
        asset_external_ids: Sequence[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        metadata: dict[str, Any] | None = None,
        external_id_prefix: str | None = None,
        created_time: dict[str, Any] | None = None,
        last_updated_time: dict[str, Any] | None = None,
        partitions: int | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> TimeSeriesList:
        """`List over time series <https://developer.cognite.com/api#tag/Time-series/operation/listTimeSeries>`_

        Fetches time series as they are iterated over, so you keep a limited number of objects in memory.

        Args:
            name (str | None): Name of the time series. Often referred to as tag.
            unit (str | None): Unit of the time series.
            unit_external_id (str | None): Filter on unit external ID.
            unit_quantity (str | None): Filter on unit quantity.
            is_string (bool | None): Whether the time series is an string time series.
            is_step (bool | None): Whether the time series is a step (piecewise constant) time series.
            asset_ids (Sequence[int] | None): List time series related to these assets.
            asset_external_ids (Sequence[str] | None): List time series related to these assets.
            asset_subtree_ids (int | Sequence[int] | None): Asset subtree id or list of asset subtree ids to filter on.
            asset_subtree_external_ids (str | Sequence[str] | None): Asset external id or list of asset subtree external ids to filter on.
            data_set_ids (int | Sequence[int] | None): Return only time series in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | Sequence[str] | None): Return only time series in the specified data set(s) with this external id / these external ids.
            metadata (dict[str, Any] | None): Custom, application specific metadata. String key -> String value
            external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
            created_time (dict[str, Any] | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (dict[str, Any] | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            partitions (int | None): Retrieve time series in parallel using this number of workers. Also requires `limit=None` to be passed.
            limit (int | None): Maximum number of time series to return.  Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            TimeSeriesList: The requested time series.

        Examples:

            List time series::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.list(limit=5)

            Iterate over time series::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for ts in c.time_series:
                ...     ts # do something with the time_series

            Iterate over chunks of time series to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for ts_list in c.time_series(chunk_size=2500):
                ...     ts_list # do something with the time_series
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

        api_subversion = self._get_api_subversion(unit_external_id, unit_quantity)

        return self._list(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            method="POST",
            filter=filter,
            limit=limit,
            partitions=partitions,
            api_subversion=api_subversion,
        )

    def _get_api_subversion(self, unit_external_id: str | None, unit_quantity: str | None) -> str | None:
        api_subversion: str | None = None
        if unit_external_id is not None or unit_quantity is not None:
            api_subversion = "beta"
            self._unit_warning.warn()
        return api_subversion
