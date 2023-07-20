from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Literal, Optional, Sequence, Union, cast, overload

from cognite.client._api.datapoints import DatapointsAPI
from cognite.client._api.datapoints_subscriptions import DatapointsSubscriptionAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import LIST_LIMIT_DEFAULT
from cognite.client.data_classes import (
    TimeSeries,
    TimeSeriesAggregate,
    TimeSeriesFilter,
    TimeSeriesList,
    TimeSeriesUpdate,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import process_asset_subtree_ids, process_data_set_ids

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class TimeSeriesAPI(APIClient):
    _RESOURCE_PATH = "/timeseries"

    def __init__(self, config: ClientConfig, api_version: Optional[str], cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.data = DatapointsAPI(config, api_version, cognite_client)
        self.subscriptions = DatapointsSubscriptionAPI(config, api_version, cognite_client)

    def __call__(
        self,
        chunk_size: Optional[int] = None,
        name: Optional[str] = None,
        unit: Optional[str] = None,
        is_string: Optional[bool] = None,
        is_step: Optional[bool] = None,
        asset_ids: Optional[Sequence[int]] = None,
        asset_external_ids: Optional[Sequence[str]] = None,
        asset_subtree_ids: Optional[Union[int, Sequence[int]]] = None,
        asset_subtree_external_ids: Optional[Union[str, Sequence[str]]] = None,
        data_set_ids: Optional[Union[int, Sequence[int]]] = None,
        data_set_external_ids: Optional[Union[str, Sequence[str]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        external_id_prefix: Optional[str] = None,
        created_time: Optional[Dict[str, Any]] = None,
        last_updated_time: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        partitions: Optional[int] = None,
    ) -> Union[Iterator[TimeSeries], Iterator[TimeSeriesList]]:
        """Iterate over time series

        Fetches time series as they are iterated over, so you keep a limited number of objects in memory.

        Args:
            chunk_size (int, optional): Number of time series to return in each chunk. Defaults to yielding one time series a time.
            name (str): Name of the time series. Often referred to as tag.
            unit (str): Unit of the time series.
            is_string (bool): Whether the time series is an string time series.
            is_step (bool): Whether the time series is a step (piecewise constant) time series.
            asset_ids (Sequence[int], optional): List time series related to these assets.
            asset_external_ids  (Sequence[str], optional): List time series related to these assets.
            asset_subtree_ids (Union[int, Sequence[int]]): Asset subtree id or list of asset subtree ids to filter on.
            asset_subtree_external_ids (Union[str, Sequence[str]]): Asset external id or list of asset subtree external ids to filter on.
            data_set_ids (Union[int, Sequence[int]]): Return only time series in the specified data set(s) with this id / these ids.
            data_set_external_ids (Union[str, Sequence[str]]): Return only time series in the specified data set(s) with this external id / these external ids.
            metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
            created_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
            limit (int, optional): Maximum number of time series to return. Defaults to return all items.
            partitions (int): Retrieve assets in parallel using this number of workers. Also requires `limit=None` to be passed.

        Yields:
            Union[TimeSeries, TimeSeriesList]: yields TimeSeries one by one if chunk_size is not specified, else TimeSeriesList objects.
        """
        asset_subtree_ids_processed = process_asset_subtree_ids(asset_subtree_ids, asset_subtree_external_ids)
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = TimeSeriesFilter(
            name=name,
            unit=unit,
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
        ).dump(camel_case=True)
        return self._list_generator(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            method="POST",
            chunk_size=chunk_size,
            filter=filter,
            limit=limit,
            partitions=partitions,
        )

    def __iter__(self) -> Iterator[TimeSeries]:
        """Iterate over time series

        Fetches time series as they are iterated over, so you keep a limited number of metadata objects in memory.

        Yields:
            TimeSeries: yields TimeSeries one by one.
        """
        return cast(Iterator[TimeSeries], self())

    def retrieve(self, id: Optional[int] = None, external_id: Optional[str] = None) -> Optional[TimeSeries]:
        """`Retrieve a single time series by id. <https://developer.cognite.com/api#tag/Time-series/operation/getTimeSeriesByIds>`_

        Args:
            id (int, optional): ID
            external_id (str, optional): External ID

        Returns:
            Optional[TimeSeries]: Requested time series or None if it does not exist.

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
        return self._retrieve_multiple(list_cls=TimeSeriesList, resource_cls=TimeSeries, identifiers=identifiers)

    def retrieve_multiple(
        self,
        ids: Optional[Sequence[int]] = None,
        external_ids: Optional[Sequence[str]] = None,
        ignore_unknown_ids: bool = False,
    ) -> TimeSeriesList:
        """`Retrieve multiple time series by id. <https://developer.cognite.com/api#tag/Time-series/operation/getTimeSeriesByIds>`_

        Args:
            ids (Sequence[int], optional): IDs
            external_ids (Sequence[str], optional): External IDs
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
        )

    def list(
        self,
        name: Optional[str] = None,
        unit: Optional[str] = None,
        is_string: Optional[bool] = None,
        is_step: Optional[bool] = None,
        asset_ids: Optional[Sequence[int]] = None,
        asset_external_ids: Optional[Sequence[str]] = None,
        asset_subtree_ids: Optional[Union[int, Sequence[int]]] = None,
        asset_subtree_external_ids: Optional[Union[str, Sequence[str]]] = None,
        data_set_ids: Optional[Union[int, Sequence[int]]] = None,
        data_set_external_ids: Optional[Union[str, Sequence[str]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        external_id_prefix: Optional[str] = None,
        created_time: Optional[Dict[str, Any]] = None,
        last_updated_time: Optional[Dict[str, Any]] = None,
        partitions: Optional[int] = None,
        limit: int = LIST_LIMIT_DEFAULT,
    ) -> TimeSeriesList:
        """`List over time series <https://developer.cognite.com/api#tag/Time-series/operation/listTimeSeries>`_

        Fetches time series as they are iterated over, so you keep a limited number of objects in memory.

        Args:
            name (str): Name of the time series. Often referred to as tag.
            unit (str): Unit of the time series.
            is_string (bool): Whether the time series is an string time series.
            is_step (bool): Whether the time series is a step (piecewise constant) time series.
            asset_ids (Sequence[int], optional): List time series related to these assets.
            asset_external_ids (Sequence[str], optional): List time series related to these assets.
            asset_subtree_ids (Union[int, Sequence[int]]): Asset subtree id or list of asset subtree ids to filter on.
            asset_subtree_external_ids (Union[str, Sequence[str]]): Asset external id or list of asset subtree external ids to filter on.
            data_set_ids (Union[int, Sequence[int]]): Return only time series in the specified data set(s) with this id / these ids.
            data_set_external_ids (Union[str, Sequence[str]]): Return only time series in the specified data set(s) with this external id / these external ids.
            metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
            created_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
            limit (int, optional): Maximum number of time series to return.  Defaults to 25. Set to -1, float("inf") or None to return all items.
            partitions (int): Retrieve time series in parallel using this number of workers. Also requires `limit=None` to be passed.


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
        return self._list(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            method="POST",
            filter=filter,
            limit=limit,
            partitions=partitions,
        )

    def aggregate(self, filter: Optional[Union[TimeSeriesFilter, Dict]] = None) -> List[TimeSeriesAggregate]:
        """`Aggregate time series <https://developer.cognite.com/api#tag/Time-series/operation/aggregateTimeSeries>`_

        Args:
            filter (Union[TimeSeriesFilter, Dict]): Filter on time series filter with exact match

        Returns:
            List[TimeSeriesAggregate]: List of sequence aggregates

        Examples:

            List time series::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.aggregate(filter={"unit": "kpa"})
        """

        return self._aggregate(filter=filter, cls=TimeSeriesAggregate)

    @overload
    def create(self, time_series: Sequence[TimeSeries]) -> TimeSeriesList:
        ...

    @overload
    def create(self, time_series: TimeSeries) -> TimeSeries:
        ...

    def create(self, time_series: Union[TimeSeries, Sequence[TimeSeries]]) -> Union[TimeSeries, TimeSeriesList]:
        """`Create one or more time series. <https://developer.cognite.com/api#tag/Time-series/operation/postTimeSeries>`_

        Args:
            time_series (Union[TimeSeries, Sequence[TimeSeries]]): TimeSeries or list of TimeSeries to create.

        Returns:
            Union[TimeSeries, TimeSeriesList]: The created time series.

        Examples:

            Create a new time series::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TimeSeries
                >>> c = CogniteClient()
                >>> ts = c.time_series.create(TimeSeries(name="my ts"))
        """
        return self._create_multiple(list_cls=TimeSeriesList, resource_cls=TimeSeries, items=time_series)

    def delete(
        self,
        id: Optional[Union[int, Sequence[int]]] = None,
        external_id: Optional[Union[str, Sequence[str]]] = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more time series. <https://developer.cognite.com/api#tag/Time-series/operation/deleteTimeSeries>`_

        Args:
            id (Union[int, Sequence[int]): Id or list of ids
            external_id (Union[str, Sequence[str]]): External ID or list of external ids
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            None

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
    def update(self, item: Sequence[Union[TimeSeries, TimeSeriesUpdate]]) -> TimeSeriesList:
        ...

    @overload
    def update(self, item: Union[TimeSeries, TimeSeriesUpdate]) -> TimeSeries:
        ...

    def update(
        self, item: Union[TimeSeries, TimeSeriesUpdate, Sequence[Union[TimeSeries, TimeSeriesUpdate]]]
    ) -> Union[TimeSeries, TimeSeriesList]:
        """`Update one or more time series. <https://developer.cognite.com/api#tag/Time-series/operation/alterTimeSeries>`_

        Args:
            item (Union[TimeSeries, TimeSeriesUpdate, Sequence[Union[TimeSeries, TimeSeriesUpdate]]]): Time series to update

        Returns:
            Union[TimeSeries, TimeSeriesList]: Updated time series.

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
        return self._update_multiple(
            list_cls=TimeSeriesList, resource_cls=TimeSeries, update_cls=TimeSeriesUpdate, items=item
        )

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
            mode (Literal["patch", "replace"])): Whether to patch or replace in the case the time series are existing. If
                                                you set 'patch', the call will only update fields with non-null values (default).
                                                Setting 'replace' will unset any fields that are not specified.

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
        name: Optional[str] = None,
        description: Optional[str] = None,
        query: Optional[str] = None,
        filter: Optional[Union[TimeSeriesFilter, Dict]] = None,
        limit: int = 100,
    ) -> TimeSeriesList:
        """`Search for time series. <https://developer.cognite.com/api#tag/Time-series/operation/searchTimeSeries>`_
        Primarily meant for human-centric use-cases and data exploration, not for programs, since matching and ordering may change over time. Use the `list` function if stable or exact matches are required.

        Args:
            name (str, optional): Prefix and fuzzy search on name.
            description (str, optional): Prefix and fuzzy search on description.
            query (str, optional): Search on name and description using wildcard search on each of the words (separated
                by spaces). Retrieves results where at least one word must match. Example: 'some other'
            filter (Union[TimeSeriesFilter, Dict], optional): Filter to apply. Performs exact match on these fields.
            limit (int, optional): Max number of results to return.

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
        return self._search(
            list_cls=TimeSeriesList,
            search={"name": name, "description": description, "query": query},
            filter=filter or {},
            limit=limit,
        )
