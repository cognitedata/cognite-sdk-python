"""
===============================================================================
59576134bc85388b50b689e0f98eed35
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

import datetime
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal, overload
from zoneinfo import ZoneInfo

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_DATAPOINTS_CHUNK_SIZE
from cognite.client._sync_api.synthetic_time_series import SyncSyntheticDatapointsAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import (
    Datapoints,
    DatapointsArray,
    DatapointsArrayList,
    DatapointsList,
    DatapointsQuery,
    LatestDatapointQuery,
)
from cognite.client.data_classes.data_modeling.ids import NodeId
from cognite.client.data_classes.datapoint_aggregates import Aggregate
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    import pandas as pd


class SyncDatapointsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.synthetic = SyncSyntheticDatapointsAPI(async_client)

    @overload
    def __call__(
        self,
        queries: DatapointsQuery,
        *,
        return_arrays: Literal[True] = True,
        chunk_size_datapoints: int = DEFAULT_DATAPOINTS_CHUNK_SIZE,
        chunk_size_time_series: int | None = None,
    ) -> Iterator[DatapointsArray]: ...

    @overload
    def __call__(
        self,
        queries: Sequence[DatapointsQuery],
        *,
        return_arrays: Literal[True] = True,
        chunk_size_datapoints: int = DEFAULT_DATAPOINTS_CHUNK_SIZE,
        chunk_size_time_series: int | None = None,
    ) -> Iterator[DatapointsArrayList]: ...

    @overload
    def __call__(
        self,
        queries: DatapointsQuery,
        *,
        return_arrays: Literal[False],
        chunk_size_datapoints: int = DEFAULT_DATAPOINTS_CHUNK_SIZE,
        chunk_size_time_series: int | None = None,
    ) -> Iterator[Datapoints]: ...

    @overload
    def __call__(
        self,
        queries: Sequence[DatapointsQuery],
        *,
        return_arrays: Literal[False],
        chunk_size_datapoints: int = DEFAULT_DATAPOINTS_CHUNK_SIZE,
        chunk_size_time_series: int | None = None,
    ) -> Iterator[DatapointsList]: ...

    def __call__(
        self,
        queries: DatapointsQuery | Sequence[DatapointsQuery],
        *,
        chunk_size_datapoints: int = DEFAULT_DATAPOINTS_CHUNK_SIZE,
        chunk_size_time_series: int | None = None,
        return_arrays: bool = True,
    ) -> Iterator[DatapointsArray | DatapointsArrayList | Datapoints | DatapointsList]:
        """
        `Iterate through datapoints in chunks, for one or more time series. <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getMultiTimeSeriesDatapoints>`_

        Note:
            Control memory usage by specifying ``chunk_size_time_series``, how many time series to iterate simultaneously and ``chunk_size_datapoints``,
            how many datapoints to yield per iteration (per individual time series). See full example in examples. Note that in order to make efficient
            use of the API request limits, this method will never hold less than 100k datapoints in memory at a time, per time series.

            If you run with memory constraints, use ``return_arrays=True`` (the default).

            No empty chunk is ever returned.

        Args:
            queries (DatapointsQuery | Sequence[DatapointsQuery]): Query, or queries, using id, external_id or instance_id for the time series to fetch data for, with individual settings specified. The options 'limit' and 'include_outside_points' are not supported when iterating.
            chunk_size_datapoints (int): The number of datapoints per time series to yield per iteration. Must evenly divide 100k OR be an integer multiple of 100k. Default: 100_000.
            chunk_size_time_series (int | None): The max number of time series to yield per iteration (varies as time series get exhausted, but is never empty). Default: None (all given queries are iterated at the same time).
            return_arrays (bool): Whether to return the datapoints as numpy arrays. Default: True.

        Yields:
            DatapointsArray | DatapointsArrayList | Datapoints | DatapointsList: If return_arrays=True, a ``DatapointsArray`` object containing the datapoints chunk, or a ``DatapointsArrayList`` if multiple time series were asked for. When False, a ``Datapoints`` object containing the datapoints chunk, or a ``DatapointsList`` if multiple time series were asked for.

        Examples:

            Iterate through the datapoints of a single time series with external_id="foo", in chunks of 25k:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DatapointsQuery
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> query = DatapointsQuery(external_id="foo", start="2w-ago")
                >>> for chunk in client.time_series.data(query, chunk_size_datapoints=25_000):
                ...     pass  # do something with the datapoints chunk

            Iterate through datapoints from multiple time series, and do not return them as memory-efficient numpy arrays.
            As one or more time series get exhausted (no more data), they are no longer part of the returned "chunk list".
            Note that the order is still preserved (for the remaining).

            If you run with ``chunk_size_time_series=None``, an easy way to check when a time series is exhausted is to
            use the ``.get`` method, as illustrated below:

                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> queries = [
                ...     DatapointsQuery(id=123),
                ...     DatapointsQuery(external_id="foo"),
                ...     DatapointsQuery(instance_id=NodeId("my-space", "my-ts-xid"))
                ... ]
                >>> for chunk_lst in client.time_series.data(query, return_arrays=False):
                ...     if chunk_lst.get(id=123) is None:
                ...         print("Time series with id=123 has no more datapoints!")

            A likely use case for iterating datapoints is to clone data from one project to another, while keeping a low memory
            footprint and without having to write very custom logic involving count aggregates (which won't work for string data)
            or do time-domain splitting yourself.

            Here's an example of how to do so efficiently, while including bad- and uncertain data (``ignore_bad_datapoints=False``) and
            copying status codes (``include_status=True``). This is automatically taken care of when the Datapoints(-Array) objects are passed
            directly to an insert method. The only assumption below is that the time series have already been created in the target project.

                >>> from cognite.client.utils import MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS
                >>> target_client = CogniteClient()
                >>> ts_to_copy = client.time_series.list(data_set_external_ids="my-use-case")
                >>> queries = [
                ...     DatapointsQuery(
                ...         external_id=ts.external_id,
                ...         include_status=True,
                ...         ignore_bad_datapoints=False,
                ...         start=MIN_TIMESTAMP_MS,
                ...         end=MAX_TIMESTAMP_MS + 1,  # end is exclusive
                ...     )
                ...     for ts in ts_to_copy
                ... ]
                >>> for dps_chunk in client.time_series.data(
                ...     queries,  # may be several thousand time series...
                ...     chunk_size_time_series=20,  # control memory usage by specifying how many to iterate at a time
                ...     chunk_size_datapoints=100_000,
                ... ):
                ...     target_client.time_series.data.insert_multiple(
                ...         [{"external_id": dps.external_id, "datapoints": dps} for dps in dps_chunk]
                ...     )
        """
        yield from SyncIterator(
            self.__async_client.time_series.data(  # type: ignore [call-overload]
                queries=queries,
                chunk_size_datapoints=chunk_size_datapoints,
                chunk_size_time_series=chunk_size_time_series,
                return_arrays=return_arrays,
            )
        )

    @overload
    def retrieve(
        self,
        *,
        id: int | DatapointsQuery,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> Datapoints | None: ...

    @overload
    def retrieve(
        self,
        *,
        id: Sequence[int | DatapointsQuery],
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> DatapointsList: ...

    @overload
    def retrieve(
        self,
        *,
        external_id: str | DatapointsQuery,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> Datapoints | None: ...

    @overload
    def retrieve(
        self,
        *,
        external_id: SequenceNotStr[str | DatapointsQuery],
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> DatapointsList: ...

    @overload
    def retrieve(
        self,
        *,
        instance_id: NodeId | DatapointsQuery,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> Datapoints | None: ...

    @overload
    def retrieve(
        self,
        *,
        instance_id: Sequence[NodeId | DatapointsQuery],
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> DatapointsList: ...

    @overload
    def retrieve(
        self,
        *,
        id: None | int | DatapointsQuery | Sequence[int | DatapointsQuery],
        external_id: None | str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery],
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> DatapointsList: ...

    @overload
    def retrieve(
        self,
        *,
        id: None | int | DatapointsQuery | Sequence[int | DatapointsQuery],
        instance_id: None | NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery],
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> DatapointsList: ...

    @overload
    def retrieve(
        self,
        *,
        external_id: None | str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery],
        instance_id: None | NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery],
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> DatapointsList: ...

    @overload
    def retrieve(
        self,
        *,
        id: None | int | DatapointsQuery | Sequence[int | DatapointsQuery],
        external_id: None | str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery],
        instance_id: None | NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery],
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> DatapointsList: ...

    def retrieve(
        self,
        *,
        id: None | int | DatapointsQuery | Sequence[int | DatapointsQuery] = None,
        external_id: None | str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery] = None,
        instance_id: None | NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery] = None,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> Datapoints | DatapointsList | None:
        """
        `Retrieve datapoints for one or more time series. <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getMultiTimeSeriesDatapoints>`_

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. Make *one* call to retrieve and fetch all time series in go, rather than making multiple calls (if your memory allows it). The SDK will optimize retrieval strategy for you!
            2. For best speed, and significantly lower memory usage, consider using ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            3. Unlimited queries (``limit=None``) are most performant as they are always fetched in parallel, for any number of requested time series, even one.
            4. Limited queries, (e.g. ``limit=500_000``) are much less performant, at least for large limits, as each individual time series is fetched serially (we can't predict where on the timeline the datapoints are). Thus parallelisation is only used when asking for multiple "limited" time series.
            5. Try to avoid specifying `start` and `end` to be very far from the actual data: If you have data from 2000 to 2015, don't use start=0 (1970).
            6. Using ``timezone`` and/or calendar granularities like month/quarter/year in aggregate queries comes at a penalty as they are expensive for the API to compute.

        Warning:
            When using the AsyncCogniteClient, always ``await`` the result of this method and never run multiple calls concurrently (e.g. using asyncio.gather).
            You can pass as many queries as you like to a single call, and the SDK will optimize the retrieval strategy for you intelligently.

        Tip:
            To read datapoints efficiently, while keeping a low memory footprint e.g. to copy from one project to another, check out :py:meth:`~DatapointsAPI.__call__`.
            It allows you to iterate through datapoints in chunks, and also control how many time series to iterate at the same time.

        Time series support status codes like Good, Uncertain and Bad. You can read more in the Cognite Data Fusion developer documentation on
        `status codes. <https://docs.cognite.com/dev/concepts/reference/status_codes/>`_

        Args:
            id (None | int | DatapointsQuery | Sequence[int | DatapointsQuery]): Id, dict (with id) or (mixed) sequence of these. See examples below.
            external_id (None | str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery]): External id, dict (with external id) or (mixed) sequence of these. See examples below.
            instance_id (None | NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery]): Instance id or sequence of instance ids.
            start (int | str | datetime.datetime | None): Inclusive start. Default: 1970-01-01 UTC.
            end (int | str | datetime.datetime | None): Exclusive end. Default: "now"
            aggregates (Aggregate | str | list[Aggregate | str] | None): Single aggregate or list of aggregates to retrieve. Available options: ``average``, ``continuous_variance``, ``count``, ``count_bad``, ``count_good``, ``count_uncertain``, ``discrete_variance``, ``duration_bad``, ``duration_good``, ``duration_uncertain``, ``interpolation``, ``max``, ``max_datapoint``, ``min``, ``min_datapoint``, ``step_interpolation``, ``sum`` and ``total_variation``. Default: None (raw datapoints returned)
            granularity (str | None): The granularity to fetch aggregates at. Can be given as an abbreviation or spelled out for clarity: ``s/second(s)``, ``m/minute(s)``, ``h/hour(s)``, ``d/day(s)``, ``w/week(s)``, ``mo/month(s)``, ``q/quarter(s)``, or ``y/year(s)``. Examples: ``30s``, ``5m``, ``1day``, ``2weeks``. Default: None.
            timezone (str | datetime.timezone | ZoneInfo | None): For raw datapoints, which timezone to use when displaying (will not affect what is retrieved). For aggregates, which timezone to align to for granularity 'hour' and longer. Align to the start of the hour, day or month. For timezones of type Region/Location, like 'Europe/Oslo', pass a string or ``ZoneInfo`` instance. The aggregate duration will then vary, typically due to daylight saving time. You can also use a fixed offset from UTC by passing a string like '+04:00', 'UTC-7' or 'UTC-02:30' or an instance of ``datetime.timezone``. Note: Historical timezones with second offset are not supported, and timezones with minute offsets (e.g. UTC+05:30 or Asia/Kolkata) may take longer to execute.
            target_unit (str | None): The unit_external_id of the datapoints returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system (str | None): The unit system of the datapoints returned. Cannot be used with target_unit.
            limit (int | None): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether to include outside points. Not allowed when fetching aggregates. Default: False
            ignore_unknown_ids (bool): Whether to ignore missing time series rather than raising an exception. Default: False
            include_status (bool): Also return the status code, an integer, for each datapoint in the response. Only relevant for raw datapoint queries, and the object aggregates ``min_datapoint`` and ``max_datapoint``.
            ignore_bad_datapoints (bool): Treat datapoints with a bad status code as if they do not exist. If set to false, raw queries will include bad datapoints in the response, and aggregates will in general omit the time period between a bad datapoint and the next good datapoint. Also, the period between a bad datapoint and the previous good datapoint will be considered constant. Default: True.
            treat_uncertain_as_bad (bool): Treat datapoints with uncertain status codes as bad. If false, treat datapoints with uncertain status codes as good. Used for both raw queries and aggregates. Default: True.

        Returns:
            Datapoints | DatapointsList | None: A ``Datapoints`` object containing the requested data, or a ``DatapointsList`` if multiple time series were asked for (the ordering is ids first, then external_ids). If `ignore_unknown_ids` is `True`, a single time series is requested and it is not found, the function will return `None`.

        Examples:

            You can specify the identifiers of the datapoints you wish to retrieve in a number of ways. In this example
            we are using the time-ago format, ``"2w-ago"`` to get raw data for the time series with id=42 from 2 weeks ago up until now.
            You can also use the time-ahead format, like ``"3d-ahead"``, to specify a relative time in the future.

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> dps = client.time_series.data.retrieve(id=42, start="2w-ago")
                >>> # You can also use instance_id:
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> dps = client.time_series.data.retrieve(instance_id=NodeId("ts-space", "foo"))

            Although raw datapoints are returned by default, you can also get aggregated values, such as `max` or `average`. You may also fetch more than one time series simultaneously. Here we are
            getting daily averages and maximum values for all of 2018, for two different time series, where we're specifying `start` and `end` as integers
            (milliseconds after epoch). In the below example, we fetch them using their external ids:

                >>> dps_lst = client.time_series.data.retrieve(
                ...    external_id=["foo", "bar"],
                ...    start=1514764800000,
                ...    end=1546300800000,
                ...    aggregates=["max", "average"],
                ...    granularity="1d")

            In the two code examples above, we have a `dps` object (an instance of ``Datapoints``), and a `dps_lst` object (an instance of ``DatapointsList``).
            On `dps`, which in this case contains raw datapoints, you may access the underlying data directly by using the `.value` attribute. This works for
            both numeric and string (raw) datapoints, but not aggregates - they must be accessed by their respective names, because you're allowed to fetch
            all available aggregates simultaneously, and they are stored on the same object:

                >>> raw_data = dps.value
                >>> first_dps = dps_lst[0]  # optionally: `dps_lst.get(external_id="foo")`
                >>> avg_data = first_dps.average
                >>> max_data = first_dps.max

            You may also slice a ``Datapoints`` object (you get ``Datapoints`` back), or ask for "a row of data" at a single index in same way you would do with a
            built-in `list` (you get a `Datapoint` object back, note the singular name). You'll also get `Datapoint` objects when iterating through a ``Datapoints``
            object, but this should generally be avoided (consider this a performance warning):

                >>> dps_slice = dps[-10:]  # Last ten values
                >>> dp = dps[3]  # The third value
                >>> for dp in dps_slice:
                ...     pass  # do something!

            All parameters can be individually set if you use and pass ``DatapointsQuery`` objects (even ``ignore_unknown_ids``, contrary to the API).
            If you also pass top-level parameters, these will be overruled by the individual parameters (where both exist, so think of these as defaults).
            You are free to mix any kind of ids and external ids: Single identifiers, single DatapointsQuery objects and (mixed) lists of these.

            Let's say you want different aggregates and end-times for a few time series (when only fetching a single aggregate, you may pass
            the string directly for convenience):

                >>> from cognite.client.data_classes import DatapointsQuery
                >>> dps_lst = client.time_series.data.retrieve(
                ...     id=[
                ...         DatapointsQuery(id=42, end="1d-ago", aggregates="average"),
                ...         DatapointsQuery(id=69, end="2d-ahead", aggregates=["average"]),
                ...         DatapointsQuery(id=96, end="3d-ago", aggregates=["min", "max", "count"]),
                ...     ],
                ...     external_id=DatapointsQuery(external_id="foo", aggregates="max"),
                ...     start="5d-ago",
                ...     granularity="1h")

            Certain aggregates are very useful when they follow the calendar, for example electricity consumption per day, week, month
            or year. You may request such calendar-based aggregates in a specific timezone to make them even more useful: daylight savings (DST)
            will be taken care of automatically and the datapoints will be aligned to the timezone. Note: Calendar granularities and timezone
            can be used independently. To get monthly local aggregates in Oslo, Norway you can do:

                >>> dps = client.time_series.data.retrieve(
                ...     id=123,
                ...     aggregates="sum",
                ...     granularity="1month",
                ...     timezone="Europe/Oslo")

            When requesting multiple time series, an easy way to get the datapoints of a specific one is to use the `.get` method
            on the returned ``DatapointsList`` object, then specify if you want `id` or `external_id`. Note: If you fetch a time series
            by using `id`, you can still access it with its `external_id` (and the opposite way around), if you know it:

                >>> from datetime import datetime, timezone
                >>> utc = timezone.utc
                >>> dps_lst = client.time_series.data.retrieve(
                ...     start=datetime(1907, 10, 14, tzinfo=utc),
                ...     end=datetime(1907, 11, 6, tzinfo=utc),
                ...     id=[42, 43, 44, ..., 499, 500],
                ... )
                >>> ts_350 = dps_lst.get(id=350)  # ``Datapoints`` object

            ...but what happens if you request some duplicate ids or external_ids? In this example we will show how to get data from
            multiple disconnected periods. Let's say you're tasked to train a machine learning model to recognize a specific failure mode
            of a system, and you want the training data to only be from certain periods (when an alarm was on/high). Assuming these alarms
            are stored as events in CDF, with both start- and end times, we can use these directly in the query.

            After fetching, the `.get` method will return a list of ``Datapoints`` instead, (assuming we have more than one event) in the
            same order, similar to how slicing works with non-unique indices on Pandas DataFrames:

                >>> periods = client.events.list(type="alarm", subtype="pressure")
                >>> sensor_xid = "foo-pressure-bar"
                >>> dps_lst = client.time_series.data.retrieve(
                ...     id=[42, 43, 44],
                ...     external_id=[
                ...         DatapointsQuery(external_id=sensor_xid, start=ev.start_time, end=ev.end_time)
                ...         for ev in periods
                ...     ])
                >>> ts_44 = dps_lst.get(id=44)  # Single ``Datapoints`` object
                >>> ts_lst = dps_lst.get(external_id=sensor_xid)  # List of ``len(periods)`` ``Datapoints`` objects

            The API has an endpoint to :py:meth:`~DatapointsAPI.retrieve_latest`, i.e. "before", but not "after". Luckily, we can emulate that behaviour easily.
            Let's say we have a very dense time series and do not want to fetch all of the available raw data (or fetch less precise
            aggregate data), just to get the very first datapoint of every month (from e.g. the year 2000 through 2010):

                >>> import itertools
                >>> month_starts = [
                ...     datetime(year, month, 1, tzinfo=utc)
                ...     for year, month in itertools.product(range(2000, 2011), range(1, 13))]
                >>> dps_lst = client.time_series.data.retrieve(
                ...     external_id=[DatapointsQuery(external_id="foo", start=start) for start in month_starts],
                ...     limit=1)

            To get *all* historic and future datapoints for a time series, e.g. to do a backup, you may want to import the two integer
            constants: ``MIN_TIMESTAMP_MS`` and ``MAX_TIMESTAMP_MS``, to make sure you do not miss any. **Performance warning**: This pattern of
            fetching datapoints from the entire valid time domain is slower and shouldn't be used for regular "day-to-day" queries:

                >>> from cognite.client.utils import MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS
                >>> dps_backup = client.time_series.data.retrieve(
                ...     id=123,
                ...     start=MIN_TIMESTAMP_MS,
                ...     end=MAX_TIMESTAMP_MS + 1)  # end is exclusive

            If you have a time series with 'unit_external_id' set, you can use the 'target_unit' parameter to convert the datapoints
            to the desired unit. In the example below, we are converting temperature readings from a sensor measured and stored in Celsius,
            to Fahrenheit (we're assuming that the time series has e.g. ``unit_external_id="temperature:deg_c"`` ):

                >>> client.time_series.data.retrieve(
                ...   id=42, start="2w-ago", target_unit="temperature:deg_f")

            Or alternatively, you can use the 'target_unit_system' parameter to convert the datapoints to the desired unit system:

                >>> client.time_series.data.retrieve(
                ...   id=42, start="2w-ago", target_unit_system="Imperial")

            To retrieve status codes for a time series, pass ``include_status=True``. This is only possible for raw datapoint queries.
            You would typically also pass ``ignore_bad_datapoints=False`` to not hide all the datapoints that are marked as uncertain or bad,
            which is the API's default behaviour. You may also use ``treat_uncertain_as_bad`` to control how uncertain values are interpreted.

                >>> dps = client.time_series.data.retrieve(
                ...   id=42, include_status=True, ignore_bad_datapoints=False)
                >>> dps.status_code  # list of integer codes, e.g.: [0, 1073741824, 2147483648]
                >>> dps.status_symbol  # list of symbolic representations, e.g. [Good, Uncertain, Bad]

            There are six aggregates directly related to status codes, three for count: 'count_good', 'count_uncertain' and 'count_bad', and
            three for duration: 'duration_good', 'duration_uncertain' and 'duration_bad'. These may be fetched as any other aggregate.
            It is important to note that status codes may influence how other aggregates are computed: Aggregates will in general omit the
            time period between a bad datapoint and the next good datapoint. Also, the period between a bad datapoint and the previous good
            datapoint will be considered constant. To put simply, what 'average' may return depends on your setting for 'ignore_bad_datapoints'
            and 'treat_uncertain_as_bad' (in the presence of uncertain/bad datapoints).
        """
        return run_sync(
            self.__async_client.time_series.data.retrieve(
                id=id,
                external_id=external_id,
                instance_id=instance_id,
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                timezone=timezone,
                target_unit=target_unit,
                target_unit_system=target_unit_system,
                limit=limit,
                include_outside_points=include_outside_points,
                ignore_unknown_ids=ignore_unknown_ids,
                include_status=include_status,
                ignore_bad_datapoints=ignore_bad_datapoints,
                treat_uncertain_as_bad=treat_uncertain_as_bad,
            )
        )

    @overload
    def retrieve_arrays(
        self,
        *,
        id: int | DatapointsQuery,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> DatapointsArray | None: ...

    @overload
    def retrieve_arrays(
        self,
        *,
        id: Sequence[int | DatapointsQuery],
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> DatapointsArrayList: ...

    @overload
    def retrieve_arrays(
        self,
        *,
        external_id: str | DatapointsQuery,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> DatapointsArray | None: ...

    @overload
    def retrieve_arrays(
        self,
        *,
        external_id: SequenceNotStr[str | DatapointsQuery],
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> DatapointsArrayList: ...

    @overload
    def retrieve_arrays(
        self,
        *,
        instance_id: NodeId | DatapointsQuery,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> DatapointsArray | None: ...

    @overload
    def retrieve_arrays(
        self,
        *,
        instance_id: Sequence[NodeId | DatapointsQuery],
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> DatapointsArrayList: ...

    def retrieve_arrays(
        self,
        *,
        id: None | int | DatapointsQuery | Sequence[int | DatapointsQuery] = None,
        external_id: None | str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery] = None,
        instance_id: None | NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery] = None,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> DatapointsArray | DatapointsArrayList | None:
        """
        `Retrieve datapoints for one or more time series. <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getMultiTimeSeriesDatapoints>`_

        Note:
            This method requires ``numpy`` to be installed.

        Time series support status codes like Good, Uncertain and Bad. You can read more in the Cognite Data Fusion developer documentation on
        `status codes. <https://docs.cognite.com/dev/concepts/reference/status_codes/>`_

        Args:
            id (None | int | DatapointsQuery | Sequence[int | DatapointsQuery]): Id, dict (with id) or (mixed) sequence of these. See examples below.
            external_id (None | str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery]): External id, dict (with external id) or (mixed) sequence of these. See examples below.
            instance_id (None | NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery]): Instance id or sequence of instance ids.
            start (int | str | datetime.datetime | None): Inclusive start. Default: 1970-01-01 UTC.
            end (int | str | datetime.datetime | None): Exclusive end. Default: "now"
            aggregates (Aggregate | str | list[Aggregate | str] | None): Single aggregate or list of aggregates to retrieve. Available options: ``average``, ``continuous_variance``, ``count``, ``count_bad``, ``count_good``, ``count_uncertain``, ``discrete_variance``, ``duration_bad``, ``duration_good``, ``duration_uncertain``, ``interpolation``, ``max``, ``max_datapoint``, ``min``, ``min_datapoint``, ``step_interpolation``, ``sum`` and ``total_variation``. Default: None (raw datapoints returned)
            granularity (str | None): The granularity to fetch aggregates at. Can be given as an abbreviation or spelled out for clarity: ``s/second(s)``, ``m/minute(s)``, ``h/hour(s)``, ``d/day(s)``, ``w/week(s)``, ``mo/month(s)``, ``q/quarter(s)``, or ``y/year(s)``. Examples: ``30s``, ``5m``, ``1day``, ``2weeks``. Default: None.
            timezone (str | datetime.timezone | ZoneInfo | None): For raw datapoints, which timezone to use when displaying (will not affect what is retrieved). For aggregates, which timezone to align to for granularity 'hour' and longer. Align to the start of the hour, day or month. For timezones of type Region/Location, like 'Europe/Oslo', pass a string or ``ZoneInfo`` instance. The aggregate duration will then vary, typically due to daylight saving time. You can also use a fixed offset from UTC by passing a string like '+04:00', 'UTC-7' or 'UTC-02:30' or an instance of ``datetime.timezone``. Note: Historical timezones with second offset are not supported, and timezones with minute offsets (e.g. UTC+05:30 or Asia/Kolkata) may take longer to execute.
            target_unit (str | None): The unit_external_id of the datapoints returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system (str | None): The unit system of the datapoints returned. Cannot be used with target_unit.
            limit (int | None): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether to include outside points. Not allowed when fetching aggregates. Default: False
            ignore_unknown_ids (bool): Whether to ignore missing time series rather than raising an exception. Default: False
            include_status (bool): Also return the status code, an integer, for each datapoint in the response. Only relevant for raw datapoint queries, and the object aggregates ``min_datapoint`` and ``max_datapoint``.
            ignore_bad_datapoints (bool): Treat datapoints with a bad status code as if they do not exist. If set to false, raw queries will include bad datapoints in the response, and aggregates will in general omit the time period between a bad datapoint and the next good datapoint. Also, the period between a bad datapoint and the previous good datapoint will be considered constant. Default: True.
            treat_uncertain_as_bad (bool): Treat datapoints with uncertain status codes as bad. If false, treat datapoints with uncertain status codes as good. Used for both raw queries and aggregates. Default: True.

        Returns:
            DatapointsArray | DatapointsArrayList | None: A ``DatapointsArray`` object containing the requested data, or a ``DatapointsArrayList`` if multiple time series were asked for (the ordering is ids first, then external_ids). If `ignore_unknown_ids` is `True`, a single time series is requested and it is not found, the function will return `None`.

        Note:
            For many more usage examples, check out the :py:meth:`~DatapointsAPI.retrieve` method which accepts exactly the same arguments.

            When retrieving raw datapoints with ``ignore_bad_datapoints=False``, bad datapoints with the value NaN can not be distinguished from those
            missing a value (due to being stored in a numpy array). To solve this, all missing values have their timestamp recorded in a set you may access:
            ``dps.null_timestamps``. If you chose to pass a ``DatapointsArray`` to an insert method, this will be inspected automatically to replicate correctly
            (inserting status codes will soon be supported).

        Examples:

            Get weekly ``min`` and ``max`` aggregates for a time series with id=42 since the year 2000, then compute the range of values:

                >>> from cognite.client import CogniteClient
                >>> from datetime import datetime, timezone
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> dps = client.time_series.data.retrieve_arrays(
                ...     id=42,
                ...     start=datetime(2020, 1, 1, tzinfo=timezone.utc),
                ...     aggregates=["min", "max"],
                ...     granularity="7d")
                >>> weekly_range = dps.max - dps.min

            Get up-to 2 million raw datapoints for the last 48 hours for a noisy time series with external_id="ts-noisy",
            then use a small and wide moving average filter to smooth it out:

                >>> import numpy as np
                >>> dps = client.time_series.data.retrieve_arrays(
                ...     external_id="ts-noisy",
                ...     start="2d-ago",
                ...     limit=2_000_000)
                >>> smooth = np.convolve(dps.value, np.ones(5) / 5)  # doctest: +SKIP
                >>> smoother = np.convolve(dps.value, np.ones(20) / 20)  # doctest: +SKIP

            Get raw datapoints for multiple time series, that may or may not exist, from the last 2 hours, then find the
            largest gap between two consecutive values for all time series, also taking the previous value into account (outside point).

                >>> id_lst = [42, 43, 44]
                >>> dps_lst = client.time_series.data.retrieve_arrays(
                ...     id=id_lst,
                ...     start="2h-ago",
                ...     include_outside_points=True,
                ...     ignore_unknown_ids=True)
                >>> largest_gaps = [np.max(np.diff(dps.timestamp)) for dps in dps_lst]

            Get raw datapoints for a time series with external_id="bar" from the last 10 weeks, then convert to a ``pandas.Series``
            (you can of course also use the ``to_pandas()`` convenience method if you want a ``pandas.DataFrame``):

                >>> import pandas as pd
                >>> dps = client.time_series.data.retrieve_arrays(external_id="bar", start="10w-ago")
                >>> series = pd.Series(dps.value, index=dps.timestamp)
        """
        return run_sync(
            self.__async_client.time_series.data.retrieve_arrays(  # type: ignore [call-overload, misc]
                id=id,
                external_id=external_id,
                instance_id=instance_id,
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                timezone=timezone,
                target_unit=target_unit,
                target_unit_system=target_unit_system,
                limit=limit,
                include_outside_points=include_outside_points,
                ignore_unknown_ids=ignore_unknown_ids,
                include_status=include_status,
                ignore_bad_datapoints=ignore_bad_datapoints,
                treat_uncertain_as_bad=treat_uncertain_as_bad,
            )
        )

    def retrieve_dataframe(
        self,
        *,
        id: None | int | DatapointsQuery | Sequence[int | DatapointsQuery] = None,
        external_id: None | str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery] = None,
        instance_id: None | NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery] = None,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        uniform_index: bool = False,
        include_status: bool = False,
        include_unit: bool = True,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
    ) -> pd.DataFrame:
        """
        Get datapoints directly in a pandas dataframe.

        Time series support status codes like Good, Uncertain and Bad. You can read more in the Cognite Data Fusion developer documentation on
        `status codes. <https://docs.cognite.com/dev/concepts/reference/status_codes/>`_

        Note:
            For many more usage examples, check out the :py:meth:`~DatapointsAPI.retrieve` method which accepts exactly the same arguments.

        Args:
            id (None | int | DatapointsQuery | Sequence[int | DatapointsQuery]): Id, DatapointsQuery or (mixed) sequence of these. See examples.
            external_id (None | str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery]): External id, DatapointsQuery or (mixed) sequence of these. See examples.
            instance_id (None | NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery]): Instance id, DatapointsQuery or (mixed) sequence of these. See examples.
            start (int | str | datetime.datetime | None): Inclusive start. Default: 1970-01-01 UTC.
            end (int | str | datetime.datetime | None): Exclusive end. Default: "now"
            aggregates (Aggregate | str | list[Aggregate | str] | None): Single aggregate or list of aggregates to retrieve. Available options: ``average``, ``continuous_variance``, ``count``, ``count_bad``, ``count_good``, ``count_uncertain``, ``discrete_variance``, ``duration_bad``, ``duration_good``, ``duration_uncertain``, ``interpolation``, ``max``, ``max_datapoint``, ``min``, ``min_datapoint``, ``step_interpolation``, ``sum`` and ``total_variation``. Default: None (raw datapoints returned)
            granularity (str | None): The granularity to fetch aggregates at. Can be given as an abbreviation or spelled out for clarity: ``s/second(s)``, ``m/minute(s)``, ``h/hour(s)``, ``d/day(s)``, ``w/week(s)``, ``mo/month(s)``, ``q/quarter(s)``, or ``y/year(s)``. Examples: ``30s``, ``5m``, ``1day``, ``2weeks``. Default: None.
            timezone (str | datetime.timezone | ZoneInfo | None): For raw datapoints, which timezone to use when displaying (will not affect what is retrieved). For aggregates, which timezone to align to for granularity 'hour' and longer. Align to the start of the hour, -day or -month. For timezones of type Region/Location, like 'Europe/Oslo', pass a string or ``ZoneInfo`` instance. The aggregate duration will then vary, typically due to daylight saving time. You can also use a fixed offset from UTC by passing a string like '+04:00', 'UTC-7' or 'UTC-02:30' or an instance of ``datetime.timezone``. Note: Historical timezones with second offset are not supported, and timezones with minute offsets (e.g. UTC+05:30 or Asia/Kolkata) may take longer to execute.
            target_unit (str | None): The unit_external_id of the datapoints returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system (str | None): The unit system of the datapoints returned. Cannot be used with target_unit.
            limit (int | None): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether to include outside points. Not allowed when fetching aggregates. Default: False
            ignore_unknown_ids (bool): Whether to ignore missing time series rather than raising an exception. Default: False
            ignore_bad_datapoints (bool): Treat datapoints with a bad status code as if they do not exist. If set to false, raw queries will include bad datapoints in the response, and aggregates will in general omit the time period between a bad datapoint and the next good datapoint. Also, the period between a bad datapoint and the previous good datapoint will be considered constant. Default: True.
            treat_uncertain_as_bad (bool): Treat datapoints with uncertain status codes as bad. If false, treat datapoints with uncertain status codes as good. Used for both raw queries and aggregates. Default: True.
            uniform_index (bool): If only querying aggregates AND a single granularity is used (that's NOT a calendar granularity like month/quarter/year) AND no limit is used AND no timezone is used, specifying `uniform_index=True` will return a dataframe with an equidistant datetime index from the earliest `start` to the latest `end` (missing values will be NaNs). If these requirements are not met, a ValueError is raised. Default: False
            include_status (bool): Also return the status code, an integer, for each datapoint in the response. Only relevant for raw datapoint queries, and the object aggregates ``min_datapoint`` and ``max_datapoint``. Also adds the status info as a separate level in the columns (MultiIndex).
            include_unit (bool): Include the unit_external_id in the dataframe columns, if present (separate MultiIndex level)
            include_aggregate_name (bool): Include aggregate in the dataframe columns, if present (separate MultiIndex level)
            include_granularity_name (bool): Include granularity in the dataframe columns, if present (separate MultiIndex level)

        Returns:
            pd.DataFrame: A pandas DataFrame containing the requested time series. The ordering of columns is ids first, then external_ids, and lastly instance_ids. For time series with multiple aggregates, they will be sorted in alphabetical order ("average" before "max").

        Tip:
            Pandas DataFrames have one shared index, so when you fetch datapoints from multiple time series, the final index will be
            the union of all the timestamps. Thus, unless all time series have the exact same timestamps, the various columns will contain
            NaNs to fill the "missing" values. For lower memory usage on unaligned data, use the :py:meth:`~DatapointsAPI.retrieve_arrays` method.

        Warning:
            If you have duplicated time series in your query, the dataframe columns will also contain duplicates.

            When retrieving raw datapoints with ``ignore_bad_datapoints=False``, bad datapoints with the value NaN can not be distinguished from those
            missing a value (due to being stored in a numpy array); all will become NaNs in the dataframe.

        Examples:

            Get a pandas dataframe using a single time series external ID, with data from the last two weeks,
            but with no more than 100 datapoints:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> df = client.time_series.data.retrieve_dataframe(
                ...     external_id="foo",
                ...     start="2w-ago",
                ...     end="now",
                ...     limit=100)

            Get the pandas dataframe with a uniform index (fixed spacing between points) of 1 day, for two time series with
            individually specified aggregates, from 1990 through 2020:

                >>> from datetime import datetime, timezone
                >>> from cognite.client.data_classes import DatapointsQuery
                >>> df = client.time_series.data.retrieve_dataframe(
                ...     external_id=[
                ...         DatapointsQuery(external_id="foo", aggregates="discrete_variance"),
                ...         DatapointsQuery(external_id="bar", aggregates=["total_variation", "continuous_variance"]),
                ...     ],
                ...     granularity="1d",
                ...     start=datetime(1990, 1, 1, tzinfo=timezone.utc),
                ...     end=datetime(2020, 12, 31, tzinfo=timezone.utc),
                ...     uniform_index=True)

            Get a pandas dataframe containing the 'average' aggregate for two time series using a monthly granularity,
            starting Jan 1, 1970 all the way up to present, without having the aggregate name in the columns:

                >>> df = client.time_series.data.retrieve_dataframe(
                ...     external_id=["foo", "bar"],
                ...     aggregates="average",
                ...     granularity="1mo",
                ...     include_aggregate_name=False)

            You may also use ``pandas.Timestamp`` to define start and end. Here we fetch using instance_id:

                >>> import pandas as pd
                >>> df = client.time_series.data.retrieve_dataframe(
                ...     instance_id=NodeId("my-space", "my-ts-xid"),
                ...     start=pd.Timestamp("2023-01-01"),
                ...     end=pd.Timestamp("2023-02-01"))
        """
        return run_sync(
            self.__async_client.time_series.data.retrieve_dataframe(
                id=id,
                external_id=external_id,
                instance_id=instance_id,
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                timezone=timezone,
                target_unit=target_unit,
                target_unit_system=target_unit_system,
                limit=limit,
                include_outside_points=include_outside_points,
                ignore_unknown_ids=ignore_unknown_ids,
                ignore_bad_datapoints=ignore_bad_datapoints,
                treat_uncertain_as_bad=treat_uncertain_as_bad,
                uniform_index=uniform_index,
                include_status=include_status,
                include_unit=include_unit,
                include_aggregate_name=include_aggregate_name,
                include_granularity_name=include_granularity_name,
            )
        )

    @overload
    def retrieve_latest(
        self,
        id: int | LatestDatapointQuery,
        *,
        before: None | int | str | datetime.datetime = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> Datapoints | None: ...

    @overload
    def retrieve_latest(
        self,
        id: Sequence[int | LatestDatapointQuery],
        *,
        before: None | int | str | datetime.datetime = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> DatapointsList: ...

    @overload
    def retrieve_latest(
        self,
        *,
        id: int | LatestDatapointQuery,
        before: None | int | str | datetime.datetime = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> Datapoints | None: ...

    @overload
    def retrieve_latest(
        self,
        *,
        id: Sequence[int | LatestDatapointQuery],
        before: None | int | str | datetime.datetime = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> DatapointsList: ...

    @overload
    def retrieve_latest(
        self,
        *,
        external_id: str | LatestDatapointQuery,
        before: None | int | str | datetime.datetime = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> Datapoints | None: ...

    @overload
    def retrieve_latest(
        self,
        *,
        external_id: SequenceNotStr[str | LatestDatapointQuery],
        before: None | int | str | datetime.datetime = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> DatapointsList: ...

    @overload
    def retrieve_latest(
        self,
        *,
        instance_id: NodeId | LatestDatapointQuery,
        before: None | int | str | datetime.datetime = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> Datapoints | None: ...

    @overload
    def retrieve_latest(
        self,
        *,
        instance_id: Sequence[NodeId | LatestDatapointQuery],
        external_id: None = None,
        before: None | int | str | datetime.datetime = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> DatapointsList: ...

    @overload
    def retrieve_latest(
        self,
        *,
        id: int | LatestDatapointQuery | Sequence[int | LatestDatapointQuery] | None,
        external_id: str | LatestDatapointQuery | SequenceNotStr[str | LatestDatapointQuery] | None,
        instance_id: NodeId | LatestDatapointQuery | Sequence[NodeId | LatestDatapointQuery] | None,
        before: None | int | str | datetime.datetime = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> DatapointsList: ...

    @overload
    def retrieve_latest(
        self,
        *,
        id: int | LatestDatapointQuery | Sequence[int | LatestDatapointQuery] | None,
        external_id: str | LatestDatapointQuery | SequenceNotStr[str | LatestDatapointQuery] | None,
        before: None | int | str | datetime.datetime = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> DatapointsList: ...

    @overload
    def retrieve_latest(
        self,
        *,
        id: int | LatestDatapointQuery | Sequence[int | LatestDatapointQuery] | None,
        instance_id: NodeId | LatestDatapointQuery | Sequence[NodeId | LatestDatapointQuery] | None,
        before: None | int | str | datetime.datetime = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> DatapointsList: ...

    @overload
    def retrieve_latest(
        self,
        *,
        external_id: str | LatestDatapointQuery | SequenceNotStr[str | LatestDatapointQuery] | None,
        instance_id: NodeId | LatestDatapointQuery | Sequence[NodeId | LatestDatapointQuery] | None,
        before: None | int | str | datetime.datetime = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> DatapointsList: ...

    def retrieve_latest(
        self,
        id: int | LatestDatapointQuery | Sequence[int | LatestDatapointQuery] | None = None,
        external_id: str | LatestDatapointQuery | SequenceNotStr[str | LatestDatapointQuery] | None = None,
        instance_id: NodeId | LatestDatapointQuery | Sequence[NodeId | LatestDatapointQuery] | None = None,
        before: None | int | str | datetime.datetime = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> Datapoints | DatapointsList | None:
        """
        `Get the latest datapoint for one or more time series <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getLatest>`_

        Time series support status codes like Good, Uncertain and Bad. You can read more in the Cognite Data Fusion developer documentation on
        `status codes. <https://docs.cognite.com/dev/concepts/reference/status_codes/>`_

        Args:
            id (int | LatestDatapointQuery | Sequence[int | LatestDatapointQuery] | None): Id or list of ids.
            external_id (str | LatestDatapointQuery | SequenceNotStr[str | LatestDatapointQuery] | None): External id or list of external ids.
            instance_id (NodeId | LatestDatapointQuery | Sequence[NodeId | LatestDatapointQuery] | None): Instance id or list of instance ids.
            before (None | int | str | datetime.datetime): Get latest datapoint before this time. Not used when passing 'LatestDatapointQuery'.
            target_unit (str | None): The unit_external_id of the datapoint returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system (str | None): The unit system of the datapoint returned. Cannot be used with target_unit.
            include_status (bool): Also return the status code, an integer, for each datapoint in the response.
            ignore_bad_datapoints (bool): Prevent datapoints with a bad status code to be returned. Default: True.
            treat_uncertain_as_bad (bool): Treat uncertain status codes as bad. If false, treat uncertain as good. Default: True.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            Datapoints | DatapointsList | None: A Datapoints object containing the requested data, or a DatapointsList if multiple were requested. If `ignore_unknown_ids` is `True`, a single time series is requested and it is not found, the function will return `None`.

        Examples:

            Getting the latest datapoint in a time series. This method returns a Datapoints object, so the datapoint
            (if it exists) will be the first element:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.time_series.data.retrieve_latest(id=1)[0]

            You can also use external_id or instance_id; single identifier or list of identifiers:

                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> res = client.time_series.data.retrieve_latest(
                ...     external_id=["foo", "bar"],
                ...     instance_id=NodeId("my-space", "my-ts-xid"))

            You can also get the first datapoint before a specific time:

                >>> res = client.time_series.data.retrieve_latest(id=1, before="2d-ago")[0]

            You can also get the first datapoint before a specific time in the future e.g. forecast data:

                >>> res = client.time_series.data.retrieve_latest(id=1, before="2d-ahead")[0]

            You can also retrieve the datapoint in a different unit or unit system:

                >>> res = client.time_series.data.retrieve_latest(id=1, target_unit="temperature:deg_f")[0]
                >>> res = client.time_series.data.retrieve_latest(id=1, target_unit_system="Imperial")[0]

            You may also pass an instance of LatestDatapointQuery:

                >>> from cognite.client.data_classes import LatestDatapointQuery
                >>> res = client.time_series.data.retrieve_latest(id=LatestDatapointQuery(id=1, before=60_000))[0]

            If you need the latest datapoint for multiple time series, simply give a list of ids. Note that we are
            using external ids here, but either will work:

                >>> res = client.time_series.data.retrieve_latest(external_id=["abc", "def"])
                >>> latest_abc = res[0][0]
                >>> latest_def = res[1][0]

            If you for example need to specify a different value of 'before' for each time series, you may pass several
            LatestDatapointQuery objects. These will override any parameter passed directly to the function and also allows
            for individual customisation of 'target_unit', 'target_unit_system', 'include_status', 'ignore_bad_datapoints'
            and 'treat_uncertain_as_bad'.

                >>> from datetime import datetime, timezone
                >>> id_queries = [
                ...     123,
                ...     LatestDatapointQuery(id=456, before="1w-ago"),
                ...     LatestDatapointQuery(id=789, before=datetime(2018,1,1, tzinfo=timezone.utc)),
                ...     LatestDatapointQuery(id=987, target_unit="temperature:deg_f")]
                >>> ext_id_queries = [
                ...     "foo",
                ...     LatestDatapointQuery(external_id="abc", before="3h-ago", target_unit_system="Imperial"),
                ...     LatestDatapointQuery(external_id="def", include_status=True),
                ...     LatestDatapointQuery(external_id="ghi", treat_uncertain_as_bad=False),
                ...     LatestDatapointQuery(external_id="jkl", include_status=True, ignore_bad_datapoints=False)]
                >>> res = client.time_series.data.retrieve_latest(
                ...     id=id_queries, external_id=ext_id_queries)
        """
        return run_sync(
            self.__async_client.time_series.data.retrieve_latest(
                id=id,
                external_id=external_id,
                instance_id=instance_id,
                before=before,
                target_unit=target_unit,
                target_unit_system=target_unit_system,
                include_status=include_status,
                ignore_bad_datapoints=ignore_bad_datapoints,
                treat_uncertain_as_bad=treat_uncertain_as_bad,
                ignore_unknown_ids=ignore_unknown_ids,
            )
        )

    def insert(
        self,
        datapoints: Datapoints
        | DatapointsArray
        | Sequence[dict[str, int | float | str | datetime.datetime]]
        | Sequence[
            tuple[int | float | datetime.datetime, int | float | str]
            | tuple[int | float | datetime.datetime, int | float | str, int]
        ],
        id: int | None = None,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
    ) -> None:
        """
        Insert datapoints into a time series

        Timestamps can be represented as milliseconds since epoch or datetime objects. Note that naive datetimes
        are interpreted to be in the local timezone (not UTC), adhering to Python conventions for datetime handling.

        Time series support status codes like Good, Uncertain and Bad. You can read more in the Cognite Data Fusion developer documentation on
        `status codes. <https://docs.cognite.com/dev/concepts/reference/status_codes/>`_

        Args:
            datapoints (Datapoints | DatapointsArray | Sequence[dict[str, int | float | str | datetime.datetime]] | Sequence[tuple[int | float | datetime.datetime, int | float | str] | tuple[int | float | datetime.datetime, int | float | str, int]]): The datapoints you wish to insert. Can either be a list of tuples, a list of dictionaries, a Datapoints object or a DatapointsArray object. See examples below.
            id (int | None): Id of time series to insert datapoints into.
            external_id (str | None): External id of time series to insert datapoint into.
            instance_id (NodeId | None): Instance ID of time series to insert datapoints into.

        Note:
            All datapoints inserted without a status code (or symbol) is assumed to be good (code 0). To mark a value, pass
            either the status code (int) or status symbol (str). Only one of code and symbol is required. If both are given,
            they must match or an API error will be raised.

            Datapoints marked bad can take on any of the following values: None (missing), NaN, and +/- Infinity. It is also not
            restricted by the normal numeric range [-1e100, 1e100] (i.e. can be any valid float64).

        Examples:

            Your datapoints can be a list of tuples where the first element is the timestamp and the second element is the value.
            The third element is optional and may contain the status code for the datapoint. To pass by symbol, a dictionary must be used.

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import StatusCode
                >>> from datetime import datetime, timezone
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> datapoints = [
                ...     (datetime(2018,1,1, tzinfo=timezone.utc), 1000),
                ...     (datetime(2018,1,2, tzinfo=timezone.utc), 2000, StatusCode.Good),
                ...     (datetime(2018,1,3, tzinfo=timezone.utc), 3000, StatusCode.Uncertain),
                ...     (datetime(2018,1,4, tzinfo=timezone.utc), None, StatusCode.Bad),
                ... ]
                >>> client.time_series.data.insert(datapoints, id=1)

            The timestamp can be given by datetime as above, or in milliseconds since epoch. Status codes can also be
            passed as normal integers; this is necessary if a subcategory or modifier flag is needed, e.g. 3145728: 'GoodClamped':

                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> datapoints = [
                ...     (150000000000, 1000),
                ...     (160000000000, 2000, 3145728),
                ...     (170000000000, 2000, 2147483648),  # Same as StatusCode.Bad
                ... ]
                >>> client.time_series.data.insert(datapoints, instance_id=NodeId("my-space", "my-ts-xid"))

            Or they can be a list of dictionaries:

                >>> import math
                >>> datapoints = [
                ...     {"timestamp": 150000000000, "value": 1000},
                ...     {"timestamp": 160000000000, "value": 2000},
                ...     {"timestamp": 170000000000, "value": 3000, "status": {"code": 0}},
                ...     {"timestamp": 180000000000, "value": 4000, "status": {"symbol": "Uncertain"}},
                ...     {"timestamp": 190000000000, "value": math.nan, "status": {"code": StatusCode.Bad, "symbol": "Bad"}},
                ... ]
                >>> client.time_series.data.insert(datapoints, external_id="abcd")

            Or they can be a Datapoints or DatapointsArray object (with raw datapoints only). Note that the id or external_id
            set on these objects are not inspected/used (as they belong to the "from-time-series", and not the "to-time-series"),
            and so you must explicitly pass the identifier of the time series you want to insert into, which in this example is
            `external_id="foo"`.

            If the Datapoints or DatapointsArray are fetched with status codes, these will be automatically used in the insert:

                >>> data = client.time_series.data.retrieve(
                ...     external_id="abc",
                ...     start="1w-ago",
                ...     end="now",
                ...     include_status=True,
                ...     ignore_bad_datapoints=False,
                ... )
                >>> client.time_series.data.insert(data, external_id="foo")
        """
        return run_sync(
            self.__async_client.time_series.data.insert(
                datapoints=datapoints, id=id, external_id=external_id, instance_id=instance_id
            )
        )

    def insert_multiple(
        self, datapoints: list[dict[str, str | int | list | Datapoints | DatapointsArray | NodeId]]
    ) -> None:
        """
        `Insert datapoints into multiple time series <https://api-docs.cognite.com/20230101/tag/Time-series/operation/postMultiTimeSeriesDatapoints>`_

        Timestamps can be represented as milliseconds since epoch or datetime objects. Note that naive datetimes
        are interpreted to be in the local timezone (not UTC), adhering to Python conventions for datetime handling.

        Time series support status codes like Good, Uncertain and Bad. You can read more in the Cognite Data Fusion developer documentation on
        `status codes. <https://docs.cognite.com/dev/concepts/reference/status_codes/>`_

        Args:
            datapoints (list[dict[str, str | int | list | Datapoints | DatapointsArray | NodeId]]): The datapoints you wish to insert along with the ids of the time series. See examples below.

        Note:
            All datapoints inserted without a status code (or symbol) is assumed to be good (code 0). To mark a value, pass
            either the status code (int) or status symbol (str). Only one of code and symbol is required. If both are given,
            they must match or an API error will be raised.

            Datapoints marked bad can take on any of the following values: None (missing), NaN, and +/- Infinity. It is also not
            restricted by the normal numeric range [-1e100, 1e100] (i.e. can be any valid float64).

        Examples:

            Your datapoints can be a list of dictionaries, each containing datapoints for a different (presumably) time series. These dictionaries
            must have the key "datapoints" (containing the data) specified as a ``Datapoints`` object, a ``DatapointsArray`` object, or list of either
            tuples `(timestamp, value)` or dictionaries, `{"timestamp": ts, "value": value}`.

            When passing tuples, the third element is optional and may contain the status code for the datapoint. To pass by symbol, a dictionary must be used.


                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> from cognite.client.data_classes import StatusCode
                >>> from datetime import datetime, timezone
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> to_insert = [
                ...     {"id": 1, "datapoints": [
                ...         (datetime(2018,1,1, tzinfo=timezone.utc), 1000),
                ...         (datetime(2018,1,2, tzinfo=timezone.utc), 2000, StatusCode.Good)],
                ...     },
                ...     {"external_id": "foo", "datapoints": [
                ...         (datetime(2018,1,3, tzinfo=timezone.utc), 3000),
                ...         (datetime(2018,1,4, tzinfo=timezone.utc), 4000, StatusCode.Uncertain)],
                ...     },
                ...     {"instance_id": NodeId("my-space", "my-ts-xid"), "datapoints": [
                ...         (datetime(2018,1,5, tzinfo=timezone.utc), 5000),
                ...         (datetime(2018,1,6, tzinfo=timezone.utc), None, StatusCode.Bad)],
                ...     }
                ... ]

            Passing datapoints using the dictionary format with timestamp given in milliseconds since epoch:

                >>> import math
                >>> to_insert.append(
                ...     {"external_id": "bar", "datapoints": [
                ...         {"timestamp": 170000000, "value": 7000},
                ...         {"timestamp": 180000000, "value": 8000, "status": {"symbol": "Uncertain"}},
                ...         {"timestamp": 190000000, "value": None, "status": {"code": StatusCode.Bad}},
                ...         {"timestamp": 200000000, "value": math.inf, "status": {"code": StatusCode.Bad, "symbol": "Bad"}},
                ... ]})

            If the Datapoints or DatapointsArray are fetched with status codes, these will be automatically used in the insert:

                >>> data_to_clone = client.time_series.data.retrieve(
                ...     external_id="bar", include_status=True, ignore_bad_datapoints=False)
                >>> to_insert.append({"external_id": "bar-clone", "datapoints": data_to_clone})
                >>> client.time_series.data.insert_multiple(to_insert)
        """
        return run_sync(self.__async_client.time_series.data.insert_multiple(datapoints=datapoints))

    def delete_range(
        self,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime,
        id: int | None = None,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
    ) -> None:
        """
        Delete a range of datapoints from a time series.

        Args:
            start (int | str | datetime.datetime): Inclusive start of delete range
            end (int | str | datetime.datetime): Exclusive end of delete range
            id (int | None): Id of time series to delete data from
            external_id (str | None): External id of time series to delete data from
            instance_id (NodeId | None): Instance ID of time series to delete data from

        Examples:

            Deleting the last week of data from a time series:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.time_series.data.delete_range(start="1w-ago", end="now", id=1)

            Deleting the data from now until 2 days in the future from a time series containing e.g. forecasted data:

                >>> client.time_series.data.delete_range(start="now", end="2d-ahead", id=1)
        """
        return run_sync(
            self.__async_client.time_series.data.delete_range(
                start=start, end=end, id=id, external_id=external_id, instance_id=instance_id
            )
        )

    def delete_ranges(self, ranges: list[dict[str, Any]]) -> None:
        """
        `Delete a range of datapoints from multiple time series. <https://api-docs.cognite.com/20230101/tag/Time-series/operation/deleteDatapoints>`_

        Args:
            ranges (list[dict[str, Any]]): The list of datapoint ids along with time range to delete. See examples below.

        Examples:

            Each element in the list ranges must be specify either id or external_id, and a range:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> ranges = [{"id": 1, "start": "2d-ago", "end": "now"},
                ...           {"external_id": "abc", "start": "2d-ago", "end": "2d-ahead"}]
                >>> client.time_series.data.delete_ranges(ranges)
        """
        return run_sync(self.__async_client.time_series.data.delete_ranges(ranges=ranges))

    def insert_dataframe(self, df: pd.DataFrame, dropna: bool = True) -> None:
        """
        Insert a dataframe containing datapoints to one or more time series.

        The index of the dataframe must contain the timestamps (pd.DatetimeIndex). The column identifiers
        must contain the IDs (``int``), external IDs (``str``) or instance IDs (``NodeId`` or 2-tuple (space, ext. ID))
        of the already existing time series to which the datapoints from that particular column will be written.

        Note:
            The column identifiers must be unique.

        Args:
            df (pd.DataFrame):  Pandas DataFrame object containing the time series.
            dropna (bool): Set to True to ignore NaNs in the given DataFrame, applied per column. Default: True.

        Warning:
            You can not insert datapoints with status codes using this method (``insert_dataframe``), you'll need
            to use the :py:meth:`~DatapointsAPI.insert` method instead (or :py:meth:`~DatapointsAPI.insert_multiple`)!

        Examples:
            Post a dataframe with white noise to three time series, one using ID, one using external id
            and one using instance id:

                >>> import numpy as np
                >>> import pandas as pd
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> node_id = NodeId("my-space", "my-ts-xid")
                >>> df = pd.DataFrame(
                ...     {
                ...         123: np.random.normal(0, 1, 100),
                ...         "foo": np.random.normal(0, 1, 100),
                ...         node_id: np.random.normal(0, 1, 100),
                ...     },
                ...     index=pd.date_range(start="2018-01-01", periods=100, freq="1d")
                ... )
                >>> client.time_series.data.insert_dataframe(df)
        """
        return run_sync(self.__async_client.time_series.data.insert_dataframe(df=df, dropna=dropna))
