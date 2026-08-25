from __future__ import annotations

import asyncio
import datetime
import functools
import itertools
import math
from collections.abc import AsyncIterator, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    TypeVar,
    cast,
    overload,
)
from zoneinfo import ZoneInfo

from cognite.client._api.datapoint_tasks import (
    BaseDpsFetchSubtask,
    _DpsQueryValidator,
    _FullDatapointsQuery,
)
from cognite.client._api.datapoints_io import (
    ChunkingDpsFetcher,
    DatapointsPoster,
    EagerDpsFetcher,
    RetrieveLatestDpsFetcher,
    StateDatapointsPoster,
    _InsertDatapoint,
)
from cognite.client._api.synthetic_time_series import SyntheticDatapointsAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_DATAPOINTS_CHUNK_SIZE
from cognite.client.data_classes import (
    Datapoints,
    DatapointsArray,
    DatapointsArrayList,
    DatapointsList,
    DatapointsQuery,
    LatestDatapoint,
    LatestDatapointList,
    LatestDatapointQuery,
    StateDatapointsInsert,
)
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.datapoint_aggregates import Aggregate
from cognite.client.utils._auxiliary import (
    find_duplicates,
    is_positive_int,
    split_into_chunks,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import Identifier
from cognite.client.utils._importing import local_import
from cognite.client.utils._pandas_helpers import to_pandas_timestamp
from cognite.client.utils._time import (
    timestamp_to_ms,
)
from cognite.client.utils._validation import validate_user_input_dict_with_identifier
from cognite.client.utils.useful_types import SequenceNotStr, is_sequence_not_str

if TYPE_CHECKING:
    import pandas as pd

    from cognite.client import AsyncCogniteClient
    from cognite.client._api.datapoints_io import DpsFetchStrategy
    from cognite.client.config import ClientConfig


PoolSubtaskType = tuple[float, int, BaseDpsFetchSubtask]

_T = TypeVar("_T")


class DatapointsAPI(APIClient):
    _RESOURCE_PATH = "/timeseries/data"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.synthetic = SyntheticDatapointsAPI(config, api_version, cognite_client)
        self._FETCH_TS_LIMIT = 100
        self._DPS_LIMIT_AGG = 10_000
        self._DPS_LIMIT_RAW = 100_000
        self._DPS_INSERT_LIMIT = 100_000
        self._RETRIEVE_LATEST_LIMIT = 100
        self._POST_DPS_OBJECTS_LIMIT = 10_000

        self.query_validator = _DpsQueryValidator(dps_limit_raw=self._DPS_LIMIT_RAW, dps_limit_agg=self._DPS_LIMIT_AGG)
        self._insert_states_warning = FeaturePreviewWarning(
            api_maturity="beta",
            sdk_maturity="alpha",
            feature_name="State time series datapoints",
            pluralize=True,
        )

    def _get_semaphore(self, operation: Literal["read", "write", "delete"]) -> asyncio.BoundedSemaphore:
        from cognite.client import global_config

        return global_config.concurrency_settings.datapoints._semaphore_factory(
            operation, project=self._cognite_client.config.project
        )

    @overload
    def __call__(
        self,
        queries: DatapointsQuery,
        *,
        return_arrays: Literal[True] = True,
        chunk_size_datapoints: int = DEFAULT_DATAPOINTS_CHUNK_SIZE,
        chunk_size_time_series: int | None = None,
    ) -> AsyncIterator[DatapointsArray]: ...

    @overload
    def __call__(
        self,
        queries: Sequence[DatapointsQuery],
        *,
        return_arrays: Literal[True] = True,
        chunk_size_datapoints: int = DEFAULT_DATAPOINTS_CHUNK_SIZE,
        chunk_size_time_series: int | None = None,
    ) -> AsyncIterator[DatapointsArrayList]: ...

    @overload
    def __call__(
        self,
        queries: DatapointsQuery,
        *,
        return_arrays: Literal[False],
        chunk_size_datapoints: int = DEFAULT_DATAPOINTS_CHUNK_SIZE,
        chunk_size_time_series: int | None = None,
    ) -> AsyncIterator[Datapoints]: ...

    @overload
    def __call__(
        self,
        queries: Sequence[DatapointsQuery],
        *,
        return_arrays: Literal[False],
        chunk_size_datapoints: int = DEFAULT_DATAPOINTS_CHUNK_SIZE,
        chunk_size_time_series: int | None = None,
    ) -> AsyncIterator[DatapointsList]: ...

    async def __call__(
        self,
        queries: DatapointsQuery | Sequence[DatapointsQuery],
        *,
        chunk_size_datapoints: int = DEFAULT_DATAPOINTS_CHUNK_SIZE,
        chunk_size_time_series: int | None = None,
        return_arrays: bool = True,
    ) -> AsyncIterator[DatapointsArray | DatapointsArrayList | Datapoints | DatapointsList]:
        """`Iterate through datapoints in chunks, for one or more time series <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getMultiTimeSeriesDatapoints>`_.

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
                ...     DatapointsQuery(instance_id=NodeId("my-space", "my-ts-xid")),
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
        # To make efficient usage of the API, we don't want a chunk size like 10 to send a million API requests when we can
        # get 10k/100k datapoints per request. Thus, we round up the given chunk size to the nearest integer multiple of 100k,
        # then subdivide and yield client-side (we use the raw limit also when dealing with aggregates):
        request_limit = self._DPS_LIMIT_RAW * math.ceil(chunk_size_datapoints / self._DPS_LIMIT_RAW)
        if not is_positive_int(chunk_size_datapoints) or (
            chunk_size_datapoints != request_limit and request_limit % chunk_size_datapoints
        ):
            raise ValueError(
                "The 'chunk_size_datapoints' must be a positive integer that evenly divides 100k OR an integer multiple of 100k "
                f"(to ensure efficient API usage), not {chunk_size_datapoints}."
            )

        if not (chunk_size_time_series is None or is_positive_int(chunk_size_time_series)):
            raise ValueError(
                f"'chunk_size_time_series' must be a positive integer or None, not {chunk_size_time_series}"
            )

        user_queries = [queries] if (is_single := isinstance(queries, DatapointsQuery)) else queries
        dps_lst_cls: type[DatapointsArrayList | DatapointsList] = (
            DatapointsArrayList if return_arrays else DatapointsList
        )
        for uq in user_queries:
            if uq.include_outside_points is True or uq.limit is not DatapointsQuery._NOT_SET:
                raise ValueError(
                    "When iterating datapoints, the options 'include_outside_points' and 'limit' are not supported."
                )

        if dupes := find_duplicates(uq.identifier.as_primitive() for uq in user_queries):
            raise ValueError(f"When iterating datapoints, identifiers must be unique! Duplicates found for: {dupes}")

        alive_queries = {
            uq.identifier: DatapointsQuery.valid_from_user_query(uq, limit=request_limit, include_outside_points=False)
            for uq in user_queries
        }
        self.query_validator(alive_queries.values())

        dps_lst: DatapointsArrayList | DatapointsList
        chunk_fn = functools.partial(split_into_chunks, chunk_size=chunk_size_datapoints)

        while alive_queries:
            to_fetch_queries = list(itertools.islice(alive_queries.values(), chunk_size_time_series))
            fetcher = self._select_dps_fetch_strategy(to_fetch_queries)(self, to_fetch_queries)
            if return_arrays:
                dps_lst = await fetcher.fetch_all_datapoints_numpy()
            else:
                dps_lst = await fetcher.fetch_all_datapoints()
            self._update_alive_queries_and_do_manual_cursoring(alive_queries, dps_lst, to_fetch_queries, request_limit)

            # We should never yield an empty chunk, so we filter out empty or exhausted time series from result
            # (need to rebuild to not keep references to those empty in various private "id lookups")
            dps_lst = dps_lst_cls(list(filter(None, dps_lst)))
            if not any(dps_lst):
                if alive_queries:
                    continue
                break

            if chunk_size_datapoints == request_limit:
                yield dps_lst[0] if is_single else dps_lst
            elif is_single:
                for chunk in chunk_fn(dps_lst[0]):
                    yield chunk  # type: ignore [misc]
            else:
                for all_chunks in itertools.zip_longest(*map(chunk_fn, dps_lst)):
                    # Filter out dps as ts get exhausted, then rebuild the Dps(Array)List container and yield chunk:
                    yield dps_lst_cls(list(filter(None, all_chunks)))  # type: ignore [arg-type]

    @staticmethod
    def _update_alive_queries_and_do_manual_cursoring(
        alive_queries: dict[Identifier, DatapointsQuery],
        dps_lst: DatapointsArrayList | DatapointsList,
        to_fetch_queries: list[DatapointsQuery],
        request_limit: int,
    ) -> None:
        for query in to_fetch_queries:
            ident = query.identifier
            dps = dps_lst.get(**{ident.name(camel_case=False): ident.as_primitive()})
            if isinstance(dps, list):
                raise RuntimeError(
                    "When iterating datapoints, identifiers must be unique! You cannot get around this by passing "
                    "several of [id, external_id, instance_id] for the same underlying time series."
                )
            # Update query.start for next iteration if ts is not yet exhausted:
            if dps and len(dps) == request_limit:
                new_start = dps[-1].timestamp + 1
                if query.end_ms > new_start:
                    query.start = new_start  # manual cursoring ftw
                    continue
            alive_queries.pop(ident)

    @overload
    async def retrieve(
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
    async def retrieve(
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
    async def retrieve(
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
    async def retrieve(
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
    async def retrieve(
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
    async def retrieve(
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
    async def retrieve(
        self,
        *,
        id: int | DatapointsQuery | Sequence[int | DatapointsQuery] | None,
        external_id: str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery] | None,
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
    async def retrieve(
        self,
        *,
        id: int | DatapointsQuery | Sequence[int | DatapointsQuery] | None,
        instance_id: NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery] | None,
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
    async def retrieve(
        self,
        *,
        external_id: str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery] | None,
        instance_id: NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery] | None,
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
    async def retrieve(
        self,
        *,
        id: int | DatapointsQuery | Sequence[int | DatapointsQuery] | None,
        external_id: str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery] | None,
        instance_id: NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery] | None,
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

    async def retrieve(
        self,
        *,
        id: int | DatapointsQuery | Sequence[int | DatapointsQuery] | None = None,
        external_id: str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery] | None = None,
        instance_id: NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery] | None = None,
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
        """`Retrieve datapoints for one or more time series <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getMultiTimeSeriesDatapoints>`_.

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
            id (int | DatapointsQuery | Sequence[int | DatapointsQuery] | None): Id, dict (with id) or (mixed) sequence of these. See examples below.
            external_id (str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery] | None): External id, dict (with external id) or (mixed) sequence of these. See examples below.
            instance_id (NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery] | None): Instance id or sequence of instance ids.
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
            we are using the time-ago format, ``"2w-ago"`` to get raw data for a time series from 2 weeks ago up until now.
            You can also use the time-ahead format, like ``"3d-ahead"``, to specify a relative time in the future.

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> dps = client.time_series.data.retrieve(
                ...     instance_id=NodeId("ts-space", "foo"), start="2w-ago"
                ... )

            Although raw datapoints are returned by default, you can also get aggregated values, such as `max` or `average`. You may also fetch more than one time series simultaneously. Here we are
            getting daily averages and maximum values for all of 2018, for two different time series, where we're specifying `start` and `end` as integers
            (milliseconds after epoch). In the below example, we fetch them using their external ids:

                >>> dps_lst = client.time_series.data.retrieve(
                ...     external_id=["foo", "bar"],
                ...     start=1514764800000,
                ...     end=1546300800000,
                ...     aggregates=["max", "average"],
                ...     granularity="1d",
                ... )

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
                ...     granularity="1h",
                ... )

            Certain aggregates are very useful when they follow the calendar, for example electricity consumption per day, week, month
            or year. You may request such calendar-based aggregates in a specific timezone to make them even more useful: daylight savings (DST)
            will be taken care of automatically and the datapoints will be aligned to the timezone. Note: Calendar granularities and timezone
            can be used independently. To get monthly local aggregates in Oslo, Norway you can do:

                >>> dps = client.time_series.data.retrieve(
                ...     id=123, aggregates="sum", granularity="1month", timezone="Europe/Oslo"
                ... )

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
                ...     ],
                ... )
                >>> ts_44 = dps_lst.get(id=44)  # Single ``Datapoints`` object
                >>> ts_lst = dps_lst.get(
                ...     external_id=sensor_xid
                ... )  # List of ``len(periods)`` ``Datapoints`` objects

            The API has an endpoint to :py:meth:`~DatapointsAPI.retrieve_latest`, i.e. "before", but not "after". Luckily, we can emulate that behaviour easily.
            Let's say we have a very dense time series and do not want to fetch all of the available raw data (or fetch less precise
            aggregate data), just to get the very first datapoint of every month (from e.g. the year 2000 through 2010):

                >>> import itertools
                >>> month_starts = [
                ...     datetime(year, month, 1, tzinfo=utc)
                ...     for year, month in itertools.product(range(2000, 2011), range(1, 13))
                ... ]
                >>> dps_lst = client.time_series.data.retrieve(
                ...     external_id=[
                ...         DatapointsQuery(external_id="foo", start=start) for start in month_starts
                ...     ],
                ...     limit=1,
                ... )

            To get *all* historic and future datapoints for a time series, e.g. to do a backup, you may want to import the two integer
            constants: ``MIN_TIMESTAMP_MS`` and ``MAX_TIMESTAMP_MS``, to make sure you do not miss any. **Performance warning**: This pattern of
            fetching datapoints from the entire valid time domain is slower and shouldn't be used for regular "day-to-day" queries:

                >>> from cognite.client.utils import MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS
                >>> dps_backup = client.time_series.data.retrieve(
                ...     id=123, start=MIN_TIMESTAMP_MS, end=MAX_TIMESTAMP_MS + 1
                ... )  # end is exclusive

            If you have a time series with 'unit_external_id' set, you can use the 'target_unit' parameter to convert the datapoints
            to the desired unit. In the example below, we are converting temperature readings from a sensor measured and stored in Celsius,
            to Fahrenheit (we're assuming that the time series has e.g. ``unit_external_id="temperature:deg_c"`` ):

                >>> client.time_series.data.retrieve(
                ...     id=42, start="2w-ago", target_unit="temperature:deg_f"
                ... )

            Or alternatively, you can use the 'target_unit_system' parameter to convert the datapoints to the desired unit system:

                >>> client.time_series.data.retrieve(
                ...     id=42, start="2w-ago", target_unit_system="Imperial"
                ... )

            To retrieve status codes for a time series, pass ``include_status=True``. This is only possible for raw datapoint queries.
            You would typically also pass ``ignore_bad_datapoints=False`` to not hide all the datapoints that are marked as uncertain or bad,
            which is the API's default behaviour. You may also use ``treat_uncertain_as_bad`` to control how uncertain values are interpreted.

                >>> dps = client.time_series.data.retrieve(
                ...     id=42, include_status=True, ignore_bad_datapoints=False
                ... )
                >>> dps.status_code  # list of integer codes, e.g.: [0, 1073741824, 2147483648]
                >>> dps.status_symbol  # list of symbolic representations, e.g. [Good, Uncertain, Bad]

            There are six aggregates directly related to status codes, three for count: 'count_good', 'count_uncertain' and 'count_bad', and
            three for duration: 'duration_good', 'duration_uncertain' and 'duration_bad'. These may be fetched as any other aggregate.
            It is important to note that status codes may influence how other aggregates are computed: Aggregates will in general omit the
            time period between a bad datapoint and the next good datapoint. Also, the period between a bad datapoint and the previous good
            datapoint will be considered constant. To put simply, what 'average' may return depends on your setting for 'ignore_bad_datapoints'
            and 'treat_uncertain_as_bad' (in the presence of uncertain/bad datapoints).
        """
        query = _FullDatapointsQuery(
            start=start,
            end=end,
            id=id,
            external_id=external_id,
            instance_id=instance_id,
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
        self.query_validator(parsed_queries := query.parse_into_queries())
        dps_lst = await self._select_dps_fetch_strategy(parsed_queries)(self, parsed_queries).fetch_all_datapoints()

        if not query.is_single_identifier:
            return dps_lst
        elif not dps_lst:
            return None
        return dps_lst[0]

    @overload
    async def retrieve_arrays(
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
    async def retrieve_arrays(
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
    async def retrieve_arrays(
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
    async def retrieve_arrays(
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
    async def retrieve_arrays(
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
    async def retrieve_arrays(
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

    async def retrieve_arrays(
        self,
        *,
        id: int | DatapointsQuery | Sequence[int | DatapointsQuery] | None = None,
        external_id: str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery] | None = None,
        instance_id: NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery] | None = None,
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
        """`Retrieve datapoints for one or more time series <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getMultiTimeSeriesDatapoints>`_.

        Note:
            This method requires ``numpy`` to be installed.

        Time series support status codes like Good, Uncertain and Bad. You can read more in the Cognite Data Fusion developer documentation on
        `status codes. <https://docs.cognite.com/dev/concepts/reference/status_codes/>`_

        Args:
            id (int | DatapointsQuery | Sequence[int | DatapointsQuery] | None): Id, dict (with id) or (mixed) sequence of these. See examples below.
            external_id (str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery] | None): External id, dict (with external id) or (mixed) sequence of these. See examples below.
            instance_id (NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery] | None): Instance id or sequence of instance ids.
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

            Get weekly ``min`` and ``max`` aggregates for a time series using instance_id, then compute the range of values:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> from datetime import datetime, timezone
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> dps = client.time_series.data.retrieve_arrays(
                ...     instance_id=NodeId("my-space", "my-ts-xid"),
                ...     start=datetime(2020, 1, 1, tzinfo=timezone.utc),
                ...     aggregates=["min", "max"],
                ...     granularity="7d",
                ... )
                >>> weekly_range = dps.max - dps.min

            Get up-to 2 million raw datapoints for the last 48 hours for a noisy time series with external_id="ts-noisy",
            then use a small and wide moving average filter to smooth it out:

                >>> import numpy as np
                >>> dps = client.time_series.data.retrieve_arrays(
                ...     external_id="ts-noisy", start="2d-ago", limit=2_000_000
                ... )
                >>> smooth = np.convolve(dps.value, np.ones(5) / 5)  # doctest: +SKIP
                >>> smoother = np.convolve(dps.value, np.ones(20) / 20)  # doctest: +SKIP

            Get raw datapoints for multiple time series, that may or may not exist, from the last 2 hours, then find the
            largest gap between two consecutive values for all time series, also taking the previous value into account (outside point).

                >>> id_lst = [42, 43, 44]
                >>> dps_lst = client.time_series.data.retrieve_arrays(
                ...     id=id_lst, start="2h-ago", include_outside_points=True, ignore_unknown_ids=True
                ... )
                >>> largest_gaps = [np.max(np.diff(dps.timestamp)) for dps in dps_lst]

            Get raw datapoints for a time series with external_id="bar" from the last 10 weeks, then convert to a ``pandas.Series``
            (you can of course also use the ``to_pandas()`` convenience method if you want a ``pandas.DataFrame``):

                >>> import pandas as pd
                >>> dps = client.time_series.data.retrieve_arrays(external_id="bar", start="10w-ago")
                >>> series = pd.Series(dps.value, index=dps.timestamp)
        """
        local_import("numpy")  # Verify that numpy is available or raise CogniteImportError
        query = _FullDatapointsQuery(
            start=start,
            end=end,
            id=id,
            external_id=external_id,
            instance_id=instance_id,
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
        self.query_validator(parsed_queries := query.parse_into_queries())
        dps_lst = await self._select_dps_fetch_strategy(parsed_queries)(
            self, parsed_queries
        ).fetch_all_datapoints_numpy()

        if not query.is_single_identifier:
            return dps_lst
        elif not dps_lst:
            return None
        return dps_lst[0]

    async def retrieve_dataframe(
        self,
        *,
        id: int | DatapointsQuery | Sequence[int | DatapointsQuery] | None = None,
        external_id: str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery] | None = None,
        instance_id: NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery] | None = None,
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
        """Get datapoints directly in a pandas dataframe.

        Time series support status codes like Good, Uncertain and Bad. You can read more in the Cognite Data Fusion developer documentation on
        `status codes. <https://docs.cognite.com/dev/concepts/reference/status_codes/>`_

        Note:
            For many more usage examples, check out the :py:meth:`~DatapointsAPI.retrieve` method which accepts exactly the same arguments.

        Args:
            id (int | DatapointsQuery | Sequence[int | DatapointsQuery] | None): Id, DatapointsQuery or (mixed) sequence of these. See examples.
            external_id (str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery] | None): External id, DatapointsQuery or (mixed) sequence of these. See examples.
            instance_id (NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery] | None): Instance id, DatapointsQuery or (mixed) sequence of these. See examples.
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

            Get a pandas dataframe using a single time series instance ID, with data from the last two weeks,
            but with no more than 100 datapoints:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> df = client.time_series.data.retrieve_dataframe(
                ...     instance_id=NodeId("my-space", "my-ts-xid"), start="2w-ago", end="now", limit=100
                ... )

            Get the pandas dataframe with a uniform index (fixed spacing between points) of 1 day, for two time series with
            individually specified aggregates, from 1990 through 2020:

                >>> from datetime import datetime, timezone
                >>> from cognite.client.data_classes import DatapointsQuery
                >>> df = client.time_series.data.retrieve_dataframe(
                ...     external_id=[
                ...         DatapointsQuery(external_id="foo", aggregates="discrete_variance"),
                ...         DatapointsQuery(
                ...             external_id="bar", aggregates=["total_variation", "continuous_variance"]
                ...         ),
                ...     ],
                ...     granularity="1d",
                ...     start=datetime(1990, 1, 1, tzinfo=timezone.utc),
                ...     end=datetime(2020, 12, 31, tzinfo=timezone.utc),
                ...     uniform_index=True,
                ... )

            Get a pandas dataframe containing the 'average' aggregate for two time series using a monthly granularity,
            starting Jan 1, 1970 all the way up to present, without having the aggregate name in the columns:

                >>> df = client.time_series.data.retrieve_dataframe(
                ...     external_id=["foo", "bar"],
                ...     aggregates="average",
                ...     granularity="1mo",
                ...     include_aggregate_name=False,
                ... )

            You may also use ``pandas.Timestamp`` to define start and end:

                >>> import pandas as pd
                >>> df = client.time_series.data.retrieve_dataframe(
                ...     external_id="foo",
                ...     start=pd.Timestamp("2023-01-01"),
                ...     end=pd.Timestamp("2023-02-01"),
                ... )
        """
        _, pd = local_import("numpy", "pandas")  # Verify that deps are available or raise CogniteImportError
        query = _FullDatapointsQuery(
            start=start,
            end=end,
            id=id,
            external_id=external_id,
            instance_id=instance_id,
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
        self.query_validator(parsed_queries := query.parse_into_queries())
        fetcher = self._select_dps_fetch_strategy(parsed_queries)(self, parsed_queries)

        if not uniform_index:
            result = await fetcher.fetch_all_datapoints_numpy()
            return result.to_pandas(
                include_aggregate_name=include_aggregate_name,
                include_granularity_name=include_granularity_name,
                include_status=include_status,
                include_unit=include_unit,
            )
        # Uniform index requires extra validation and processing:
        uses_tz_or_calendar_gran = any(q.use_cursors for q in fetcher.all_queries)
        grans_given = {q.granularity for q in fetcher.all_queries}
        is_limited = any(q.limit is not None for q in fetcher.all_queries)
        if fetcher.raw_queries or len(grans_given) > 1 or is_limited or uses_tz_or_calendar_gran:
            raise ValueError(
                "Cannot return a uniform index when asking for aggregates with multiple granularities "
                f"({grans_given or []}) OR when (partly) querying raw datapoints OR when a finite limit is used "
                "OR when timezone is used OR when a calendar granularity is used (e.g. month/quarter/year)"
            )
        result = await fetcher.fetch_all_datapoints_numpy()
        df = result.to_pandas(
            include_aggregate_name=include_aggregate_name,
            include_granularity_name=include_granularity_name,
            include_status=include_status,
            include_unit=include_unit,
        )
        start = to_pandas_timestamp(min(q.start_ms for q in fetcher.agg_queries))
        end = to_pandas_timestamp(max(q.end_ms for q in fetcher.agg_queries))
        (granularity,) = grans_given
        # Pandas understand "Cognite granularities" except `m` (minutes) which we must translate:
        freq = cast(str, granularity).replace("m", "min")
        return df.reindex(pd.date_range(start=start, end=end, freq=freq, inclusive="left"))

    @overload
    async def retrieve_latest(
        self,
        id: int | LatestDatapointQuery,
        *,
        before: int | str | datetime.datetime | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> LatestDatapoint | None: ...

    @overload
    async def retrieve_latest(
        self,
        id: Sequence[int | LatestDatapointQuery],
        *,
        before: int | str | datetime.datetime | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> LatestDatapointList: ...

    @overload
    async def retrieve_latest(
        self,
        *,
        id: int | LatestDatapointQuery,
        before: int | str | datetime.datetime | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> LatestDatapoint | None: ...

    @overload
    async def retrieve_latest(
        self,
        *,
        id: Sequence[int | LatestDatapointQuery],
        before: int | str | datetime.datetime | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> LatestDatapointList: ...

    @overload
    async def retrieve_latest(
        self,
        *,
        external_id: str | LatestDatapointQuery,
        before: int | str | datetime.datetime | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> LatestDatapoint | None: ...

    @overload
    async def retrieve_latest(
        self,
        *,
        external_id: SequenceNotStr[str | LatestDatapointQuery],
        before: int | str | datetime.datetime | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> LatestDatapointList: ...

    @overload
    async def retrieve_latest(
        self,
        *,
        instance_id: NodeId | LatestDatapointQuery,
        before: int | str | datetime.datetime | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> LatestDatapoint | None: ...

    @overload
    async def retrieve_latest(
        self,
        *,
        instance_id: Sequence[NodeId | LatestDatapointQuery],
        external_id: None = None,
        before: int | str | datetime.datetime | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> LatestDatapointList: ...

    @overload
    async def retrieve_latest(
        self,
        *,
        id: int | LatestDatapointQuery | Sequence[int | LatestDatapointQuery] | None,
        external_id: str | LatestDatapointQuery | SequenceNotStr[str | LatestDatapointQuery] | None,
        instance_id: NodeId | LatestDatapointQuery | Sequence[NodeId | LatestDatapointQuery] | None,
        before: int | str | datetime.datetime | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> LatestDatapointList: ...

    @overload
    async def retrieve_latest(
        self,
        *,
        id: int | LatestDatapointQuery | Sequence[int | LatestDatapointQuery] | None,
        external_id: str | LatestDatapointQuery | SequenceNotStr[str | LatestDatapointQuery] | None,
        before: int | str | datetime.datetime | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> LatestDatapointList: ...

    @overload
    async def retrieve_latest(
        self,
        *,
        id: int | LatestDatapointQuery | Sequence[int | LatestDatapointQuery] | None,
        instance_id: NodeId | LatestDatapointQuery | Sequence[NodeId | LatestDatapointQuery] | None,
        before: int | str | datetime.datetime | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> LatestDatapointList: ...

    @overload
    async def retrieve_latest(
        self,
        *,
        external_id: str | LatestDatapointQuery | SequenceNotStr[str | LatestDatapointQuery] | None,
        instance_id: NodeId | LatestDatapointQuery | Sequence[NodeId | LatestDatapointQuery] | None,
        before: int | str | datetime.datetime | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> LatestDatapointList: ...

    async def retrieve_latest(
        self,
        id: int | LatestDatapointQuery | Sequence[int | LatestDatapointQuery] | None = None,
        external_id: str | LatestDatapointQuery | SequenceNotStr[str | LatestDatapointQuery] | None = None,
        instance_id: NodeId | LatestDatapointQuery | Sequence[NodeId | LatestDatapointQuery] | None = None,
        before: int | str | datetime.datetime | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> LatestDatapoint | LatestDatapointList | None:
        """`Get the latest datapoint for one or more time series <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getLatest>`_.

        Time series support status codes like Good, Uncertain and Bad. You can read more in the Cognite Data Fusion developer documentation on
        `status codes. <https://docs.cognite.com/dev/concepts/reference/status_codes/>`_

        Args:
            id (int | LatestDatapointQuery | Sequence[int | LatestDatapointQuery] | None): Id or list of ids.
            external_id (str | LatestDatapointQuery | SequenceNotStr[str | LatestDatapointQuery] | None): External id or list of external ids.
            instance_id (NodeId | LatestDatapointQuery | Sequence[NodeId | LatestDatapointQuery] | None): Instance id or list of instance ids.
            before (int | str | datetime.datetime | None): Get latest datapoint before this time. Not used when passing 'LatestDatapointQuery'.
            target_unit (str | None): The unit_external_id of the datapoint returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system (str | None): The unit system of the datapoint returned. Cannot be used with target_unit.
            include_status (bool): Also return the status code, an integer, for each datapoint in the response.
            ignore_bad_datapoints (bool): Prevent datapoints with a bad status code to be returned. Default: True.
            treat_uncertain_as_bad (bool): Treat uncertain status codes as bad. If false, treat uncertain as good. Default: True.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            LatestDatapoint | LatestDatapointList | None: A LatestDatapoint object containing the latest datapoint (if it exists), or a LatestDatapointList if multiple time series were requested. If `ignore_unknown_ids` is `True`, a single time series is requested and it is not found, the function will return `None`.

        Examples:

            Getting the latest datapoint in a time series:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.time_series.data.retrieve_latest(
                ...     instance_id=NodeId("my-space", "my-ts-xid")
                ... )
                >>> if res:  # Check if datapoint exists
                ...     print(res.timestamp, res.value)

            You can also use id or external_id; single identifier or list of identifiers:

                >>> res = client.time_series.data.retrieve_latest(id=1, external_id=["foo", "bar"])

            You can also get the latest datapoint before a specific time:

                >>> res = client.time_series.data.retrieve_latest(id=1, before="2d-ago")

            You can also get the latest datapoint before a specific time in the future e.g. forecast data:

                >>> res = client.time_series.data.retrieve_latest(id=1, before="2d-ahead")

            You can also retrieve the datapoint in a different unit or unit system:

                >>> res = client.time_series.data.retrieve_latest(id=1, target_unit="temperature:deg_f")
                >>> res = client.time_series.data.retrieve_latest(id=1, target_unit_system="Imperial")

            You may also pass an instance of LatestDatapointQuery:

                >>> from cognite.client.data_classes import LatestDatapointQuery
                >>> res = client.time_series.data.retrieve_latest(
                ...     id=LatestDatapointQuery(id=1, before=60_000)
                ... )

            If you need the latest datapoint for multiple time series, simply give a list of ids. Note that we are
            using external ids here, but either will work:

                >>> res = client.time_series.data.retrieve_latest(external_id=["abc", "def"])
                >>> latest_abc = res[0]
                >>> latest_def = res[1]

            If you for example need to specify a different value of 'before' for each time series, you may pass several
            LatestDatapointQuery objects. These will override any parameter passed directly to the function and also allows
            for individual customisation of 'target_unit', 'target_unit_system', 'include_status', 'ignore_bad_datapoints'
            and 'treat_uncertain_as_bad'.

                >>> from datetime import datetime, timezone
                >>> id_queries = [
                ...     123,
                ...     LatestDatapointQuery(id=456, before="1w-ago"),
                ...     LatestDatapointQuery(id=789, before=datetime(2018, 1, 1, tzinfo=timezone.utc)),
                ...     LatestDatapointQuery(id=987, target_unit="temperature:deg_f"),
                ... ]
                >>> ext_id_queries = [
                ...     "foo",
                ...     LatestDatapointQuery(
                ...         external_id="abc", before="3h-ago", target_unit_system="Imperial"
                ...     ),
                ...     LatestDatapointQuery(external_id="def", include_status=True),
                ...     LatestDatapointQuery(external_id="ghi", treat_uncertain_as_bad=False),
                ...     LatestDatapointQuery(
                ...         external_id="jkl", include_status=True, ignore_bad_datapoints=False
                ...     ),
                ... ]
                >>> res = client.time_series.data.retrieve_latest(
                ...     id=id_queries, external_id=ext_id_queries
                ... )
        """
        fetcher = RetrieveLatestDpsFetcher(
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
            dps_client=self,
        )
        res = await fetcher.fetch_datapoints()
        if not fetcher.input_is_singleton:
            return LatestDatapointList._load(res)
        elif not res:
            return None
        return LatestDatapoint._load(res[0])

    async def insert(
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
        """Insert datapoints into a time series.

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

            State time series are not supported by this method; use :py:meth:`~DatapointsAPI.insert_states` instead.

        Examples:

            Your datapoints can be a list of tuples where the first element is the timestamp and the second element is the value.
            The third element is optional and may contain the status code for the datapoint. To pass by symbol, a dictionary must be used.

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import StatusCode
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> from datetime import datetime, timezone
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> datapoints = [
                ...     (datetime(2018, 1, 1, tzinfo=timezone.utc), 1000),
                ...     (datetime(2018, 1, 2, tzinfo=timezone.utc), 2000, StatusCode.Good),
                ...     (datetime(2018, 1, 3, tzinfo=timezone.utc), 3000, StatusCode.Uncertain),
                ...     (datetime(2018, 1, 4, tzinfo=timezone.utc), None, StatusCode.Bad),
                ... ]
                >>> client.time_series.data.insert(
                ...     datapoints, instance_id=NodeId("my-space", "my-ts-xid")
                ... )

            The timestamp can be given by datetime as above, or in milliseconds since epoch. Status codes can also be
            passed as normal integers; this is necessary if a subcategory or modifier flag is needed, e.g. 3145728: 'GoodClamped':

                >>> datapoints = [
                ...     (150000000000, 1000),
                ...     (160000000000, 2000, 3145728),
                ...     (170000000000, 2000, 2147483648),  # Same as StatusCode.Bad
                ... ]
                >>> client.time_series.data.insert(datapoints, id=1)

            Or they can be a list of dictionaries:

                >>> import math
                >>> datapoints = [
                ...     {"timestamp": 150000000000, "value": 1000},
                ...     {"timestamp": 160000000000, "value": 2000},
                ...     {"timestamp": 170000000000, "value": 3000, "status": {"code": 0}},
                ...     {"timestamp": 180000000000, "value": 4000, "status": {"symbol": "Uncertain"}},
                ...     {
                ...         "timestamp": 190000000000,
                ...         "value": math.nan,
                ...         "status": {"code": StatusCode.Bad, "symbol": "Bad"},
                ...     },
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

        post_dps_object = Identifier.of_either(id, external_id, instance_id).as_dict()
        post_dps_object["datapoints"] = datapoints
        await DatapointsPoster(self).insert([post_dps_object])

    async def insert_multiple(
        self, datapoints: list[dict[str, str | int | list | Datapoints | DatapointsArray | NodeId]]
    ) -> None:
        """`Insert datapoints into multiple time series <https://api-docs.cognite.com/20230101/tag/Time-series/operation/postMultiTimeSeriesDatapoints>`_.

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

            State time series are not supported by this method; use :py:meth:`~DatapointsAPI.insert_states` instead.

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
                ...     {
                ...         "id": 1,
                ...         "datapoints": [
                ...             (datetime(2018, 1, 1, tzinfo=timezone.utc), 1000),
                ...             (datetime(2018, 1, 2, tzinfo=timezone.utc), 2000, StatusCode.Good),
                ...         ],
                ...     },
                ...     {
                ...         "external_id": "foo",
                ...         "datapoints": [
                ...             (datetime(2018, 1, 3, tzinfo=timezone.utc), 3000),
                ...             (datetime(2018, 1, 4, tzinfo=timezone.utc), 4000, StatusCode.Uncertain),
                ...         ],
                ...     },
                ...     {
                ...         "instance_id": NodeId("my-space", "my-ts-xid"),
                ...         "datapoints": [
                ...             (datetime(2018, 1, 5, tzinfo=timezone.utc), 5000),
                ...             (datetime(2018, 1, 6, tzinfo=timezone.utc), None, StatusCode.Bad),
                ...         ],
                ...     },
                ... ]

            Passing datapoints using the dictionary format with timestamp given in milliseconds since epoch:

                >>> import math
                >>> to_insert.append(
                ...     {
                ...         "external_id": "bar",
                ...         "datapoints": [
                ...             {"timestamp": 170000000, "value": 7000},
                ...             {
                ...                 "timestamp": 180000000,
                ...                 "value": 8000,
                ...                 "status": {"symbol": "Uncertain"},
                ...             },
                ...             {
                ...                 "timestamp": 190000000,
                ...                 "value": None,
                ...                 "status": {"code": StatusCode.Bad},
                ...             },
                ...             {
                ...                 "timestamp": 200000000,
                ...                 "value": math.inf,
                ...                 "status": {"code": StatusCode.Bad, "symbol": "Bad"},
                ...             },
                ...         ],
                ...     }
                ... )

            If the Datapoints or DatapointsArray are fetched with status codes, these will be automatically used in the insert:

                >>> data_to_clone = client.time_series.data.retrieve(
                ...     external_id="bar", include_status=True, ignore_bad_datapoints=False
                ... )
                >>> to_insert.append({"external_id": "bar-clone", "datapoints": data_to_clone})
                >>> client.time_series.data.insert_multiple(to_insert)
        """
        if not isinstance(datapoints, Sequence):
            raise TypeError("Input to 'insert_multiple' must be a list of dictionaries")
        await DatapointsPoster(self).insert(datapoints)

    async def insert_states(self, items: Sequence[StateDatapointsInsert]) -> None:
        """Insert datapoints into one or more state time series.

        State time series are a specialized time series type designed for tracking discrete operational
        states of industrial equipment. Unlike numeric or string time series, they have a predefined set
        of valid states and support specialized aggregations optimized for analyzing state changes over
        time. Each state is a ``(numericValue, stringValue)`` pair, e.g. ``(1, "on")`` or ``(0, "off")``,
        and the set of valid pairs for a given time series is defined by its associated state set.

        Each datapoint may carry a numeric value, a string value, or both (they must be consistent
        with the time series' state set). It may also carry only a status code/symbol, e.g. to mark a
        period as ``Bad``.

        Warning:
            State time series are in `public beta <https://docs.cognite.com/cdf/product_feature_status#public-beta>`_.

        Args:
            items (Sequence[StateDatapointsInsert]): One ``StateDatapointsInsert`` per target state time series. Each carries the ``instance_id`` and the datapoints to write.

        Examples:

            Insert state datapoints into two state time series, by using their numeric state values:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import (
                ...     StateDatapointsInsert,
                ...     StateDatapointWrite,
                ...     StatusCode,
                ... )
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>>
                >>> first_insert = StateDatapointsInsert(
                ...     instance_id=NodeId("my-space", "first-state-ts"),
                ...     datapoints=[
                ...         StateDatapointWrite(1700000000000, -1),
                ...         StateDatapointWrite(1700000001000, 13),
                ...     ],
                ... )
                >>> second_insert = StateDatapointsInsert(
                ...     instance_id=("my-space", "second-state-ts"),  # tuple form is accepted
                ...     datapoints=[
                ...         StateDatapointWrite(datetime(2018, 7, 2), 42),
                ...         StateDatapointWrite(datetime(2018, 7, 8), 0),
                ...     ],
                ... )
                >>> client.time_series.data.insert_states([first_insert, second_insert])

            The datapoints to insert can also be given by the string state value (or a matching combination).
            Status codes can also be specified:

                >>> datapoints = [
                ...     StateDatapointWrite(11, numeric_value=0),
                ...     StateDatapointWrite(12, string_value="OFF"),
                ...     StateDatapointWrite(13, numeric_value=0, string_value="OFF"),
                ...     StateDatapointWrite(14, 1, status_code=StatusCode.Good),
                ...     StateDatapointWrite(15, string_value="OFF", status_symbol=StatusCode.Uncertain),
                ...     # Datapoints marked bad can have no numeric/string value:
                ...     StateDatapointWrite(16, status_code=StatusCode.Bad),
                ... ]

            Datapoints can also be given as dicts, matching the API's JSON shape (status codes/symbols
            must be given as a nested ``status`` sub-dict, matching the API):

                >>> client.time_series.data.insert_states(
                ...     [
                ...         StateDatapointsInsert(
                ...             instance_id=NodeId("my-space", "my-state-ts"),
                ...             datapoints=[
                ...                 {
                ...                     "timestamp": 1700000000000,
                ...                     "numeric_value": 0,
                ...                     "string_value": "off",
                ...                 },
                ...                 {
                ...                     "timestamp": 1700000001000,
                ...                     "numeric_value": 1,
                ...                     "string_value": "on",
                ...                 },
                ...                 {"timestamp": 1700000002000, "status": {"symbol": "Bad"}},
                ...             ],
                ...         )
                ...     ]
                ... )

        """
        self._insert_states_warning.warn()
        if not is_sequence_not_str(items):
            raise TypeError("Input to 'insert_states' must be a sequence of 'StateDatapointsInsert'")
        for item in items:
            if not isinstance(item, StateDatapointsInsert):
                raise TypeError(f"Each element of 'items' must be a 'StateDatapointsInsert', not {type(item).__name__}")
        await StateDatapointsPoster(self).insert(list(items))

    async def delete_range(
        self,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime,
        id: int | None = None,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
    ) -> None:
        """Delete a range of datapoints from a time series.

        Args:
            start (int | str | datetime.datetime): Inclusive start of delete range
            end (int | str | datetime.datetime): Exclusive end of delete range
            id (int | None): Id of time series to delete data from
            external_id (str | None): External id of time series to delete data from
            instance_id (NodeId | None): Instance ID of time series to delete data from

        Examples:

            Deleting the last week of data from a time series:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.time_series.data.delete_range(
                ...     start="1w-ago", end="now", instance_id=NodeId("my-space", "my-ts-xid")
                ... )

            Deleting the data from now until 2 days in the future from a time series containing e.g. forecasted data:

                >>> client.time_series.data.delete_range(start="now", end="2d-ahead", id=1)
        """
        start_ms = timestamp_to_ms(start)
        end_ms = timestamp_to_ms(end)
        if end_ms <= start_ms:
            raise ValueError(f"{end=} must be larger than {start=}")

        identifier = Identifier.of_either(id, external_id, instance_id).as_dict()
        delete_dps_object = {**identifier, "inclusiveBegin": start_ms, "exclusiveEnd": end_ms}
        await self._delete_datapoints_ranges([delete_dps_object])

    async def delete_ranges(self, ranges: list[dict[str, Any]]) -> None:
        """`Delete a range of datapoints from multiple time series <https://api-docs.cognite.com/20230101/tag/Time-series/operation/deleteDatapoints>`_.

        Args:
            ranges (list[dict[str, Any]]): The list of datapoint ids along with time range to delete. See examples below.

        Examples:

            Each element in the list ranges must be specify either id or external_id, and a range:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> ranges = [
                ...     {"id": 1, "start": "2d-ago", "end": "now"},
                ...     {"external_id": "abc", "start": "2d-ago", "end": "2d-ahead"},
                ... ]
                >>> client.time_series.data.delete_ranges(ranges)
        """
        valid_ranges = []
        for time_range in ranges:
            identifier = validate_user_input_dict_with_identifier(time_range, required_keys={"start", "end"})
            valid_range = dict(
                **identifier.as_dict(),
                inclusiveBegin=timestamp_to_ms(time_range["start"]),
                exclusiveEnd=timestamp_to_ms(time_range["end"]),
            )
            valid_ranges.append(valid_range)
        await self._delete_datapoints_ranges(valid_ranges)

    async def _delete_datapoints_ranges(self, delete_range_objects: list[dict]) -> None:
        await self._post(
            url_path=self._RESOURCE_PATH + "/delete",
            json={"items": delete_range_objects},
            semaphore=self._get_semaphore("delete"),
        )

    async def insert_dataframe(self, df: pd.DataFrame, dropna: bool = True) -> None:
        """Insert a dataframe containing datapoints to one or more time series.

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
                ...     index=pd.date_range(start="2018-01-01", periods=100, freq="1d"),
                ... )
                >>> client.time_series.data.insert_dataframe(df)
        """
        np, pd = local_import("numpy", "pandas")
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError(f"DataFrame index must be `pd.DatetimeIndex`, got: {type(df.index)}")
        if df.columns.has_duplicates:
            raise ValueError(f"DataFrame columns must be unique. Duplicated cols: {find_duplicates(df.columns)}.")
        if np.isinf(df.select_dtypes(include="number")).any(axis=None):
            raise ValueError("DataFrame contains one or more (+/-) Infinity. Remove them in order to insert the data.")
        if not dropna and df.isna().any(axis=None):
            raise ValueError("DataFrame contains one or more NaNs. Remove them or pass `dropna=True` to insert.")

        dps = []
        idx = df.index.to_numpy("datetime64[ms]").astype(np.int64)
        for column_id, col in df.items():
            mask = col.notna()
            datapoints = list(map(_InsertDatapoint, idx[mask], col[mask]))
            if not datapoints:
                continue

            match column_id:
                case int():
                    dps.append({"datapoints": datapoints, "id": column_id})
                case str():
                    dps.append({"datapoints": datapoints, "external_id": column_id})
                case NodeId() | (str(), str()):
                    dps.append({"datapoints": datapoints, "instance_id": NodeId.load(column_id)})
                case _:
                    raise ValueError(
                        f"Column identifiers must be either 'int' (ID), 'str' (external ID), or 'NodeId' "
                        f"(or 2-tuple (space, ext. ID)) (instance ID), not {type(column_id)}"
                    )
        await self.insert_multiple(dps)  # type: ignore[arg-type]

    def _select_dps_fetch_strategy(self, queries: list[DatapointsQuery]) -> type[DpsFetchStrategy]:
        semaphore = self._get_semaphore("read")

        # We decide the fetching strategy based on how many time series the user has requested VS the
        # max concurrency we allow for datapoints requests. When the number of time series is small enough
        # to fit within the semaphore limit, all time series can have their separate request initially;
        # (these fetch tasks will dynamically split based on density, in order to make full use of the "pool"):
        if len(queries) <= semaphore._bound_value:  # type: ignore[attr-defined]
            return EagerDpsFetcher
        # Fetch a smaller, chunked batch of dps from all time series - which allows us to do some rudimentary
        # guesstimation of dps density - then chunk away:
        return ChunkingDpsFetcher
