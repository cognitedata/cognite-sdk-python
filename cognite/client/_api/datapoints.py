from __future__ import annotations

import contextlib
import functools
import heapq
import itertools
import math
import time
import warnings
from abc import ABC, abstractmethod
from collections.abc import Mapping
from datetime import datetime, timedelta
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    MutableSequence,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from cognite.client._api.datapoint_tasks import (
    BaseDpsFetchSubtask,
    BaseTaskOrchestrator,
    DatapointsPayload,
    DatapointsPayloadItem,
    _DatapointsQuery,
    _SingleTSQueryBase,
    _SingleTSQueryValidator,
)
from cognite.client._api.synthetic_time_series import SyntheticDatapointsAPI
from cognite.client._api_client import APIClient
from cognite.client.data_classes.datapoints import (
    Aggregate,
    Datapoints,
    DatapointsArray,
    DatapointsArrayList,
    DatapointsList,
    LatestDatapointQuery,
)
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils._auxiliary import (
    exactly_one_is_not_none,
    find_duplicates,
    split_into_chunks,
    split_into_n_parts,
)
from cognite.client.utils._concurrency import execute_tasks, get_executor
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import Identifier, IdentifierSequence
from cognite.client.utils._importing import import_as_completed, import_legacy_protobuf, local_import
from cognite.client.utils._time import (
    _unit_in_days,
    align_large_granularity,
    get_granularity_multiplier_and_unit,
    in_timedelta,
    pandas_date_range_tz,
    timestamp_to_ms,
    to_fixed_utc_intervals,
    to_pandas_freq,
    validate_timezone,
)
from cognite.client.utils._validation import assert_type, validate_user_input_dict_with_identifier

if not import_legacy_protobuf():
    from cognite.client._proto.data_point_list_response_pb2 import DataPointListItem, DataPointListResponse
else:
    from cognite.client._proto_legacy.data_point_list_response_pb2 import (  # type: ignore [assignment]
        DataPointListItem,
        DataPointListResponse,
    )

if TYPE_CHECKING:
    from concurrent.futures import Future, ThreadPoolExecutor

    import pandas as pd

    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


as_completed = import_as_completed()

_TSQueryList = List[_SingleTSQueryBase]
PoolSubtaskType = Tuple[float, int, BaseDpsFetchSubtask]

_T = TypeVar("_T")
_TResLst = TypeVar("_TResLst", DatapointsList, DatapointsArrayList)


def select_dps_fetch_strategy(dps_client: DatapointsAPI, user_query: _DatapointsQuery) -> DpsFetchStrategy:
    validator = _SingleTSQueryValidator(
        user_query,
        dps_limit_raw=dps_client._DPS_LIMIT_RAW,
        dps_limit_agg=dps_client._DPS_LIMIT_AGG,
    )
    all_queries = validator.validate_and_create_single_queries()
    agg_queries, raw_queries = split_queries_into_raw_and_aggs(all_queries)

    api_subversion = None
    if validator.beta_api_subversion_is_needed():
        api_subversion = "beta"
        dps_client._unit_warning.warn()

    # Running mode is decided based on how many time series are requested VS. number of workers:
    if len(all_queries) <= (max_workers := dps_client._config.max_workers):
        # Start shooting requests from the hip immediately:
        return EagerDpsFetcher(dps_client, all_queries, agg_queries, raw_queries, max_workers, api_subversion)
    # Fetch a smaller, chunked batch of dps from all time series - which allows us to do some rudimentary
    # guesstimation of dps density - then chunk away:
    return ChunkingDpsFetcher(dps_client, all_queries, agg_queries, raw_queries, max_workers, api_subversion)


def split_queries_into_raw_and_aggs(all_queries: _TSQueryList) -> tuple[_TSQueryList, _TSQueryList]:
    split_qs: tuple[_TSQueryList, _TSQueryList] = [], []
    for query in all_queries:
        split_qs[query.is_raw_query].append(query)
    return split_qs


class DpsFetchStrategy(ABC):
    def __init__(
        self,
        dps_client: DatapointsAPI,
        all_queries: _TSQueryList,
        agg_queries: _TSQueryList,
        raw_queries: _TSQueryList,
        max_workers: int,
        api_subversion: str | None,
    ) -> None:
        self.dps_client = dps_client
        self.all_queries = all_queries
        self.agg_queries = agg_queries
        self.raw_queries = raw_queries
        self.max_workers = max_workers
        self.api_subversion = api_subversion
        self.n_queries = len(all_queries)

        # Fetching datapoints relies on protobuf, which, depending on OS and major version used
        # might be running in pure python or compiled C code. We issue a warning if we can determine
        # that the user is running in pure python mode (quite a bit slower...)
        with contextlib.suppress(ImportError):
            from google.protobuf.descriptor import _USE_C_DESCRIPTORS

            if _USE_C_DESCRIPTORS is False:
                warnings.warn(
                    "Your installation of 'protobuf' is missing compiled C binaries, and will run in pure-python mode, "
                    "which causes datapoints fetching to be ~5x slower. To verify, set the environment variable "
                    "`PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=cpp` before running (this will cause the code to fail). "
                    "The easiest fix is probably to pin your 'protobuf' dependency to major version 4 (or higher), "
                    "see: https://developers.google.com/protocol-buffers/docs/news/2022-05-06#python-updates",
                    UserWarning,
                    stacklevel=3,
                )

    def fetch_all_datapoints(self) -> DatapointsList:
        pool = get_executor(max_workers=self.max_workers)
        return DatapointsList(
            [ts_task.get_result() for ts_task in self._fetch_all(pool, use_numpy=False)],  # type: ignore [arg-type]
            cognite_client=self.dps_client._cognite_client,
        )

    def fetch_all_datapoints_numpy(self) -> DatapointsArrayList:
        pool = get_executor(max_workers=self.max_workers)
        return DatapointsArrayList(
            [ts_task.get_result() for ts_task in self._fetch_all(pool, use_numpy=True)],  # type: ignore [arg-type]
            cognite_client=self.dps_client._cognite_client,
        )

    def _make_dps_request_using_protobuf(self, payload: DatapointsPayload) -> bytes:
        return self.dps_client._do_request(
            json=payload,
            method="POST",
            url_path=f"{self.dps_client._RESOURCE_PATH}/list",
            accept="application/protobuf",
            timeout=self.dps_client._config.timeout,
            api_subversion=self.api_subversion,
        ).content

    def _request_datapoints(self, payload: DatapointsPayload) -> Sequence[DataPointListItem]:
        (res := DataPointListResponse()).MergeFromString(self._make_dps_request_using_protobuf(payload))
        return res.items

    @staticmethod
    def _raise_if_missing(to_raise: set[_SingleTSQueryBase]) -> None:
        if to_raise:
            raise CogniteNotFoundError(not_found=[q.identifier.as_dict(camel_case=False) for q in to_raise])

    @abstractmethod
    def _fetch_all(self, pool: ThreadPoolExecutor, use_numpy: bool) -> Iterator[BaseTaskOrchestrator]:
        raise NotImplementedError


class EagerDpsFetcher(DpsFetchStrategy):
    """A datapoints fetching strategy to make small queries as fast as possible.

    Is used when the number of time series to fetch is smaller than or equal to the number of `max_workers`, so
    that each worker only fetches datapoints for a single time series per request (this maximises throughput
    according to the API docs). This does -not- mean that we assign a time series to each worker! All available
    workers will fetch data for the same time series to speed up fetching. To make this work, the time domain is
    split based on the density of datapoints returned and other heuristics like granularity (e.g. given '1h', at
    most 168 datapoints exist per week).
    """

    def _fetch_all(self, pool: ThreadPoolExecutor, use_numpy: bool) -> Iterator[BaseTaskOrchestrator]:
        missing_to_raise: set[_SingleTSQueryBase] = set()
        futures_dct, ts_task_lookup = self._create_initial_tasks(pool, use_numpy)

        # Run until all top level tasks are complete:
        while futures_dct:
            future = next(as_completed(futures_dct))
            ts_task = (subtask := futures_dct.pop(future)).parent
            res = self._get_result_with_exception_handling(future, ts_task, ts_task_lookup, missing_to_raise)
            if res is None:
                continue
            elif missing_to_raise:
                # We are going to raise anyway, kill task:
                ts_task.is_done = True
                continue

            # We may dynamically split subtasks based on what % of time range was returned:
            if new_subtasks := subtask.store_partial_result(res):
                self._queue_new_subtasks(pool, futures_dct, new_subtasks)
            if ts_task.is_done:
                # Reduce peak memory consumption by finalizing as soon as tasks finish:
                ts_task.finalize_datapoints()
                continue
            elif subtask.is_done:
                continue
            # Put the subtask back into the pool:
            self._queue_new_subtasks(pool, futures_dct, [subtask])

        self._raise_if_missing(missing_to_raise)

        # Return only non-missing time series tasks in correct order given by `all_queries`:
        return filter(None, map(ts_task_lookup.get, self.all_queries))

    def _create_initial_tasks(
        self,
        pool: ThreadPoolExecutor,
        use_numpy: bool,
    ) -> tuple[dict[Future, BaseDpsFetchSubtask], dict[_SingleTSQueryBase, BaseTaskOrchestrator]]:
        futures_dct: dict[Future, BaseDpsFetchSubtask] = {}
        ts_task_lookup = {}
        for query in self.all_queries:
            ts_task = ts_task_lookup[query] = query.task_orchestrator(query=query, eager_mode=True, use_numpy=use_numpy)
            for subtask in ts_task.split_into_subtasks(self.max_workers, self.n_queries):
                payload = DatapointsPayload(items=[subtask.get_next_payload_item()], ignoreUnknownIds=False)
                future = pool.submit(self._request_datapoints, payload)
                futures_dct[future] = subtask
        return futures_dct, ts_task_lookup

    def _queue_new_subtasks(
        self,
        pool: ThreadPoolExecutor,
        futures_dct: dict[Future, BaseDpsFetchSubtask],
        new_subtasks: Sequence[BaseDpsFetchSubtask],
    ) -> None:
        for subtask in new_subtasks:
            payload = DatapointsPayload(items=[subtask.get_next_payload_item()])
            future = pool.submit(self._request_datapoints, payload)
            futures_dct[future] = subtask

    def _get_result_with_exception_handling(
        self,
        future: Future,
        ts_task: BaseTaskOrchestrator,
        ts_task_lookup: dict[_SingleTSQueryBase, BaseTaskOrchestrator],
        missing_to_raise: set[_SingleTSQueryBase],
    ) -> DataPointListItem | None:
        try:
            return future.result()[0]
        except CogniteAPIError as err:
            # If the error is not "missing ts", we immediately reraise:
            if not err.missing or err.code != 400:
                raise
            # The query decides if we can ignore it. If not, we store it so that we later can
            # raise one exception with -all- missing-non-ignorable time series:
            if not ts_task.query.ignore_unknown_ids:
                missing_to_raise.add(ts_task.query)

            ts_task.is_done = True
            ts_task_lookup.pop(ts_task.query, None)
            return None


class ChunkingDpsFetcher(DpsFetchStrategy):
    """A datapoints fetching strategy to make large queries faster through the grouping of more than one
    time series per request.

    The main underlying assumption is that "the more time series are queried, the lower the average density".

    Is used when the number of time series to fetch is larger than the number of `max_workers`. How many
    time series are chunked per request is dynamic and is decided by the overall number to fetch, their
    individual number of datapoints and whether raw- or aggregate datapoints are asked for since
    they are independent in requests - as long as the total number of time series does not exceed `_FETCH_TS_LIMIT`.
    """

    def __init__(self, *args: Any) -> None:
        super().__init__(*args)
        self._counter = itertools.count().__next__
        # To chunk efficiently, we have subtask pools (heap queues) that we use to prioritise subtasks
        # when building/combining subtasks into a full query:
        self.raw_subtask_pool: list[PoolSubtaskType] = []
        self.agg_subtask_pool: list[PoolSubtaskType] = []
        self.subtask_pools = (self.agg_subtask_pool, self.raw_subtask_pool)

    def _fetch_all(self, pool: ThreadPoolExecutor, use_numpy: bool) -> Iterator[BaseTaskOrchestrator]:
        # The initial tasks are important - as they tell us which time series are missing, which
        # are string, which are sparse... We use this info when we choose the best fetch-strategy.
        ts_task_lookup, missing_to_raise = {}, set()
        initial_query_limits, initial_futures_dct = self._create_initial_tasks(pool)

        for future in as_completed(initial_futures_dct):
            res_lst = future.result()
            new_ts_tasks, chunk_missing = self._create_ts_tasks_and_handle_missing(
                res_lst, initial_futures_dct.pop(future), initial_query_limits, use_numpy
            )
            missing_to_raise.update(chunk_missing)
            ts_task_lookup.update(new_ts_tasks)

        self._raise_if_missing(missing_to_raise)

        if ts_tasks_left := self._update_queries_with_new_chunking_limit(ts_task_lookup):
            self._add_to_subtask_pools(
                chain.from_iterable(
                    task.split_into_subtasks(max_workers=self.max_workers, n_tot_queries=len(ts_tasks_left))
                    for task in ts_tasks_left
                )
            )
            futures_dct: dict[Future, list[BaseDpsFetchSubtask]] = {}
            self._queue_new_subtasks(pool, futures_dct)
            self._fetch_until_complete(pool, futures_dct, ts_task_lookup)

        # Return only non-missing time series tasks in correct order given by `all_queries`:
        return filter(None, map(ts_task_lookup.get, self.all_queries))

    def _fetch_until_complete(
        self,
        pool: ThreadPoolExecutor,
        futures_dct: dict[Future, list[BaseDpsFetchSubtask]],
        ts_task_lookup: dict[_SingleTSQueryBase, BaseTaskOrchestrator],
    ) -> None:
        while futures_dct:
            future = next(as_completed(futures_dct))
            res_lst, subtask_lst = future.result(), futures_dct.pop(future)
            for subtask, res in zip(subtask_lst, res_lst):
                # We may dynamically split subtasks based on what % of time range was returned:
                if new_subtasks := subtask.store_partial_result(res):
                    self._add_to_subtask_pools(new_subtasks)
                if not subtask.is_done:
                    self._add_to_subtask_pools([subtask])
            # Check each ts task in current batch once if finished:
            for ts_task in {sub.parent for sub in subtask_lst}:
                if ts_task.is_done:
                    ts_task.finalize_datapoints()
            self._queue_new_subtasks(pool, futures_dct)

    def _create_initial_tasks(
        self, pool: ThreadPoolExecutor
    ) -> tuple[dict[_SingleTSQueryBase, int], dict[Future, tuple[_TSQueryList, _TSQueryList]]]:
        initial_query_limits: dict[_SingleTSQueryBase, int] = {}
        initial_futures_dct: dict[Future, tuple[_TSQueryList, _TSQueryList]] = {}
        # Optimal queries uses the entire worker pool. We may be forced to use more (queue) when we
        # can't fit all individual time series (maxes out at `_FETCH_TS_LIMIT * max_workers`):
        n_queries = max(self.max_workers, math.ceil(self.n_queries / self.dps_client._FETCH_TS_LIMIT))
        splitter: Callable[[list[_T]], Iterator[list[_T]]] = functools.partial(split_into_n_parts, n=n_queries)
        for query_chunks in zip(splitter(self.agg_queries), splitter(self.raw_queries)):
            # Agg and raw limits are independent in the query, so we max out on both:
            items = []
            for queries, max_lim in zip(query_chunks, (self.dps_client._DPS_LIMIT_AGG, self.dps_client._DPS_LIMIT_RAW)):
                maxed_limits = self._find_initial_query_limits([q.capped_limit for q in queries], max_lim)
                initial_query_limits.update(chunk_query_limits := dict(zip(queries, maxed_limits)))
                for query, limit in chunk_query_limits.items():
                    (item := query.to_payload_item())["limit"] = limit
                    items.append(item)

            payload = DatapointsPayload(items=items, ignoreUnknownIds=True)
            future = pool.submit(self._request_datapoints, payload)
            initial_futures_dct[future] = query_chunks
        return initial_query_limits, initial_futures_dct

    def _create_ts_tasks_and_handle_missing(
        self,
        res: Sequence[DataPointListItem],
        chunk_queues: tuple[_TSQueryList, _TSQueryList],
        initial_query_limits: dict[_SingleTSQueryBase, int],
        use_numpy: bool,
    ) -> tuple[dict[_SingleTSQueryBase, BaseTaskOrchestrator], set[_SingleTSQueryBase]]:
        if len(res) == sum(map(len, chunk_queues)):
            to_raise: set[_SingleTSQueryBase] = set()
        else:
            # We have at least 1 missing time series:
            chunk_queues, to_raise = self._handle_missing_ts(res, *chunk_queues)

        # Align initial res with corresponding queries and create tasks:
        ts_tasks = {
            query: query.task_orchestrator(
                query=query,
                eager_mode=False,
                use_numpy=use_numpy,
                first_dps_batch=res,
                first_limit=initial_query_limits[query],
            )
            for res, query in zip(res, chain(*chunk_queues))
        }
        return ts_tasks, to_raise

    def _add_to_subtask_pools(self, new_subtasks: Iterable[BaseDpsFetchSubtask]) -> None:
        for task in new_subtasks:
            # We leverage how tuples are compared to prioritise items. First `payload limit` (to easily group
            # smaller queries), then counter to always break ties, but keep order (never use tasks themselves):
            limit = min(task.parent.get_remaining_limit(), task.max_query_limit)
            new_subtask: PoolSubtaskType = (limit, self._counter(), task)
            heapq.heappush(self.subtask_pools[task.is_raw_query], new_subtask)

    def _queue_new_subtasks(
        self,
        pool: ThreadPoolExecutor,
        futures_dct: dict[Future, list[BaseDpsFetchSubtask]],
    ) -> None:
        while pool._work_queue.empty() and any(self.subtask_pools):
            # While the number of unstarted tasks is 0 and we have unqueued subtasks in one of the pools,
            # we keep combining subtasks into "chunked dps requests" to feed to the thread pool
            payload, subtask_lst = self._combine_subtasks_into_new_request()
            future = pool.submit(self._request_datapoints, payload)
            futures_dct[future] = subtask_lst
            # Yield thread control (or qsize will increase despite idle workers):
            time.sleep(0.0001)

    def _combine_subtasks_into_new_request(
        self,
    ) -> tuple[DatapointsPayload, list[BaseDpsFetchSubtask]]:
        next_items: list[DatapointsPayloadItem] = []
        next_subtasks: list[BaseDpsFetchSubtask] = []
        fetch_limits = (self.dps_client._DPS_LIMIT_AGG, self.dps_client._DPS_LIMIT_RAW)
        agg_pool, raw_pool = self.subtask_pools
        for task_pool, request_max_limit, is_raw in zip(self.subtask_pools, fetch_limits, (False, True)):
            if not task_pool:
                continue
            limit_used = 0  # Dps limit for raw and agg is independent (in the same query)
            while task_pool:
                if len(next_items) + 1 > self.dps_client._FETCH_TS_LIMIT:
                    # Hard limit on N ts, quit immediately (even if below dps limit):
                    payload = DatapointsPayload(items=next_items)
                    return payload, next_subtasks

                # Highest priority task i.e. the smallest limit, is always at index 0 (heap magic):
                *_, next_task = task_pool[0]
                next_payload_item = next_task.get_next_payload_item()
                next_limit = next_payload_item["limit"]

                if limit_used + next_limit <= request_max_limit:
                    next_items.append(next_payload_item)
                    next_subtasks.append(next_task)
                    limit_used += next_limit
                    heapq.heappop(task_pool)
                else:
                    break
        return DatapointsPayload(items=next_items), next_subtasks

    @staticmethod
    def _decide_individual_query_limit(
        query: _SingleTSQueryBase, ts_task: BaseTaskOrchestrator, n_ts_limit: int
    ) -> int:
        # For a better estimate, we use first ts of first batch instead of `query.start`:
        batch_start, batch_end = ts_task.start_ts_first_batch, ts_task.end_ts_first_batch
        est_remaining_dps = ts_task.n_dps_first_batch * (query.end - batch_end) / (batch_end - batch_start)
        # To use the full request limit on a single ts, the estimate must be >> max_limit (raw/agg dependent):
        if est_remaining_dps > 5 * (max_limit := query.max_query_limit):
            return max_limit
        # To build full queries, we want dps chunk sizes that easily combines to 100 %, e.g. we don't want
        # 1/3, because 1/3 + 1/2 -> 83 % full, but 1/8 is fine as 4 x 1/8 + 1/2 = 100 %:
        for chunk_size in (1, 2, 4, 8, 16, 32):
            if est_remaining_dps > max_limit // chunk_size:
                return max_limit // (2 * chunk_size)
        return max_limit // n_ts_limit

    def _update_queries_with_new_chunking_limit(
        self, ts_task_lookup: dict[_SingleTSQueryBase, BaseTaskOrchestrator]
    ) -> list[BaseTaskOrchestrator]:
        remaining_tasks = {}
        for query, ts_task in ts_task_lookup.items():
            if ts_task.is_done:
                ts_task.finalize_datapoints()
            else:
                remaining_tasks[query] = ts_task
        tot_raw = sum(q.is_raw_query for q in remaining_tasks)
        if tot_raw <= self.max_workers >= len(remaining_tasks) - tot_raw:
            # Number of raw and agg tasks independently <= max_workers, so we're basically doing "eager fetching",
            # but it's worth noting that we'll still chunk 1 raw + 1 agg per query (if they exist).
            return list(remaining_tasks.values())

        # Many tasks left, decide how we'll chunk'em by estimating which are dense (and need little to
        # no chunking), and which are not (...and may be grouped - and how "tightly"):
        for query, ts_task in remaining_tasks.items():
            est_limit = self._decide_individual_query_limit(query, ts_task, self.dps_client._FETCH_TS_LIMIT)
            query.override_max_query_limit(est_limit)

        return list(remaining_tasks.values())

    @staticmethod
    def _find_initial_query_limits(limits: list[int], max_limit: int) -> list[int]:
        actual_lims = [0] * len(limits)
        not_done = set(range(len(limits)))
        while not_done:
            part = max_limit // len(not_done)
            if not part:
                # We still might not have not reached max_limit, but we can no longer distribute evenly
                break
            rm_idx = set()
            for i in not_done:
                i_part = min(part, limits[i])  # A query of limit=10 does not need more of max_limit than 10
                actual_lims[i] += i_part
                max_limit -= i_part
                if i_part == limits[i]:
                    rm_idx.add(i)
                else:
                    limits[i] -= i_part
            not_done -= rm_idx
        return actual_lims

    @staticmethod
    def _handle_missing_ts(
        res: Sequence[DataPointListItem],
        agg_queries: _TSQueryList,
        raw_queries: _TSQueryList,
    ) -> tuple[tuple[_TSQueryList, _TSQueryList], set[_SingleTSQueryBase]]:
        to_raise = set()
        not_missing = {("id", r.id) for r in res}.union(("externalId", r.externalId) for r in res)
        for query in chain(agg_queries, raw_queries):
            query.is_missing = query.identifier.as_tuple() not in not_missing
            # Only raise for those time series that can't be missing (individually customisable parameter):
            if query.is_missing and not query.ignore_unknown_ids:
                to_raise.add(query)
        agg_queries = [q for q in agg_queries if not q.is_missing]
        raw_queries = [q for q in raw_queries if not q.is_missing]
        return (agg_queries, raw_queries), to_raise


class DatapointsAPI(APIClient):
    _RESOURCE_PATH = "/timeseries/data"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.synthetic = SyntheticDatapointsAPI(config, api_version, cognite_client)
        self._FETCH_TS_LIMIT = 100
        self._DPS_LIMIT_AGG = 10_000
        self._DPS_LIMIT_RAW = 100_000
        self._DPS_INSERT_LIMIT = 100_000
        self._RETRIEVE_LATEST_LIMIT = 100
        self._POST_DPS_OBJECTS_LIMIT = 10_000
        self._GRANULARITY_HOURS_LIMIT = 100_000
        self._unit_warning = FeaturePreviewWarning("beta", "alpha", feature_name="Datapoints Target Unit")

    def retrieve(
        self,
        *,
        id: None | int | dict[str, Any] | Sequence[int | dict[str, Any]] = None,
        external_id: None | str | dict[str, Any] | Sequence[str | dict[str, Any]] = None,
        start: int | str | datetime | None = None,
        end: int | str | datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
    ) -> Datapoints | DatapointsList | None:
        """`Retrieve datapoints for one or more time series. <https://developer.cognite.com/api#tag/Time-series/operation/getMultiTimeSeriesDatapoints>`_

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. For best speed, and significantly lower memory usage, consider using ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            2. Only unlimited queries with (``limit=None``) are fetched in parallel so specifying a large finite ``limit`` like 1 million, comes with severe performance penalty as data is fetched serially.
            3. Try to avoid specifying `start` and `end` to be very far from the actual data: If you have data from 2000 to 2015, don't set start=0 (1970).

        Args:
            id (None | int | dict[str, Any] | Sequence[int | dict[str, Any]]): Id, dict (with id) or (mixed) sequence of these. See examples below.
            external_id (None | str | dict[str, Any] | Sequence[str | dict[str, Any]]): External id, dict (with external id) or (mixed) sequence of these. See examples below.
            start (int | str | datetime | None): Inclusive start. Default: 1970-01-01 UTC.
            end (int | str | datetime | None): Exclusive end. Default: "now"
            aggregates (Aggregate | str | list[Aggregate | str] | None): Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity (str | None): The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            target_unit (str | None): The unit_external_id of the data points returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system (str | None): The unit system of the data points returned. Cannot be used with target_unit.
            limit (int | None): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether to include outside points. Not allowed when fetching aggregates. Default: False
            ignore_unknown_ids (bool): Whether to ignore missing time series rather than raising an exception. Default: False

        Returns:
            Datapoints | DatapointsList | None: A ``Datapoints`` object containing the requested data, or a ``DatapointsList`` if multiple time series were asked for (the ordering is ids first, then external_ids). If `ignore_unknown_ids` is `True`, a single time series is requested and it is not found, the function will return `None`.

        Examples:

            You can specify the identifiers of the datapoints you wish to retrieve in a number of ways. In this example
            we are using the time-ago format to get raw data for the time series with id=42 from 2 weeks ago up until now::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> dps = client.time_series.data.retrieve(id=42, start="2w-ago")

            You can also get aggregated values, such as `max` or `average`. You may also fetch more than one time series simultaneously. Here we are
            getting daily averages and maximum values for all of 2018, for two different time series, where we're specifying `start` and `end` as integers
            (milliseconds after epoch). Note that we are fetching them using their external ids::

                >>> dps_lst = client.time_series.data.retrieve(
                ...    external_id=["foo", "bar"],
                ...    start=1514764800000,
                ...    end=1546300800000,
                ...    aggregates=["max", "average"],
                ...    granularity="1d")

            In the two code examples above, we have a `dps` object (an instance of ``Datapoints``), and a `dps_lst` object (an instance of ``DatapointsList``).
            On `dps`, which in this case contains raw datapoints, you may access the underlying data directly by using the `.value` attribute. This works for
            both numeric and string (raw) datapoints, but not aggregates - they must be accessed by their respective names, because you're allowed to fetch up
            to 10 aggregates simultaneously, and they are stored on the same object::

                >>> raw_data = dps.value
                >>> first_dps = dps_lst[0]  # optionally: `dps_lst.get(external_id="foo")`
                >>> avg_data = first_dps.average
                >>> max_data = first_dps.max

            You may also slice a ``Datapoints`` object (you get ``Datapoints`` back), or ask for "a row of data" at a single index in same way you would do with a
            built-in `list` (you get a `Datapoint` object back, note the singular name). You'll also get `Datapoint` objects when iterating through a ``Datapoints``
            object, but this should generally be avoided (consider this a performance warning)::

                >>> dps_slice = dps[-10:]  # Last ten values
                >>> dp = dps[3]  # The third value
                >>> for dp in dps_slice:
                ...     pass  # do something!

            All parameters can be individually set if you pass (one or more) dictionaries (even `ignore_unknown_ids`, contrary to the API).
            If you also pass top-level parameters, these will be overruled by the individual parameters (where both exist). You are free to
            mix any kind of ids and external ids: Single identifiers, single dictionaries and (mixed) lists of these.

            Let's say you want different aggregates and end-times for a few time series (when only fetching a single aggregate, you may pass
            the string directly for convenience)::

                >>> dps_lst = client.time_series.data.retrieve(
                ...     id=[
                ...         {"id": 42, "end": "1d-ago", "aggregates": "average"},
                ...         {"id": 69, "end": "2d-ago", "aggregates": ["average"]},
                ...         {"id": 96, "end": "3d-ago", "aggregates": ["min", "max", "count"]},
                ...     ],
                ...     external_id={"external_id": "foo", "aggregates": "max"},
                ...     start="5d-ago",
                ...     granularity="1h")

            When requesting multiple time series, an easy way to get the datapoints of a specific one is to use the `.get` method
            on the returned ``DatapointsList`` object, then specify if you want `id` or `external_id`. Note: If you fetch a time series
            by using `id`, you can still access it with its `external_id` (and the opposite way around), if you know it::

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
            same order, similar to how slicing works with non-unique indices on Pandas DataFrames::

                >>> periods = client.events.list(type="alarm", subtype="pressure")
                >>> sensor_xid = "foo-pressure-bar"
                >>> dps_lst = client.time_series.data.retrieve(
                ...     id=[42, 43, 44],
                ...     external_id=[
                ...         {"external_id": sensor_xid, "start": ev.start_time, "end": ev.end_time}
                ...         for ev in periods
                ...     ])
                >>> ts_44 = dps_lst.get(id=44)  # Single ``Datapoints`` object
                >>> ts_lst = dps_lst.get(external_id=sensor_xid)  # List of ``len(periods)`` ``Datapoints`` objects

            The API has an endpoint to "retrieve latest (before)", but not "after". Luckily, we can emulate that behaviour easily.
            Let's say we have a very dense time series and do not want to fetch all of the available raw data (or fetch less precise
            aggregate data), just to get the very first datapoint of every month (from e.g. the year 2000 through 2010)::

                >>> import itertools
                >>> month_starts = [
                ...     datetime(year, month, 1, tzinfo=utc)
                ...     for year, month in itertools.product(range(2000, 2011), range(1, 13))]
                >>> dps_lst = client.time_series.data.retrieve(
                ...     external_id=[{"external_id": "foo", "start": start} for start in month_starts],
                ...     limit=1)

            To get *all* historic and future datapoints for a time series, e.g. to do a backup, you may want to import the two integer
            constants: `MIN_TIMESTAMP_MS` and `MAX_TIMESTAMP_MS`, to make sure you do not miss any. Performance warning: This pattern of
            fetching datapoints from the entire valid time domain is slower and shouldn't be used for regular "day-to-day" queries::

                >>> from cognite.client.utils import MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS
                >>> dps_backup = client.time_series.data.retrieve(
                ...     id=123,
                ...     start=MIN_TIMESTAMP_MS,
                ...     end=MAX_TIMESTAMP_MS + 1)  # end is exclusive

            Another example here is just to showcase the great flexibility of the `retrieve` endpoint, with a very custom query::

                >>> ts1 = 1337
                >>> ts2 = {
                ...     "id": 42,
                ...     "start": -12345,  # Overrides `start` arg. below
                ...     "end": "1h-ago",
                ...     "limit": 1000,  # Overrides `limit` arg. below
                ...     "include_outside_points": True,
                ... }
                >>> ts3 = {
                ...     "id": 11,
                ...     "end": "1h-ago",
                ...     "aggregates": ["max"],
                ...     "granularity": "42h",
                ...     "include_outside_points": False,
                ...     "ignore_unknown_ids": True,  # Overrides `ignore_unknown_ids` arg. below
                ... }
                >>> dps_lst = client.time_series.data.retrieve(
                ...    id=[ts1, ts2, ts3], start="2w-ago", limit=None, ignore_unknown_ids=False)

            If we have created a timeseries set with 'unit_external_id' we can use the 'target_unit' parameter to convert the datapoints to the desired unit.
            In the example below, we assume that the timeseries is set with unit_external_id 'temperature:deg_c' and id='42'.

                >>> client.time_series.data.retrieve(
                ...   id=42, start="2w-ago", limit=None, target_unit="temperature:deg_f")

            Or alternatively, we can use the 'target_unit_system' parameter to convert the datapoints to the desired unit system.

                >>> client.time_series.data.retrieve(
                ...   id=42, start="2w-ago", limit=None, target_unit_system="Imperial")

        """
        query = _DatapointsQuery(
            start=start,
            end=end,
            id=id,
            external_id=external_id,
            aggregates=aggregates,
            granularity=granularity,
            target_unit=target_unit,
            target_unit_system=target_unit_system,
            limit=limit,
            include_outside_points=include_outside_points,
            ignore_unknown_ids=ignore_unknown_ids,
        )
        fetcher = select_dps_fetch_strategy(self, user_query=query)

        dps_lst = fetcher.fetch_all_datapoints()
        if not query.is_single_identifier:
            return dps_lst
        elif not dps_lst and ignore_unknown_ids:
            return None
        return dps_lst[0]

    def retrieve_arrays(
        self,
        *,
        id: None | int | dict[str, Any] | Sequence[int | dict[str, Any]] = None,
        external_id: None | str | dict[str, Any] | Sequence[str | dict[str, Any]] = None,
        start: int | str | datetime | None = None,
        end: int | str | datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
    ) -> DatapointsArray | DatapointsArrayList | None:
        """`Retrieve datapoints for one or more time series. <https://developer.cognite.com/api#tag/Time-series/operation/getMultiTimeSeriesDatapoints>`_

        **Note**: This method requires ``numpy`` to be installed.

        Args:
            id (None | int | dict[str, Any] | Sequence[int | dict[str, Any]]): Id, dict (with id) or (mixed) sequence of these. See examples below.
            external_id (None | str | dict[str, Any] | Sequence[str | dict[str, Any]]): External id, dict (with external id) or (mixed) sequence of these. See examples below.
            start (int | str | datetime | None): Inclusive start. Default: 1970-01-01 UTC.
            end (int | str | datetime | None): Exclusive end. Default: "now"
            aggregates (Aggregate | str | list[Aggregate | str] | None): Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity (str | None): The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            target_unit (str | None): The unit_external_id of the data points returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system (str | None): The unit system of the data points returned. Cannot be used with target_unit.
            limit (int | None): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether to include outside points. Not allowed when fetching aggregates. Default: False
            ignore_unknown_ids (bool): Whether to ignore missing time series rather than raising an exception. Default: False

        Returns:
            DatapointsArray | DatapointsArrayList | None: A ``DatapointsArray`` object containing the requested data, or a ``DatapointsArrayList`` if multiple time series were asked for (the ordering is ids first, then external_ids). If `ignore_unknown_ids` is `True`, a single time series is requested and it is not found, the function will return `None`.

        Examples:

            **Note:** For many more usage examples, check out the :py:meth:`~DatapointsAPI.retrieve` method which accepts exactly the same arguments.

            Get weekly ``min`` and ``max`` aggregates for a time series with id=42 since the year 2000, then compute the range of values:

                >>> from cognite.client import CogniteClient
                >>> from datetime import datetime, timezone
                >>> client = CogniteClient()
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
        local_import("numpy")  # Verify that numpy is available or raise CogniteImportError
        query = _DatapointsQuery(
            start=start,
            end=end,
            id=id,
            external_id=external_id,
            aggregates=aggregates,
            granularity=granularity,
            target_unit=target_unit,
            target_unit_system=target_unit_system,
            limit=limit,
            include_outside_points=include_outside_points,
            ignore_unknown_ids=ignore_unknown_ids,
        )
        fetcher = select_dps_fetch_strategy(self, user_query=query)

        dps_lst = fetcher.fetch_all_datapoints_numpy()
        if not query.is_single_identifier:
            return dps_lst
        elif not dps_lst and ignore_unknown_ids:
            return None
        return dps_lst[0]

    def retrieve_dataframe(
        self,
        *,
        id: None | int | dict[str, Any] | Sequence[int | dict[str, Any]] = None,
        external_id: None | str | dict[str, Any] | Sequence[str | dict[str, Any]] = None,
        start: int | str | datetime | None = None,
        end: int | str | datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: Literal["id", "external_id"] = "external_id",
    ) -> pd.DataFrame:
        """Get datapoints directly in a pandas dataframe.

        **Note**: If you have duplicated time series in your query, the dataframe columns will also contain duplicates.

        Args:
            id (None | int | dict[str, Any] | Sequence[int | dict[str, Any]]): Id, dict (with id) or (mixed) sequence of these. See examples below.
            external_id (None | str | dict[str, Any] | Sequence[str | dict[str, Any]]): External id, dict (with external id) or (mixed) sequence of these. See examples below.
            start (int | str | datetime | None): Inclusive start. Default: 1970-01-01 UTC.
            end (int | str | datetime | None): Exclusive end. Default: "now"
            aggregates (Aggregate | str | list[Aggregate | str] | None): Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity (str | None): The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            target_unit (str | None): The unit_external_id of the data points returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system (str | None): The unit system of the data points returned. Cannot be used with target_unit.
            limit (int | None): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether to include outside points. Not allowed when fetching aggregates. Default: False
            ignore_unknown_ids (bool): Whether to ignore missing time series rather than raising an exception. Default: False
            uniform_index (bool): If only querying aggregates AND a single granularity is used AND no limit is used, specifying `uniform_index=True` will return a dataframe with an equidistant datetime index from the earliest `start` to the latest `end` (missing values will be NaNs). If these requirements are not met, a ValueError is raised. Default: False
            include_aggregate_name (bool): Include 'aggregate' in the column name, e.g. `my-ts|average`. Ignored for raw time series. Default: True
            include_granularity_name (bool): Include 'granularity' in the column name, e.g. `my-ts|12h`. Added after 'aggregate' when present. Ignored for raw time series. Default: False
            column_names (Literal["id", "external_id"]): Use either ids or external ids as column names. Time series missing external id will use id as backup. Default: "external_id"

        Returns:
            pd.DataFrame: A pandas DataFrame containing the requested time series. The ordering of columns is ids first, then external_ids. For time series with multiple aggregates, they will be sorted in alphabetical order ("average" before "max").

        Examples:

            Get a pandas dataframe using a single id, and use this id as column name, with no more than 100 datapoints:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> df = client.time_series.data.retrieve_dataframe(
                ...     id=12345,
                ...     start="2w-ago",
                ...     end="now",
                ...     limit=100,
                ...     column_names="id")

            Get the pandas dataframe with a uniform index (fixed spacing between points) of 1 day, for two time series with
            individually specified aggregates, from 1990 through 2020:

                >>> from datetime import datetime, timezone
                >>> df = client.time_series.data.retrieve_dataframe(
                ...     id=[
                ...         {"external_id": "foo", "aggregates": ["discrete_variance"]},
                ...         {"external_id": "bar", "aggregates": ["total_variation", "continuous_variance"]},
                ...     ],
                ...     granularity="1d",
                ...     start=datetime(1990, 1, 1, tzinfo=timezone.utc),
                ...     end=datetime(2020, 12, 31, tzinfo=timezone.utc),
                ...     uniform_index=True)

            Get a pandas dataframe containing the 'average' aggregate for two time series using a 30-day granularity,
            starting Jan 1, 1970 all the way up to present, without having the aggregate name in the column names:

                >>> df = client.time_series.data.retrieve_dataframe(
                ...     external_id=["foo", "bar"],
                ...     aggregates=["average"],
                ...     granularity="30d",
                ...     include_aggregate_name=False)

            Remember that pandas.Timestamp is a subclass of datetime, so you can use Timestamps as start and
            end arguments:

                >>> import pandas as pd
                >>> df = client.time_series.data.retrieve_dataframe(
                ...     external_id="foo",
                ...     start=pd.Timestamp("2023-01-01"),
                ...     end=pd.Timestamp("2023-02-01"),
                ...     )

        """
        _, pd = local_import("numpy", "pandas")  # Verify that deps are available or raise CogniteImportError
        if column_names not in {"id", "external_id"}:
            raise ValueError(f"Given parameter {column_names=} must be one of 'id' or 'external_id'")

        query = _DatapointsQuery(
            start=start,
            end=end,
            id=id,
            external_id=external_id,
            aggregates=aggregates,
            granularity=granularity,
            target_unit=target_unit,
            target_unit_system=target_unit_system,
            limit=limit,
            include_outside_points=include_outside_points,
            ignore_unknown_ids=ignore_unknown_ids,
        )
        fetcher = select_dps_fetch_strategy(self, user_query=query)

        if not uniform_index:
            return fetcher.fetch_all_datapoints_numpy().to_pandas(
                column_names, include_aggregate_name, include_granularity_name
            )
        # Uniform index requires extra validation and processing:
        grans_given = {q.granularity for q in fetcher.all_queries}
        is_limited = any(q.limit is not None for q in fetcher.all_queries)
        if fetcher.raw_queries or len(grans_given) > 1 or is_limited:
            raise ValueError(
                "Cannot return a uniform index when asking for aggregates with multiple granularities "
                f"({grans_given}) OR when (partly) querying raw datapoints OR when a finite limit is used."
            )

        df = fetcher.fetch_all_datapoints_numpy().to_pandas(
            column_names, include_aggregate_name, include_granularity_name
        )
        start = pd.Timestamp(min(q.start for q in fetcher.agg_queries), unit="ms")
        end = pd.Timestamp(max(q.end for q in fetcher.agg_queries), unit="ms")
        (granularity,) = grans_given
        # Pandas understand "Cognite granularities" except `m` (minutes) which we must translate:
        freq = cast(str, granularity).replace("m", "T")
        return df.reindex(pd.date_range(start=start, end=end, freq=freq, inclusive="left"))

    def retrieve_dataframe_in_tz(
        self,
        *,
        id: int | Sequence[int] | None = None,
        external_id: str | Sequence[str] | None = None,
        start: datetime,
        end: datetime,
        aggregates: Aggregate | str | Sequence[Aggregate | str] | None = None,
        granularity: str | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        ignore_unknown_ids: bool = False,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: Literal["id", "external_id"] = "external_id",
    ) -> pd.DataFrame:
        """Get datapoints directly in a pandas dataframe in the same time zone as start and end.

        Note:
            This is a convenience method. It builds on top of the methods ``retrieve_arrays`` and ``retrieve_dataframe``.
            It enables you to get correct aggregates in your local time zone with daily, weekly, monthly, quarterly, and yearly
            aggregates with automatic handling for daylight saving time (DST) transitions. If your time zone observes DST,
            and your query crosses at least one DST-boundary, granularities like "3 days" or "1 week", that used to represent
            fixed durations, no longer do so. To understand why, let's illustrate with an example: A typical time zone
            (above the equator) that observes DST will skip one hour ahead during spring, leading to a day that is only
            23 hours long, and oppositely in the fall, turning back the clock one hour, yielding a 25-hour-long day.

        In short, this method works as follows:
            1. Get the time zone from start and end (must be equal).
            2. Split the time range from start to end into intervals based on DST boundaries.
            3. Create a query for each interval and pass all to the retrieve_arrays method.
            4. Stack the resulting arrays into a single column in the resulting DataFrame.

        Warning:
            The queries to ``retrieve_arrays`` are translated to a multiple of hours. This means that time zones that
            are not a whole hour offset from UTC are not supported (yet). The same is true for time zones that observe
            DST with an offset from standard time that is not a multiple of 1 hour.

        Args:
            id (int | Sequence[int] | None): ID or list of IDs.
            external_id (str | Sequence[str] | None): External ID or list of External IDs.
            start (datetime): Inclusive start, must be time zone aware.
            end (datetime): Exclusive end, must be time zone aware and have the same time zone as start.
            aggregates (Aggregate | str | Sequence[Aggregate | str] | None): Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity (str | None): The granularity to fetch aggregates at, supported are: second, minute, hour, day, week, month, quarter and year. Default: None.
            target_unit (str | None): The unit_external_id of the data points returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system (str | None): The unit system of the data points returned. Cannot be used with target_unit.
            ignore_unknown_ids (bool): Whether to ignore missing time series rather than raising an exception. Default: False
            uniform_index (bool): If querying aggregates, specifying `uniform_index=True` will return a dataframe with an index with constant spacing between timestamps decided by granularity all the way from `start` to `end` (missing values will be NaNs). Default: False
            include_aggregate_name (bool): Include 'aggregate' in the column name, e.g. `my-ts|average`. Ignored for raw time series. Default: True
            include_granularity_name (bool): Include 'granularity' in the column name, e.g. `my-ts|12h`. Added after 'aggregate' when present. Ignored for raw time series. Default: False
            column_names (Literal["id", "external_id"]): Use either ids or external ids as column names. Time series missing external id will use id as backup. Default: "external_id"

        Returns:
            pd.DataFrame: A pandas DataFrame containing the requested time series with a DatetimeIndex localized in the given time zone.

        Examples:

            Get a pandas dataframe in the time zone of Oslo, Norway:

                >>> from cognite.client import CogniteClient
                >>> # In Python >=3.9 you may import directly from `zoneinfo`
                >>> from cognite.client.utils import ZoneInfo
                >>> client = CogniteClient()
                >>> df = client.time_series.data.retrieve_dataframe_in_tz(
                ...     id=12345,
                ...     start=datetime(2023, 1, 1, tzinfo=ZoneInfo("Europe/Oslo")),
                ...     end=datetime(2023, 2, 1, tzinfo=ZoneInfo("Europe/Oslo")),
                ...     aggregates="average",
                ...     granularity="1week",
                ...     column_names="id")

            Get a pandas dataframe with the sum and continuous variance of the time series with external id "foo" and "bar",
            for each quarter from 2020 to 2022 returned in the time zone of Oslo, Norway:

                >>> from cognite.client import CogniteClient
                >>> # In Python >=3.9 you may import directly from `zoneinfo`
                >>> from cognite.client.utils import ZoneInfo
                >>> client = CogniteClient()
                >>> df = client.time_series.data.retrieve_dataframe(
                ...     external_id=["foo", "bar"],
                ...     aggregates=["sum", "continuous_variance"],
                ...     granularity="1quarter",
                ...     start=datetime(2020, 1, 1, tzinfo=ZoneInfo("Europe/Oslo")),
                ...     end=datetime(2022, 12, 31, tzinfo=ZoneInfo("Europe/Oslo")))

        Tip:
            You can also use shorter granularities such as second(s), minute(s), hour(s), which do not require
            any special handling of DST. The longer granularities at your disposal, which are adjusted for DST, are:
            day(s), week(s), month(s), quarter(s) and year(s). All the granularities support a one-letter version
            ``s``, ``m``, ``h``, ``d``, ``w``, ``q``, and ``y``, except for month, to avoid confusion with minutes.
            Furthermore, the granularity is expected to be given as a lowercase.
        """
        _, pd = local_import("numpy", "pandas")  # Verify that deps are available or raise CogniteImportError

        if not exactly_one_is_not_none(id, external_id):
            raise ValueError("Either input id(s) or external_id(s)")

        if exactly_one_is_not_none(aggregates, granularity):
            raise ValueError(
                "Got only one of 'aggregates' and 'granularity'. "
                "Pass both to get aggregates, or neither to get raw data"
            )

        tz = validate_timezone(start, end)
        if aggregates is None and granularity is None:
            # Raw Data only need to convert the timezone
            return (
                self.retrieve_dataframe(
                    id=id,
                    external_id=external_id,
                    start=start,
                    end=end,
                    aggregates=aggregates,
                    granularity=granularity,
                    target_unit=target_unit,
                    target_unit_system=target_unit_system,
                    ignore_unknown_ids=ignore_unknown_ids,
                    uniform_index=uniform_index,
                    include_aggregate_name=include_aggregate_name,
                    include_granularity_name=include_granularity_name,
                    column_names=column_names,
                    limit=None,
                )
                .tz_localize("utc")
                .tz_convert(str(tz))
            )

        assert isinstance(granularity, str)  # mypy

        if in_timedelta(granularity) / timedelta(hours=1) > self._GRANULARITY_HOURS_LIMIT:
            multiplier, unit = get_granularity_multiplier_and_unit(granularity)
            days = _unit_in_days(unit)
            limit = math.floor(timedelta(hours=self._GRANULARITY_HOURS_LIMIT) / timedelta(days=days))
            raise ValueError(f"Granularity above the maximum limit, {limit} {unit}s.")

        identifiers = IdentifierSequence.load(id, external_id)
        if not identifiers.are_unique():
            duplicated = find_duplicates(identifiers.as_primitives())
            raise ValueError(f"The following identifiers were not unique: {duplicated}")

        intervals = to_fixed_utc_intervals(start, end, granularity)

        queries = [
            {**ident_dct, "aggregates": aggregates, **interval}
            for ident_dct, interval in itertools.product(identifiers.as_dicts(), intervals)
        ]

        arrays = self.retrieve_arrays(
            limit=None,
            ignore_unknown_ids=ignore_unknown_ids,
            target_unit=target_unit,
            target_unit_system=target_unit_system,
            **{identifiers[0].name(): queries},  # type: ignore [arg-type]
        )
        assert isinstance(arrays, DatapointsArrayList)  # mypy
        arrays.concat_duplicate_ids()
        df = (
            arrays.to_pandas(column_names, include_aggregate_name, include_granularity_name)
            .tz_localize("utc")
            .tz_convert(str(tz))
        )

        if uniform_index:
            freq = to_pandas_freq(granularity, start)
            start, end = align_large_granularity(start, end, granularity)
            return df.reindex(pandas_date_range_tz(start, end, freq, inclusive="left"))

        return df

    def retrieve_latest(
        self,
        id: int | LatestDatapointQuery | list[int | LatestDatapointQuery] | None = None,
        external_id: str | LatestDatapointQuery | list[str | LatestDatapointQuery] | None = None,
        before: None | int | str | datetime = None,
        ignore_unknown_ids: bool = False,
    ) -> Datapoints | DatapointsList | None:
        """`Get the latest datapoint for one or more time series <https://developer.cognite.com/api#tag/Time-series/operation/getLatest>`_

        Args:
            id (int | LatestDatapointQuery | list[int | LatestDatapointQuery] | None): Id or list of ids.
            external_id (str | LatestDatapointQuery | list[str | LatestDatapointQuery] | None): External id or list of external ids.
            before (None | int | str | datetime): (Union[int, str, datetime]): Get latest datapoint before this time. Not used when passing 'LatestDatapointQuery'.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            Datapoints | DatapointsList | None: A Datapoints object containing the requested data, or a DatapointsList if multiple were requested. If `ignore_unknown_ids` is `True`, a single time series is requested and it is not found, the function will return `None`.

        Examples:

            Getting the latest datapoint in a time series. This method returns a Datapoints object, so the datapoint will
            be the first element::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.data.retrieve_latest(id=1)[0]

            You can also get the first datapoint before a specific time::

                >>> res = c.time_series.data.retrieve_latest(id=1, before="2d-ago")[0]

            You may also pass an instance of LatestDatapointQuery:

                >>> from cognite.client.data_classes import LatestDatapointQuery
                >>> res = c.time_series.data.retrieve_latest(id=LatestDatapointQuery(id=1, before=60_000))[0]

            If you need the latest datapoint for multiple time series, simply give a list of ids. Note that we are
            using external ids here, but either will work::

                >>> res = c.time_series.data.retrieve_latest(external_id=["abc", "def"])
                >>> latest_abc = res[0][0]
                >>> latest_def = res[1][0]

            If you need to specify a different value of 'before' for each time series, you may pass several
            LatestDatapointQuery objects::

                >>> from datetime import datetime, timezone
                >>> id_queries = [
                ...     123,
                ...     LatestDatapointQuery(id=456, before="1w-ago"),
                ...     LatestDatapointQuery(id=789, before=datetime(2018,1,1, tzinfo=timezone.utc))]
                >>> res = c.time_series.data.retrieve_latest(
                ...     id=id_queries,
                ...     external_id=LatestDatapointQuery(external_id="abc", before="3h-ago"))
        """
        fetcher = RetrieveLatestDpsFetcher(id, external_id, before, ignore_unknown_ids, self)
        res = fetcher.fetch_datapoints()
        if not fetcher.input_is_singleton:
            return DatapointsList._load(res, cognite_client=self._cognite_client)
        elif not res and ignore_unknown_ids:
            return None
        return Datapoints._load(res[0], cognite_client=self._cognite_client)

    def insert(
        self,
        datapoints: Datapoints
        | DatapointsArray
        | Sequence[dict[str, int | float | str | datetime]]
        | Sequence[tuple[int | float | datetime, int | float | str]],
        id: int | None = None,
        external_id: str | None = None,
    ) -> None:
        """Insert datapoints into a time series

        Timestamps can be represented as milliseconds since epoch or datetime objects.

        Args:
            datapoints (Datapoints | DatapointsArray | Sequence[dict[str, int | float | str | datetime]] | Sequence[tuple[int | float | datetime, int | float | str]]): The datapoints you wish to insert. Can either be a list of tuples, a list of dictionaries, a Datapoints object or a DatapointsArray object. See examples below.
            id (int | None): Id of time series to insert datapoints into.
            external_id (str | None): External id of time series to insert datapoint into.

        Examples:

            Your datapoints can be a list of tuples where the first element is the timestamp and the second element is the value::

                >>> from cognite.client import CogniteClient
                >>> from datetime import datetime, timezone
                >>> c = CogniteClient()
                >>> # With datetime objects:
                >>> datapoints = [
                ...     (datetime(2018,1,1, tzinfo=timezone.utc), 1000),
                ...     (datetime(2018,1,2, tzinfo=timezone.utc), 2000),
                ... ]
                >>> c.time_series.data.insert(datapoints, id=1)
                >>> # With ms since epoch:
                >>> datapoints = [(150000000000, 1000), (160000000000, 2000)]
                >>> c.time_series.data.insert(datapoints, id=2)

            Or they can be a list of dictionaries::

                >>> datapoints = [
                ...     {"timestamp": 150000000000, "value": 1000},
                ...     {"timestamp": 160000000000, "value": 2000},
                ... ]
                >>> c.time_series.data.insert(datapoints, external_id="abcd")

            Or they can be a Datapoints or DatapointsArray object (with raw datapoints only). Note that the id or external_id
            set on these objects are not inspected/used (as they belong to the "from-time-series", and not the "to-time-series"),
            and so you must explicitly pass the identifier of the time series you want to insert into, which in this example is
            `external_id="foo"`::

                >>> data = c.time_series.data.retrieve(external_id="abc", start="1w-ago", end="now")
                >>> c.time_series.data.insert(data, external_id="foo")
        """
        post_dps_object = Identifier.of_either(id, external_id).as_dict()
        dps_to_post: Sequence[dict[str, int | float | str | datetime]] | Sequence[
            tuple[int | float | datetime, int | float | str]
        ]
        if isinstance(datapoints, (Datapoints, DatapointsArray)):
            dps_to_post = DatapointsPoster._extract_raw_data_from_dps_container(datapoints)
        else:
            dps_to_post = datapoints

        post_dps_object["datapoints"] = dps_to_post
        dps_poster = DatapointsPoster(self)
        dps_poster.insert([post_dps_object])

    def insert_multiple(self, datapoints: list[dict[str, str | int | list | Datapoints | DatapointsArray]]) -> None:
        """`Insert datapoints into multiple time series <https://developer.cognite.com/api#tag/Time-series/operation/postMultiTimeSeriesDatapoints>`_

        Args:
            datapoints (list[dict[str, str | int | list | Datapoints | DatapointsArray]]): The datapoints you wish to insert along with the ids of the time series. See examples below.

        Examples:

            Your datapoints can be a list of dictionaries, each containing datapoints for a different (presumably) time series. These dictionaries
            must have the key "datapoints" (containing the data) specified as a ``Datapoints`` object, a ``DatapointsArray`` object, or list of either
            tuples `(timestamp, value)` or dictionaries, `{"timestamp": ts, "value": value}`::

                >>> from cognite.client import CogniteClient
                >>> from datetime import datetime, timezone
                >>> client = CogniteClient()

                >>> datapoints = []
                >>> # With datetime objects and id
                >>> datapoints.append(
                ...     {"id": 1, "datapoints": [
                ...         (datetime(2018,1,1,tzinfo=timezone.utc), 1000),
                ...         (datetime(2018,1,2,tzinfo=timezone.utc), 2000),
                ... ]})

                >>> # With ms since epoch and external_id:
                >>> datapoints.append({"external_id": "foo", "datapoints": [(150000000000, 1000), (160000000000, 2000)]})

                >>> # With raw data in a Datapoints object (or DatapointsArray):
                >>> data_to_clone = client.time_series.data.retrieve(external_id="bar")
                >>> datapoints.append({"external_id": "bar-clone", "datapoints": data_to_clone})
                >>> client.time_series.data.insert_multiple(datapoints)
        """
        for dps_dct in datapoints:
            # Extract data inplace for any Datapoints and/or DatapointsArray:
            if isinstance(dps_dct, Mapping) and isinstance(dps_dct["datapoints"], (Datapoints, DatapointsArray)):
                dps_dct["datapoints"] = DatapointsPoster._extract_raw_data_from_dps_container(dps_dct["datapoints"])
        dps_poster = DatapointsPoster(self)
        dps_poster.insert(datapoints)

    def delete_range(
        self,
        start: int | str | datetime,
        end: int | str | datetime,
        id: int | None = None,
        external_id: str | None = None,
    ) -> None:
        """Delete a range of datapoints from a time series.

        Args:
            start (int | str | datetime): Inclusive start of delete range
            end (int | str | datetime): Exclusive end of delete range
            id (int | None): Id of time series to delete data from
            external_id (str | None): External id of time series to delete data from

        Examples:

            Deleting the last week of data from a time series::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.time_series.data.delete_range(start="1w-ago", end="now", id=1)
        """
        start = timestamp_to_ms(start)
        end = timestamp_to_ms(end)
        assert end > start, "end must be larger than start"

        identifier = Identifier.of_either(id, external_id).as_dict()
        delete_dps_object = {**identifier, "inclusiveBegin": start, "exclusiveEnd": end}
        self._delete_datapoints_ranges([delete_dps_object])

    def delete_ranges(self, ranges: list[dict[str, Any]]) -> None:
        """`Delete a range of datapoints from multiple time series. <https://developer.cognite.com/api#tag/Time-series/operation/deleteDatapoints>`_

        Args:
            ranges (list[dict[str, Any]]): The list of datapoint ids along with time range to delete. See examples below.

        Examples:

            Each element in the list ranges must be specify either id or external_id, and a range::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> ranges = [{"id": 1, "start": "2d-ago", "end": "now"},
                ...           {"external_id": "abc", "start": "2d-ago", "end": "now"}]
                >>> c.time_series.data.delete_ranges(ranges)
        """
        valid_ranges = []
        for time_range in ranges:
            valid_range = validate_user_input_dict_with_identifier(time_range, required_keys={"start", "end"})
            valid_range.update(
                inclusiveBegin=timestamp_to_ms(time_range["start"]),
                exclusiveEnd=timestamp_to_ms(time_range["end"]),
            )
            valid_ranges.append(valid_range)
        self._delete_datapoints_ranges(valid_ranges)

    def _delete_datapoints_ranges(self, delete_range_objects: list[dict]) -> None:
        self._post(url_path=self._RESOURCE_PATH + "/delete", json={"items": delete_range_objects})

    def insert_dataframe(self, df: pd.DataFrame, external_id_headers: bool = True, dropna: bool = True) -> None:
        """Insert a dataframe (columns must be unique).

        The index of the dataframe must contain the timestamps (pd.DatetimeIndex). The names of the columns specify
        the ids or external ids of the time series to which the datapoints will be written.

        Said time series must already exist.

        Args:
            df (pd.DataFrame):  Pandas DataFrame object containing the time series.
            external_id_headers (bool): Interpret the column names as external id. Pass False if using ids. Default: True.
            dropna (bool): Set to True to ignore NaNs in the given DataFrame, applied per column. Default: True.

        Examples:
            Post a dataframe with white noise::

                >>> import numpy as np
                >>> import pandas as pd
                >>> from cognite.client import CogniteClient
                >>>
                >>> client = CogniteClient()
                >>> ts_xid = "my-foo-ts"
                >>> idx = pd.date_range(start="2018-01-01", periods=100, freq="1d")
                >>> noise = np.random.normal(0, 1, 100)
                >>> df = pd.DataFrame({ts_xid: noise}, index=idx)
                >>> client.time_series.data.insert_dataframe(df)
        """
        np, pd = cast(Any, local_import("numpy", "pandas"))
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
            datapoints = list(zip(idx[mask], col[mask]))
            if not datapoints:
                continue
            if external_id_headers:
                dps.append({"datapoints": datapoints, "externalId": column_id})
            else:
                dps.append({"datapoints": datapoints, "id": int(column_id)})
        self.insert_multiple(dps)


class DatapointsBin:
    def __init__(self, dps_objects_limit: int, dps_limit: int) -> None:
        self.dps_objects_limit = dps_objects_limit
        self.dps_limit = dps_limit
        self.current_num_datapoints = 0
        self.dps_object_list: list[dict] = []

    def add(self, dps_object: dict[str, Any]) -> None:
        self.current_num_datapoints += len(dps_object["datapoints"])
        self.dps_object_list.append(dps_object)

    def will_fit(self, number_of_dps: int) -> bool:
        will_fit_dps = (self.current_num_datapoints + number_of_dps) <= self.dps_limit
        will_fit_dps_objects = (len(self.dps_object_list) + 1) <= self.dps_objects_limit
        return will_fit_dps and will_fit_dps_objects


class DatapointsPoster:
    def __init__(self, dps_client: DatapointsAPI) -> None:
        self.dps_client = dps_client
        self.limit = self.dps_client._DPS_INSERT_LIMIT
        self.bins: list[DatapointsBin] = []

    def insert(self, dps_object_list: list[dict[str, Any]]) -> None:
        valid_dps_object_list = self._validate_dps_objects(dps_object_list)
        binned_dps_object_lists = self._bin_datapoints(valid_dps_object_list)
        self._insert_datapoints_concurrently(binned_dps_object_lists)

    @staticmethod
    def _extract_raw_data_from_dps_container(
        dps: Datapoints | DatapointsArray,
    ) -> list[tuple[int, str]] | list[tuple[int, float]]:
        if dps.value is None:
            raise ValueError(
                "Only raw datapoints are supported when inserting data from ``Datapoints`` or ``DatapointsArray``"
            )
        if (n_ts := len(dps.timestamp)) != (n_dps := len(dps.value)):
            raise ValueError(f"Number of timestamps ({n_ts}) does not match number of datapoints ({n_dps}) to insert")

        if isinstance(dps, Datapoints):
            return cast(List[Tuple[int, Any]], list(zip(dps.timestamp, dps.value)))
        ts = dps.timestamp.astype("datetime64[ms]").astype("int64")
        # Using `tolist()` converts to the nearest compatible built-in Python type (in C code):
        return list(zip(ts.tolist(), dps.value.tolist()))

    @staticmethod
    def _validate_dps_objects(dps_object_list: list[dict[str, Any]]) -> list[dict]:
        valid_dps_objects = []
        for dps_object in dps_object_list:
            valid_dps_object = cast(
                Dict[str, Union[int, str, List[Tuple[int, Any]]]],
                validate_user_input_dict_with_identifier(dps_object, required_keys={"datapoints"}),
            )
            valid_dps_object["datapoints"] = DatapointsPoster._validate_and_format_datapoints(dps_object["datapoints"])
            valid_dps_objects.append(valid_dps_object)
        return valid_dps_objects

    @staticmethod
    def _validate_and_format_datapoints(
        datapoints: list[dict[str, Any]] | list[tuple[int | float | datetime, int | float | str]],
    ) -> list[tuple[int, Any]]:
        assert_type(datapoints, "datapoints", [list])
        assert len(datapoints) > 0, "No datapoints provided"
        assert_type(datapoints[0], "datapoints element", [tuple, dict])

        valid_datapoints = []
        if isinstance(datapoints[0], tuple):
            valid_datapoints = [(timestamp_to_ms(t), v) for t, v in datapoints]
        elif isinstance(datapoints[0], dict):
            for dp in datapoints:
                dp = cast(Dict[str, Any], dp)
                assert "timestamp" in dp, "A datapoint is missing the 'timestamp' key"
                assert "value" in dp, "A datapoint is missing the 'value' key"
                valid_datapoints.append((timestamp_to_ms(dp["timestamp"]), dp["value"]))
        return valid_datapoints

    def _bin_datapoints(self, dps_object_list: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
        for dps_object in dps_object_list:
            for i in range(0, len(dps_object["datapoints"]), self.limit):
                dps_object_chunk = {k: dps_object[k] for k in ["id", "externalId"] if k in dps_object}
                dps_object_chunk["datapoints"] = dps_object["datapoints"][i : i + self.limit]
                for bin in self.bins:
                    if bin.will_fit(len(dps_object_chunk["datapoints"])):
                        bin.add(dps_object_chunk)
                        break
                else:
                    bin = DatapointsBin(self.limit, self.dps_client._POST_DPS_OBJECTS_LIMIT)
                    bin.add(dps_object_chunk)
                    self.bins.append(bin)
        binned_dps_object_list = []
        for bin in self.bins:
            binned_dps_object_list.append(bin.dps_object_list)
        return binned_dps_object_list

    def _insert_datapoints_concurrently(self, dps_object_lists: list[list[dict[str, Any]]]) -> None:
        tasks = [(dps_object_list,) for dps_object_list in dps_object_lists]
        summary = execute_tasks(self._insert_datapoints, tasks, max_workers=self.dps_client._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda x: x[0],
            task_list_element_unwrap_fn=lambda x: {k: x[k] for k in ["id", "externalId"] if k in x},
        )

    def _insert_datapoints(self, post_dps_objects: list[dict[str, Any]]) -> None:
        # convert to memory intensive format as late as possible and clean up after
        for it in post_dps_objects:
            it["datapoints"] = [{"timestamp": t, "value": v} for t, v in it["datapoints"]]
        self.dps_client._post(url_path=self.dps_client._RESOURCE_PATH, json={"items": post_dps_objects})
        for it in post_dps_objects:
            del it["datapoints"]


class RetrieveLatestDpsFetcher:
    def __init__(
        self,
        id: None | int | LatestDatapointQuery | list[int | LatestDatapointQuery],
        external_id: None | str | LatestDatapointQuery | list[str | LatestDatapointQuery],
        before: None | int | str | datetime,
        ignore_unknown_ids: bool,
        dps_client: DatapointsAPI,
    ) -> None:
        self.before_settings: dict[tuple[str, int], None | int | str | datetime] = {}
        self.default_before = before
        self.ignore_unknown_ids = ignore_unknown_ids
        self.dps_client = dps_client

        parsed_ids = cast(Union[None, int, Sequence[int]], self._parse_user_input(id, "id"))
        parsed_xids = cast(Union[None, str, Sequence[str]], self._parse_user_input(external_id, "external_id"))
        self._is_singleton = IdentifierSequence.load(parsed_ids, parsed_xids).is_singleton()
        self._all_identifiers = self._prepare_requests(parsed_ids, parsed_xids)

    @property
    def input_is_singleton(self) -> bool:
        return self._is_singleton

    @staticmethod
    def _get_and_check_identifier(
        query: LatestDatapointQuery,
        identifier_type: Literal["id", "external_id"],
    ) -> int | str:
        if (as_primitive := getattr(query, identifier_type)) is None:
            raise ValueError(f"Missing '{identifier_type}' from: '{query}'")
        return as_primitive

    def _parse_user_input(
        self,
        user_input: Any,
        identifier_type: Literal["id", "external_id"],
    ) -> int | str | list[int] | list[str] | None:
        if user_input is None:
            return None
        # We depend on 'IdentifierSequence.load' to parse given ids/xids later, so we need to
        # memorize the individual 'before'-settings when/where given:
        elif isinstance(user_input, LatestDatapointQuery):
            as_primitive = self._get_and_check_identifier(user_input, identifier_type)
            self.before_settings[(identifier_type, 0)] = user_input.before
            return as_primitive
        elif isinstance(user_input, MutableSequence):
            user_input = user_input[:]  # Modify a shallow copy to avoid side effects
            for i, inp in enumerate(user_input):
                if isinstance(inp, LatestDatapointQuery):
                    as_primitive = self._get_and_check_identifier(inp, identifier_type)
                    self.before_settings[(identifier_type, i)] = inp.before
                    user_input[i] = as_primitive  # mutating while iterating like a boss
        return user_input

    def _prepare_requests(
        self, parsed_ids: None | int | Sequence[int], parsed_xids: None | str | Sequence[str]
    ) -> list[dict]:
        all_ids, all_xids = [], []
        if parsed_ids is not None:
            all_ids = IdentifierSequence.load(parsed_ids, None).as_dicts()
        if parsed_xids is not None:
            all_xids = IdentifierSequence.load(None, parsed_xids).as_dicts()

        # In the API, missing 'before' defaults to 'now'. As we want to get the most up-to-date datapoint, we don't
        # specify a particular timestamp for 'now' in order to possibly get a datapoint a few hundred ms fresher:
        for identifiers, identifier_type in zip([all_ids, all_xids], ["id", "external_id"]):
            for i, dct in enumerate(identifiers):
                i_before = self.before_settings.get((identifier_type, i), self.default_before)
                if "now" != i_before is not None:  # mypy doesn't understand 'i_before not in {"now", None}'
                    dct["before"] = timestamp_to_ms(i_before)
        all_ids.extend(all_xids)
        return all_ids

    def fetch_datapoints(self) -> list[dict[str, Any]]:
        tasks = [
            {
                "url_path": self.dps_client._RESOURCE_PATH + "/latest",
                "json": {"items": chunk, "ignoreUnknownIds": self.ignore_unknown_ids},
            }
            for chunk in split_into_chunks(self._all_identifiers, self.dps_client._RETRIEVE_LATEST_LIMIT)
        ]
        tasks_summary = execute_tasks(self.dps_client._post, tasks, max_workers=self.dps_client._config.max_workers)
        tasks_summary.raise_first_encountered_exception()

        return tasks_summary.joined_results(lambda res: res.json()["items"])
