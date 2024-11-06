from __future__ import annotations

import datetime
import functools
import heapq
import itertools
import math
import time
import warnings
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, MutableSequence, Sequence
from itertools import chain
from operator import itemgetter
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NamedTuple,
    TypeGuard,
    TypeVar,
    cast,
)

from typing_extensions import Self

from cognite.client._api.datapoint_tasks import (
    BaseDpsFetchSubtask,
    BaseTaskOrchestrator,
    _FullDatapointsQuery,
)
from cognite.client._api.synthetic_time_series import SyntheticDatapointsAPI
from cognite.client._api_client import APIClient
from cognite.client._proto.data_point_list_response_pb2 import DataPointListItem, DataPointListResponse
from cognite.client.data_classes import (
    Datapoints,
    DatapointsArray,
    DatapointsArrayList,
    DatapointsList,
    DatapointsQuery,
    LatestDatapointQuery,
)
from cognite.client.data_classes.data_modeling.ids import NodeId
from cognite.client.data_classes.datapoints import Aggregate, _DatapointsPayload, _DatapointsPayloadItem
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils import _json
from cognite.client.utils._auxiliary import (
    exactly_one_is_not_none,
    find_duplicates,
    split_into_chunks,
    split_into_n_parts,
    unpack_items_in_payload,
)
from cognite.client.utils._concurrency import ConcurrencySettings, execute_tasks
from cognite.client.utils._identifier import Identifier, IdentifierSequence, IdentifierSequenceCore
from cognite.client.utils._importing import import_as_completed, local_import
from cognite.client.utils._time import (
    ZoneInfo,
    align_large_granularity,
    pandas_date_range_tz,
    timestamp_to_ms,
    to_fixed_utc_intervals,
    to_pandas_freq,
    validate_timezone,
)
from cognite.client.utils._validation import validate_user_input_dict_with_identifier
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from concurrent.futures import Future, ThreadPoolExecutor

    import pandas as pd

    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


as_completed = import_as_completed()

_TSQueryList = list[DatapointsQuery]
PoolSubtaskType = tuple[float, int, BaseDpsFetchSubtask]

_T = TypeVar("_T")
_TResLst = TypeVar("_TResLst", DatapointsList, DatapointsArrayList)


def select_dps_fetch_strategy(dps_client: DatapointsAPI, full_query: _FullDatapointsQuery) -> DpsFetchStrategy:
    all_queries = full_query.parse_into_queries()
    full_query.validate(all_queries, dps_limit_raw=dps_client._DPS_LIMIT_RAW, dps_limit_agg=dps_client._DPS_LIMIT_AGG)
    agg_queries, raw_queries = split_queries_into_raw_and_aggs(all_queries)

    # Running mode is decided based on how many time series are requested VS. number of workers:
    if len(all_queries) <= (max_workers := dps_client._config.max_workers):
        # Start shooting requests from the hip immediately:
        return EagerDpsFetcher(dps_client, all_queries, agg_queries, raw_queries, max_workers)
    # Fetch a smaller, chunked batch of dps from all time series - which allows us to do some rudimentary
    # guesstimation of dps density - then chunk away:
    return ChunkingDpsFetcher(dps_client, all_queries, agg_queries, raw_queries, max_workers)


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
    ) -> None:
        self.dps_client = dps_client
        self.all_queries = all_queries
        self.agg_queries = agg_queries
        self.raw_queries = raw_queries
        self.max_workers = max_workers
        self.n_queries = len(all_queries)

    def fetch_all_datapoints(self) -> DatapointsList:
        pool = ConcurrencySettings.get_executor(max_workers=self.max_workers)
        return DatapointsList(
            [ts_task.get_result() for ts_task in self._fetch_all(pool, use_numpy=False)],  # type: ignore [arg-type]
            cognite_client=self.dps_client._cognite_client,
        )

    def fetch_all_datapoints_numpy(self) -> DatapointsArrayList:
        pool = ConcurrencySettings.get_executor(max_workers=self.max_workers)
        return DatapointsArrayList(
            [ts_task.get_result() for ts_task in self._fetch_all(pool, use_numpy=True)],  # type: ignore [arg-type]
            cognite_client=self.dps_client._cognite_client,
        )

    def _request_datapoints(self, payload: _DatapointsPayload) -> Sequence[DataPointListItem]:
        (res := DataPointListResponse()).MergeFromString(
            self.dps_client._do_request(
                json=payload,
                method="POST",
                url_path=f"{self.dps_client._RESOURCE_PATH}/list",
                accept="application/protobuf",
                timeout=self.dps_client._config.timeout,
            ).content
        )
        return res.items

    @staticmethod
    def _raise_if_missing(to_raise: set[DatapointsQuery]) -> None:
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
        missing_to_raise: set[DatapointsQuery] = set()
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
    ) -> tuple[dict[Future, BaseDpsFetchSubtask], dict[DatapointsQuery, BaseTaskOrchestrator]]:
        futures_dct: dict[Future, BaseDpsFetchSubtask] = {}
        ts_task_lookup = {}
        for query in self.all_queries:
            ts_task = ts_task_lookup[query] = query.task_orchestrator(query=query, eager_mode=True, use_numpy=use_numpy)
            for subtask in ts_task.split_into_subtasks(self.max_workers, self.n_queries):
                payload = _DatapointsPayload(items=[subtask.get_next_payload_item()], ignoreUnknownIds=False)
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
            payload = _DatapointsPayload(items=[subtask.get_next_payload_item()])
            future = pool.submit(self._request_datapoints, payload)
            futures_dct[future] = subtask

    def _get_result_with_exception_handling(
        self,
        future: Future,
        ts_task: BaseTaskOrchestrator,
        ts_task_lookup: dict[DatapointsQuery, BaseTaskOrchestrator],
        missing_to_raise: set[DatapointsQuery],
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
        ts_task_lookup: dict[DatapointsQuery, BaseTaskOrchestrator],
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
    ) -> tuple[dict[DatapointsQuery, int], dict[Future, tuple[_TSQueryList, _TSQueryList]]]:
        initial_query_limits: dict[DatapointsQuery, int] = {}
        initial_futures_dct: dict[Future, tuple[_TSQueryList, _TSQueryList]] = {}
        # Optimal queries uses the entire worker pool. We may be forced to use more (queue) when we
        # can't fit all individual time series (maxes out at `_FETCH_TS_LIMIT * max_workers`):
        n_queries = max(self.max_workers, math.ceil(self.n_queries / self.dps_client._FETCH_TS_LIMIT))
        splitter: Callable[[list[_T]], Iterator[list[_T]]] = functools.partial(split_into_n_parts, n=n_queries)
        for query_chunks in zip(splitter(self.agg_queries), splitter(self.raw_queries)):
            if not any(query_chunks):
                break  # Not all workers needed (at least for now)
            # Agg and raw limits are independent in the query, so we max out on both:
            items = []
            for queries, max_lim in zip(query_chunks, (self.dps_client._DPS_LIMIT_AGG, self.dps_client._DPS_LIMIT_RAW)):
                maxed_limits = self._find_initial_query_limits([q.capped_limit for q in queries], max_lim)
                initial_query_limits.update(chunk_query_limits := dict(zip(queries, maxed_limits)))
                for query, limit in chunk_query_limits.items():
                    (item := query.to_payload_item())["limit"] = limit
                    items.append(item)

            payload = _DatapointsPayload(items=items, ignoreUnknownIds=True)
            future = pool.submit(self._request_datapoints, payload)
            initial_futures_dct[future] = query_chunks
        return initial_query_limits, initial_futures_dct

    def _create_ts_tasks_and_handle_missing(
        self,
        res: Sequence[DataPointListItem],
        chunk_queues: tuple[_TSQueryList, _TSQueryList],
        initial_query_limits: dict[DatapointsQuery, int],
        use_numpy: bool,
    ) -> tuple[dict[DatapointsQuery, BaseTaskOrchestrator], set[DatapointsQuery]]:
        if len(res) == sum(map(len, chunk_queues)):
            to_raise: set[DatapointsQuery] = set()
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
            heapq.heappush(self.subtask_pools[task.parent.query.is_raw_query], new_subtask)

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
    ) -> tuple[_DatapointsPayload, list[BaseDpsFetchSubtask]]:
        next_items: list[_DatapointsPayloadItem] = []
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
                    payload = _DatapointsPayload(items=next_items)
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
        return _DatapointsPayload(items=next_items), next_subtasks

    @staticmethod
    def _decide_individual_query_limit(query: DatapointsQuery, ts_task: BaseTaskOrchestrator, n_ts_limit: int) -> int:
        # For a better estimate, we use first ts of first batch instead of `query.start`:
        batch_start, batch_end = ts_task.start_ts_first_batch, ts_task.end_ts_first_batch
        est_remaining_dps = ts_task.n_dps_first_batch * (query.end_ms - batch_end) / (batch_end - batch_start)
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
        self, ts_task_lookup: dict[DatapointsQuery, BaseTaskOrchestrator]
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
            query.max_query_limit = est_limit

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
    ) -> tuple[tuple[_TSQueryList, _TSQueryList], set[DatapointsQuery]]:
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

    def retrieve(
        self,
        *,
        id: None | int | DatapointsQuery | Sequence[int | DatapointsQuery] = None,
        external_id: None | str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery] = None,
        instance_id: None | NodeId | Sequence[NodeId] | DatapointsQuery | Sequence[NodeId | DatapointsQuery] = None,
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
        """`Retrieve datapoints for one or more time series. <https://developer.cognite.com/api#tag/Time-series/operation/getMultiTimeSeriesDatapoints>`_

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. Make *one* call to retrieve and fetch all time series in go, rather than making multiple calls (if your memory allows it). The SDK will optimize retrieval strategy for you!
            2. For best speed, and significantly lower memory usage, consider using ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            3. Unlimited queries (``limit=None``) are most performant as they are always fetched in parallel, for any number of requested time series.
            4. Limited queries, (e.g. ``limit=500_000``) are much less performant, at least for large limits, as each individual time series is fetched serially (we can't predict where on the timeline the datapoints are). Thus parallelisation is only used when asking for multiple "limited" time series.
            5. Try to avoid specifying `start` and `end` to be very far from the actual data: If you have data from 2000 to 2015, don't use start=0 (1970).
            6. Using ``timezone`` and/or calendar granularities like month/quarter/year in aggregate queries comes at a penalty.

        Time series support status codes like Good, Uncertain and Bad. You can read more in the Cognite Data Fusion developer documentation on
        `status codes. <https://developer.cognite.com/dev/concepts/reference/quality_codes/>`_

        Args:
            id (None | int | DatapointsQuery | Sequence[int | DatapointsQuery]): Id, dict (with id) or (mixed) sequence of these. See examples below.
            external_id (None | str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery]): External id, dict (with external id) or (mixed) sequence of these. See examples below.
            instance_id (None | NodeId | Sequence[NodeId] | DatapointsQuery | Sequence[NodeId | DatapointsQuery]): Instance id or sequence of instance ids.
            start (int | str | datetime.datetime | None): Inclusive start. Default: 1970-01-01 UTC.
            end (int | str | datetime.datetime | None): Exclusive end. Default: "now"
            aggregates (Aggregate | str | list[Aggregate | str] | None): Single aggregate or list of aggregates to retrieve. Available options: ``average``, ``continuous_variance``, ``count``, ``count_bad``, ``count_good``, ``count_uncertain``, ``discrete_variance``, ``duration_bad``, ``duration_good``, ``duration_uncertain``, ``interpolation``, ``max``, ``min``, ``step_interpolation``, ``sum`` and ``total_variation``. Default: None (raw datapoints returned)
            granularity (str | None): The granularity to fetch aggregates at. Can be given as an abbreviation or spelled out for clarity: ``s/second(s)``, ``m/minute(s)``, ``h/hour(s)``, ``d/day(s)``, ``w/week(s)``, ``mo/month(s)``, ``q/quarter(s)``, or ``y/year(s)``. Examples: ``30s``, ``5m``, ``1day``, ``2weeks``. Default: None.
            timezone (str | datetime.timezone | ZoneInfo | None): For raw datapoints, which timezone to use when displaying (will not affect what is retrieved). For aggregates, which timezone to align to for granularity 'hour' and longer. Align to the start of the hour, day or month. For timezones of type Region/Location, like 'Europe/Oslo', pass a string or ``ZoneInfo`` instance. The aggregate duration will then vary, typically due to daylight saving time. You can also use a fixed offset from UTC by passing a string like '+04:00', 'UTC-7' or 'UTC-02:30' or an instance of ``datetime.timezone``. Note: Historical timezones with second offset are not supported, and timezones with minute offsets (e.g. UTC+05:30 or Asia/Kolkata) may take longer to execute.
            target_unit (str | None): The unit_external_id of the datapoints returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system (str | None): The unit system of the datapoints returned. Cannot be used with target_unit.
            limit (int | None): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether to include outside points. Not allowed when fetching aggregates. Default: False
            ignore_unknown_ids (bool): Whether to ignore missing time series rather than raising an exception. Default: False
            include_status (bool): Also return the status code, an integer, for each datapoint in the response. Only relevant for raw datapoint queries, not aggregates.
            ignore_bad_datapoints (bool): Treat datapoints with a bad status code as if they do not exist. If set to false, raw queries will include bad datapoints in the response, and aggregates will in general omit the time period between a bad datapoint and the next good datapoint. Also, the period between a bad datapoint and the previous good datapoint will be considered constant. Default: True.
            treat_uncertain_as_bad (bool): Treat datapoints with uncertain status codes as bad. If false, treat datapoints with uncertain status codes as good. Used for both raw queries and aggregates. Default: True.

        Returns:
            Datapoints | DatapointsList | None: A ``Datapoints`` object containing the requested data, or a ``DatapointsList`` if multiple time series were asked for (the ordering is ids first, then external_ids). If `ignore_unknown_ids` is `True`, a single time series is requested and it is not found, the function will return `None`.

        Examples:

            You can specify the identifiers of the datapoints you wish to retrieve in a number of ways. In this example
            we are using the time-ago format to get raw data for the time series with id=42 from 2 weeks ago up until now.

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
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
            object, but this should generally be avoided (consider this a performance warning)::

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
                ...         DatapointsQuery(id=69, end="2d-ago", aggregates=["average"]),
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
            same order, similar to how slicing works with non-unique indices on Pandas DataFrames::

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
            aggregate data), just to get the very first datapoint of every month (from e.g. the year 2000 through 2010)::

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
        fetcher = select_dps_fetch_strategy(self, full_query=query)
        dps_lst = fetcher.fetch_all_datapoints()

        if not query.is_single_identifier:
            return dps_lst
        elif not dps_lst and ignore_unknown_ids:
            return None
        return dps_lst[0]

    def retrieve_arrays(
        self,
        *,
        id: None | int | DatapointsQuery | Sequence[int | DatapointsQuery] = None,
        external_id: None | str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery] = None,
        instance_id: None | NodeId | Sequence[NodeId] | DatapointsQuery | Sequence[NodeId | DatapointsQuery] = None,
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
        """`Retrieve datapoints for one or more time series. <https://developer.cognite.com/api#tag/Time-series/operation/getMultiTimeSeriesDatapoints>`_

        Note:
            This method requires ``numpy`` to be installed.

        Time series support status codes like Good, Uncertain and Bad. You can read more in the Cognite Data Fusion developer documentation on
        `status codes. <https://developer.cognite.com/dev/concepts/reference/quality_codes/>`_

        Args:
            id (None | int | DatapointsQuery | Sequence[int | DatapointsQuery]): Id, dict (with id) or (mixed) sequence of these. See examples below.
            external_id (None | str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery]): External id, dict (with external id) or (mixed) sequence of these. See examples below.
            instance_id (None | NodeId | Sequence[NodeId] | DatapointsQuery | Sequence[NodeId | DatapointsQuery]): Instance id or sequence of instance ids.
            start (int | str | datetime.datetime | None): Inclusive start. Default: 1970-01-01 UTC.
            end (int | str | datetime.datetime | None): Exclusive end. Default: "now"
            aggregates (Aggregate | str | list[Aggregate | str] | None): Single aggregate or list of aggregates to retrieve. Available options: ``average``, ``continuous_variance``, ``count``, ``count_bad``, ``count_good``, ``count_uncertain``, ``discrete_variance``, ``duration_bad``, ``duration_good``, ``duration_uncertain``, ``interpolation``, ``max``, ``min``, ``step_interpolation``, ``sum`` and ``total_variation``. Default: None (raw datapoints returned)
            granularity (str | None): The granularity to fetch aggregates at. Can be given as an abbreviation or spelled out for clarity: ``s/second(s)``, ``m/minute(s)``, ``h/hour(s)``, ``d/day(s)``, ``w/week(s)``, ``mo/month(s)``, ``q/quarter(s)``, or ``y/year(s)``. Examples: ``30s``, ``5m``, ``1day``, ``2weeks``. Default: None.
            timezone (str | datetime.timezone | ZoneInfo | None): For raw datapoints, which timezone to use when displaying (will not affect what is retrieved). For aggregates, which timezone to align to for granularity 'hour' and longer. Align to the start of the hour, day or month. For timezones of type Region/Location, like 'Europe/Oslo', pass a string or ``ZoneInfo`` instance. The aggregate duration will then vary, typically due to daylight saving time. You can also use a fixed offset from UTC by passing a string like '+04:00', 'UTC-7' or 'UTC-02:30' or an instance of ``datetime.timezone``. Note: Historical timezones with second offset are not supported, and timezones with minute offsets (e.g. UTC+05:30 or Asia/Kolkata) may take longer to execute.
            target_unit (str | None): The unit_external_id of the datapoints returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system (str | None): The unit system of the datapoints returned. Cannot be used with target_unit.
            limit (int | None): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether to include outside points. Not allowed when fetching aggregates. Default: False
            ignore_unknown_ids (bool): Whether to ignore missing time series rather than raising an exception. Default: False
            include_status (bool): Also return the status code, an integer, for each datapoint in the response. Only relevant for raw datapoint queries, not aggregates.
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
        fetcher = select_dps_fetch_strategy(self, full_query=query)

        dps_lst = fetcher.fetch_all_datapoints_numpy()
        if not query.is_single_identifier:
            return dps_lst
        elif not dps_lst and ignore_unknown_ids:
            return None
        return dps_lst[0]

    def retrieve_dataframe(
        self,
        *,
        id: None | int | DatapointsQuery | Sequence[int | DatapointsQuery] = None,
        external_id: None | str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery] = None,
        instance_id: None | NodeId | Sequence[NodeId] | DatapointsQuery | Sequence[NodeId | DatapointsQuery] = None,
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
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: Literal["id", "external_id", "instance_id"] = "instance_id",
    ) -> pd.DataFrame:
        """Get datapoints directly in a pandas dataframe.

        Time series support status codes like Good, Uncertain and Bad. You can read more in the Cognite Data Fusion developer documentation on
        `status codes. <https://developer.cognite.com/dev/concepts/reference/quality_codes/>`_

        Note:
            For many more usage examples, check out the :py:meth:`~DatapointsAPI.retrieve` method which accepts exactly the same arguments.

        Args:
            id (None | int | DatapointsQuery | Sequence[int | DatapointsQuery]): Id, dict (with id) or (mixed) sequence of these. See examples below.
            external_id (None | str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery]): External id, dict (with external id) or (mixed) sequence of these. See examples below.
            instance_id (None | NodeId | Sequence[NodeId] | DatapointsQuery | Sequence[NodeId | DatapointsQuery]): Instance id or sequence of instance ids.
            start (int | str | datetime.datetime | None): Inclusive start. Default: 1970-01-01 UTC.
            end (int | str | datetime.datetime | None): Exclusive end. Default: "now"
            aggregates (Aggregate | str | list[Aggregate | str] | None): Single aggregate or list of aggregates to retrieve. Available options: ``average``, ``continuous_variance``, ``count``, ``count_bad``, ``count_good``, ``count_uncertain``, ``discrete_variance``, ``duration_bad``, ``duration_good``, ``duration_uncertain``, ``interpolation``, ``max``, ``min``, ``step_interpolation``, ``sum`` and ``total_variation``. Default: None (raw datapoints returned)
            granularity (str | None): The granularity to fetch aggregates at. Can be given as an abbreviation or spelled out for clarity: ``s/second(s)``, ``m/minute(s)``, ``h/hour(s)``, ``d/day(s)``, ``w/week(s)``, ``mo/month(s)``, ``q/quarter(s)``, or ``y/year(s)``. Examples: ``30s``, ``5m``, ``1day``, ``2weeks``. Default: None.
            timezone (str | datetime.timezone | ZoneInfo | None): For raw datapoints, which timezone to use when displaying (will not affect what is retrieved). For aggregates, which timezone to align to for granularity 'hour' and longer. Align to the start of the hour, -day or -month. For timezones of type Region/Location, like 'Europe/Oslo', pass a string or ``ZoneInfo`` instance. The aggregate duration will then vary, typically due to daylight saving time. You can also use a fixed offset from UTC by passing a string like '+04:00', 'UTC-7' or 'UTC-02:30' or an instance of ``datetime.timezone``. Note: Historical timezones with second offset are not supported, and timezones with minute offsets (e.g. UTC+05:30 or Asia/Kolkata) may take longer to execute.
            target_unit (str | None): The unit_external_id of the datapoints returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system (str | None): The unit system of the datapoints returned. Cannot be used with target_unit.
            limit (int | None): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether to include outside points. Not allowed when fetching aggregates. Default: False
            ignore_unknown_ids (bool): Whether to ignore missing time series rather than raising an exception. Default: False
            include_status (bool): Also return the status code, an integer, for each datapoint in the response. Only relevant for raw datapoint queries, not aggregates.
            ignore_bad_datapoints (bool): Treat datapoints with a bad status code as if they do not exist. If set to false, raw queries will include bad datapoints in the response, and aggregates will in general omit the time period between a bad datapoint and the next good datapoint. Also, the period between a bad datapoint and the previous good datapoint will be considered constant. Default: True.
            treat_uncertain_as_bad (bool): Treat datapoints with uncertain status codes as bad. If false, treat datapoints with uncertain status codes as good. Used for both raw queries and aggregates. Default: True.
            uniform_index (bool): If only querying aggregates AND a single granularity is used AND no limit is used, specifying `uniform_index=True` will return a dataframe with an equidistant datetime index from the earliest `start` to the latest `end` (missing values will be NaNs). If these requirements are not met, a ValueError is raised. Default: False
            include_aggregate_name (bool): Include 'aggregate' in the column name, e.g. `my-ts|average`. Ignored for raw time series. Default: True
            include_granularity_name (bool): Include 'granularity' in the column name, e.g. `my-ts|12h`. Added after 'aggregate' when present. Ignored for raw time series. Default: False
            column_names (Literal['id', 'external_id', 'instance_id']): Use either instance IDs, external IDs or IDs as column names. Time series missing instance ID will use external ID if it exists then ID as backup. Default: "instance_id"

        Returns:
            pd.DataFrame: A pandas DataFrame containing the requested time series. The ordering of columns is ids first, then external_ids. For time series with multiple aggregates, they will be sorted in alphabetical order ("average" before "max").

        Warning:
            If you have duplicated time series in your query, the dataframe columns will also contain duplicates.

            When retrieving raw datapoints with ``ignore_bad_datapoints=False``, bad datapoints with the value NaN can not be distinguished from those
            missing a value (due to being stored in a numpy array); all will become NaNs in the dataframe.

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

            Get a pandas dataframe containing the 'average' aggregate for two time series using a 30-day granularity,
            starting Jan 1, 1970 all the way up to present, without having the aggregate name in the column names:

                >>> df = client.time_series.data.retrieve_dataframe(
                ...     external_id=["foo", "bar"],
                ...     aggregates="average",
                ...     granularity="30d",
                ...     include_aggregate_name=False)

            You may also use ``pandas.Timestamp`` to define start and end:

                >>> import pandas as pd
                >>> df = client.time_series.data.retrieve_dataframe(
                ...     external_id="foo",
                ...     start=pd.Timestamp("2023-01-01"),
                ...     end=pd.Timestamp("2023-02-01"))
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
        fetcher = select_dps_fetch_strategy(self, full_query=query)

        if not uniform_index:
            return fetcher.fetch_all_datapoints_numpy().to_pandas(
                column_names, include_aggregate_name, include_granularity_name, include_status=include_status
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
        df = fetcher.fetch_all_datapoints_numpy().to_pandas(
            column_names, include_aggregate_name, include_granularity_name, include_status=include_status
        )
        start = pd.Timestamp(min(q.start_ms for q in fetcher.agg_queries), unit="ms")
        end = pd.Timestamp(max(q.end_ms for q in fetcher.agg_queries), unit="ms")
        (granularity,) = grans_given
        # Pandas understand "Cognite granularities" except `m` (minutes) which we must translate:
        freq = cast(str, granularity).replace("m", "min")
        return df.reindex(pd.date_range(start=start, end=end, freq=freq, inclusive="left"))

    # TODO: Deprecated, don't add support for new features like instance_id
    def retrieve_dataframe_in_tz(
        self,
        *,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        start: datetime.datetime,
        end: datetime.datetime,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: Literal["id", "external_id"] = "external_id",
    ) -> pd.DataFrame:
        """Get datapoints directly in a pandas dataframe in the same timezone as ``start`` and ``end``.

        .. admonition:: Deprecation Warning

            This SDK function is deprecated and will be removed in the next major release. Reason: Cognite Data
            Fusion now has native support for timezone and calendar-based aggregations. Please consider migrating
            already today: The API also supports fixed offsets, yields more accurate results and have better support
            for exotic timezones and unusual DST offsets. You can use the normal retrieve methods instead, just
            pass 'timezone' as a parameter.

        Args:
            id (int | Sequence[int] | None): ID or list of IDs.
            external_id (str | SequenceNotStr[str] | None): External ID or list of External IDs.
            start (datetime.datetime): Inclusive start, must be timezone aware.
            end (datetime.datetime): Exclusive end, must be timezone aware and have the same timezone as start.
            aggregates (Aggregate | str | list[Aggregate | str] | None): Single aggregate or list of aggregates to retrieve. Available options: ``average``, ``continuous_variance``, ``count``, ``count_bad``, ``count_good``, ``count_uncertain``, ``discrete_variance``, ``duration_bad``, ``duration_good``, ``duration_uncertain``, ``interpolation``, ``max``, ``min``, ``step_interpolation``, ``sum`` and ``total_variation``. Default: None (raw datapoints returned)
            granularity (str | None): The granularity to fetch aggregates at. Can be given as an abbreviation or spelled out for clarity: ``s/second(s)``, ``m/minute(s)``, ``h/hour(s)``, ``d/day(s)``, ``w/week(s)``, ``mo/month(s)``, ``q/quarter(s)``, or ``y/year(s)``. Examples: ``30s``, ``5m``, ``1day``, ``2weeks``. Default: None.
            target_unit (str | None): The unit_external_id of the datapoints returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system (str | None): The unit system of the datapoints returned. Cannot be used with target_unit.
            ignore_unknown_ids (bool): Whether to ignore missing time series rather than raising an exception. Default: False
            include_status (bool): Also return the status code, an integer, for each datapoint in the response. Only relevant for raw datapoint queries, not aggregates.
            ignore_bad_datapoints (bool): Treat datapoints with a bad status code as if they do not exist. If set to false, raw queries will include bad datapoints in the response, and aggregates will in general omit the time period between a bad datapoint and the next good datapoint. Also, the period between a bad datapoint and the previous good datapoint will be considered constant. Default: True.
            treat_uncertain_as_bad (bool): Treat datapoints with uncertain status codes as bad. If false, treat datapoints with uncertain status codes as good. Used for both raw queries and aggregates. Default: True.
            uniform_index (bool): If querying aggregates with a non-calendar granularity, specifying ``uniform_index=True`` will return a dataframe with an index with constant spacing between timestamps decided by granularity all the way from `start` to `end` (missing values will be NaNs). Default: False
            include_aggregate_name (bool): Include 'aggregate' in the column name, e.g. `my-ts|average`. Ignored for raw time series. Default: True
            include_granularity_name (bool): Include 'granularity' in the column name, e.g. `my-ts|12h`. Added after 'aggregate' when present. Ignored for raw time series. Default: False
            column_names (Literal['id', 'external_id']): Use either ids or external ids as column names. Time series missing external id will use id as backup. Default: "external_id"

        Returns:
            pd.DataFrame: A pandas DataFrame containing the requested time series with a DatetimeIndex localized in the given timezone.
        """
        warnings.warn(
            (
                "This SDK method, `retrieve_dataframe_in_tz`, is deprecated and will be removed in the next major release. "
                "Reason: Cognite Data Fusion now has native support for timezone and calendar-based aggregations. Please "
                "consider migrating already today: The API also supports fixed offsets, yields more accurate results and "
                "have better support for exotic timezones and unusual DST offsets. You can use the normal retrieve methods "
                "instead, just pass 'timezone' as a parameter."
            ),
            UserWarning,
        )
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
            # For raw data, we only need to convert the timezone:
            return (
                # TODO: include_outside_points is missing
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
                    include_status=include_status,
                    ignore_bad_datapoints=ignore_bad_datapoints,
                    treat_uncertain_as_bad=treat_uncertain_as_bad,
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
            include_status=include_status,
            ignore_bad_datapoints=ignore_bad_datapoints,
            treat_uncertain_as_bad=treat_uncertain_as_bad,
            target_unit=target_unit,
            target_unit_system=target_unit_system,
            **{identifiers[0].name(): queries},  # type: ignore [arg-type]
        )
        assert isinstance(arrays, DatapointsArrayList)  # mypy

        arrays.concat_duplicate_ids()
        for arr in arrays:
            # In case 'include_granularity_name' is used, we don't want '2quarters' to show up as '4343h':
            arr.granularity = granularity
        df = (
            arrays.to_pandas(column_names, include_aggregate_name, include_granularity_name)
            .tz_localize("utc")
            .tz_convert(str(tz))
        )
        if uniform_index:
            freq = to_pandas_freq(granularity, start)
            # TODO: Bug, "small" granularities like s/m/h raise here:
            start, end = align_large_granularity(start, end, granularity)
            return df.reindex(pandas_date_range_tz(start, end, freq, inclusive="left"))
        return df

    def retrieve_latest(
        self,
        id: int | LatestDatapointQuery | list[int | LatestDatapointQuery] | None = None,
        external_id: str | LatestDatapointQuery | list[str | LatestDatapointQuery] | None = None,
        before: None | int | str | datetime.datetime = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> Datapoints | DatapointsList | None:
        """`Get the latest datapoint for one or more time series <https://developer.cognite.com/api#tag/Time-series/operation/getLatest>`_

        Time series support status codes like Good, Uncertain and Bad. You can read more in the Cognite Data Fusion developer documentation on
        `status codes. <https://developer.cognite.com/dev/concepts/reference/quality_codes/>`_

        Args:
            id (int | LatestDatapointQuery | list[int | LatestDatapointQuery] | None): Id or list of ids.
            external_id (str | LatestDatapointQuery | list[str | LatestDatapointQuery] | None): External id or list of external ids.
            before (None | int | str | datetime.datetime): (Union[int, str, datetime]): Get latest datapoint before this time. Not used when passing 'LatestDatapointQuery'.
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

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.time_series.data.retrieve_latest(id=1)[0]

            You can also get the first datapoint before a specific time::

                >>> res = client.time_series.data.retrieve_latest(id=1, before="2d-ago")[0]

            You can also retrieve the datapoint in a different unit or unit system::

                >>> res = client.time_series.data.retrieve_latest(id=1, target_unit="temperature:deg_f")[0]
                >>> res = client.time_series.data.retrieve_latest(id=1, target_unit_system="Imperial")[0]

            You may also pass an instance of LatestDatapointQuery:

                >>> from cognite.client.data_classes import LatestDatapointQuery
                >>> res = client.time_series.data.retrieve_latest(id=LatestDatapointQuery(id=1, before=60_000))[0]

            If you need the latest datapoint for multiple time series, simply give a list of ids. Note that we are
            using external ids here, but either will work::

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
        fetcher = RetrieveLatestDpsFetcher(
            id=id,
            external_id=external_id,
            before=before,
            target_unit=target_unit,
            target_unit_system=target_unit_system,
            include_status=include_status,
            ignore_bad_datapoints=ignore_bad_datapoints,
            treat_uncertain_as_bad=treat_uncertain_as_bad,
            ignore_unknown_ids=ignore_unknown_ids,
            dps_client=self,
        )
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
        | Sequence[dict[str, int | float | str | datetime.datetime]]
        | Sequence[tuple[int | float | datetime.datetime, int | float | str]],
        id: int | None = None,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
    ) -> None:
        """Insert datapoints into a time series

        Timestamps can be represented as milliseconds since epoch or datetime objects. Note that naive datetimes
        are interpreted to be in the local timezone (not UTC), adhering to Python conventions for datetime handling.

        Time series support status codes like Good, Uncertain and Bad. You can read more in the Cognite Data Fusion developer documentation on
        `status codes. <https://developer.cognite.com/dev/concepts/reference/quality_codes/>`_

        Args:
            datapoints (Datapoints | DatapointsArray | Sequence[dict[str, int | float | str | datetime.datetime]] | Sequence[tuple[int | float | datetime.datetime, int | float | str]]): The datapoints you wish to insert. Can either be a list of tuples, a list of dictionaries, a Datapoints object or a DatapointsArray object. See examples below.
            id (int | None): Id of time series to insert datapoints into.
            external_id (str | None): External id of time series to insert datapoint into.
            instance_id (NodeId | None): (Alpha) Instance ID of time series to insert datapoints into.

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
                >>> datapoints = [
                ...     (datetime(2018,1,1, tzinfo=timezone.utc), 1000),
                ...     (datetime(2018,1,2, tzinfo=timezone.utc), 2000, StatusCode.Good),
                ...     (datetime(2018,1,3, tzinfo=timezone.utc), 3000, StatusCode.Uncertain),
                ...     (datetime(2018,1,4, tzinfo=timezone.utc), None, StatusCode.Bad),
                ... ]
                >>> client.time_series.data.insert(datapoints, id=1)

            The timestamp can be given by datetime as above, or in milliseconds since epoch. Status codes can also be
            passed as normal integers; this is necessary if a subcategory or modifier flag is needed, e.g. 3145728: 'GoodClamped':

                >>> datapoints = [
                ...     (150000000000, 1000),
                ...     (160000000000, 2000, 3145728),
                ...     (160000000000, 2000, 2147483648),  # Same as StatusCode.Bad
                ... ]
                >>> client.time_series.data.insert(datapoints, id=2)

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

        post_dps_object = Identifier.of_either(id, external_id, instance_id).as_dict()
        post_dps_object["datapoints"] = datapoints
        DatapointsPoster(self).insert([post_dps_object])

    def insert_multiple(self, datapoints: list[dict[str, str | int | list | Datapoints | DatapointsArray]]) -> None:
        """`Insert datapoints into multiple time series <https://developer.cognite.com/api#tag/Time-series/operation/postMultiTimeSeriesDatapoints>`_

        Timestamps can be represented as milliseconds since epoch or datetime objects. Note that naive datetimes
        are interpreted to be in the local timezone (not UTC), adhering to Python conventions for datetime handling.

        Time series support status codes like Good, Uncertain and Bad. You can read more in the Cognite Data Fusion developer documentation on
        `status codes. <https://developer.cognite.com/dev/concepts/reference/quality_codes/>`_

        Args:
            datapoints (list[dict[str, str | int | list | Datapoints | DatapointsArray]]): The datapoints you wish to insert along with the ids of the time series. See examples below.

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
                >>> from cognite.client.data_classes import StatusCode
                >>> from datetime import datetime, timezone
                >>> client = CogniteClient()
                >>> to_insert = [
                ...     {"id": 1, "datapoints": [
                ...         (datetime(2018,1,1, tzinfo=timezone.utc), 1000),
                ...         (datetime(2018,1,2, tzinfo=timezone.utc), 2000, StatusCode.Good),
                ...         (datetime(2018,1,3, tzinfo=timezone.utc), 3000, StatusCode.Uncertain),
                ...         (datetime(2018,1,4, tzinfo=timezone.utc), None, StatusCode.Bad),
                ... ]}]

            Passing datapoints using the dictionary format with timestamp given in milliseconds since epoch:

                >>> import math
                >>> to_insert.append(
                ...     {"external_id": "foo", "datapoints": [
                ...         {"timestamp": 170000000, "value": 4000},
                ...         {"timestamp": 180000000, "value": 5000, "status": {"symbol": "Uncertain"}},
                ...         {"timestamp": 190000000, "value": None, "status": {"code": StatusCode.Bad}},
                ...         {"timestamp": 190000000, "value": math.inf, "status": {"code": StatusCode.Bad, "symbol": "Bad"}},
                ... ]})

            If the Datapoints or DatapointsArray are fetched with status codes, these will be automatically used in the insert:

                >>> data_to_clone = client.time_series.data.retrieve(
                ...     external_id="bar", include_status=True, ignore_bad_datapoints=False)
                >>> to_insert.append({"external_id": "bar-clone", "datapoints": data_to_clone})
                >>> client.time_series.data.insert_multiple(to_insert)
        """
        if not isinstance(datapoints, Sequence):
            raise ValueError("Input must be a list of dictionaries")
        DatapointsPoster(self).insert(datapoints)

    def delete_range(
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
            instance_id (NodeId | None): (Alpha) Instance ID of time series to delete data from

        Examples:

            Deleting the last week of data from a time series::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.time_series.data.delete_range(start="1w-ago", end="now", id=1)
        """
        start_ms = timestamp_to_ms(start)
        end_ms = timestamp_to_ms(end)
        if end_ms <= start_ms:
            raise ValueError(f"{end=} must be larger than {start=}")

        identifier = Identifier.of_either(id, external_id, instance_id).as_dict()
        delete_dps_object = {**identifier, "inclusiveBegin": start_ms, "exclusiveEnd": end_ms}
        self._delete_datapoints_ranges([delete_dps_object])

    def delete_ranges(self, ranges: list[dict[str, Any]]) -> None:
        """`Delete a range of datapoints from multiple time series. <https://developer.cognite.com/api#tag/Time-series/operation/deleteDatapoints>`_

        Args:
            ranges (list[dict[str, Any]]): The list of datapoint ids along with time range to delete. See examples below.

        Examples:

            Each element in the list ranges must be specify either id or external_id, and a range::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> ranges = [{"id": 1, "start": "2d-ago", "end": "now"},
                ...           {"external_id": "abc", "start": "2d-ago", "end": "now"}]
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

        Warning:
            You can not insert datapoints with status codes using this method (``insert_dataframe``), you'll need
            to use the :py:meth:`~DatapointsAPI.insert` method instead (or :py:meth:`~DatapointsAPI.insert_multiple`)!

        Examples:
            Post a dataframe with white noise:

                >>> import numpy as np
                >>> import pandas as pd
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> ts_xid = "my-foo-ts"
                >>> idx = pd.date_range(start="2018-01-01", periods=100, freq="1d")
                >>> noise = np.random.normal(0, 1, 100)
                >>> df = pd.DataFrame({ts_xid: noise}, index=idx)
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
            if external_id_headers:
                dps.append({"datapoints": datapoints, "externalId": column_id})
            else:
                dps.append({"datapoints": datapoints, "id": int(column_id)})
        self.insert_multiple(dps)


class _InsertDatapoint(NamedTuple):
    ts: int | datetime.datetime
    value: str | float
    status_code: int | None = None
    status_symbol: str | None = None

    @classmethod
    def from_dict(cls, dct: dict[str, Any]) -> Self:
        if status := dct.get("status"):
            return cls(dct["timestamp"], dct["value"], status.get("code"), status.get("symbol"))
        return cls(dct["timestamp"], dct["value"])

    def dump(self) -> dict[str, Any]:
        dumped: dict[str, Any] = {"timestamp": timestamp_to_ms(self.ts), "value": self.value}
        if self.status_code:  # also skip if 0
            dumped["status"] = {"code": self.status_code}
        if self.status_symbol and self.status_symbol != "Good":
            dumped.setdefault("status", {})["symbol"] = self.status_symbol
        # Out-of-range float values must be passed as strings:
        dumped["value"] = _json.convert_nonfinite_float_to_str(dumped["value"])
        return dumped


class DatapointsPoster:
    def __init__(self, dps_client: DatapointsAPI) -> None:
        self.dps_client = dps_client
        self.dps_limit = self.dps_client._DPS_INSERT_LIMIT
        self.ts_limit = self.dps_client._POST_DPS_OBJECTS_LIMIT
        self.max_workers = self.dps_client._config.max_workers

    def insert(self, dps_object_lst: list[dict[str, Any]]) -> None:
        to_insert = self._verify_and_prepare_dps_objects(dps_object_lst)
        # To ensure we stay below the max limit on objects per request, we first chunk based on it:
        # (with 10k limit this is almost always just one chunk)
        tasks = [
            (task,)  # execute_tasks demands tuples
            for chunk in split_into_chunks(to_insert, self.ts_limit)
            for task in self._create_payload_tasks(chunk)
        ]
        summary = execute_tasks(self._insert_datapoints, tasks, max_workers=self.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=itemgetter(0),
            task_list_element_unwrap_fn=IdentifierSequenceCore.extract_identifiers,
        )

    def _verify_and_prepare_dps_objects(
        self, dps_object_lst: list[dict[str, Any]]
    ) -> list[tuple[Identifier, list[_InsertDatapoint]]]:
        dps_to_insert: dict[Identifier, list[_InsertDatapoint]] = defaultdict(list)
        for obj in dps_object_lst:
            identifier = validate_user_input_dict_with_identifier(obj, required_keys={"datapoints"})
            validated_dps = self._parse_and_validate_dps(obj["datapoints"])
            dps_to_insert[identifier].extend(validated_dps)
        return list(dps_to_insert.items())

    def _parse_and_validate_dps(self, dps: Datapoints | DatapointsArray | list[tuple | dict]) -> list[_InsertDatapoint]:
        if not dps:
            raise ValueError("No datapoints provided")

        if isinstance(dps, Datapoints):
            self._verify_dps_object_for_insertion(dps)
            return self._extract_raw_data_from_datapoints(dps)
        elif isinstance(dps, DatapointsArray):
            self._verify_dps_object_for_insertion(dps)
            return self._extract_raw_data_from_datapoints_array(dps)

        if not isinstance(dps, SequenceNotStr):
            raise TypeError(f"Datapoints to be inserted must be a list, not {type(dps)}")
        if self._dps_are_insert_ready(dps):
            return dps  # Internal SDK shortcut to avoid casting
        elif self._dps_are_tuples(dps):
            return [_InsertDatapoint(*tpl) for tpl in dps]
        elif self._dps_are_dicts(dps):
            try:
                return [_InsertDatapoint.from_dict(dp) for dp in dps]
            except KeyError:
                raise KeyError(
                    "A datapoint is missing one or both keys ['value', 'timestamp']. Note: 'status' is optional."
                )
        raise TypeError(
            "Datapoints to be inserted must be of type Datapoints or DatapointsArray (with raw datapoints), "
            f"or be a list containing tuples or dicts, not {type(dps[0])}"
        )

    @staticmethod
    def _dps_are_insert_ready(dps: list[Any]) -> TypeGuard[list[_InsertDatapoint]]:
        # Not a real type guard, but we don't want to make millions of isinstance checks when
        # the documentation is clear on what types we accept:
        return isinstance(dps[0], _InsertDatapoint)

    @staticmethod
    def _dps_are_tuples(dps: list[Any]) -> TypeGuard[list[tuple]]:
        # Not a real type guard, see '_dps_are_insert_ready'.
        return isinstance(dps[0], tuple)

    @staticmethod
    def _dps_are_dicts(dps: list[Any]) -> TypeGuard[list[dict]]:
        # Not a real type guard, see '_dps_are_insert_ready'.
        return isinstance(dps[0], dict)

    def _create_payload_tasks(
        self, post_dps_objects: list[tuple[Identifier, list[_InsertDatapoint]]]
    ) -> Iterator[list[dict[str, Any]]]:
        payload = []
        n_left = self.dps_limit
        for identifier, dps in post_dps_objects:
            for next_chunk, is_full in self._split_datapoints(dps, n_left, self.dps_limit):
                payload.append({**identifier.as_dict(), "datapoints": next_chunk})
                if is_full:
                    yield payload
                    payload = []
                    n_left = self.dps_limit
                else:
                    n_left -= len(next_chunk)
        if payload:
            yield payload

    def _insert_datapoints(self, payload: list[dict[str, Any]]) -> None:
        # Convert to memory intensive format as late as possible (and clean up after insert)
        for dct in payload:
            dct["datapoints"] = [dp.dump() for dp in dct["datapoints"]]
        headers: dict[str, str] | None = None

        self.dps_client._post(url_path=self.dps_client._RESOURCE_PATH, json={"items": payload}, headers=headers)
        for dct in payload:
            dct["datapoints"].clear()

    @staticmethod
    def _split_datapoints(lst: list[_T], n_first: int, n: int) -> Iterator[tuple[list[_T], bool]]:
        # Returns chunks with a boolean answering "are we there yet"
        chunk = lst[:n_first]
        yield chunk, len(chunk) == n_first

        for i in range(n_first, len(lst), n):
            chunk = lst[i : i + n]
            yield lst[i : i + n], len(chunk) == n

    @staticmethod
    def _verify_dps_object_for_insertion(dps: Datapoints | DatapointsArray) -> None:
        if dps.value is None:
            raise ValueError(f"Only raw datapoints are supported when inserting data from ``{type(dps).__name__}``")
        if (n_ts := len(dps.timestamp)) != (n_dps := len(dps.value)):
            raise ValueError(f"Number of timestamps ({n_ts}) does not match number of datapoints ({n_dps}) to insert")

        if dps.status_code is not None and dps.status_symbol is not None:
            if n_ts != (n_codes := len(dps.status_code)):
                raise ValueError(
                    f"Number of status codes ({n_codes}) does not match the number of datapoints ({n_ts}) to insert"
                )
        elif exactly_one_is_not_none(dps.status_code, dps.status_symbol):
            # Let's not silently ignore someone that have manually instantiated a dps object with just one status:
            raise ValueError("One of status code/symbol is missing on datapoints object")

    def _extract_raw_data_from_datapoints(self, dps: Datapoints) -> list[_InsertDatapoint]:
        if dps.status_code is None:
            return list(map(_InsertDatapoint, dps.timestamp, dps.value))  # type: ignore [arg-type]
        return list(map(_InsertDatapoint, dps.timestamp, dps.value, dps.status_code))  # type: ignore [arg-type]

    def _extract_raw_data_from_datapoints_array(self, dps: DatapointsArray) -> list[_InsertDatapoint]:
        # Using `tolist()` converts to the nearest compatible built-in Python type (in C code):
        values = dps.value.tolist()  # type: ignore [union-attr]
        timestamps = dps.timestamp.astype("datetime64[ms]").astype("int64").tolist()

        if dps.null_timestamps:
            # 'Missing' and NaN can not be differentiated when we read from numpy arrays:
            values = [None if ts in dps.null_timestamps else dp for ts, dp in zip(timestamps, values)]

        if dps.status_code is None:
            return list(map(_InsertDatapoint, timestamps, values))
        return list(map(_InsertDatapoint, timestamps, values, dps.status_code.tolist()))


class RetrieveLatestDpsFetcher:
    def __init__(
        self,
        id: None | int | LatestDatapointQuery | list[int | LatestDatapointQuery],
        external_id: None | str | LatestDatapointQuery | list[str | LatestDatapointQuery],
        before: None | int | str | datetime.datetime,
        target_unit: None | str,
        target_unit_system: None | str,
        include_status: bool,
        ignore_bad_datapoints: bool,
        treat_uncertain_as_bad: bool,
        ignore_unknown_ids: bool,
        dps_client: DatapointsAPI,
    ) -> None:
        self.default_before = before
        self.default_unit = target_unit
        self.default_unit_system = target_unit_system
        self.default_include_status = include_status
        self.default_ignore_bad_datapoints = ignore_bad_datapoints
        self.default_treat_uncertain_as_bad = treat_uncertain_as_bad

        self.settings_before: dict[tuple[str, int], None | int | str | datetime.datetime] = {}
        self.settings_target_unit: dict[tuple[str, int], None | str] = {}
        self.settings_target_unit_system: dict[tuple[str, int], None | str] = {}
        self.settings_include_status: dict[tuple[str, int], bool | None] = {}
        self.settings_ignore_bad_datapoints: dict[tuple[str, int], bool | None] = {}
        self.settings_treat_uncertain_as_bad: dict[tuple[str, int], bool | None] = {}

        self.ignore_unknown_ids = ignore_unknown_ids
        self.dps_client = dps_client

        parsed_ids = cast(None | int | Sequence[int], self._parse_user_input(id, "id"))
        parsed_xids = cast(None | str | SequenceNotStr[str], self._parse_user_input(external_id, "external_id"))
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
        if query.identifier.name() != identifier_type:
            raise ValueError(f"Missing '{identifier_type}' from: '{query}'")
        return query.identifier.as_primitive()

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
            idx = (identifier_type, 0)
            self.settings_before[idx] = user_input.before
            self.settings_target_unit[idx] = user_input.target_unit
            self.settings_target_unit_system[idx] = user_input.target_unit_system
            self.settings_include_status[idx] = user_input.include_status
            self.settings_ignore_bad_datapoints[idx] = user_input.ignore_bad_datapoints
            self.settings_treat_uncertain_as_bad[idx] = user_input.treat_uncertain_as_bad
            return as_primitive
        elif isinstance(user_input, MutableSequence):
            user_input = user_input[:]  # Modify a shallow copy to avoid side effects
            for i, inp in enumerate(user_input):
                if isinstance(inp, LatestDatapointQuery):
                    as_primitive = self._get_and_check_identifier(inp, identifier_type)
                    idx = (identifier_type, i)
                    self.settings_before[idx] = inp.before
                    self.settings_target_unit[idx] = inp.target_unit
                    self.settings_target_unit_system[idx] = inp.target_unit_system
                    self.settings_include_status[idx] = inp.include_status
                    self.settings_ignore_bad_datapoints[idx] = inp.ignore_bad_datapoints
                    self.settings_treat_uncertain_as_bad[idx] = inp.treat_uncertain_as_bad
                    user_input[i] = as_primitive  # mutating while iterating like a boss
        return user_input

    def _prepare_requests(
        self, parsed_ids: None | int | Sequence[int], parsed_xids: None | str | SequenceNotStr[str]
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
                idx = (identifier_type, i)
                i_before = self.settings_before.get(idx) or self.default_before
                if "now" != i_before is not None:  # mypy doesn't understand 'i_before not in {"now", None}'
                    dct["before"] = timestamp_to_ms(i_before)

                i_target_unit = self.settings_target_unit.get(idx) or self.default_unit
                i_target_unit_system = self.settings_target_unit_system.get(idx) or self.default_unit_system
                if i_target_unit is not None and i_target_unit_system is not None:
                    raise ValueError("You must use either 'target_unit' or 'target_unit_system', not both.")
                if i_target_unit is not None:
                    dct["targetUnit"] = i_target_unit
                if i_target_unit_system is not None:
                    dct["targetUnitSystem"] = i_target_unit_system

                # Careful logic: "Not given" vs "given" vs "default" with "truthy/falsy":
                if (
                    self.settings_include_status.get(idx) is True
                    or self.settings_include_status.get(idx) is None
                    and self.default_include_status is True
                ):
                    dct["includeStatus"] = True

                if (
                    self.settings_ignore_bad_datapoints.get(idx) is False
                    or self.settings_ignore_bad_datapoints.get(idx) is None
                    and self.default_ignore_bad_datapoints is False
                ):
                    dct["ignoreBadDataPoints"] = False

                if (
                    self.settings_treat_uncertain_as_bad.get(idx) is False
                    or self.settings_treat_uncertain_as_bad.get(idx) is None
                    and self.default_treat_uncertain_as_bad is False
                ):
                    dct["treatUncertainAsBad"] = False

        all_ids.extend(all_xids)
        return all_ids

    def _post_fix_status_codes_and_stringified_floats(self, result: list[dict[str, Any]]) -> list[dict[str, Any]]:
        # Due to 'ignore_unknown_ids', we can't just zip queries & results and iterate... sadness
        if self.ignore_unknown_ids and len(result) < len(self._all_identifiers):
            ids_exists = (
                {("id", r["id"]) for r in result}
                .union({("xid", r["externalId"]) for r in result})
                .difference({("xid", None)})
            )  # fmt: skip
            self._all_identifiers = [
                query
                for query in self._all_identifiers
                if ids_exists.intersection((("id", query.get("id")), ("xid", query.get("externalId"))))
            ]
        for query, res in zip(self._all_identifiers, result):
            if not (dps := res["datapoints"]):
                continue
            (dp,) = dps
            if query.get("includeStatus") is True:
                dp.setdefault("status", {"code": 0, "symbol": "Good"})  # Not returned from API by default
            if query.get("ignoreBadDataPoints") is False:
                # Bad data can have value missing (we translate to None):
                dp.setdefault("value", None)
                if not res["isString"]:
                    dp["value"] = _json.convert_to_float(dp["value"])
        return result

    def fetch_datapoints(self) -> list[dict[str, Any]]:
        tasks = [
            {
                "url_path": self.dps_client._RESOURCE_PATH + "/latest",
                "json": {"items": chunk, "ignoreUnknownIds": self.ignore_unknown_ids},
            }
            for chunk in split_into_chunks(self._all_identifiers, self.dps_client._RETRIEVE_LATEST_LIMIT)
        ]
        tasks_summary = execute_tasks(self.dps_client._post, tasks, max_workers=self.dps_client._config.max_workers)
        tasks_summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=unpack_items_in_payload,
            task_list_element_unwrap_fn=IdentifierSequenceCore.extract_identifiers,
        )
        result = tasks_summary.joined_results(lambda res: res.json()["items"])
        return self._post_fix_status_codes_and_stringified_floats(result)
