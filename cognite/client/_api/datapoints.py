from __future__ import annotations

import functools
import heapq
import itertools
import math
import statistics
from abc import ABC, abstractmethod
from concurrent.futures import CancelledError, as_completed
from copy import copy
from datetime import datetime
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
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from cognite.client._api.datapoint_tasks import (
    DPS_LIMIT,
    DPS_LIMIT_AGG,
    BaseConcurrentTask,
    BaseDpsFetchSubtask,
    CustomDatapoints,
    DatapointsPayload,
    SplittingFetchSubtask,
    _DatapointsQuery,
    _SingleTSQueryBase,
    _SingleTSQueryValidator,
)
from cognite.client._api.synthetic_time_series import SyntheticDatapointsAPI
from cognite.client._api_client import APIClient
from cognite.client._proto.data_point_list_response_pb2 import DataPointListItem, DataPointListResponse
from cognite.client.data_classes.datapoints import Datapoints, DatapointsArray, DatapointsArrayList, DatapointsList
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils._auxiliary import assert_type, local_import, split_into_chunks, split_into_n_parts
from cognite.client.utils._concurrency import collect_exc_info_and_raise, execute_tasks_concurrently
from cognite.client.utils._identifier import Identifier, IdentifierSequence
from cognite.client.utils._priority_tpe import PriorityThreadPoolExecutor
from cognite.client.utils._time import timestamp_to_ms

if TYPE_CHECKING:
    from concurrent.futures import Future

    import pandas as pd


POST_DPS_OBJECTS_LIMIT = 10_000
FETCH_TS_LIMIT = 100
RETRIEVE_LATEST_LIMIT = 100


TSQueryList = List[_SingleTSQueryBase]
PoolSubtaskType = Tuple[int, float, float, BaseDpsFetchSubtask]

T = TypeVar("T")


def select_dps_fetch_strategy(dps_client: DatapointsAPI, user_query: _DatapointsQuery) -> DpsFetchStrategy:
    max_workers = dps_client._config.max_workers
    if max_workers < 1:  # Dps fetching does not use fn `execute_tasks_concurrently`, so we must check:
        raise RuntimeError(f"Invalid option for `{max_workers=}`. Must be at least 1")

    all_queries = _SingleTSQueryValidator(user_query).validate_and_create_single_queries()
    agg_queries, raw_queries = split_queries_into_raw_and_aggs(all_queries)

    # Running mode is decided based on how many time series are requested VS. number of workers:
    if len(all_queries) <= max_workers:
        # Start shooting requests from the hip immediately:
        return EagerDpsFetcher(dps_client, all_queries, agg_queries, raw_queries, max_workers)
    # Fetch a smaller, chunked batch of dps from all time series - which allows us to do some rudimentary
    # guesstimation of dps density - then chunk away:
    return ChunkingDpsFetcher(dps_client, all_queries, agg_queries, raw_queries, max_workers)


def split_queries_into_raw_and_aggs(all_queries: TSQueryList) -> Tuple[TSQueryList, TSQueryList]:
    split_qs: Tuple[TSQueryList, TSQueryList] = [], []
    for query in all_queries:
        split_qs[query.is_raw_query].append(query)
    return split_qs


class DpsFetchStrategy(ABC):
    def __init__(
        self,
        dps_client: DatapointsAPI,
        all_queries: TSQueryList,
        agg_queries: TSQueryList,
        raw_queries: TSQueryList,
        max_workers: int,
    ) -> None:
        self.dps_client = dps_client
        self.all_queries = all_queries
        self.agg_queries = agg_queries
        self.raw_queries = raw_queries
        self.max_workers = max_workers
        self.n_queries = len(all_queries)

    def fetch_all_datapoints(self) -> DatapointsList:
        with PriorityThreadPoolExecutor(max_workers=self.max_workers) as pool:
            ordered_results = self._fetch_all(pool, use_numpy=False)
        return self._finalize_tasks(ordered_results)

    def fetch_all_datapoints_numpy(self) -> DatapointsArrayList:
        with PriorityThreadPoolExecutor(max_workers=self.max_workers) as pool:
            ordered_results = self._fetch_all(pool, use_numpy=True)
        return self._finalize_tasks_numpy(ordered_results)

    def _finalize_tasks(
        self,
        ordered_results: List[BaseConcurrentTask],
    ) -> DatapointsList:
        return DatapointsList(
            [ts_task.get_result() for ts_task in ordered_results],
            cognite_client=self.dps_client._cognite_client,
        )

    def _finalize_tasks_numpy(
        self,
        ordered_results: List[BaseConcurrentTask],
    ) -> DatapointsArrayList:
        return DatapointsArrayList(
            [ts_task.get_result() for ts_task in ordered_results],
            cognite_client=self.dps_client._cognite_client,
        )

    @abstractmethod
    def _fetch_all(self, pool: PriorityThreadPoolExecutor, use_numpy: bool) -> List[BaseConcurrentTask]:
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

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._make_dps_request_using_protobuf = functools.partial(
            self.dps_client._do_request,
            method="POST",
            url_path=self.dps_client._RESOURCE_PATH + "/list",
            accept="application/protobuf",
            timeout=self.dps_client._config.timeout,
        )

    def __request_datapoints_jit(
        self,
        task: SplittingFetchSubtask,
        payload: Optional[CustomDatapoints] = None,
    ) -> Optional[DataPointListResponse]:
        # Note: We delay getting the next payload as much as possible; this way, when we count number of
        # points left to fetch JIT, we have the most up-to-date estimate (and may quit early):
        if (item := task.get_next_payload()) is None:
            return None

        dps_payload: DatapointsPayload = cast(DatapointsPayload, copy(payload) or {})
        dps_payload["items"] = [item]
        (res := DataPointListResponse()).MergeFromString(
            self._make_dps_request_using_protobuf(json=dps_payload).content
        )
        return res

    def _fetch_all(self, pool: PriorityThreadPoolExecutor, use_numpy: bool) -> List[BaseConcurrentTask]:
        futures_dct, ts_task_lookup = self._create_initial_tasks(pool, use_numpy)

        # Run until all top level tasks are complete:
        while futures_dct:
            future = next(as_completed(futures_dct))
            ts_task = (subtask := futures_dct.pop(future)).parent
            res = self._get_result_with_exception_handling(future, ts_task, ts_task_lookup, futures_dct)
            if res is None:
                continue
            # We may dynamically split subtasks based on what % of time range was returned:
            if new_subtasks := subtask.store_partial_result(res):
                self._queue_new_subtasks(pool, futures_dct, new_subtasks)
            if ts_task.is_done:  # "Parent" ts task might be done before a subtask is finished
                if all(parent.is_done for parent in ts_task_lookup.values()):
                    pool.shutdown(wait=False)
                    break
                if ts_task.has_limit:
                    # For finished limited queries, cancel all unstarted futures for same parent:
                    self._cancel_futures_for_finished_ts_task(ts_task, futures_dct)
                continue
            elif subtask.is_done:
                continue
            self._queue_new_subtasks(pool, futures_dct, [subtask])
        # Return only non-missing time series tasks in correct order given by `all_queries`:
        return list(filter(None, map(ts_task_lookup.get, self.all_queries)))

    def _create_initial_tasks(
        self,
        pool: PriorityThreadPoolExecutor,
        use_numpy: bool,
    ) -> Tuple[Dict[Future, BaseDpsFetchSubtask], Dict[_SingleTSQueryBase, BaseConcurrentTask]]:
        futures_dct: Dict[Future, BaseDpsFetchSubtask] = {}
        ts_task_lookup, payload = {}, {"ignoreUnknownIds": False}
        for query in self.all_queries:
            ts_task = ts_task_lookup[query] = query.ts_task_type(query=query, eager_mode=True, use_numpy=use_numpy)
            for subtask in ts_task.split_into_subtasks(self.max_workers, self.n_queries):
                future = pool.submit(self.__request_datapoints_jit, subtask, payload, priority=subtask.priority)
                futures_dct[future] = subtask
        return futures_dct, ts_task_lookup

    def _queue_new_subtasks(
        self,
        pool: PriorityThreadPoolExecutor,
        futures_dct: Dict[Future, BaseDpsFetchSubtask],
        new_subtasks: Sequence[BaseDpsFetchSubtask],
    ) -> None:
        for task in new_subtasks:
            future = pool.submit(self.__request_datapoints_jit, task, priority=task.priority)
            futures_dct[future] = task

    def _get_result_with_exception_handling(
        self,
        future: Future,
        ts_task: BaseConcurrentTask,
        ts_task_lookup: Dict[_SingleTSQueryBase, BaseConcurrentTask],
        futures_dct: Dict[Future, BaseDpsFetchSubtask],
    ) -> Optional[DataPointListItem]:
        try:
            if (res := future.result()) is not None:
                return res.items[0]
            return None
        except CancelledError:
            return None
        except CogniteAPIError as e:
            if not (e.code == 400 and e.missing and ts_task.query.ignore_unknown_ids):
                collect_exc_info_and_raise([e])
            elif ts_task.is_done:
                return None
            ts_task.is_done = True
            del ts_task_lookup[ts_task.query]
            self._cancel_futures_for_finished_ts_task(ts_task, futures_dct)
            return None

    def _cancel_futures_for_finished_ts_task(
        self, ts_task: BaseConcurrentTask, futures_dct: Dict[Future, BaseDpsFetchSubtask]
    ) -> None:
        for future, subtask in futures_dct.copy().items():
            # TODO: Change to loop over parent.subtasks?
            if subtask.parent is ts_task:
                future.cancel()
                del futures_dct[future]


class ChunkingDpsFetcher(DpsFetchStrategy):
    """A datapoints fetching strategy to make large queries faster through the grouping of more than one
    time series per request.

    The main underlying assumption is that "the more time series are queried, the lower the average density".

    Is used when the number of time series to fetch is larger than the number of `max_workers`. How many
    time series are chunked per request is dynamic and is decided by the overall number to fetch, their
    individual number of datapoints and wheter or not raw- or aggregate datapoints are asked for since
    they are independent in requests - as long as the total number of time series does not exceed FETCH_TS_LIMIT.
    """

    def __init__(self, *args: Any) -> None:
        super().__init__(*args)
        self.counter = itertools.count()
        self._make_dps_request_using_protobuf = functools.partial(
            self.dps_client._do_request,
            method="POST",
            url_path=self.dps_client._RESOURCE_PATH + "/list",
            accept="application/protobuf",
            timeout=self.dps_client._config.timeout,
        )
        # To chunk efficiently, we have subtask pools (heap queues) that we use to prioritise subtasks
        # when building/combining subtasks into a full query:
        self.raw_subtask_pool: List[PoolSubtaskType] = []
        self.agg_subtask_pool: List[PoolSubtaskType] = []
        self.subtask_pools = (self.agg_subtask_pool, self.raw_subtask_pool)
        # Combined partial queries storage (chunked, but not enough to fill a request):
        self.next_items: List[CustomDatapoints] = []
        self.next_subtasks: List[BaseDpsFetchSubtask] = []

    def _fetch_all(self, pool: PriorityThreadPoolExecutor, use_numpy: bool) -> List[BaseConcurrentTask]:
        # The initial tasks are important - as they tell us which time series are missing, which
        # are string, which are sparse... We use this info when we choose the best fetch-strategy.
        ts_task_lookup, missing_to_raise = {}, set()
        initial_query_limits, initial_futures_dct = self._create_initial_tasks(pool)

        for future in as_completed(initial_futures_dct):
            res_lst = future.result()
            chunk_agg_qs, chunk_raw_qs = initial_futures_dct.pop(future)
            new_ts_tasks, chunk_missing = self._create_ts_tasks_and_handle_missing(
                res_lst, chunk_agg_qs, chunk_raw_qs, initial_query_limits, use_numpy
            )
            missing_to_raise.update(chunk_missing)
            ts_task_lookup.update(new_ts_tasks)

        if missing_to_raise:
            raise CogniteNotFoundError(not_found=[q.identifier.as_dict(camel_case=False) for q in missing_to_raise])

        if ts_tasks_left := self._update_queries_with_new_chunking_limit(ts_task_lookup):
            self._add_to_subtask_pools(
                chain.from_iterable(
                    task.split_into_subtasks(max_workers=self.max_workers, n_tot_queries=len(ts_tasks_left))
                    for task in ts_tasks_left
                )
            )
            futures_dct: Dict[Future, List[BaseDpsFetchSubtask]] = {}
            self._queue_new_subtasks(pool, futures_dct)
            self._fetch_until_complete(pool, futures_dct, ts_task_lookup)
        # Return only non-missing time series tasks in correct order given by `all_queries`:
        return list(filter(None, map(ts_task_lookup.get, self.all_queries)))

    def _fetch_until_complete(
        self,
        pool: PriorityThreadPoolExecutor,
        futures_dct: Dict[Future, List[BaseDpsFetchSubtask]],
        ts_task_lookup: Dict[_SingleTSQueryBase, BaseConcurrentTask],
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
            # Check each parent in current batch once if we may cancel some queued subtasks:
            if done_ts_tasks := {sub.parent for sub in subtask_lst if sub.parent.is_done}:
                self._cancel_subtasks(done_ts_tasks)

            self._queue_new_subtasks(pool, futures_dct)

            if all(task.is_done for task in ts_task_lookup.values()):
                pool.shutdown(wait=False)
                return None

    def request_datapoints(self, payload: DatapointsPayload) -> Sequence[DataPointListItem]:
        (res := DataPointListResponse()).MergeFromString(self._make_dps_request_using_protobuf(json=payload).content)
        return res.items

    def _create_initial_tasks(
        self, pool: PriorityThreadPoolExecutor
    ) -> Tuple[Dict[_SingleTSQueryBase, int], Dict[Future, Tuple[TSQueryList, TSQueryList]]]:
        initial_query_limits: Dict[_SingleTSQueryBase, int] = {}
        initial_futures_dct: Dict[Future, Tuple[TSQueryList, TSQueryList]] = {}
        # Optimal queries uses the entire worker pool. We may be forced to use more (queue) when we
        # can't fit all individual time series (maxes out at `FETCH_TS_LIMIT * max_workers`):
        n_queries = max(self.max_workers, math.ceil(self.n_queries / FETCH_TS_LIMIT))
        splitter: Callable[[List[T]], Iterator[List[T]]] = functools.partial(split_into_n_parts, n=n_queries)
        for query_chunks in zip(splitter(self.agg_queries), splitter(self.raw_queries)):
            # Agg and raw limits are independent in the query, so we max out on both:
            items = []
            for queries, max_lim in zip(query_chunks, [DPS_LIMIT_AGG, DPS_LIMIT]):
                maxed_limits = self._find_initial_query_limits([q.capped_limit for q in queries], max_lim)
                initial_query_limits.update(chunk_query_limits := dict(zip(queries, maxed_limits)))
                items.extend([{**q.to_payload(), "limit": lim} for q, lim in chunk_query_limits.items()])

            payload = {"ignoreUnknownIds": True, "items": items}
            future = pool.submit(self.request_datapoints, payload, priority=0)
            initial_futures_dct[future] = query_chunks
        return initial_query_limits, initial_futures_dct

    def _create_ts_tasks_and_handle_missing(
        self,
        res: Sequence[DataPointListItem],
        chunk_agg_qs: TSQueryList,
        chunk_raw_qs: TSQueryList,
        initial_query_limits: Dict[_SingleTSQueryBase, int],
        use_numpy: bool,
    ) -> Tuple[Dict[_SingleTSQueryBase, BaseConcurrentTask], Set[_SingleTSQueryBase]]:
        if len(res) == len(chunk_agg_qs) + len(chunk_raw_qs):
            to_raise: Set[_SingleTSQueryBase] = set()
        else:
            # We have at least 1 missing time series:
            chunk_agg_qs, chunk_raw_qs, to_raise = self._handle_missing_ts(res, chunk_agg_qs, chunk_raw_qs)
        self._update_queries_is_string(res, chunk_raw_qs)
        # Align initial res with corresponding queries and create tasks:
        ts_tasks = {
            query: query.ts_task_type(
                query=query,
                eager_mode=False,
                use_numpy=use_numpy,
                first_dps_batch=res,
                first_limit=initial_query_limits[query],
            )
            for res, query in zip(res, chain(chunk_agg_qs, chunk_raw_qs))
        }
        return ts_tasks, to_raise

    def _add_to_subtask_pools(self, new_subtasks: Iterable[BaseDpsFetchSubtask]) -> None:
        for task in new_subtasks:
            # We leverage how tuples are compared to prioritise items. First `priority`, then `payload limit`
            # (to easily group smaller queries), then counter to always break ties, but keep order (never use tasks themselves):
            limit = min(task.n_dps_left, task.max_query_limit)
            new_subtask: PoolSubtaskType = (task.priority, limit, next(self.counter), task)
            heapq.heappush(self.subtask_pools[task.is_raw_query], new_subtask)

    def _queue_new_subtasks(
        self, pool: PriorityThreadPoolExecutor, futures_dct: Dict[Future, List[BaseDpsFetchSubtask]]
    ) -> None:
        qsize = pool._work_queue.qsize()  # Approximate size of the queue (number of unstarted tasks)
        if qsize > 2 * self.max_workers:
            # Each worker has more than 2 tasks already awaiting in the thread pool queue already, so we
            # hold off on combining new subtasks just yet (allows better prioritisation as more new tasks arrive).
            return None
        # When pool queue has few awaiting tasks, we empty the subtasks pool into a partial request:
        return_partial_payload = qsize <= min(5, math.ceil(self.max_workers / 2))
        combined_requests = self._combine_subtasks_into_requests(return_partial_payload)

        for payload, subtask_lst, priority in combined_requests:
            future = pool.submit(self.request_datapoints, payload, priority=priority)
            futures_dct[future] = subtask_lst

    def _combine_subtasks_into_requests(
        self,
        return_partial_payload: bool,
    ) -> Iterator[Tuple[DatapointsPayload, List[BaseDpsFetchSubtask], float]]:

        while any(self.subtask_pools):  # As long as both are not empty
            payload_at_max_items, payload_is_full = False, [False, False]
            for task_pool, request_max_limit, is_raw in zip(
                self.subtask_pools, (DPS_LIMIT_AGG, DPS_LIMIT), [False, True]
            ):
                if not task_pool:
                    continue
                limit_used = 0
                if self.next_items:  # Happens when we continue building on a previous "partial payload"
                    limit_used = sum(  # Tally up either raw or agg query `limit_used`
                        item["limit"]
                        for item, task in zip(self.next_items, self.next_subtasks)
                        if task.is_raw_query is is_raw
                    )
                while task_pool:
                    if len(self.next_items) + 1 > FETCH_TS_LIMIT:
                        payload_at_max_items = True
                        break
                    # Highest priority task is always at index 0 (heap magic):
                    *_, next_task = task_pool[0]
                    next_payload = next_task.get_next_payload()
                    if next_payload is None or next_task.is_done:
                        # Parent task finished before subtask and has been marked done already:
                        heapq.heappop(task_pool)  # Pop to remove from heap
                        continue
                    next_limit = next_payload["limit"]
                    if limit_used + next_limit <= request_max_limit:
                        self.next_items.append(next_payload)
                        self.next_subtasks.append(next_task)
                        limit_used += next_limit
                        heapq.heappop(task_pool)
                    else:
                        payload_is_full[is_raw] = True
                        break

                payload_done = (
                    payload_at_max_items
                    or all(payload_is_full)
                    or (payload_is_full[0] and not self.raw_subtask_pool)
                    or (payload_is_full[1] and not self.agg_subtask_pool)
                    or (return_partial_payload and not any(self.subtask_pools))
                )
                if payload_done:
                    if not len(self.next_subtasks):
                        # Happens with limited queries as more and more "later" tasks get cancelled.
                        break
                    priority = statistics.mean(task.priority for task in self.next_subtasks)
                    payload: DatapointsPayload = {"items": self.next_items[:]}
                    yield payload, self.next_subtasks[:], priority

                    self.next_items, self.next_subtasks = [], []
                    break

    def _update_queries_with_new_chunking_limit(
        self, ts_task_lookup: Dict[_SingleTSQueryBase, BaseConcurrentTask]
    ) -> List[BaseConcurrentTask]:
        queries = [query for query, task in ts_task_lookup.items() if not task.is_done]
        tot_raw = sum(q.is_raw_query for q in queries)
        tot_agg = len(queries) - tot_raw
        n_raw_chunk = min(FETCH_TS_LIMIT, math.ceil((tot_raw or 1) / 10))
        n_agg_chunk = min(FETCH_TS_LIMIT, math.ceil((tot_agg or 1) / 10))
        max_limit_raw = math.floor(DPS_LIMIT / n_raw_chunk)
        max_limit_agg = math.floor(DPS_LIMIT_AGG / n_agg_chunk)
        for query in queries:
            if query.is_raw_query:
                query.override_max_query_limit(max_limit_raw)
            else:
                query.override_max_query_limit(max_limit_agg)
        return [ts_task_lookup[query] for query in queries]

    def _cancel_subtasks(self, done_ts_tasks: Set[BaseConcurrentTask]) -> None:
        for ts_task in done_ts_tasks:
            # We do -not- want to iterate/mutate the heapqs, so we mark subtasks as done instead:
            for subtask in ts_task.subtasks:
                subtask.is_done = True

    @staticmethod
    def _find_initial_query_limits(limits: List[int], max_limit: int) -> List[int]:
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
    def _update_queries_is_string(res: Sequence[DataPointListItem], queries: TSQueryList) -> None:
        is_string = {("id", r.id) for r in res if r.isString}.union(
            ("externalId", r.externalId) for r in res if r.isString
        )
        for q in queries:
            q.is_string = q.identifier.as_tuple() in is_string

    @staticmethod
    def _handle_missing_ts(
        res: Sequence[DataPointListItem],
        agg_queries: TSQueryList,
        raw_queries: TSQueryList,
    ) -> Tuple[TSQueryList, TSQueryList, Set[_SingleTSQueryBase]]:
        missing, to_raise = set(), set()
        not_missing = {("id", r.id) for r in res}.union(("externalId", r.externalId) for r in res)
        for query in chain(agg_queries, raw_queries):
            # Update _SingleTSQueryBase objects with `is_missing` status:
            query.is_missing = query.identifier.as_tuple() not in not_missing
            if query.is_missing:
                missing.add(query)
                # Only raise for those time series that can't be missing (individually customisable parameter):
                if not query.ignore_unknown_ids:
                    to_raise.add(query)
        agg_queries = [q for q in agg_queries if not q.is_missing]
        raw_queries = [q for q in raw_queries if not q.is_missing]
        return agg_queries, raw_queries, to_raise


class DatapointsAPI(APIClient):
    _RESOURCE_PATH = "/timeseries/data"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.synthetic = SyntheticDatapointsAPI(
            self._config, api_version=self._api_version, cognite_client=self._cognite_client
        )

    # Why are we using List instead of Sequence in type hint? It's because `Sequence[str]` and `str`
    # are indistinguishable... thus using Sequence gives mypy error "Overloaded function signatures X and Y
    # overlap with incompatible return types". See: https://stackoverflow.com/a/49887206/3887338
    def retrieve(
        self,
        *,
        id: Union[None, int, Dict[str, Any], List[Union[int, Dict[str, Any]]]] = None,
        external_id: Union[None, str, Dict[str, Any], List[Union[str, Dict[str, Any]]]] = None,
        start: Union[int, str, datetime, None] = None,
        end: Union[int, str, datetime, None] = None,
        aggregates: Union[str, List[str], None] = None,
        granularity: Optional[str] = None,
        limit: Optional[int] = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
    ) -> Union[None, Datapoints, DatapointsList]:
        """`Retrieve datapoints for one or more time series. <https://docs.cognite.com/api/v1/#operation/getMultiTimeSeriesDatapoints>`_

        **Performance hint:**: For better performance and lower memory usage, consider using `retrieve_arrays(...)` which uses `numpy.ndarrays` for data storage.

        Args:
            start (Union[int, str, datetime, None]): Inclusive start. Default: 1970-01-01 UTC.
            end (Union[int, str, datetime, None]): Exclusive end. Default: "now"
            id (Union[None, int, Dict[str, Any], List[Union[int, Dict[str, Any]]]]): Id, dict (with id) or (mixed) sequence of these. See examples below.
            external_id (Union[None, str, Dict[str, Any], List[Union[str, Dict[str, Any]]]]): External id, dict (with external id) or (mixed) sequence of these. See examples below.
            aggregates (Union[str, List[str], None]): Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity (str): The granularity to fetch aggregates at. e.g. '1s', '2h', '10d'. Default: None.
            limit (int): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether or not to include outside points. Not allowed when fetching aggregates. Default: False
            ignore_unknown_ids (bool): Whether or not to ignore missing time series rather than raising an exception. Default: False

        Returns:
            Union[None, Datapoints, DatapointsList]: A `Datapoints` object containing the requested data, or a `DatapointsList` if multiple time series were asked for. If `ignore_unknown_ids` is `True`, a single time series is requested and it is not found, the function will return `None`. The ordering is first ids, then external_ids.

        Examples:

            You can specify the identifiers of the datapoints you wish to retrieve in a number of ways. In this example
            we are using the time-ago format to get raw data for the time series with id=42 from 2 weeks ago up until now::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> dps = client.time_series.data.retrieve(id=42, start="2w-ago")

            You can also get aggregated values, such as `max` or `average`. You may also fetch more than one time series simultaneously. Here we are
            getting daily averages for all of 2018 for two different time series, specifying start and end as integers (milliseconds after epoch).
            Note that we are fetching them using their external ids::

                >>> from datetime import datetime, timezone
                >>> utc = timezone.utc
                >>> dps_lst = client.time_series.data.retrieve(
                ...    external_id=["foo", "bar"],
                ...    start=1514764800000,
                ...    end=1546300800000,
                ...    aggregates=["average"],
                ...    granularity="1d")

            All parameters can be individually set if you pass (one or more) dictionaries (even `ignore_unknown_ids`, contrary to the API).
            If you also pass top-level parameters, these will be overruled by the individual parameters (where both exist). You are free to
            mix any kind of ids and external ids: single identifiers, single dictionaries and (mixed) lists of these.

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
            on the returned `DatapointsList` object, then specify if you want `id` or `external_id`. Note: If you fetch a time series
            by using `id`, you can still access it with its `external_id` (and the opposite way around), if you know it::

                >>> dps_lst = client.time_series.data.retrieve(
                ...     id=[42, 43, 44, ..., 499, 500], start="2w-ago")
                >>> ts_350 = dps_lst.get(id=350)  # `Datapoints` object

            ...but what happens if you request duplicate `id`s or `external_id`s? Let's say you need to fetch data from multiple
            disconnected periods, e.g. stock prices only from recessions. In this case the `.get` method will return a list of `Datapoints`
            instead, in the same order (similar to how slicing works with non-unique indices on Pandas DataFrames)::

                >>> dps_lst = client.time_series.data.retrieve(
                ...     id=[
                ...         42, 43, 44, 45,
                ...         {"id": 350, "start": datetime(1907, 10, 14, tzinfo=utc), "end": datetime(1907, 11, 6, tzinfo=utc)},
                ...         {"id": 350, "start": datetime(1929, 9, 4, tzinfo=utc), "end": datetime(1929, 11, 13, tzinfo=utc)},
                ...     ])
                >>> ts_44 = dps_lst.get(id=44)  # Single `Datapoints` object
                >>> ts_350_lst = dps_lst.get(id=350)  # List of two `Datapoints` objects

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

            The last example here is just to showcase the great flexibility of the `retrieve` endpoint, with a very custom query::

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
        """
        query = _DatapointsQuery(
            start=start,
            end=end,
            id=id,
            external_id=external_id,
            aggregates=aggregates,
            granularity=granularity,
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
        id: Union[None, int, Dict[str, Any], List[Union[int, Dict[str, Any]]]] = None,
        external_id: Union[None, str, Dict[str, Any], List[Union[str, Dict[str, Any]]]] = None,
        start: Union[int, str, datetime, None] = None,
        end: Union[int, str, datetime, None] = None,
        aggregates: Union[str, List[str], None] = None,
        granularity: Optional[str] = None,
        limit: Optional[int] = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
    ) -> Union[None, DatapointsArray, DatapointsArrayList]:
        """`Retrieve datapoints for one or more time series. <https://docs.cognite.com/api/v1/#operation/getMultiTimeSeriesDatapoints>`_

        **Note**: This method requires `numpy`.

        Args:
            start (Union[int, str, datetime, None]): Inclusive start. Default: 1970-01-01 UTC.
            end (Union[int, str, datetime, None]): Exclusive end. Default: "now"
            id (Union[None, int, Dict[str, Any], List[Union[int, Dict[str, Any]]]]): Id, dict (with id) or (mixed) sequence of these. See examples below.
            external_id (Union[None, str, Dict[str, Any], List[Union[str, Dict[str, Any]]]]): External id, dict (with external id) or (mixed) sequence of these. See examples below.
            aggregates (Union[str, List[str], None]): Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity (str): The granularity to fetch aggregates at. e.g. '1s', '2h', '10d'. Default: None.
            limit (int): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether or not to include outside points. Not allowed when fetching aggregates. Default: False
            ignore_unknown_ids (bool): Whether or not to ignore missing time series rather than raising an exception. Default: False

        Returns:
            Union[None, DatapointsArray, DatapointsArrayList]: A `DatapointsArray` object containing the requested data, or a `DatapointsArrayList` if multiple time series were asked for. If `ignore_unknown_ids` is `True`, a single time series is requested and it is not found, the function will return `None`. The ordering is first ids, then external_ids.

        Examples:

            **Note:** For more usage examples, see `DatapointsAPI.retrieve` method (which accepts exactly the same arguments).

            Get weekly `min` and `max` aggregates for a time series with id=42 since the year 2000, then compute the range of values:

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

            Get raw datapoints for a time series with external_id="bar" from the last 10 weeks, then convert to a `pandas.Series`
            (you can of course also use the `to_pandas()` convenience method if you want a `pandas.DataFrame`):

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
        id: Union[None, int, Dict[str, Any], List[Union[int, Dict[str, Any]]]] = None,
        external_id: Union[None, str, Dict[str, Any], List[Union[str, Dict[str, Any]]]] = None,
        start: Union[int, str, datetime, None] = None,
        end: Union[int, str, datetime, None] = None,
        aggregates: Union[str, List[str], None] = None,
        granularity: Optional[str] = None,
        limit: Optional[int] = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        column_names: Literal["id", "external_id"] = "external_id",
    ) -> pd.DataFrame:
        """Get datapoints directly in a pandas dataframe.

        **Note**: If you have duplicated time series in your query, the dataframe columns will also contain duplicates.

        Args:
            start (Union[int, str, datetime, None]): Inclusive start. Default: 1970-01-01 UTC.
            end (Union[int, str, datetime, None]): Exclusive end. Default: "now"
            id (Union[None, int, Dict[str, Any], List[Union[int, Dict[str, Any]]]]): Id, dict (with id) or (mixed) sequence of these. See examples below.
            external_id (Union[None, str, Dict[str, Any], List[Union[str, Dict[str, Any]]]]): External id, dict (with external id) or (mixed) sequence of these. See examples below.
            aggregates (Union[str, List[str], None]): Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity (str): The granularity to fetch aggregates at. e.g. '1s', '2h', '10d'. Default: None.
            limit (int): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether or not to include outside points. Not allowed when fetching aggregates. Default: False
            ignore_unknown_ids (bool): Whether or not to ignore missing time series rather than raising an exception. Default: False
            uniform_index (bool): If only querying aggregates AND a single granularity is used AND no limit is used, specifying `uniform_index=True` will return a dataframe with an
                equidistant datetime index from the earliest `start` to the latest `end` (missing values will be NaNs). If these requirements are not met, a ValueError is raised. Default: False
            include_aggregate_name (bool): Include 'aggregate' in the column name, e.g. `my-ts|average`. Ignored for raw time series. Default: True
            column_names ("id" | "external_id"): Use either ids or external ids as column names. Time series missing external id will use id as backup. Default: "external_id"

        Returns:
            pandas.DataFrame: A pandas DataFrame containing the requested time series. The ordering of columns is ids first, then external_ids. For time series with multiple aggregates, they will be sorted in alphabetical order ("average" before "max").

        Examples:

            Get a pandas dataframe using a single id, and use this id as column name, with no more than 100 datapoints::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> df = client.time_series.data.retrieve_dataframe(
                ...     id=12345,
                ...     start="2w-ago",
                ...     end="now",
                ...     limit=100,
                ...     column_names="id")

            Get the pandas dataframe with a uniform index (fixed spacing between points) of 1 day, for two time series with
            individually specified aggregates, from 1990 through 2020::

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

            Get a pandas dataframe containing the 'average' aggregate for two time series using a 30 day granularity,
            starting Jan 1, 1970 all the way up to present, without having the aggregate name in the column names::

                >>> df = client.time_series.data.retrieve_dataframe(
                ...     external_id=["foo", "bar"],
                ...     aggregates=["average"],
                ...     granularity="30d",
                ...     include_aggregate_name=False)
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
            limit=limit,
            include_outside_points=include_outside_points,
            ignore_unknown_ids=ignore_unknown_ids,
        )
        fetcher = select_dps_fetch_strategy(self, user_query=query)
        if uniform_index:
            grans_given = set(q.granularity for q in fetcher.all_queries)
            is_limited = any(q.limit is not None for q in fetcher.all_queries)
            if fetcher.raw_queries or len(grans_given) > 1 or is_limited:
                raise ValueError(
                    "Cannot return a uniform index when asking for aggregates with multiple granularities "
                    f"({grans_given}) OR when (partly) querying raw datapoints OR when a finite limit is used."
                )
        df = fetcher.fetch_all_datapoints_numpy().to_pandas(column_names, include_aggregate_name)
        if not uniform_index:
            return df

        start = pd.Timestamp(min(q.start for q in fetcher.agg_queries), unit="ms")
        end = pd.Timestamp(max(q.end for q in fetcher.agg_queries), unit="ms")
        (granularity,) = grans_given
        # Pandas understand "Cognite granularities" except `m` (minutes) which we must translate:
        freq = cast(str, granularity).replace("m", "T")
        return df.reindex(pd.date_range(start=start, end=end, freq=freq, inclusive="left"))

    def retrieve_latest(
        self,
        id: Union[int, List[int]] = None,
        external_id: Union[str, List[str]] = None,
        before: Union[int, str, datetime] = None,
        ignore_unknown_ids: bool = False,
    ) -> Union[Datapoints, DatapointsList]:
        """`Get the latest datapoint for one or more time series <https://docs.cognite.com/api/v1/#operation/getLatest>`_

        Args:
            id (Union[int, List[int]]): Id or list of ids.
            external_id (Union[str, List[str]): External id or list of external ids.
            before: (Union[int, str, datetime]): Get latest datapoint before this time.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            Union[Datapoints, DatapointsList]: A Datapoints object containing the requested data, or a list of such objects.

        Examples:

            Getting the latest datapoint in a time series. This method returns a Datapoints object, so the datapoint will
            be the first element::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.data.retrieve_latest(id=1)[0]

            You can also get the first datapoint before a specific time::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.data.retrieve_latest(id=1, before="2d-ago")[0]

            If you need the latest datapoint for multiple time series simply give a list of ids. Note that we are
            using external ids here, but either will work::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.time_series.data.retrieve_latest(external_id=["abc", "def"])
                >>> latest_abc = res[0][0]
                >>> latest_def = res[1][0]
        """
        id_seq = IdentifierSequence.load(id, external_id)
        all_ids = id_seq.as_dicts()
        if before is not None:
            before = timestamp_to_ms(before)
            for id_ in all_ids:
                id_["before"] = timestamp_to_ms(before)

        tasks = [
            {
                "url_path": self._RESOURCE_PATH + "/latest",
                "json": {"items": chunk, "ignoreUnknownIds": ignore_unknown_ids},
            }
            for chunk in split_into_chunks(all_ids, RETRIEVE_LATEST_LIMIT)
        ]
        tasks_summary = execute_tasks_concurrently(self._post, tasks, max_workers=self._config.max_workers)
        if tasks_summary.exceptions:
            raise tasks_summary.exceptions[0]
        res = tasks_summary.joined_results(lambda res: res.json()["items"])
        if id_seq.is_singleton():
            return Datapoints._load(res[0], cognite_client=self._cognite_client)
        return DatapointsList._load(res, cognite_client=self._cognite_client)

    def insert(
        self,
        datapoints: Union[
            Datapoints,
            DatapointsArray,
            Sequence[Dict[str, Union[int, float, str, datetime]]],
            Sequence[Tuple[Union[int, float, datetime], Union[int, float, str]]],
        ],
        id: int = None,
        external_id: str = None,
    ) -> None:
        """Insert datapoints into a time series

        Timestamps can be represented as milliseconds since epoch or datetime objects.

        Args:
            datapoints(Union[List[Dict], List[Tuple],Datapoints]): The datapoints you wish to insert. Can either be a list of tuples,
                a list of dictionaries, a Datapoints object or a DatapointsArray object. See examples below.
            id (int): Id of time series to insert datapoints into.
            external_id (str): External id of time series to insert datapoint into.

        Returns:
            None

        Examples:

            Your datapoints can be a list of tuples where the first element is the timestamp and the second element is
            the value::


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
                >>> c.time_series.data.insert(datapoints, external_id="def")

            Or they can be a Datapoints or DatapointsArray object (raw datapoints only)::

                >>> data = c.time_series.data.retrieve(external_id="abc", start="1w-ago", end="now")
                >>> c.time_series.data.insert(data, external_id="def")
        """
        post_dps_object = Identifier.of_either(id, external_id).as_dict()
        dps_to_post: Union[
            Sequence[Dict[str, Union[int, float, str, datetime]]],
            Sequence[Tuple[Union[int, float, datetime], Union[int, float, str]]],
        ]
        if isinstance(datapoints, (Datapoints, DatapointsArray)):
            if datapoints.value is None:
                raise ValueError(
                    "When inserting data using a `Datapoints` or `DatapointsArray` object, only raw datapoints are supported"
                )
            if isinstance(datapoints, Datapoints):
                dps_to_post = cast(List[Tuple[int, Any]], list(zip(datapoints.timestamp, datapoints.value)))
            else:
                # Using `tolist()` converts to the nearest compatible built-in Python type:
                assert datapoints.timestamp is not None
                ts = datapoints.timestamp.astype("datetime64[ms]").astype("int64")
                dps_to_post = list(zip(ts.tolist(), datapoints.value.tolist()))
        else:
            dps_to_post = datapoints

        post_dps_object["datapoints"] = dps_to_post
        dps_poster = DatapointsPoster(self)
        dps_poster.insert([post_dps_object])

    def insert_multiple(self, datapoints: List[Dict[str, Union[str, int, List]]]) -> None:
        """`Insert datapoints into multiple time series <https://docs.cognite.com/api/v1/#operation/postMultiTimeSeriesDatapoints>`_

        Args:
            datapoints (List[Dict]): The datapoints you wish to insert along with the ids of the time series.
                See examples below.

        Returns:
            None

        Examples:

            Your datapoints can be a list of tuples where the first element is the timestamp and the second element is
            the value::

                >>> from cognite.client import CogniteClient
                >>> from datetime import datetime, timezone
                >>> c = CogniteClient()

                >>> datapoints = []
                >>> # With datetime objects and id
                >>> datapoints.append(
                ...     {"id": 1, "datapoints": [
                ...         (datetime(2018,1,1,tzinfo=timezone.utc), 1000),
                ...         (datetime(2018,1,2,tzinfo=timezone.utc), 2000)
                ... ]})
                >>> # with ms since epoch and externalId
                >>> datapoints.append({"externalId": 1, "datapoints": [(150000000000, 1000), (160000000000, 2000)]})
                >>> c.time_series.data.insert_multiple(datapoints)
        """
        dps_poster = DatapointsPoster(self)
        dps_poster.insert(datapoints)

    def delete_range(
        self, start: Union[int, str, datetime], end: Union[int, str, datetime], id: int = None, external_id: str = None
    ) -> None:
        """Delete a range of datapoints from a time series.

        Args:
            start (Union[int, str, datetime]): Inclusive start of delete range
            end (Union[int, str, datetime]): Exclusvie end of delete range
            id (int): Id of time series to delete data from
            external_id (str): External id of time series to delete data from

        Returns:
            None

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

    def delete_ranges(self, ranges: List[Dict[str, Any]]) -> None:
        """`Delete a range of datapoints from multiple time series. <https://docs.cognite.com/api/v1/#operation/deleteDatapoints>`_

        Args:
            ranges (List[Dict[str, Any]]): The list of datapoint ids along with time range to delete. See examples below.

        Returns:
            None

        Examples:

            Each element in the list ranges must be specify either id or externalId, and a range::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> ranges = [{"id": 1, "start": "2d-ago", "end": "now"},
                ...             {"externalId": "abc", "start": "2d-ago", "end": "now"}]
                >>> c.time_series.data.delete_ranges(ranges)
        """
        valid_ranges = []
        for range in ranges:
            for key in range:
                if key not in ("id", "externalId", "start", "end"):
                    raise AssertionError(
                        "Invalid key '{}' in range. Must contain 'start', 'end', and 'id' or 'externalId".format(key)
                    )
            id = range.get("id")
            external_id = range.get("externalId")
            valid_range = Identifier.of_either(id, external_id).as_dict()
            start = timestamp_to_ms(range["start"])
            end = timestamp_to_ms(range["end"])
            valid_range.update({"inclusiveBegin": start, "exclusiveEnd": end})
            valid_ranges.append(valid_range)
        self._delete_datapoints_ranges(valid_ranges)

    def _delete_datapoints_ranges(self, delete_range_objects: List[Union[Dict]]) -> None:
        self._post(url_path=self._RESOURCE_PATH + "/delete", json={"items": delete_range_objects})

    def insert_dataframe(self, df: pd.DataFrame, external_id_headers: bool = True, dropna: bool = True) -> None:
        """Insert a dataframe.

        The index of the dataframe must contain the timestamps. The names of the remaining columns specify the ids or external ids of
        the time series to which column contents will be written.

        Said time series must already exist.

        Args:
            df (pandas.DataFrame):  Pandas DataFrame object containing the time series.
            external_id_headers (bool): Interpret the column names as external id. Pass False if using ids. Default: True.
            dropna (bool): Set to True to ignore NaNs in the given DataFrame, applied per column. Default: True.

        Returns:
            None

        Examples:
            Post a dataframe with white noise::

                >>> import numpy as np
                >>> import pandas as pd
                >>> from cognite.client import CogniteClient
                >>>
                >>> c = CogniteClient()
                >>> ts_xid = "my-foo-ts"
                >>> idx = pd.date_range(start="2018-01-01", periods=100, freq="1d")
                >>> noise = np.random.normal(0, 1, 100)
                >>> df = pd.DataFrame({ts_xid: noise}, index=idx)
                >>> c.time_series.data.insert_dataframe(df)
        """
        np = cast(Any, local_import("numpy"))
        if np.isinf(df.select_dtypes(include=[np.number])).any(axis=None):
            raise ValueError("Dataframe contains one or more (+/-) Infinity. Remove them in order to insert the data.")
        if not dropna:
            if df.isnull().any(axis=None):
                raise ValueError(
                    "Dataframe contains one or more NaNs. Remove or pass `dropna=True` in order to insert the data."
                )
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
    def __init__(self, dps_objects_limit: int, dps_limit: int):
        self.dps_objects_limit = dps_objects_limit
        self.dps_limit = dps_limit
        self.current_num_datapoints = 0
        self.dps_object_list: List[dict] = []

    def add(self, dps_object: Dict[str, Any]) -> None:
        self.current_num_datapoints += len(dps_object["datapoints"])
        self.dps_object_list.append(dps_object)

    def will_fit(self, number_of_dps: int) -> bool:
        will_fit_dps = (self.current_num_datapoints + number_of_dps) <= self.dps_limit
        will_fit_dps_objects = (len(self.dps_object_list) + 1) <= self.dps_objects_limit
        return will_fit_dps and will_fit_dps_objects


class DatapointsPoster:
    def __init__(self, client: DatapointsAPI) -> None:
        self.client = client
        self.bins: List[DatapointsBin] = []

    def insert(self, dps_object_list: List[Dict[str, Any]]) -> None:
        valid_dps_object_list = self._validate_dps_objects(dps_object_list)
        binned_dps_object_lists = self._bin_datapoints(valid_dps_object_list)
        self._insert_datapoints_concurrently(binned_dps_object_lists)

    @staticmethod
    def _validate_dps_objects(dps_object_list: List[Dict[str, Any]]) -> List[dict]:
        valid_dps_objects = []
        for dps_object in dps_object_list:
            for key in dps_object:
                if key not in ("id", "externalId", "datapoints"):
                    raise ValueError(
                        "Invalid key '{}' in datapoints. Must contain 'datapoints', and 'id' or 'externalId".format(key)
                    )
            valid_dps_object = {k: dps_object[k] for k in ["id", "externalId"] if k in dps_object}
            valid_dps_object["datapoints"] = DatapointsPoster._validate_and_format_datapoints(dps_object["datapoints"])
            valid_dps_objects.append(valid_dps_object)
        return valid_dps_objects

    @staticmethod
    def _validate_and_format_datapoints(
        datapoints: Union[
            List[Dict[str, Any]],
            List[Tuple[Union[int, float, datetime], Union[int, float, str]]],
        ],
    ) -> List[Tuple[int, Any]]:
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

    def _bin_datapoints(self, dps_object_list: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        for dps_object in dps_object_list:
            for i in range(0, len(dps_object["datapoints"]), DPS_LIMIT):
                dps_object_chunk = {k: dps_object[k] for k in ["id", "externalId"] if k in dps_object}
                dps_object_chunk["datapoints"] = dps_object["datapoints"][i : i + DPS_LIMIT]
                for bin in self.bins:
                    if bin.will_fit(len(dps_object_chunk["datapoints"])):
                        bin.add(dps_object_chunk)
                        break
                else:
                    bin = DatapointsBin(DPS_LIMIT, POST_DPS_OBJECTS_LIMIT)
                    bin.add(dps_object_chunk)
                    self.bins.append(bin)
        binned_dps_object_list = []
        for bin in self.bins:
            binned_dps_object_list.append(bin.dps_object_list)
        return binned_dps_object_list

    def _insert_datapoints_concurrently(self, dps_object_lists: List[List[Dict[str, Any]]]) -> None:
        tasks = []
        for dps_object_list in dps_object_lists:
            tasks.append((dps_object_list,))
        summary = execute_tasks_concurrently(
            self._insert_datapoints, tasks, max_workers=self.client._config.max_workers
        )
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda x: x[0],
            task_list_element_unwrap_fn=lambda x: {k: x[k] for k in ["id", "externalId"] if k in x},
        )

    def _insert_datapoints(self, post_dps_objects: List[Dict[str, Any]]) -> None:
        # convert to memory intensive format as late as possible and clean up after
        for it in post_dps_objects:
            it["datapoints"] = [{"timestamp": t, "value": v} for t, v in it["datapoints"]]
        self.client._post(url_path=self.client._RESOURCE_PATH, json={"items": post_dps_objects})
        for it in post_dps_objects:
            del it["datapoints"]
