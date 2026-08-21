from __future__ import annotations

import asyncio
import datetime
import heapq
import itertools
import math
from abc import ABC, abstractmethod
from collections import Counter, defaultdict
from collections.abc import AsyncIterator, Iterable, Iterator, MutableSequence, Sequence
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
)
from cognite.client._proto.data_point_list_response_pb2 import DataPointListItem, DataPointListResponse
from cognite.client.data_classes import (
    Datapoints,
    DatapointsArray,
    DatapointsArrayList,
    DatapointsList,
    DatapointsQuery,
    LatestDatapointQuery,
)
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils import _json_extended as _json
from cognite.client.utils._auxiliary import (
    exactly_one_is_not_none,
    split_into_chunks,
    split_into_n_parts,
    unpack_items,
    unpack_items_in_payload,
)
from cognite.client.utils._concurrency import AsyncSDKTask, execute_async_tasks
from cognite.client.utils._identifier import Identifier, IdentifierSequence, IdentifierSequenceCore
from cognite.client.utils._time import (
    timestamp_to_ms,
)
from cognite.client.utils._validation import validate_user_input_dict_with_identifier
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client._api.datapoints import DatapointsAPI


PoolSubtaskType = tuple[float, int, BaseDpsFetchSubtask]

_T = TypeVar("_T")


class DpsFetchStrategy(ABC):
    def __init__(self, dps_client: DatapointsAPI, all_queries: list[DatapointsQuery]) -> None:
        from cognite.client import global_config

        self.dps_client = dps_client
        self.all_queries = all_queries
        self.agg_queries, self.raw_queries = self.split_queries(all_queries)
        self.concurrency_limit = global_config.concurrency_settings.datapoints.read
        self.n_queries = len(all_queries)
        self.semaphore = dps_client._get_semaphore("read")

    @staticmethod
    def split_queries(all_queries: list[DatapointsQuery]) -> tuple[list[DatapointsQuery], list[DatapointsQuery]]:
        split_qs: tuple[list[DatapointsQuery], list[DatapointsQuery]] = [], []
        for query in all_queries:
            split_qs[query.is_raw_query].append(query)
        return split_qs

    async def fetch_all_datapoints(self) -> DatapointsList:
        return DatapointsList(
            [ts_task.get_result(use_numpy=False) async for ts_task in self._fetch_all(use_numpy=False)],
        ).set_client_ref(self.dps_client._cognite_client)

    async def fetch_all_datapoints_numpy(self) -> DatapointsArrayList:
        return DatapointsArrayList(
            [ts_task.get_result(use_numpy=True) async for ts_task in self._fetch_all(use_numpy=True)],
        ).set_client_ref(self.dps_client._cognite_client)

    async def _request_datapoints(self, payload: dict[str, Any]) -> Sequence[DataPointListItem]:
        (res := DataPointListResponse()).MergeFromString(
            (
                await self.dps_client._post(
                    f"{self.dps_client._RESOURCE_PATH}/list",
                    json=payload,
                    headers={"accept": "application/protobuf"},
                    semaphore=self.semaphore,
                )
            ).content
        )
        return res.items

    async def _raise_if_missing(
        self, to_raise: set[DatapointsQuery], futures_dct: dict[asyncio.Task, Any] | None = None
    ) -> None:
        if not to_raise:
            return
        if futures_dct:
            await self._cancel_remaining(futures_dct)
        raise CogniteNotFoundError(
            "Time series not found",
            code=400,
            missing=[q.identifier.as_dict(camel_case=False) for q in to_raise],
            x_request_id="<no failing request was made>",
            cluster=self.dps_client._config.cdf_cluster,
            project=self.dps_client._config.project,
        )

    async def _raise_if_unknown_failed(
        self, unknown_failed: list[BaseException], futures_dct: dict[asyncio.Task, Any] | None = None
    ) -> None:
        if not unknown_failed:
            return
        if futures_dct:
            await self._cancel_remaining(futures_dct)
        # Typically this happens when asking for aggregates for a string time series.
        # We don't do anything fancy like ExceptionGroup (Python 3.11+), just raise the first:
        raise unknown_failed[0]

    @staticmethod
    async def _cancel_remaining(futures_dct: dict[asyncio.Task, Any]) -> None:
        # We already know we're going to raise so we cancel any outstanding task:
        for task in futures_dct:
            task.cancel()
        await asyncio.gather(*futures_dct, return_exceptions=True)
        futures_dct.clear()

    @abstractmethod
    def _fetch_all(self, use_numpy: bool) -> AsyncIterator[BaseTaskOrchestrator]:
        raise NotImplementedError


class EagerDpsFetcher(DpsFetchStrategy):
    """A datapoints fetching strategy to make small queries as fast as possible.

    Is used when the number of time series to fetch is smaller than or equal to the number of `concurrency_limit`, so
    that each worker only fetches datapoints for a single time series per request (this maximises throughput
    according to the API docs). This does -not- mean that we assign a time series to each worker! All available
    workers will fetch data for the same time series to speed up fetching. To make this work, the time domain is
    split based on the density of datapoints returned and other heuristics like granularity (e.g. given '1h', at
    most 168 datapoints exist per week).
    """

    async def _fetch_all(self, use_numpy: bool) -> AsyncIterator[BaseTaskOrchestrator]:
        missing_to_raise: set[DatapointsQuery] = set()
        unknown_failed: list[BaseException] = []
        futures_dct, ts_task_lookup = self._create_initial_tasks(use_numpy)

        # Run until all top level tasks are complete:
        while futures_dct:
            done, _ = await asyncio.wait(futures_dct, return_when=asyncio.FIRST_COMPLETED)
            for future in done:  # not guaranteed to be just one done task
                ts_task = (subtask := futures_dct.pop(future)).parent
                res = self._get_result_with_exception_handling(
                    future, ts_task, ts_task_lookup, missing_to_raise, unknown_failed
                )
                if res is None:
                    continue
                elif missing_to_raise or unknown_failed:
                    # We are going to raise anyway, kill task:
                    ts_task.is_done = True  # The 'missing'/failure may come from a different task, so we need this
                    continue

                # We may dynamically split subtasks based on what % of time range was returned:
                if new_subtasks := subtask.store_partial_result(res):
                    self._queue_new_subtasks(futures_dct, new_subtasks)
                if ts_task.is_done:
                    # Reduce peak memory consumption by finalizing as soon as tasks finish:
                    ts_task.finalize_datapoints()
                    continue
                elif subtask.is_done:
                    continue
                # Put the subtask back into the pool:
                self._queue_new_subtasks(futures_dct, [subtask])

            if unknown_failed:
                # Do not break here if there are 'missing_to_raise'; we want to accumulate all missing time series
                # before raising a single CogniteNotFoundError with all identifiers.
                break

        await self._raise_if_unknown_failed(unknown_failed, futures_dct)
        await self._raise_if_missing(missing_to_raise, futures_dct)

        # Return only non-missing time series tasks in correct order given by `all_queries`:
        for task in filter(None, map(ts_task_lookup.get, self.all_queries)):
            yield task

    def _create_initial_tasks(
        self,
        use_numpy: bool,
    ) -> tuple[dict[asyncio.Task, BaseDpsFetchSubtask], dict[DatapointsQuery, BaseTaskOrchestrator]]:
        futures_dct: dict[asyncio.Task, BaseDpsFetchSubtask] = {}
        ts_task_lookup = {}
        for query in self.all_queries:
            ts_task = ts_task_lookup[query] = query.task_orchestrator(query=query, eager_mode=True, use_numpy=use_numpy)
            for subtask in ts_task.split_into_subtasks(self.concurrency_limit, self.n_queries):
                payload = {"items": [subtask.get_next_payload_item()], "ignoreUnknownIds": False}
                future = asyncio.create_task(self._request_datapoints(payload))
                futures_dct[future] = subtask
        return futures_dct, ts_task_lookup

    def _queue_new_subtasks(
        self,
        futures_dct: dict[asyncio.Task, BaseDpsFetchSubtask],
        new_subtasks: Sequence[BaseDpsFetchSubtask],
    ) -> None:
        for subtask in new_subtasks:
            payload = {"items": [subtask.get_next_payload_item()]}
            future = asyncio.create_task(self._request_datapoints(payload))
            futures_dct[future] = subtask

    def _get_result_with_exception_handling(
        self,
        future: asyncio.Task,
        ts_task: BaseTaskOrchestrator,
        ts_task_lookup: dict[DatapointsQuery, BaseTaskOrchestrator],
        missing_to_raise: set[DatapointsQuery],
        unknown_failed: list[BaseException],
    ) -> DataPointListItem | None:
        try:
            return future.result()[0]
        except CogniteAPIError as err:
            # If the error is not "missing ts", we record it to be reraised later:
            if not err.missing or err.code != 400:
                unknown_failed.append(err)
                return None
            # The query decides if we can ignore it. If not, we store it so that we later can
            # raise one exception with -all- missing-non-ignorable time series:
            if not ts_task.query.ignore_unknown_ids:
                missing_to_raise.add(ts_task.query)

            ts_task.is_done = True
            ts_task_lookup.pop(ts_task.query, None)
            return None
        except Exception as err:
            unknown_failed.append(err)
            return None


class ChunkingDpsFetcher(DpsFetchStrategy):
    """A datapoints fetching strategy to make large queries faster through the grouping of more than one
    time series per request.

    The main underlying assumption is that "the more time series are queried, the lower the average density".

    Is used when the number of time series to fetch is larger than the number of `concurrency_limit`. How many
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

    async def _fetch_all(self, use_numpy: bool) -> AsyncIterator[BaseTaskOrchestrator]:
        # The initial tasks are important - as they tell us which time series are missing, which
        # are string, which are sparse... We use this info when we choose the best fetch-strategy.
        ts_task_lookup, missing_to_raise = {}, set()
        initial_query_limits, initial_futures_dct = self._create_initial_tasks()

        # Note to future dev: As of 3.13, we can do 'async for future in asyncio.as_completed(...)' and it
        # will return the Tasks - not coroutines. Thus, we can't do this micro-optimization yet and need to
        # use asyncio.wait instead:
        unknown_failed = []
        done, _ = await asyncio.wait(initial_futures_dct, return_when=asyncio.ALL_COMPLETED)
        for future in done:
            if exc := future.exception():
                # We don't immediately reraise here to avoid 'Task exception was never retrieved' (when multiple fail):
                unknown_failed.append(exc)
                continue

            res_lst = future.result()
            new_ts_tasks, chunk_missing = self._create_ts_tasks_and_handle_missing(
                res_lst, initial_futures_dct.pop(future), initial_query_limits, use_numpy
            )
            missing_to_raise.update(chunk_missing)
            ts_task_lookup.update(new_ts_tasks)

        await self._raise_if_unknown_failed(unknown_failed, initial_futures_dct)
        await self._raise_if_missing(missing_to_raise, initial_futures_dct)

        if ts_tasks_left := self._update_queries_with_new_chunking_limit(ts_task_lookup):
            self._add_to_subtask_pools(
                chain.from_iterable(
                    task.split_into_subtasks(concurrency_limit=self.concurrency_limit, n_tot_queries=len(ts_tasks_left))
                    for task in ts_tasks_left
                )
            )
            futures_dct: dict[asyncio.Task, list[BaseDpsFetchSubtask]] = {}
            await self._queue_new_subtasks(futures_dct)
            await self._fetch_until_complete(futures_dct, unknown_failed)
            await self._raise_if_unknown_failed(unknown_failed, futures_dct)

        # Return only non-missing time series tasks in correct order given by `all_queries`:
        for task in filter(None, map(ts_task_lookup.get, self.all_queries)):
            yield task

    async def _fetch_until_complete(
        self, futures_dct: dict[asyncio.Task, list[BaseDpsFetchSubtask]], unknown_failed: list[BaseException]
    ) -> None:
        while futures_dct:
            done, _ = await asyncio.wait(futures_dct, return_when=asyncio.FIRST_COMPLETED)
            res_lst_all, subtask_lst_all = [], []
            for future in done:  # not guaranteed to be just one done task
                subtask_lst = futures_dct.pop(future)
                try:
                    res_lst = future.result()
                except Exception as exc:
                    unknown_failed.append(exc)
                    continue

                subtask_lst_all.extend(subtask_lst)
                res_lst_all.extend(res_lst)

            if unknown_failed:
                return  # We know we're going to raise, no point in continuing

            for subtask, res in zip(subtask_lst_all, res_lst_all):
                # We may dynamically split subtasks based on what % of time range was returned:
                if new_subtasks := subtask.store_partial_result(res):
                    self._add_to_subtask_pools(new_subtasks)
                if not subtask.is_done:
                    self._add_to_subtask_pools([subtask])

            # Check each ts task in current batch once if finished:
            for ts_task in {sub.parent for sub in subtask_lst_all}:
                if ts_task.is_done:
                    ts_task.finalize_datapoints()
            await self._queue_new_subtasks(futures_dct)

    def _create_initial_tasks(
        self,
    ) -> tuple[dict[DatapointsQuery, int], dict[asyncio.Task, tuple[list[DatapointsQuery], list[DatapointsQuery]]]]:
        initial_query_limits: dict[DatapointsQuery, int] = {}
        initial_futures_dct: dict[asyncio.Task, tuple[list[DatapointsQuery], list[DatapointsQuery]]] = {}
        # Optimal queries uses the entire worker pool. We may be forced to use more (queue) when we
        # can't fit all individual time series (maxes out at `_FETCH_TS_LIMIT * concurrency_limit`):
        n_queries = max(self.concurrency_limit, math.ceil(self.n_queries / self.dps_client._FETCH_TS_LIMIT))
        for query_chunks in zip(
            split_into_n_parts(self.agg_queries, n=n_queries),
            split_into_n_parts(self.raw_queries, n=n_queries),
            strict=True,
        ):
            if not any(query_chunks):
                break  # Not all workers needed (at least for now)

            # Agg and raw limits are independent in the query, so we max out on both:
            items = []
            for queries, max_lim in zip(query_chunks, (self.dps_client._DPS_LIMIT_AGG, self.dps_client._DPS_LIMIT_RAW)):
                maxed_limits = self._find_initial_query_limits([q.capped_limit for q in queries], max_lim)
                chunk_query_limits = dict(zip(queries, maxed_limits))
                initial_query_limits.update(chunk_query_limits)

                for query, limit in chunk_query_limits.items():
                    (item := query.to_payload_item())["limit"] = limit
                    items.append(item)

            payload = {"items": items, "ignoreUnknownIds": True}
            future = asyncio.create_task(self._request_datapoints(payload))
            initial_futures_dct[future] = query_chunks
        return initial_query_limits, initial_futures_dct

    def _create_ts_tasks_and_handle_missing(
        self,
        res: Sequence[DataPointListItem],
        chunk_queues: tuple[list[DatapointsQuery], list[DatapointsQuery]],
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

    async def _queue_new_subtasks(
        self,
        futures_dct: dict[asyncio.Task, list[BaseDpsFetchSubtask]],
    ) -> None:
        # This may seem silly (to ask the semaphore for unused capacity), but the logic is sound:
        # we want to combine subtasks into requests *as late as possible* for optimal chunking.
        # So, simply put, we wait to create a new request until we know we can schedule it immediately.
        # That leaves room for as many done tasks as possible to have been added to the subtask pools
        # in the meantime (we ofc also check that we have subtasks to schedule).
        while self.semaphore_has_bandwidth(self.semaphore) and any(self.subtask_pools):
            payload, subtask_lst = self._combine_subtasks_into_new_request()
            future = asyncio.create_task(self._request_datapoints(payload))
            futures_dct[future] = subtask_lst
            await asyncio.sleep(0)  # Yield control

    @staticmethod
    def semaphore_has_bandwidth(sem: asyncio.BoundedSemaphore) -> bool:
        # We should ideally not use private attributes, but the semaphore is simple enough and we don't need
        # guarantees - just a hint that it is time to combine time series task into a new "request task" that
        # we can then queue.
        return sem._value > 0

    def _combine_subtasks_into_new_request(self) -> tuple[dict[str, list], list[BaseDpsFetchSubtask]]:
        next_items: list[dict[str, Any]] = []
        next_subtasks: list[BaseDpsFetchSubtask] = []
        fetch_limits = (self.dps_client._DPS_LIMIT_AGG, self.dps_client._DPS_LIMIT_RAW)

        for task_pool, request_max_limit, is_raw in zip(self.subtask_pools, fetch_limits, (False, True)):
            if not task_pool:
                continue
            limit_used = 0  # Dps limit for raw and agg is independent (in the same query)
            while task_pool:
                if len(next_items) + 1 > self.dps_client._FETCH_TS_LIMIT:
                    # Hard limit on N ts, quit immediately (even if below dps limit):
                    payload = {"items": next_items}
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
        return {"items": next_items}, next_subtasks

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
        if tot_raw <= self.concurrency_limit >= len(remaining_tasks) - tot_raw:
            # Number of raw and agg tasks independently <= concurrency_limit, so we're basically doing "eager fetching",
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
        agg_queries: list[DatapointsQuery],
        raw_queries: list[DatapointsQuery],
    ) -> tuple[tuple[list[DatapointsQuery], list[DatapointsQuery]], set[DatapointsQuery]]:
        to_raise = set()
        not_missing = (
            {("id", r.id) for r in res}
            | {("externalId", r.externalId) for r in res}
            | {("instanceId", NodeId(r.instanceId.space, r.instanceId.externalId)) for r in res}
        )
        for query in chain(agg_queries, raw_queries):
            query.is_missing = query.identifier.as_tuple() not in not_missing
            # Only raise for those time series that can't be missing (individually customisable parameter):
            if query.is_missing and not query.ignore_unknown_ids:
                to_raise.add(query)
        agg_queries = [q for q in agg_queries if not q.is_missing]
        raw_queries = [q for q in raw_queries if not q.is_missing]
        return (agg_queries, raw_queries), to_raise


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

    async def insert(self, dps_object_lst: list[dict[str, Any]]) -> None:
        to_insert = self._verify_and_prepare_dps_objects(dps_object_lst)
        if not to_insert:
            return
        # To ensure we stay below the max limit on objects per request, we first chunk based on it:
        # (with 10k limit this is almost always just one chunk)
        tasks = [
            AsyncSDKTask(self._insert_datapoints, task)
            for chunk in split_into_chunks(to_insert, self.ts_limit)
            for task in self._create_payload_tasks(chunk)
        ]
        summary = await execute_async_tasks(tasks)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=itemgetter(0),
            task_list_element_unwrap_fn=IdentifierSequenceCore.extract_identifiers,
        )

    def _verify_and_prepare_dps_objects(
        self, dps_object_lst: list[dict[str, Any]]
    ) -> list[tuple[Identifier, list[_InsertDatapoint]]]:
        dps_to_insert: dict[Identifier, list[_InsertDatapoint]] = defaultdict(list)
        for obj in dps_object_lst:
            if not obj["datapoints"]:
                continue
            identifier = validate_user_input_dict_with_identifier(obj, required_keys={"datapoints"})
            validated_dps = self._parse_and_validate_dps(obj["datapoints"])
            dps_to_insert[identifier].extend(validated_dps)
        return list(dps_to_insert.items())

    def _parse_and_validate_dps(self, dps: Datapoints | DatapointsArray | list[tuple | dict]) -> list[_InsertDatapoint]:
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

    async def _insert_datapoints(self, payload: list[dict[str, Any]]) -> None:
        # Acquire the semaphore before converting to memory-intensive format:
        async with self.dps_client._get_semaphore("write"):
            for dct in payload:
                dct["datapoints"] = [dp.dump() for dp in dct["datapoints"]]
            await self.dps_client._post(
                url_path=self.dps_client._RESOURCE_PATH,
                json={"items": payload},
                headers=None,
                semaphore=None,  # Already holding the semaphore above
            )
        # ...and clean up after insert
        # (needed because a ref preventing gc is held to these until the whole job is done):
        for dct in payload:
            dct["datapoints"].clear()

    @staticmethod
    def _split_datapoints(lst: list[_T], n_first: int, n: int) -> Iterator[tuple[list[_T], bool]]:
        # Returns chunks with a boolean answering "are we there yet"
        chunk = lst[:n_first]
        yield chunk, len(chunk) == n_first

        for i in range(n_first, len(lst), n):
            chunk = lst[i : i + n]
            yield chunk, len(chunk) == n

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
        id: int | LatestDatapointQuery | Sequence[int | LatestDatapointQuery] | None,
        external_id: str | LatestDatapointQuery | SequenceNotStr[str | LatestDatapointQuery] | None,
        instance_id: NodeId | LatestDatapointQuery | Sequence[NodeId | LatestDatapointQuery] | None,
        before: int | str | datetime.datetime | None,
        target_unit: str | None,
        target_unit_system: str | None,
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

        self.settings_before: dict[tuple[str, int], int | str | datetime.datetime | None] = {}
        self.settings_target_unit: dict[tuple[str, int], str | None] = {}
        self.settings_target_unit_system: dict[tuple[str, int], str | None] = {}
        self.settings_include_status: dict[tuple[str, int], bool | None] = {}
        self.settings_ignore_bad_datapoints: dict[tuple[str, int], bool | None] = {}
        self.settings_treat_uncertain_as_bad: dict[tuple[str, int], bool | None] = {}

        self.ignore_unknown_ids = ignore_unknown_ids
        self.dps_client = dps_client
        self.semaphore = self.dps_client._get_semaphore("read")

        parsed_ids = cast(int | Sequence[int] | None, self._parse_user_input(id, "id"))
        parsed_xids = cast(str | SequenceNotStr[str] | None, self._parse_user_input(external_id, "external_id"))
        parsed_inst_ids = cast(NodeId | Sequence[NodeId] | None, self._parse_user_input(instance_id, "instance_id"))
        self._is_singleton = IdentifierSequence.load(parsed_ids, parsed_xids, parsed_inst_ids).is_singleton()
        self._all_identifiers = self._prepare_requests(parsed_ids, parsed_xids, parsed_inst_ids)

    @property
    def input_is_singleton(self) -> bool:
        return self._is_singleton

    @staticmethod
    def _get_and_check_identifier(
        query: LatestDatapointQuery,
        identifier_type: Literal["id", "external_id", "instance_id"],
    ) -> int | str:
        if query.identifier.name() != identifier_type:
            raise ValueError(f"Missing '{identifier_type}' from: '{query}'")
        return query.identifier.as_primitive()

    def _parse_user_input(
        self,
        user_input: Any,
        identifier_type: Literal["id", "external_id", "instance_id"],
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
        self,
        parsed_ids: int | Sequence[int] | None,
        parsed_xids: str | SequenceNotStr[str] | None,
        parsed_inst_ids: NodeId | Sequence[NodeId] | None,
    ) -> list[dict]:
        all_ids, all_xids, all_inst_ids = [], [], []
        if parsed_ids is not None:
            all_ids = IdentifierSequence.load(parsed_ids, None).as_dicts()
        if parsed_xids is not None:
            all_xids = IdentifierSequence.load(None, parsed_xids).as_dicts()
        if parsed_inst_ids is not None:
            all_inst_ids = IdentifierSequence.load(None, None, parsed_inst_ids).as_dicts()

        # We want all before = "now" (and those using the same relative time specifiers, like "4d-ago")
        # queries to get the same time domain to fetch:
        frozen_time_now = timestamp_to_ms("now")

        for identifiers, identifier_type in zip(
            [all_ids, all_xids, all_inst_ids], ["id", "external_id", "instance_id"]
        ):
            for i, dct in enumerate(identifiers):
                idx = (identifier_type, i)
                i_before = self.settings_before.get(idx) or self.default_before
                if i_before is None or i_before == "now":
                    dct["before"] = frozen_time_now
                else:
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
                if self.settings_include_status.get(idx) is True or (
                    self.settings_include_status.get(idx) is None and self.default_include_status is True
                ):
                    dct["includeStatus"] = True

                if self.settings_ignore_bad_datapoints.get(idx) is False or (
                    self.settings_ignore_bad_datapoints.get(idx) is None and self.default_ignore_bad_datapoints is False
                ):
                    dct["ignoreBadDataPoints"] = False

                if self.settings_treat_uncertain_as_bad.get(idx) is False or (
                    self.settings_treat_uncertain_as_bad.get(idx) is None
                    and self.default_treat_uncertain_as_bad is False
                ):
                    dct["treatUncertainAsBad"] = False

        all_ids.extend(all_xids)
        all_ids.extend(all_inst_ids)
        return all_ids

    def _post_fix_status_codes_and_stringified_floats_and_add_before(
        self, result: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        # Due to 'ignore_unknown_ids', we can't just zip queries & results and iterate... sadness
        if self.ignore_unknown_ids and len(result) < len(self._all_identifiers):
            # Duplicates can come from different identifier types, but they will have the same 'id':
            duplicate_check = Counter(r["id"] for r in result)
            if dupes := [id_ for id_, count in duplicate_check.items() if count > 1]:
                raise RuntimeError(
                    "When using retrieve_latest (datapoint) with ignore_unknown_ids=True, identifiers must be unique! "
                    "You cannot get around this by passing several of [id, external_id, instance_id] for the same "
                    f"underlying time series. Duplicates: {dupes}."
                )
            ids_exists = (
                {("id", r["id"]) for r in result}
                .union({("xid", r.get("externalId")) for r in result})
                .union({("inst_id", NodeId._load_if(r.get("instanceId"))) for r in result})
                .difference({("xid", None), ("inst_id", None)})
            )  # fmt: skip
            self._all_identifiers = [
                query
                for query in self._all_identifiers
                if ids_exists.intersection(
                    (
                        ("id", query.get("id")),
                        ("xid", query.get("externalId")),
                        ("inst_id", NodeId._load_if(query.get("instanceId"))),
                    )
                )
            ]
        for query, res in zip(self._all_identifiers, result):
            res["before"] = query["before"]
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

    async def fetch_datapoints(self) -> list[dict[str, Any]]:
        tasks = [
            AsyncSDKTask(
                self.dps_client._post,
                url_path=self.dps_client._RESOURCE_PATH + "/latest",
                json={"items": chunk, "ignoreUnknownIds": self.ignore_unknown_ids},
                semaphore=self.semaphore,
            )
            for chunk in split_into_chunks(self._all_identifiers, self.dps_client._RETRIEVE_LATEST_LIMIT)
        ]
        tasks_summary = await execute_async_tasks(tasks)
        tasks_summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=unpack_items_in_payload,
            task_list_element_unwrap_fn=IdentifierSequenceCore.extract_identifiers,
        )
        result = tasks_summary.joined_results(unpack_items)
        return self._post_fix_status_codes_and_stringified_floats_and_add_before(result)
