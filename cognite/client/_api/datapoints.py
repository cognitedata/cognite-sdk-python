import contextlib
import functools
import heapq
import itertools
import math
import statistics
import time
import warnings
from abc import ABC, abstractmethod
from collections.abc import Mapping
from concurrent.futures import CancelledError
from copy import copy
from datetime import datetime
from itertools import chain

from cognite.client._api.datapoint_tasks import (
    DPS_LIMIT,
    DPS_LIMIT_AGG,
    BaseDpsFetchSubtask,
    CustomDatapoints,
    DatapointsPayload,
    _DatapointsQuery,
    _SingleTSQueryBase,
    _SingleTSQueryValidator,
)
from cognite.client._api.synthetic_time_series import SyntheticDatapointsAPI
from cognite.client._api_client import APIClient
from cognite.client.data_classes.datapoints import (
    Datapoints,
    DatapointsArray,
    DatapointsArrayList,
    DatapointsList,
    LatestDatapointQuery,
)
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils._auxiliary import (
    assert_type,
    find_duplicates,
    import_legacy_protobuf,
    local_import,
    split_into_chunks,
    split_into_n_parts,
    validate_user_input_dict_with_identifier,
)
from cognite.client.utils._concurrency import collect_exc_info_and_raise, execute_tasks, get_priority_executor
from cognite.client.utils._identifier import Identifier, IdentifierSequence
from cognite.client.utils._time import timestamp_to_ms

if not import_legacy_protobuf():
    from cognite.client._proto.data_point_list_response_pb2 import DataPointListResponse
else:
    from cognite.client._proto_legacy.data_point_list_response_pb2 import DataPointListResponse

POST_DPS_OBJECTS_LIMIT = 10000
FETCH_TS_LIMIT = 100
RETRIEVE_LATEST_LIMIT = 100
TSQueryList = List[_SingleTSQueryBase]
PoolSubtaskType = Tuple[(int, float, float, BaseDpsFetchSubtask)]
T = TypeVar("T")
TResLst = TypeVar("TResLst", DatapointsList, DatapointsArrayList)


def select_dps_fetch_strategy(dps_client, user_query):
    max_workers = dps_client._config.max_workers
    if max_workers < 1:
        raise RuntimeError(f"Invalid option for `max_workers={max_workers}`. Must be at least 1")
    all_queries = _SingleTSQueryValidator(user_query).validate_and_create_single_queries()
    (agg_queries, raw_queries) = split_queries_into_raw_and_aggs(all_queries)
    if len(all_queries) <= max_workers:
        return EagerDpsFetcher(dps_client, all_queries, agg_queries, raw_queries, max_workers)
    return ChunkingDpsFetcher(dps_client, all_queries, agg_queries, raw_queries, max_workers)


def split_queries_into_raw_and_aggs(all_queries):
    split_qs: Tuple[(TSQueryList, TSQueryList)] = ([], [])
    for query in all_queries:
        split_qs[query.is_raw_query].append(query)
    return split_qs


class DpsFetchStrategy(ABC):
    def __init__(self, dps_client, all_queries, agg_queries, raw_queries, max_workers):
        self.dps_client = dps_client
        self.all_queries = all_queries
        self.agg_queries = agg_queries
        self.raw_queries = raw_queries
        self.max_workers = max_workers
        self.n_queries = len(all_queries)
        with contextlib.suppress(ImportError):
            from google.protobuf.descriptor import _USE_C_DESCRIPTORS

            if _USE_C_DESCRIPTORS is False:
                warnings.warn(
                    "Your installation of 'protobuf' is missing compiled C binaries, and will run in pure-python mode, which causes datapoints fetching to be ~5x slower. To verify, set the environment variable `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=cpp` before running (this will cause the code to fail). The easiest fix is probably to pin your 'protobuf' dependency to major version 4 (or higher), see: https://developers.google.com/protocol-buffers/docs/news/2022-05-06#python-updates",
                    UserWarning,
                    stacklevel=3,
                )

    def fetch_all_datapoints(self):
        with get_priority_executor(max_workers=self.max_workers) as pool:
            ordered_results = self._fetch_all(pool, use_numpy=False)
        return self._finalize_tasks(ordered_results, resource_lst=DatapointsList)

    def fetch_all_datapoints_numpy(self):
        with get_priority_executor(max_workers=self.max_workers) as pool:
            ordered_results = self._fetch_all(pool, use_numpy=True)
        return self._finalize_tasks(ordered_results, resource_lst=DatapointsArrayList)

    def _finalize_tasks(self, ordered_results, resource_lst):
        return resource_lst(
            [ts_task.get_result() for ts_task in ordered_results], cognite_client=self.dps_client._cognite_client
        )

    def _make_dps_request_using_protobuf(self, payload):
        return self.dps_client._do_request(
            json=payload,
            method="POST",
            url_path=(self.dps_client._RESOURCE_PATH + "/list"),
            accept="application/protobuf",
            timeout=self.dps_client._config.timeout,
        ).content

    def _request_datapoints(self, payload):
        res = DataPointListResponse()
        res.MergeFromString(self._make_dps_request_using_protobuf(payload))
        return res.items

    @abstractmethod
    def _fetch_all(self, pool, use_numpy):
        raise NotImplementedError


class EagerDpsFetcher(DpsFetchStrategy):
    def __request_datapoints_jit(self, task, payload=None):
        item = task.get_next_payload()
        if item is None:
            return None
        dps_payload: DatapointsPayload = cast(DatapointsPayload, (copy(payload) or {}))
        dps_payload["items"] = [item]
        return self._request_datapoints(dps_payload)

    def _fetch_all(self, pool, use_numpy):
        (futures_dct, ts_task_lookup) = self._create_initial_tasks(pool, use_numpy)
        while futures_dct:
            future = next(pool.as_completed(futures_dct))
            subtask = futures_dct.pop(future)
            ts_task = subtask.parent
            res = self._get_result_with_exception_handling(future, ts_task, ts_task_lookup, futures_dct)
            if res is None:
                continue
            new_subtasks = subtask.store_partial_result(res)
            if new_subtasks:
                self._queue_new_subtasks(pool, futures_dct, new_subtasks)
            if ts_task.is_done:
                if all(parent.is_done for parent in ts_task_lookup.values()):
                    pool.shutdown(wait=False)
                    break
                if ts_task.has_limit:
                    self._cancel_futures_for_finished_ts_task(ts_task, futures_dct)
                continue
            elif subtask.is_done:
                continue
            self._queue_new_subtasks(pool, futures_dct, [subtask])
        return list(filter(None, map(ts_task_lookup.get, self.all_queries)))

    def _create_initial_tasks(self, pool, use_numpy):
        futures_dct: Dict[(Future, BaseDpsFetchSubtask)] = {}
        (ts_task_lookup, payload) = ({}, {"ignoreUnknownIds": False})
        for query in self.all_queries:
            ts_task = ts_task_lookup[query] = query.ts_task_type(query=query, eager_mode=True, use_numpy=use_numpy)
            for subtask in ts_task.split_into_subtasks(self.max_workers, self.n_queries):
                future = pool.submit(self.__request_datapoints_jit, subtask, payload, priority=subtask.priority)
                futures_dct[future] = subtask
        return (futures_dct, ts_task_lookup)

    def _queue_new_subtasks(self, pool, futures_dct, new_subtasks):
        for task in new_subtasks:
            future = pool.submit(self.__request_datapoints_jit, task, priority=task.priority)
            futures_dct[future] = task

    def _get_result_with_exception_handling(self, future, ts_task, ts_task_lookup, futures_dct):
        try:
            res = future.result()
            if res is not None:
                return res[0]
            return None
        except CancelledError:
            return None
        except CogniteAPIError as e:
            future._exception = None
            if not ((e.code == 400) and e.missing and ts_task.query.ignore_unknown_ids):
                collect_exc_info_and_raise([e])
            elif ts_task.is_done:
                return None
            ts_task.is_done = True
            del ts_task_lookup[ts_task.query]
            self._cancel_futures_for_finished_ts_task(ts_task, futures_dct)
            return None

    def _cancel_futures_for_finished_ts_task(self, ts_task, futures_dct):
        for (future, subtask) in futures_dct.copy().items():
            if subtask.parent is ts_task:
                future.cancel()
                del futures_dct[future]


class ChunkingDpsFetcher(DpsFetchStrategy):
    def __init__(self, *args):
        super().__init__(*args)
        self.counter = itertools.count()
        self.raw_subtask_pool: List[PoolSubtaskType] = []
        self.agg_subtask_pool: List[PoolSubtaskType] = []
        self.subtask_pools = (self.agg_subtask_pool, self.raw_subtask_pool)

    def _fetch_all(self, pool, use_numpy):
        (ts_task_lookup, missing_to_raise) = ({}, set())
        (initial_query_limits, initial_futures_dct) = self._create_initial_tasks(pool)
        for future in pool.as_completed(initial_futures_dct):
            res_lst = future.result()
            (chunk_agg_qs, chunk_raw_qs) = initial_futures_dct.pop(future)
            (new_ts_tasks, chunk_missing) = self._create_ts_tasks_and_handle_missing(
                res_lst, chunk_agg_qs, chunk_raw_qs, initial_query_limits, use_numpy
            )
            missing_to_raise.update(chunk_missing)
            ts_task_lookup.update(new_ts_tasks)
        if missing_to_raise:
            raise CogniteNotFoundError(not_found=[q.identifier.as_dict(camel_case=False) for q in missing_to_raise])
        ts_tasks_left = self._update_queries_with_new_chunking_limit(ts_task_lookup)
        if ts_tasks_left:
            self._add_to_subtask_pools(
                chain.from_iterable(
                    task.split_into_subtasks(max_workers=self.max_workers, n_tot_queries=len(ts_tasks_left))
                    for task in ts_tasks_left
                )
            )
            futures_dct: Dict[(Future, List[BaseDpsFetchSubtask])] = {}
            self._queue_new_subtasks(pool, futures_dct)
            self._fetch_until_complete(pool, futures_dct, ts_task_lookup)
        return list(filter(None, map(ts_task_lookup.get, self.all_queries)))

    def _fetch_until_complete(self, pool, futures_dct, ts_task_lookup):
        while futures_dct:
            future = next(pool.as_completed(futures_dct))
            (res_lst, subtask_lst) = (future.result(), futures_dct.pop(future))
            for (subtask, res) in zip(subtask_lst, res_lst):
                new_subtasks = subtask.store_partial_result(res)
                if new_subtasks:
                    self._add_to_subtask_pools(new_subtasks)
                if not subtask.is_done:
                    self._add_to_subtask_pools([subtask])
            done_ts_tasks = {sub.parent for sub in subtask_lst if sub.parent.is_done}
            if done_ts_tasks:
                self._cancel_subtasks(done_ts_tasks)
            self._queue_new_subtasks(pool, futures_dct)
            if all(task.is_done for task in ts_task_lookup.values()):
                pool.shutdown(wait=False)
                return None

    def _create_initial_tasks(self, pool):
        initial_query_limits: Dict[(_SingleTSQueryBase, int)] = {}
        initial_futures_dct: Dict[(Future, Tuple[(TSQueryList, TSQueryList)])] = {}
        n_queries = max(self.max_workers, math.ceil(self.n_queries / FETCH_TS_LIMIT))
        splitter: Callable[([List[T]], Iterator[List[T]])] = functools.partial(split_into_n_parts, n=n_queries)
        for query_chunks in zip(splitter(self.agg_queries), splitter(self.raw_queries)):
            items = []
            for (queries, max_lim) in zip(query_chunks, [DPS_LIMIT_AGG, DPS_LIMIT]):
                maxed_limits = self._find_initial_query_limits([q.capped_limit for q in queries], max_lim)
                chunk_query_limits = dict(zip(queries, maxed_limits))
                initial_query_limits.update(chunk_query_limits)
                items.extend([{**q.to_payload(), "limit": lim} for (q, lim) in chunk_query_limits.items()])
            payload = {"ignoreUnknownIds": True, "items": items}
            future = pool.submit(self._request_datapoints, payload, priority=0)
            initial_futures_dct[future] = query_chunks
        return (initial_query_limits, initial_futures_dct)

    def _create_ts_tasks_and_handle_missing(self, res, chunk_agg_qs, chunk_raw_qs, initial_query_limits, use_numpy):
        if len(res) == (len(chunk_agg_qs) + len(chunk_raw_qs)):
            to_raise: Set[_SingleTSQueryBase] = set()
        else:
            (chunk_agg_qs, chunk_raw_qs, to_raise) = self._handle_missing_ts(res, chunk_agg_qs, chunk_raw_qs)
        ts_tasks = {
            query: query.ts_task_type(
                query=query,
                eager_mode=False,
                use_numpy=use_numpy,
                first_dps_batch=res,
                first_limit=initial_query_limits[query],
            )
            for (res, query) in zip(res, chain(chunk_agg_qs, chunk_raw_qs))
        }
        return (ts_tasks, to_raise)

    def _add_to_subtask_pools(self, new_subtasks):
        for task in new_subtasks:
            n_dps_left = task.get_remaining_limit()
            n_dps_left = math.inf if (n_dps_left is None) else n_dps_left
            limit = min(n_dps_left, task.max_query_limit)
            new_subtask: PoolSubtaskType = (task.priority, limit, next(self.counter), task)
            heapq.heappush(self.subtask_pools[task.is_raw_query], new_subtask)

    def _queue_new_subtasks(self, pool, futures_dct):
        while (pool._work_queue.qsize() == 0) and any(self.subtask_pools):
            new_request = self._combine_subtasks_into_new_request()
            if new_request is None:
                return
            (payload, subtask_lst, priority) = new_request
            future = pool.submit(self._request_datapoints, payload, priority=priority)
            futures_dct[future] = subtask_lst
            time.sleep(0.0001)

    def _combine_subtasks_into_new_request(self):
        next_items: List[CustomDatapoints] = []
        next_subtasks: List[BaseDpsFetchSubtask] = []
        (agg_pool, raw_pool) = self.subtask_pools
        for (task_pool, request_max_limit, is_raw) in zip(
            self.subtask_pools, (DPS_LIMIT_AGG, DPS_LIMIT), [False, True]
        ):
            if not task_pool:
                continue
            limit_used = 0
            while task_pool:
                if (len(next_items) + 1) > FETCH_TS_LIMIT:
                    priority = statistics.mean(task.priority for task in next_subtasks)
                    payload: DatapointsPayload = {"items": next_items}
                    return (payload, next_subtasks, priority)
                (*_, next_task) = task_pool[0]
                next_payload = next_task.get_next_payload()
                if (next_payload is None) or next_task.is_done:
                    heapq.heappop(task_pool)
                    continue
                next_limit = next_payload["limit"]
                if (limit_used + next_limit) <= request_max_limit:
                    next_items.append(next_payload)
                    next_subtasks.append(next_task)
                    limit_used += next_limit
                    heapq.heappop(task_pool)
                else:
                    break
        if next_items:
            priority = statistics.mean(task.priority for task in next_subtasks)
            payload = {"items": next_items}
            return (payload, next_subtasks, priority)
        return None

    @staticmethod
    def _decide_individual_query_limit(query, ts_task):
        (batch_start, batch_end) = (ts_task.start_ts_first_batch, ts_task.end_ts_first_batch)
        est_remaining_dps = (ts_task.n_dps_first_batch * (query.end - batch_end)) / (batch_end - batch_start)
        max_limit = query.max_query_limit
        if est_remaining_dps > (5 * max_limit):
            return max_limit
        for chunk_size in (1, 2, 4, 8, 16, 32):
            if est_remaining_dps > (max_limit // chunk_size):
                return max_limit // (2 * chunk_size)
        return max_limit // FETCH_TS_LIMIT

    def _update_queries_with_new_chunking_limit(self, ts_task_lookup):
        remaining_tasks = {q: t for (q, t) in ts_task_lookup.items() if (not t.is_done)}
        tot_raw = sum(q.is_raw_query for q in remaining_tasks)
        if tot_raw <= self.max_workers >= (len(remaining_tasks) - tot_raw):
            return list(remaining_tasks.values())
        for (query, ts_task) in remaining_tasks.items():
            est_limit = self._decide_individual_query_limit(query, ts_task)
            query.override_max_query_limit(est_limit)
        return list(remaining_tasks.values())

    def _cancel_subtasks(self, done_ts_tasks):
        for ts_task in done_ts_tasks:
            for subtask in ts_task.subtasks:
                subtask.is_done = True

    @staticmethod
    def _find_initial_query_limits(limits, max_limit):
        actual_lims = [0] * len(limits)
        not_done = set(range(len(limits)))
        while not_done:
            part = max_limit // len(not_done)
            if not part:
                break
            rm_idx = set()
            for i in not_done:
                i_part = min(part, limits[i])
                actual_lims[i] += i_part
                max_limit -= i_part
                if i_part == limits[i]:
                    rm_idx.add(i)
                else:
                    limits[i] -= i_part
            not_done -= rm_idx
        return actual_lims

    @staticmethod
    def _handle_missing_ts(res, agg_queries, raw_queries):
        to_raise = set()
        not_missing = {("id", r.id) for r in res}.union(("externalId", r.externalId) for r in res)
        for query in chain(agg_queries, raw_queries):
            query.is_missing = query.identifier.as_tuple() not in not_missing
            if query.is_missing and (not query.ignore_unknown_ids):
                to_raise.add(query)
        agg_queries = [q for q in agg_queries if (not q.is_missing)]
        raw_queries = [q for q in raw_queries if (not q.is_missing)]
        return (agg_queries, raw_queries, to_raise)


class DatapointsAPI(APIClient):
    _RESOURCE_PATH = "/timeseries/data"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.synthetic = SyntheticDatapointsAPI(
            self._config, api_version=self._api_version, cognite_client=self._cognite_client
        )

    def retrieve(
        self,
        *,
        id: Union[(None, int, Dict[(str, Any)], Sequence[Union[(int, Dict[(str, Any)])]])] = None,
        external_id: Union[(None, str, Dict[(str, Any)], Sequence[Union[(str, Dict[(str, Any)])]])] = None,
        start: Union[(int, str, datetime, None)] = None,
        end: Union[(int, str, datetime, None)] = None,
        aggregates: Union[(str, List[str], None)] = None,
        granularity: Optional[str] = None,
        limit: Optional[int] = None,
        include_outside_points=False,
        ignore_unknown_ids=False,
    ):
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
        elif (not dps_lst) and ignore_unknown_ids:
            return None
        return dps_lst[0]

    def retrieve_arrays(
        self,
        *,
        id: Union[(None, int, Dict[(str, Any)], Sequence[Union[(int, Dict[(str, Any)])]])] = None,
        external_id: Union[(None, str, Dict[(str, Any)], Sequence[Union[(str, Dict[(str, Any)])]])] = None,
        start: Union[(int, str, datetime, None)] = None,
        end: Union[(int, str, datetime, None)] = None,
        aggregates: Union[(str, List[str], None)] = None,
        granularity: Optional[str] = None,
        limit: Optional[int] = None,
        include_outside_points=False,
        ignore_unknown_ids=False,
    ):
        local_import("numpy")
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
        elif (not dps_lst) and ignore_unknown_ids:
            return None
        return dps_lst[0]

    def retrieve_dataframe(
        self,
        *,
        id=None,
        external_id=None,
        start=None,
        end=None,
        aggregates=None,
        granularity=None,
        limit=None,
        include_outside_points=False,
        ignore_unknown_ids=False,
        uniform_index=False,
        include_aggregate_name=True,
        include_granularity_name=False,
        column_names="external_id",
    ):
        (_, pd) = local_import("numpy", "pandas")
        if column_names not in {"id", "external_id"}:
            raise ValueError(f"Given parameter column_names={column_names} must be one of 'id' or 'external_id'")
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
            grans_given = {q.granularity for q in fetcher.all_queries}
            is_limited = any((q.limit is not None) for q in fetcher.all_queries)
            if fetcher.raw_queries or (len(grans_given) > 1) or is_limited:
                raise ValueError(
                    f"Cannot return a uniform index when asking for aggregates with multiple granularities ({grans_given}) OR when (partly) querying raw datapoints OR when a finite limit is used."
                )
        df = fetcher.fetch_all_datapoints_numpy().to_pandas(
            column_names, include_aggregate_name, include_granularity_name
        )
        if not uniform_index:
            return df
        start = pd.Timestamp(min(q.start for q in fetcher.agg_queries), unit="ms")
        end = pd.Timestamp(max(q.end for q in fetcher.agg_queries), unit="ms")
        (granularity,) = grans_given
        freq = cast(str, granularity).replace("m", "T")
        return df.reindex(pd.date_range(start=start, end=end, freq=freq, inclusive="left"))

    def retrieve_latest(self, id=None, external_id=None, before=None, ignore_unknown_ids=False):
        fetcher = RetrieveLatestDpsFetcher(id, external_id, before, ignore_unknown_ids, self)
        res = fetcher.fetch_datapoints()
        if not fetcher.input_is_singleton:
            return DatapointsList._load(res, cognite_client=self._cognite_client)
        elif (not res) and ignore_unknown_ids:
            return None
        return Datapoints._load(res[0], cognite_client=self._cognite_client)

    def insert(self, datapoints, id=None, external_id=None):
        post_dps_object = Identifier.of_either(id, external_id).as_dict()
        dps_to_post: Union[
            (
                Sequence[Dict[(str, Union[(int, float, str, datetime)])]],
                Sequence[Tuple[(Union[(int, float, datetime)], Union[(int, float, str)])]],
            )
        ]
        if isinstance(datapoints, (Datapoints, DatapointsArray)):
            dps_to_post = DatapointsPoster._extract_raw_data_from_dps_container(datapoints)
        else:
            dps_to_post = datapoints
        post_dps_object["datapoints"] = dps_to_post
        dps_poster = DatapointsPoster(self)
        dps_poster.insert([post_dps_object])

    def insert_multiple(self, datapoints):
        for dps_dct in datapoints:
            if isinstance(dps_dct, Mapping) and isinstance(dps_dct["datapoints"], (Datapoints, DatapointsArray)):
                dps_dct["datapoints"] = DatapointsPoster._extract_raw_data_from_dps_container(dps_dct["datapoints"])
        dps_poster = DatapointsPoster(self)
        dps_poster.insert(datapoints)

    def delete_range(self, start, end, id=None, external_id=None):
        start = timestamp_to_ms(start)
        end = timestamp_to_ms(end)
        assert end > start, "end must be larger than start"
        identifier = Identifier.of_either(id, external_id).as_dict()
        delete_dps_object = {**identifier, "inclusiveBegin": start, "exclusiveEnd": end}
        self._delete_datapoints_ranges([delete_dps_object])

    def delete_ranges(self, ranges):
        valid_ranges = []
        for time_range in ranges:
            valid_range = validate_user_input_dict_with_identifier(time_range, required_keys={"start", "end"})
            valid_range.update(
                inclusiveBegin=timestamp_to_ms(time_range["start"]), exclusiveEnd=timestamp_to_ms(time_range["end"])
            )
            valid_ranges.append(valid_range)
        self._delete_datapoints_ranges(valid_ranges)

    def _delete_datapoints_ranges(self, delete_range_objects):
        self._post(url_path=(self._RESOURCE_PATH + "/delete"), json={"items": delete_range_objects})

    def insert_dataframe(self, df, external_id_headers=True, dropna=True):
        (np, pd) = cast(Any, local_import("numpy", "pandas"))
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError(f"DataFrame index must be `pd.DatetimeIndex`, got: {type(df.index)}")
        if df.columns.has_duplicates:
            raise ValueError(f"DataFrame columns must be unique. Duplicated cols: {find_duplicates(df.columns)}.")
        if np.isinf(df.select_dtypes(include="number")).any(axis=None):
            raise ValueError("DataFrame contains one or more (+/-) Infinity. Remove them in order to insert the data.")
        if (not dropna) and df.isna().any(axis=None):
            raise ValueError("DataFrame contains one or more NaNs. Remove them or pass `dropna=True` to insert.")
        dps = []
        idx = df.index.to_numpy("datetime64[ms]").astype(np.int64)
        for (column_id, col) in df.items():
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
    def __init__(self, dps_objects_limit, dps_limit):
        self.dps_objects_limit = dps_objects_limit
        self.dps_limit = dps_limit
        self.current_num_datapoints = 0
        self.dps_object_list: List[dict] = []

    def add(self, dps_object):
        self.current_num_datapoints += len(dps_object["datapoints"])
        self.dps_object_list.append(dps_object)

    def will_fit(self, number_of_dps):
        will_fit_dps = (self.current_num_datapoints + number_of_dps) <= self.dps_limit
        will_fit_dps_objects = (len(self.dps_object_list) + 1) <= self.dps_objects_limit
        return will_fit_dps and will_fit_dps_objects


class DatapointsPoster:
    def __init__(self, client):
        self.client = client
        self.bins: List[DatapointsBin] = []

    def insert(self, dps_object_list):
        valid_dps_object_list = self._validate_dps_objects(dps_object_list)
        binned_dps_object_lists = self._bin_datapoints(valid_dps_object_list)
        self._insert_datapoints_concurrently(binned_dps_object_lists)

    @staticmethod
    def _extract_raw_data_from_dps_container(dps):
        if dps.value is None:
            raise ValueError(
                "Only raw datapoints are supported when inserting data from `Datapoints` or `DatapointsArray`"
            )
        (n_ts, n_dps) = (len(dps.timestamp), len(dps.value))
        if n_ts != n_dps:
            raise ValueError(f"Number of timestamps ({n_ts}) does not match number of datapoints ({n_dps}) to insert")
        if isinstance(dps, Datapoints):
            return cast(List[Tuple[(int, Any)]], list(zip(dps.timestamp, dps.value)))
        ts = dps.timestamp.astype("datetime64[ms]").astype("int64")
        return list(zip(ts.tolist(), dps.value.tolist()))

    @staticmethod
    def _validate_dps_objects(dps_object_list):
        valid_dps_objects = []
        for dps_object in dps_object_list:
            valid_dps_object = cast(
                Dict[(str, Union[(int, str, List[Tuple[(int, Any)]])])],
                validate_user_input_dict_with_identifier(dps_object, required_keys={"datapoints"}),
            )
            valid_dps_object["datapoints"] = DatapointsPoster._validate_and_format_datapoints(dps_object["datapoints"])
            valid_dps_objects.append(valid_dps_object)
        return valid_dps_objects

    @staticmethod
    def _validate_and_format_datapoints(datapoints):
        assert_type(datapoints, "datapoints", [list])
        assert len(datapoints) > 0, "No datapoints provided"
        assert_type(datapoints[0], "datapoints element", [tuple, dict])
        valid_datapoints = []
        if isinstance(datapoints[0], tuple):
            valid_datapoints = [(timestamp_to_ms(t), v) for (t, v) in datapoints]
        elif isinstance(datapoints[0], dict):
            for dp in datapoints:
                dp = cast(Dict[(str, Any)], dp)
                assert "timestamp" in dp, "A datapoint is missing the 'timestamp' key"
                assert "value" in dp, "A datapoint is missing the 'value' key"
                valid_datapoints.append((timestamp_to_ms(dp["timestamp"]), dp["value"]))
        return valid_datapoints

    def _bin_datapoints(self, dps_object_list):
        for dps_object in dps_object_list:
            for i in range(0, len(dps_object["datapoints"]), DPS_LIMIT):
                dps_object_chunk = {k: dps_object[k] for k in ["id", "externalId"] if (k in dps_object)}
                dps_object_chunk["datapoints"] = dps_object["datapoints"][i : (i + DPS_LIMIT)]
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

    def _insert_datapoints_concurrently(self, dps_object_lists):
        tasks = []
        for dps_object_list in dps_object_lists:
            tasks.append((dps_object_list,))
        summary = execute_tasks(self._insert_datapoints, tasks, max_workers=self.client._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=(lambda x: x[0]),
            task_list_element_unwrap_fn=(lambda x: {k: x[k] for k in ["id", "externalId"] if (k in x)}),
        )

    def _insert_datapoints(self, post_dps_objects):
        for it in post_dps_objects:
            it["datapoints"] = [{"timestamp": t, "value": v} for (t, v) in it["datapoints"]]
        self.client._post(url_path=self.client._RESOURCE_PATH, json={"items": post_dps_objects})
        for it in post_dps_objects:
            del it["datapoints"]


class RetrieveLatestDpsFetcher:
    def __init__(self, id, external_id, before, ignore_unknown_ids, dps_client):
        self.before_settings: Dict[(Tuple[(str, int)], Union[(None, int, str, datetime)])] = {}
        self.default_before = before
        self.ignore_unknown_ids = ignore_unknown_ids
        self.dps_client = dps_client
        parsed_ids = cast(Union[(None, int, Sequence[int])], self._parse_user_input(id, "id"))
        parsed_xids = cast(Union[(None, str, Sequence[str])], self._parse_user_input(external_id, "external_id"))
        self._is_singleton = IdentifierSequence.load(parsed_ids, parsed_xids).is_singleton()
        self._all_identifiers = self._prepare_requests(parsed_ids, parsed_xids)

    @property
    def input_is_singleton(self):
        return self._is_singleton

    @staticmethod
    def _get_and_check_identifier(query, identifier_type):
        as_primitive = getattr(query, identifier_type)
        if as_primitive is None:
            raise ValueError(f"Missing '{identifier_type}' from: '{query}'")
        return as_primitive

    def _parse_user_input(self, user_input, identifier_type):
        if user_input is None:
            return None
        elif isinstance(user_input, LatestDatapointQuery):
            as_primitive = self._get_and_check_identifier(user_input, identifier_type)
            self.before_settings[(identifier_type, 0)] = user_input.before
            return as_primitive
        elif isinstance(user_input, MutableSequence):
            user_input = user_input[:]
            for (i, inp) in enumerate(user_input):
                if isinstance(inp, LatestDatapointQuery):
                    as_primitive = self._get_and_check_identifier(inp, identifier_type)
                    self.before_settings[(identifier_type, i)] = inp.before
                    user_input[i] = as_primitive
        return user_input

    def _prepare_requests(self, parsed_ids, parsed_xids):
        (all_ids, all_xids) = ([], [])
        if parsed_ids is not None:
            all_ids = IdentifierSequence.load(parsed_ids, None).as_dicts()
        if parsed_xids is not None:
            all_xids = IdentifierSequence.load(None, parsed_xids).as_dicts()
        for (identifiers, identifier_type) in zip([all_ids, all_xids], ["id", "external_id"]):
            for (i, dct) in enumerate(identifiers):
                i_before = self.before_settings.get((identifier_type, i), self.default_before)
                if "now" != i_before is not None:
                    dct["before"] = timestamp_to_ms(i_before)
        all_ids.extend(all_xids)
        return all_ids

    def fetch_datapoints(self):
        tasks = [
            {
                "url_path": (self.dps_client._RESOURCE_PATH + "/latest"),
                "json": {"items": chunk, "ignoreUnknownIds": self.ignore_unknown_ids},
            }
            for chunk in split_into_chunks(self._all_identifiers, RETRIEVE_LATEST_LIMIT)
        ]
        tasks_summary = execute_tasks(self.dps_client._post, tasks, max_workers=self.dps_client._config.max_workers)
        if tasks_summary.exceptions:
            raise tasks_summary.exceptions[0]
        return tasks_summary.joined_results(lambda res: res.json()["items"])
