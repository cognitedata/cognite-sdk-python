from __future__ import annotations

import dataclasses
import math
import operator as op
import threading
from abc import abstractmethod
from dataclasses import InitVar, dataclass
from itertools import chain
from pprint import pprint
from typing import TYPE_CHECKING, Callable, DefaultDict, Dict, Hashable, Iterable, List, Optional, Tuple, Union

import numpy as np
import numpy.typing as npt
import pandas as pd  # noqa  TODO remove
from sortedcontainers import SortedDict, SortedList

from cognite.client._api.datapoint_constants import (
    DPS_LIMIT,
    DPS_LIMIT_AGG,
    DatapointsFromAPI,
    DatapointsTypes,
    NumpyInt64Array,
)
from cognite.client.data_classes import DatapointsArray
from cognite.client.data_classes.datapoints import SingleTSQuery
from cognite.client.utils._auxiliary import to_camel_case
from cognite.client.utils._identifier import Identifier
from cognite.client.utils._time import granularity_to_ms

LOCK = threading.Lock()

if TYPE_CHECKING:
    pprint("type checkin'")  # TODO: Remove.


class DpsUnpackFns:
    ts = op.itemgetter("timestamp")
    raw_dp = op.itemgetter("value")
    ts_dp_tpl = op.itemgetter("timestamp", "value")
    ts_count_tpl = op.itemgetter("timestamp", "count")
    count = op.itemgetter("count")

    @staticmethod
    def custom_from_aggregates(
        lst: List[str],
    ) -> Callable[[List[Dict[str, DatapointsTypes]]], Tuple[DatapointsTypes, ...]]:
        return op.itemgetter(*map(to_camel_case, lst))


def select_dps_fetching_task_type(query: SingleTSQuery) -> BaseConcurrentTask:
    conditions = (query.is_raw_query, query.limit is None)
    return {  # O'pattern matching, where art thou?
        # Raw numeric or string tasks:
        (True, True): ParallelUnlimitedRawTask,
        (True, False): ParallelLimitedRawTask,
        # Agg. numeric tasks:
        (False, True): ParallelUnlimitedAggTask,
        (False, False): ParallelLimitedAggTask,
    }[conditions]


class DefaultSortedDict(SortedDict):
    def __init__(self, default_factory: Callable = None, /, **kw):
        self.default_factory = default_factory
        super().__init__(**kw)

    def __missing__(self, key):
        self[key] = self.default_factory()
        return self[key]


def dps_container() -> DefaultDict[Hashable, List]:
    """Initialises a new sorted container for datapoints storage"""
    return DefaultSortedDict(list)


def subtask_lst() -> SortedList[BaseDpsFetchSubtask]:
    """Initialises a new sorted list for subtasks"""
    return SortedList(key=op.attrgetter("subtask_idx"))


def create_array_from_dps_container(container: DefaultSortedDict) -> npt.NDArray:
    return np.hstack(list(chain.from_iterable(container.values())))


@dataclass(eq=False)
class BaseDpsFetchSubtask:
    start: int
    end: int
    identifier: Identifier
    parent: BaseConcurrentTask
    priority: int
    is_done: bool = dataclasses.field(default=False, init=False)


@dataclass(eq=False)
class OutsideDpsFetchSubtask(BaseDpsFetchSubtask):
    """Fetches outside points and stores in parent"""

    priority: int = 0  # Should always be 'highest'

    def get_next_payload(self):
        if self.is_done:
            return None
        return self._create_payload_item()

    def _create_payload_item(self):
        return {
            **self.identifier.as_dict(),
            "start": self.start,
            "end": self.end,
            "limit": 0,  # Not a bug; it just returns the outside points
            "includeOutsidePoints": True,
        }

    def store_partial_result(self, res: DatapointsFromAPI):
        if dps := res["datapoints"]:
            self.parent._extract_outside_points(dps)
        self.is_done = True


# TODO: Split into (?):
#       - Unlimited SerialFetchSubtask
#       - Limited SerialFetchSubtask (this can quit if sum of dps of tasks prior >= limit)
@dataclass(eq=False)  # __hash__ cant be inherited (for safety), so we add eq=False for all
class SerialFetchSubtask(BaseDpsFetchSubtask):
    """Fetches datapoints serially until complete, nice and simple. Stores data in parent"""

    limit: Optional[int]
    aggregates: Optional[List[str]]
    granularity: Optional[str]
    subtask_idx: Tuple[int, ...]
    max_query_limit: int
    n_dps_left: float = dataclasses.field(default=math.inf, init=False)
    n_dps_fetched: float = dataclasses.field(default=0, init=False)
    agg_kwargs: Dict[str, Union[str, List[str]]] = dataclasses.field(default_factory=dict, init=False)

    def __post_init__(self):
        if self.limit is not None:
            self.n_dps_left = self.limit
        self.next_start = self.start
        if not self.parent.query.is_raw_query:
            self.agg_kwargs = {"aggregates": self.aggregates, "granularity": self.granularity}

    def get_next_payload(self):
        if self.is_done:
            return None
        if (remaining_limit := self.parent.remaining_limit(self)) == 0:
            # Since last time this task fetched points, earlier tasks have already fetched >= limit dps:
            self.is_done, ts_task = True, self.parent
            with LOCK:  # Keep sorted list `subtasks` from being mutated
                _ = ts_task.is_done  # Trigger a check of parent task
                # Update all subsequent subtasks to "is done":
                i_start = 1 + ts_task.subtasks.index(self)
                for task in ts_task.subtasks[i_start:]:
                    task.is_done = True
            return None
        return self._create_payload_item(remaining_limit or math.inf)

    def _create_payload_item(self, remaining_limit: float):
        return {
            **self.identifier.as_dict(),
            "start": self.next_start,
            "end": self.end,
            "limit": min(remaining_limit, self.n_dps_left, self.max_query_limit),
            **self.agg_kwargs,
        }

    def store_partial_result(self, res: DatapointsFromAPI):
        if self.parent.ts_info is None:
            # In eager mode, first task to complete gets the honor to store ts info:
            self.parent.ts_info = {k: v for k, v in res.items() if k != "datapoints"}
            self.parent.raw_dtype = np.object_ if res["isString"] else np.float64

        if not (dps := res["datapoints"]):
            self.is_done = True
            return

        n, last_ts = len(dps), dps[-1]["timestamp"]
        self.parent._unpack_and_store(self.subtask_idx, dps)
        self._update_state_for_next_payload(last_ts, n)
        if self._is_task_done(n):
            self.is_done = True

    def _update_state_for_next_payload(self, last_ts, n):
        self.next_start = last_ts + self.parent.offset_next  # Move `start` to prepare for next query
        self.n_dps_left -= n
        self.n_dps_fetched += n  # Used to quit limited queries asap

    def _is_task_done(self, n):
        return self.n_dps_left == 0 or n < self.max_query_limit or self.next_start == self.end


@dataclass(eq=False)
class SplittingFetchSubtask(SerialFetchSubtask):
    """Fetches data serially, but splits its time domain ("divide and conquer") based on the density
    of returned datapoints. Stores data in parent"""

    max_splitting_factor: int = 10
    split_subidx: int = dataclasses.field(default=0, init=False)  # Actual value doesnt matter (any int will do)
    prev_start: int = dataclasses.field(default=None, init=False)

    def store_partial_result(self, res: DatapointsFromAPI) -> Optional[List[SplittingFetchSubtask]]:
        self.prev_start = self.next_start
        super().store_partial_result(res)
        if not self.is_done:
            last_ts = res["datapoints"][-1]["timestamp"]
            return self._split_self_into_new_subtasks_if_needed(last_ts)

    def _create_subtasks_idxs(self, n_new_tasks: int) -> Iterable[int]:
        """Since this task may decide to split itself multiple times, we count backwards to keep order
        (we rely on tuple sorting logic). Example using `self.subtask_idx=(4,)`:
        - First split into e.g. 3 parts: (4,-3), (4,-2), (4,-1)
        - Next, split into 2: (4, -5) and (4, -4). These now sort before the first split."""
        end = self.split_subidx
        self.split_subidx -= n_new_tasks
        yield from ((*self.subtask_idx, i) for i in range(self.split_subidx, end))

    def _split_self_into_new_subtasks_if_needed(self, last_ts: int) -> Optional[List[BaseDpsFetchSubtask]]:
        # How many new tasks because of % of time range was fetched?
        tot_ms = self.end - (start := self.prev_start)
        remaining_ms = tot_ms - (part_ms := last_ts - start)
        ratio_retrieved = part_ms / tot_ms
        n_new_pct = math.floor(1 / ratio_retrieved)
        # How many new tasks because of limit left (if limit)?
        n_new_lim = math.inf
        if (remaining_limit := self.parent.remaining_limit(self)) is not None:
            n_new_lim = math.ceil(remaining_limit / self.max_query_limit)
        # We pick strictest criterion:
        n_new_tasks = min(n_new_lim, n_new_pct, self.max_splitting_factor + 1)  # +1 for "self next"
        if n_new_tasks <= 1:  # No point in splitting; no faster than this task just continuing
            return
        # Find a `delta_ms` thats a multiple of granularity in ms (trivial for raw queries).
        # ...we use `ceil` instead of `round` to make sure we "overshoot" `end`:
        delta_ms = self.parent.offset_next * math.ceil(remaining_ms / n_new_tasks / self.parent.offset_next)
        boundaries = [min(self.end, last_ts + delta_ms * i) for i in range(n_new_tasks + 1)]
        self.end = boundaries[1]  # We shift end of 'self' backwards

        split_idxs = self._create_subtasks_idxs(n_new_tasks)
        new_subtasks = [
            dataclasses.replace(self, start=start, end=end, limit=remaining_limit, subtask_idx=idx)  # Returns a copy
            for start, end, idx in zip(boundaries[1:-1], boundaries[2:], split_idxs)
        ]
        self.parent.subtasks.update(new_subtasks)
        return new_subtasks


@dataclass(eq=False)  # __hash__ cant be inherited (for safety), so we add eq=False for all
class BaseConcurrentTask:
    query: SingleTSQuery
    eager_mode: bool
    first_dps_batch: InitVar[DatapointsFromAPI] = None
    first_start: Optional[int] = dataclasses.field(default=None, init=False)
    offset_next: int = dataclasses.field(default=None, init=False)
    ts_info: DatapointsFromAPI = dataclasses.field(default=None, init=False)
    has_limit: Optional[bool] = dataclasses.field(default=None, init=False)
    ts_data: DefaultDict[Tuple[int, ...], List[NumpyInt64Array]] = dataclasses.field(
        default_factory=dps_container, init=False
    )
    dps_data: DefaultDict[Tuple[int, ...], List[npt.NDArray]] = dataclasses.field(
        default_factory=dps_container, init=False
    )
    subtasks: SortedList[BaseDpsFetchSubtask] = dataclasses.field(default_factory=subtask_lst, init=False)
    subtask_outside_points: Optional[BaseDpsFetchSubtask] = dataclasses.field(default=None, init=False)
    _is_done: bool = dataclasses.field(default=False, init=False)

    def __post_init__(self, first_dps_batch):
        self.has_limit = self.query.limit is not None
        # When running large queries (i.e. not "eager"), all time series have a first batch fetched before
        # further subtasks are created. This gives us e.g. outside points (if asked for) and ts info:
        if not self.eager_mode:
            dps = first_dps_batch.pop("datapoints")
            self.ts_info = first_dps_batch  # Store just the ts info
            if not dps:
                self._is_done = True
                return
            self._store_first_batch(dps)

    def split_into_subtasks(self, max_workers: int, n_tot_queries: int) -> List[BaseDpsFetchSubtask]:
        subtasks = self._create_uniformly_split_subtasks(max_workers, n_tot_queries)
        self.subtasks.update(subtasks)
        if self.eager_mode and self.query.include_outside_points:
            # In eager mode we do not get the "first dps batch" to extract outside points from:
            self.subtask_outside_points = OutsideDpsFetchSubtask(
                start=self.query.start,
                end=self.query.end,
                identifier=self.query.identifier,
                parent=self,
            )
            # Append the outside subtask to returned subtasks so that it will get fetched:
            subtasks.append(self.subtask_outside_points)
        return subtasks

    def _store_first_batch(self, dps):
        if self.query.is_raw_query and self.query.include_outside_points:
            self._extract_outside_points(dps)
            if not dps:  # We might have only gotten outside points
                self._is_done = True
                return

        # Set `start` for the first subtask:
        self.first_start = dps[-1]["timestamp"] + self.offset_next
        if self.first_start == self.query.end:
            self._is_done = True
            return

        self._unpack_and_store((0,), dps)

    def remaining_limit(self, subtask) -> Optional[int]:
        if not self.has_limit:
            return None
        # For limited queries: if the sum of fetched points of earlier tasks have already hit/surpassed
        # `limit`, we know for sure we can cancel future tasks:
        remaining = self.query.limit
        with LOCK:  # Keep sorted list `subtasks` from being mutated
            for task in self.subtasks:
                # Sum up to - but not including - given subtask:
                if task is subtask or (remaining := remaining - task.n_dps_fetched) <= 0:
                    break
        return max(0, remaining)

    @property
    def n_dps_first_batch(self) -> int:
        if self.eager_mode:
            return 0
        return len(self.ts_data[(0,)][0])

    @property
    def is_done(self) -> bool:
        if self.subtasks:
            self._is_done = self._is_done or all(task.is_done for task in self.subtasks)
        return self._is_done

    @is_done.setter
    def is_done(self, value: bool) -> None:
        self._is_done = value

    @abstractmethod
    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, n_workers_per_queries: int) -> int:
        ...

    def _create_uniformly_split_subtasks(self, max_workers: int, n_tot_queries: int) -> List[BaseDpsFetchSubtask]:
        start = self.query.start if self.eager_mode else self.first_start
        tot_ms = (end := self.query.end) - start
        n_workers_per_queries = round(max_workers / n_tot_queries)
        n_periods = self._find_number_of_subtasks_uniform_split(tot_ms, n_workers_per_queries)
        # Find a `delta_ms` thats a multiple of granularity in ms (trivial for raw queries).
        # ...we use `ceil` instead of `round` to make sure we "overshoot" `end`:
        delta_ms = self.offset_next * math.ceil(tot_ms / n_periods / self.offset_next)
        boundaries = [min(end, start + delta_ms * i) for i in range(n_periods + 1)]
        limit = self.query.limit - self.n_dps_first_batch if self.has_limit else None
        # Limited queries will be prioritised in chrono. order (to quit as early as possible):
        return [
            SplittingFetchSubtask(
                start=start,
                end=end,
                identifier=self.query.identifier,
                parent=self,
                priority=i - 1 if self.has_limit else 0,
                limit=limit,
                aggregates=self.query.aggregates_cc,
                granularity=self.query.granularity,
                subtask_idx=(i,),
                max_query_limit=self.query.max_query_limit,
            )
            for i, (start, end) in enumerate(zip(boundaries[:-1], boundaries[1:]), 1)
        ]


@dataclass(eq=False)
class ConcurrentLimitedMixin(BaseConcurrentTask):
    @property
    def is_done(self):
        if self._is_done:
            return True
        elif self.subtask_outside_points and not self.subtask_outside_points.is_done:
            return False
        elif self.subtasks:
            # Checking if subtasks are done is not enough; we need to check if the sum of
            # "len of dps takewhile is_done" has reached the limit. Additionally, each subtask might
            # need to fetch a lot of the time subdomain. We want to quit early also when the limit is
            # reached in the first (chronologically) non-finished subtask:
            i_first_in_progress = True
            n_dps_to_fetch = self.query.limit - self.n_dps_first_batch
            for i, task in enumerate(self.subtasks):
                if not (task.is_done or i_first_in_progress):
                    break
                if i_first_in_progress:
                    i_first_in_progress = False

                n_dps_to_fetch -= task.n_dps_fetched
                if n_dps_to_fetch == 0:
                    self._is_done = True
                    # Update all consecutive subtasks to "is done":
                    for task in self.subtasks[i + 1 :]:
                        task.is_done = True
                    break
                # Stop forward search as current task is not done, and limit was not reached:
                # (We risk that the next task is already done, and will thus miscount)
                if not i_first_in_progress:
                    break
            else:
                # All subtasks are done, but limit was -not- reached:
                self._is_done = True
        return self._is_done

    @is_done.setter
    def is_done(self, value: bool):  # Kill switch
        self._is_done = value


@dataclass(eq=False)
class BaseConcurrentRawTask(BaseConcurrentTask):
    offset_next: int = dataclasses.field(default=1, init=False)  # ms
    raw_dtype: np.dtype = dataclasses.field(default=None, init=False)
    dp_outside_start: Optional[Tuple[int, Union[float, str]]] = dataclasses.field(default=None, init=False)
    dp_outside_end: Optional[Tuple[int, Union[float, str]]] = dataclasses.field(default=None, init=False)

    def _create_empty_result(self) -> DatapointsArray:
        return DatapointsArray._load(
            {**self.ts_info, "timestamp": np.array([], dtype=np.int64), "value": np.array([], dtype=self.raw_dtype)}
        )

    def get_result(self) -> DatapointsArray:
        if not self.ts_data or self.query.limit == 0:
            return self._create_empty_result()
        if self.has_limit:
            self._cap_dps_at_limit()
        if self.query.include_outside_points:
            self._include_outside_points_in_result()
        return DatapointsArray._load(
            {
                **self.ts_info,
                "timestamp": create_array_from_dps_container(self.ts_data),
                "value": create_array_from_dps_container(self.dps_data),
            }
        )

    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, n_workers_per_queries: int) -> int:
        # It makes no sense to split beyond what the max-size of a query allows (for a maximally dense
        # time series), but that is rarely useful as 100k dps is just 1 min 40 sec... we guess an
        # average density of points at 1 dp/sec, giving us split-windows no smaller than ~1 day:
        return min(n_workers_per_queries, math.ceil((tot_ms / 1000) / self.query.max_query_limit))

    def _cap_dps_at_limit(self):
        # Note 1: Outside points do not count towards given limit (API specs)
        # Note 2: Lock not needed; called after pool is shut down
        count = 0
        for i, (subtask_idx, sublist) in enumerate(self.ts_data.items()):
            for j, arr in enumerate(sublist):
                if count + len(arr) < self.query.limit:
                    count += len(arr)
                    continue
                end = self.query.limit - count
                self.ts_data[subtask_idx][j] = arr[:end]
                self.dps_data[subtask_idx][j] = self.dps_data[subtask_idx][j][:end]
                # Chop off later arrays in same sublist (if any):
                self.ts_data[subtask_idx] = self.ts_data[subtask_idx][: j + 1]
                self.dps_data[subtask_idx] = self.dps_data[subtask_idx][: j + 1]
                # Remove later sublists (if any). We keep using DefaultSortedDicts due to the possibility of
                # having to insert/add 'outside points' later:
                (new_ts := dps_container()).update(self.ts_data.items()[: i + 1])
                (new_dps := dps_container()).update(self.dps_data.items()[: i + 1])
                self.ts_data, self.dps_data = new_ts, new_dps
                return

    def _include_outside_points_in_result(self):
        if self.dp_outside_start:
            start_ts, start_dp = self.dp_outside_start
            self.ts_data[(-math.inf,)].append([start_ts])
            self.dps_data[(-math.inf,)].append([start_dp])
        if self.dp_outside_end:
            end_ts, end_dp = self.dp_outside_end
            self.ts_data[(math.inf,)].append([end_ts])
            self.dps_data[(math.inf,)].append([end_dp])

    def _unpack_and_store(self, idx, dps):
        # Faster than feeding listcomp to np.array:
        self.ts_data[idx].append(np.fromiter(map(DpsUnpackFns.ts, dps), dtype=np.int64, count=len(dps)))
        self.dps_data[idx].append(np.fromiter(map(DpsUnpackFns.raw_dp, dps), dtype=self.raw_dtype, count=len(dps)))

    def _extract_outside_points(self, dps):
        first_dp = dps[0]
        if first_dp["timestamp"] < self.query.start:
            # We got a dp before `start`, this should not impact our count towards `limit`:
            self.dp_outside_start = DpsUnpackFns.ts_dp_tpl(dps.pop(0))  # Slow pop :(
        if dps:
            last_dp = dps[-1]
            if last_dp["timestamp"] >= self.query.end:  # >= because `end` is exclusive
                self.dp_outside_end = DpsUnpackFns.ts_dp_tpl(dps.pop())  # Fast pop :)


@dataclass(eq=False)
class ParallelUnlimitedRawTask(BaseConcurrentRawTask):
    pass


@dataclass(eq=False)
class ParallelLimitedRawTask(ConcurrentLimitedMixin, BaseConcurrentRawTask):
    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, n_workers_per_queries: int) -> int:
        # We make the guess that the time series has ~1 dp/sec and use this in combination with the given
        # limit to not split into too many queries (highest throughput when each request is close to max limit)
        n_estimate_periods = math.ceil((tot_ms / 1000) / self.query.max_query_limit)
        remaining_limit = self.query.limit - self.n_dps_first_batch
        n_periods = math.ceil(remaining_limit / self.query.max_query_limit)
        # Pick the smallest N from constraints:
        return min(n_workers_per_queries, n_periods, n_estimate_periods)


@dataclass(eq=False)
class BaseConcurrentAggTask(BaseConcurrentTask):
    is_count_query: bool = dataclasses.field(default=None, init=False)
    has_non_count_aggs: bool = dataclasses.field(default=None, init=False)
    agg_unpack_fn: Optional[Callable] = dataclasses.field(default=None, init=False)
    dtype_aggs: Optional[np.dtype] = dataclasses.field(default=None, init=False)
    float_aggs: List[str] = dataclasses.field(default=None, init=False)
    count_data: DefaultDict[Tuple[int, ...], List[NumpyInt64Array]] = dataclasses.field(
        default_factory=dps_container, init=False
    )

    def __post_init__(self, first_dps_batch):
        self.offset_next = granularity_to_ms(self.query.granularity)
        self._set_aggregate_vars()
        super().__post_init__(first_dps_batch)

    def _set_aggregate_vars(self):
        self.float_aggs = self.query.aggregates[:]
        self.is_count_query = "count" in self.float_aggs
        if self.is_count_query:
            self.float_aggs.remove("count")  # Only aggregate that is integer, handle separately

        self.has_non_count_aggs = bool(self.float_aggs)
        if self.has_non_count_aggs:
            self.agg_unpack_fn = DpsUnpackFns.custom_from_aggregates(self.float_aggs)
            if len(self.float_aggs) > 1:
                self.dtype_aggs = np.dtype((np.float64, len(self.float_aggs)))
            else:
                self.dtype_aggs = np.dtype(np.float64)  # (.., 1) is deprecated

    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, n_workers_per_queries: int) -> int:
        n_max_dps = tot_ms // self.offset_next  # evenly divides
        return min(n_workers_per_queries, math.ceil(n_max_dps / DPS_LIMIT_AGG))

    def _create_empty_result(self) -> DatapointsArray:
        arr_dct = {"timestamp": np.array([], dtype=np.int64)}
        if self.is_count_query:
            arr_dct["count"] = np.array([], dtype=np.int64)
        if self.has_non_count_aggs:
            arr_dct.update({agg: np.array([], dtype=np.float64) for agg in self.float_aggs})
        return DatapointsArray._load({**self.ts_info, **arr_dct})

    def get_result(self) -> DatapointsArray:
        if not self.ts_data or self.query.limit == 0:
            return self._create_empty_result()
        if self.has_limit:
            self._cap_dps_at_limit()

        arr_dct = {"timestamp": create_array_from_dps_container(self.ts_data)}
        if self.is_count_query:
            arr_dct["count"] = create_array_from_dps_container(self.count_data)
        if self.has_non_count_aggs:
            arr_lst = np.hsplit(np.vstack(list(chain.from_iterable(self.dps_data.values()))), len(self.float_aggs))
            arr_dct.update(dict(zip(self.float_aggs, map(np.squeeze, arr_lst))))
        return DatapointsArray._load({**self.ts_info, **arr_dct})

    def _cap_dps_at_limit(self):
        count, to_update = 0, ["ts_data"]
        if self.is_count_query:
            to_update.append("count_data")
        if self.has_non_count_aggs:
            to_update.append("dps_data")

        for i, (subtask_idx, sublist) in enumerate(self.ts_data.items()):
            for j, arr in enumerate(sublist):
                if count + len(arr) < self.query.limit:
                    count += len(arr)
                    continue
                end = self.query.limit - count

                for attr in to_update:
                    data = getattr(self, attr)
                    data[subtask_idx][j] = data[subtask_idx][j][:end]
                    data[subtask_idx] = data[subtask_idx][: j + 1]
                    setattr(self, attr, dict(data.items()[: i + 1]))  # regular dict (no further inserts)
                return

    def _unpack_and_store(self, idx, dps):
        n = len(dps)
        self.ts_data[idx].append(np.fromiter(map(DpsUnpackFns.ts, dps), dtype=np.int64, count=n))

        if self.is_count_query:
            self.count_data[idx].append(np.fromiter(map(DpsUnpackFns.count, dps), dtype=np.int64, count=n))

        if self.has_non_count_aggs:
            try:  # Fast method uses multi-key unpacking:
                arr = np.fromiter(map(self.agg_unpack_fn, dps), dtype=self.dtype_aggs, count=n)
            except KeyError:  # An aggregate is missing, fallback to slower `dict.get(agg)`.
                # This can happen when certain aggregates are undefined, e.g. `interpolate` at first interval.
                arr = np.array([tuple(map(dp.get, self.float_aggs)) for dp in dps], dtype=np.float64)
            self.dps_data[idx].append(arr.reshape(n, len(self.float_aggs)))


@dataclass(eq=False)
class ParallelUnlimitedAggTask(BaseConcurrentAggTask):
    pass


@dataclass(eq=False)
class ParallelLimitedAggTask(ConcurrentLimitedMixin, BaseConcurrentAggTask):
    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, n_workers_per_queries: int) -> int:
        remaining_limit = self.query.limit - self.n_dps_first_batch
        n_max_dps = min(remaining_limit, tot_ms // self.offset_next)
        return min(n_workers_per_queries, math.ceil(n_max_dps / DPS_LIMIT_AGG))
