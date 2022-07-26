from __future__ import annotations

import dataclasses
import itertools
import math
import operator as op
from dataclasses import InitVar, dataclass
from functools import cached_property
from pprint import pprint
from typing import TYPE_CHECKING, List, Optional, Tuple

import numpy as np
import numpy.typing as npt

from cognite.client._api.datapoint_constants import DatapointsFromAPI, NumpyInt64Array
from cognite.client.data_classes import DatapointsArray
from cognite.client.data_classes.datapoints import SingleTSQuery
from cognite.client.utils._auxiliary import to_camel_case

if TYPE_CHECKING:
    pprint("type checkin'")  # TODO: Remove.


class DpsUnpackFns:
    ts = op.itemgetter("timestamp")
    raw_dp = op.itemgetter("value")
    ts_dp_tpl = op.itemgetter("timestamp", "value")
    count = op.itemgetter("count")

    @staticmethod
    def custom_aggregates_no_count(lst):
        assert "count" not in lst, "count returns int, cant use in 2d float64 array"
        return op.itemgetter(*map(to_camel_case, lst))


def dps_task_type_selector(query):
    # Note: We could add "include-outside-points" to our conditions, but since
    # the "initial query" already includes all of these (when True) we don't need to!
    conditions = (query.is_string, query.is_raw_query, query.limit is None)
    return {  # O'pattern matching, where art thou?
        # String tasks:
        (True, True, True): ParallelUnlimitedRawStringTask,
        (True, True, False): ParallelLimitedRawStringTask,
        # Raw numeric tasks:
        (False, True, True): ParallelUnlimitedRawNumericTask,
        (False, True, False): ParallelLimitedRawNumericTask,
        # # Agg. numeric tasks:
        # (False, False, True): ParallelUnlimitedAggNumericTask,
        # (False, False, False): ParallelLimitedAggNumericTask,
    }[conditions]


@dataclass(eq=False)  # __hash__ cant be inherited (for safety), so we add eq=False for all
class BaseDpsTask:
    query: SingleTSQuery


@dataclass(eq=False)
class BaseSubtask(BaseDpsTask):
    subtask_idx: int
    parent: BaseConcurrentTask
    priority: int  # Lower numbers have higher priority
    is_done: bool = False


@dataclass(eq=False)
class SubtaskRawSerial(BaseSubtask):
    # Just fetches serially until complete, nice and simple. Stores data in parent
    offset_next: int = 1  # ms

    def __post_init__(self):
        self.n_dps_left = self.query.limit
        if self.n_dps_left is None:
            self.n_dps_left = math.inf
        self.next_start = self.query.start

    def store_partial_result(self, res):
        dps = res["datapoints"]
        if not dps:
            self.is_done = True
            return

        n, last_ts = len(dps), dps[-1]["timestamp"]
        self.parent._unpack_and_store(self.subtask_idx, dps)
        self.update_state_for_next_payload(last_ts, n)
        if self.is_task_done(n):
            self.is_done = True

    def _create_payload_item(self):
        return {
            **self.query.identifier_dct,
            "start": self.next_start,
            "end": self.query.end,
            "limit": min(self.n_dps_left, self.query.max_query_limit),
        }

    def get_next_payload(self):
        if self.is_done:
            return None
        return self._create_payload_item()

    def update_state_for_next_payload(self, last_ts, n):
        self.next_start = last_ts + self.offset_next  # Move `start` to prepare for next query:
        self.n_dps_left -= n

    def is_task_done(self, n):
        return self.n_dps_left == 0 or n < self.query.max_query_limit or self.next_start == self.query.end


@dataclass(eq=False)
class BaseConcurrentTask(BaseDpsTask):
    first_dps_batch: InitVar[DatapointsFromAPI]
    eager_mode: bool
    offset_next: int = 1  # ms
    first_start: Optional[int] = dataclasses.field(default=None, init=False)
    ts_info: DatapointsFromAPI = dataclasses.field(default=None, init=False)
    has_limit: Optional[bool] = dataclasses.field(default=None, init=False)
    ts_lst: List[List[NumpyInt64Array]] = dataclasses.field(default_factory=list, init=False)
    dps_lst: List[List[npt.NDArray]] = dataclasses.field(default_factory=list, init=False)
    dp_outside_start: Optional[Tuple[int, str]] = dataclasses.field(default=None, init=False)
    dp_outside_end: Optional[Tuple[int, str]] = dataclasses.field(default=None, init=False)
    subtasks: List[BaseSubtask] = dataclasses.field(default_factory=list, init=False)
    _is_done: bool = dataclasses.field(default=False, init=False)

    def __post_init__(self, first_dps_batch):
        self.has_limit = self.query.limit is not None
        # The first batch will handle everything we need for queries with `include_outside_points=True`:
        dps = first_dps_batch.pop("datapoints")
        self.ts_info = first_dps_batch  # Store just the ts info
        self._store_first_batch(dps)

    @property
    def is_done(self):
        if self.subtasks:
            self._is_done = self._is_done or all(sub.is_done for sub in self.subtasks)
        return self._is_done

    def get_result(self) -> DatapointsArray:
        if self.has_limit:
            self._cap_dps_at_limit()
        if self.query.include_outside_points:
            self._include_outside_points_in_result()
        return DatapointsArray._load(
            {
                **self.ts_info,
                "timestamp": np.hstack(list(itertools.chain.from_iterable(self.ts_lst))),
                "value": np.hstack(list(itertools.chain.from_iterable(self.dps_lst))),
            }
        )

    def split_task_into_subtasks(self, max_workers, count_agg):
        print("IN: split_task_into_subtasks", self.query.identifier, count_agg is None)
        if count_agg is None:
            # Main mode for string is and fallback for numeric ts missing count aggregate for some reason
            return self.split_task_into_subtasks_uniformly(max_workers)
        # Main mode for numeric ts:
        return self.split_task_into_subtasks_from_count_aggs(count_agg)

    def split_task_into_subtasks_from_count_aggs(self, count_agg):
        # TODO
        breakpoint()

    def _find_number_of_subtasks_uniform_split(self, tot_ms, max_workers) -> int:
        # It makes no sense to split beyond what the max-size of a query allows (for a maximally dense
        # time series), but that is rarely useful as 100k dps is just 1 min 40 sec... we guess an
        # average density of points at 1 dp/sec, giving us split-windows no smaller than ~1 day:
        return min(max_workers, math.ceil((tot_ms / 1000) / self.query.max_query_limit))

    def split_task_into_subtasks_uniformly(self, max_workers):
        start, end = self.first_start, self.query.end
        tot_ms = end - start
        n_periods = self._find_number_of_subtasks_uniform_split(tot_ms, max_workers)
        delta_ms = math.ceil(tot_ms / n_periods)  # Ceil makes sure we "overshoot" end
        # We have no count aggregate to inform us about dps distribution in time, so we split uniformly:
        boundaries = [min(end, start + delta_ms * i) for i in range(n_periods + 1)]
        # Make a separate dps bucket for each subtask:
        self.ts_lst.extend([[] for _ in range(n_periods)])
        self.dps_lst.extend([[] for _ in range(n_periods)])
        for i, (period_start, period_end) in enumerate(zip(boundaries[:-1], boundaries[1:]), 1):
            limit = self.query.limit - self.n_dps_first_batch if self.has_limit else None
            query = SingleTSQuery(**self.query.identifier_dct_sc, start=period_start, end=period_end, limit=limit)
            # Limited queries will be prioritised in chrono. order (to quit as early as possible):
            priority = i - 1 if self.has_limit else 0
            self.subtasks.append(SubtaskRawSerial(query=query, subtask_idx=i, parent=self, priority=priority))
        return self.subtasks

    def _cap_dps_at_limit(self):
        # Note 1: Outside points do not count towards given limit
        # Note 2: Lock not needed; called after pool is shut down
        count = 0
        for i, sublist in enumerate(self.ts_lst):
            for j, arr in enumerate(sublist):
                if count + len(arr) < self.query.limit:
                    count += len(arr)
                    continue
                end = self.query.limit - count
                self.ts_lst[i][j] = arr[:end]
                self.dps_lst[i][j] = self.dps_lst[i][j][:end]
                # Chop off later arrays in same sublist (if any):
                self.ts_lst[i] = self.ts_lst[i][: j + 1]
                self.dps_lst[i] = self.dps_lst[i][: j + 1]
                # Chop off later sublists (if any):
                self.ts_lst = self.ts_lst[: i + 1]
                self.dps_lst = self.dps_lst[: i + 1]
                return

    def _include_outside_points_in_result(self):
        if self.dp_outside_start:
            start_ts, start_dp = self.dp_outside_start
            self.ts_lst.insert(0, [start_ts])
            self.dps_lst.insert(0, [start_dp])
        if self.dp_outside_end:
            end_ts, end_dp = self.dp_outside_end
            self.dp_outside_end
            self.ts_lst.append([end_ts])
            self.dps_lst.append([end_dp])

    def _store_first_batch(self, dps):
        if not dps:
            self._is_done = True
            return

        if self.query.include_outside_points:
            self._extract_outside_points(dps)
            if not dps:  # We might have only gotten outside points
                self._is_done = True
                return

        # Set `start` for the first subtask:
        self.first_start = dps[-1]["timestamp"] + self.offset_next
        if self.first_start == self.query.end:
            self._is_done = True
            return

        # Make room for dps batch 0:
        self.ts_lst.append([])
        self.dps_lst.append([])
        self._unpack_and_store(0, dps)

        # In eager mode, we can infer if we got all dps (we know query limit used):
        if self.eager_mode:
            if self.has_limit:
                if len(dps) <= self.query.limit <= self.query.max_query_limit:
                    self._is_done = True
            else:
                if len(dps) < self.query.max_query_limit:
                    self._is_done = True

    def _unpack_and_store(self, subtask_idx, dps):
        # Faster than feeding listcomp to np.array:
        self.ts_lst[subtask_idx].append(np.fromiter(map(DpsUnpackFns.ts, dps), dtype=np.int64, count=len(dps)))
        self.dps_lst[subtask_idx].append(np.fromiter(map(DpsUnpackFns.raw_dp, dps), dtype=np.object_, count=len(dps)))

    def _extract_outside_points(self, dps):
        first_dp = dps[0]
        if first_dp["timestamp"] < self.query.start:
            # We got a dp before `start`, this should not impact our count towards `limit`:
            self.dp_outside_start = DpsUnpackFns.ts_dp_tpl(dps.pop(0))  # Slow pop :(
        if dps:
            last_dp = dps[-1]
            if last_dp["timestamp"] >= self.query.end:  # >= because `end` is exclusive
                self.dp_outside_end = DpsUnpackFns.ts_dp_tpl(dps.pop())  # Fast pop :)

    @cached_property
    def n_dps_first_batch(self):
        return len(self.ts_lst[0][0])


@dataclass(eq=False)
class ConcurrentUnlimitedRawTask(BaseConcurrentTask):
    pass


ParallelUnlimitedRawStringTask = ConcurrentUnlimitedRawTask
ParallelUnlimitedRawNumericTask = ConcurrentUnlimitedRawTask


@dataclass(eq=False)
class ConcurrentLimitedRawTask(BaseConcurrentTask):
    def _find_number_of_subtasks_uniform_split(self, tot_ms, max_workers) -> int:
        # We make the guess that the time series has ~1 dp/sec and use this in combination with the given
        # limit to not split into too many queries (highest throughput when each request close to max limit)
        n_estimate_periods = math.ceil((tot_ms / 1000) / self.query.max_query_limit)
        remaining_limit = self.query.limit - self.n_dps_first_batch
        n_periods = math.ceil(remaining_limit / self.query.max_query_limit)
        # Pick the smallest N from constraints:
        return min(max_workers, n_periods, n_estimate_periods)

    @property
    def is_done(self):
        if self._is_done:
            return self._is_done
        if self.subtasks:  # No lock needed; this - and storing dps - is only run by the main thread
            # Checking if subtasks are done is not enough; we need to check if the sum of
            # "len of dps takewhile is_done" has reached the limit. Additionally, each subtask might
            # need to fetch a lot of the time subdomain. We want to quit early if the limit is reached
            # in the first (chronologically) non-finished subtask:
            i_first_in_progress = True
            n_dps_to_fetch = self.query.limit - self.n_dps_first_batch
            for i, task in enumerate(self.subtasks):
                if task.is_done:
                    n_dps_to_fetch -= sum(map(len, self.ts_lst[task.subtask_idx]))
                elif i_first_in_progress:
                    n_dps_to_fetch -= sum(map(len, self.ts_lst[task.subtask_idx]))
                    i_first_in_progress = False
                else:
                    break
                if n_dps_to_fetch <= 0:
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
                # All subtasks are done, but limited was -not- reached:
                self._is_done = True
        return self._is_done


ParallelLimitedRawNumericTask = ConcurrentLimitedRawTask
ParallelLimitedRawStringTask = ConcurrentLimitedRawTask


# BELOW: UNFINISHED
# BELOW: UNFINISHED
# BELOW: UNFINISHED
# BELOW: UNFINISHED


@dataclass(eq=False)
class ParallelUnlimitedAggNumericTask:
    query: SingleTSQuery
    first_dps_batch: InitVar[DatapointsFromAPI]

    def __post_init__(self, first_dps_batch):
        pass


@dataclass(eq=False)
class ParallelLimitedAggNumericTask:
    query: SingleTSQuery
    first_dps_batch: InitVar[DatapointsFromAPI]

    def __post_init__(self, first_dps_batch):
        pass
