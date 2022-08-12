from __future__ import annotations

import dataclasses
import math
import operator as op
from abc import abstractmethod
from dataclasses import InitVar, dataclass
from functools import cached_property
from itertools import chain
from pprint import pprint
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import numpy.typing as npt

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
from cognite.client.utils._time import granularity_to_ms

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


def select_dps_fetching_task_type(query):
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
        # Agg. numeric tasks:
        (False, False, True): ParallelUnlimitedAggNumericTask,
        (False, False, False): ParallelLimitedAggNumericTask,
    }[conditions]


@dataclass(eq=False)  # __hash__ cant be inherited (for safety), so we add eq=False for all
class BaseDpsTask:
    query: SingleTSQuery


# TODO: Split into:
#       - Unlimited SerialFetchSubtask
#       - Limited SerialFetchSubtask (this can quit if sum of dps of tasks prior >= limit)
@dataclass(eq=False)
class SerialFetchSubtask(BaseDpsTask):
    # Just fetches serially until complete, nice and simple. Stores data in parent
    subtask_idx: int
    parent: BaseConcurrentTask
    priority: int  # Lower numbers have higher priority
    is_done: bool = False
    agg_kwargs: Dict[str, Union[str, List[str]]] = dataclasses.field(default_factory=dict, init=False)

    def __post_init__(self):
        self.n_dps_left = self.query.limit
        if self.n_dps_left is None:
            self.n_dps_left = math.inf
        self.next_start = self.query.start
        if not self.query.is_raw_query:
            self.agg_kwargs = {"aggregates": self.query.aggregates_cc, "granularity": self.query.granularity}

    def store_partial_result(self, res: DatapointsFromAPI):
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
            **self.agg_kwargs,
        }

    def get_next_payload(self):
        if self.is_done:
            return None
        return self._create_payload_item()

    def update_state_for_next_payload(self, last_ts, n):
        self.next_start = last_ts + self.parent.offset_next  # Move `start` to prepare for next query
        self.n_dps_left -= n

    def is_task_done(self, n):
        return self.n_dps_left == 0 or n < self.query.max_query_limit or self.next_start == self.query.end


@dataclass(eq=False)
class BaseConcurrentTask(BaseDpsTask):
    first_dps_batch: InitVar[DatapointsFromAPI]
    eager_mode: bool
    offset_next: int = dataclasses.field(default=None, init=False)
    first_start: Optional[int] = dataclasses.field(default=None, init=False)
    ts_info: DatapointsFromAPI = dataclasses.field(default=None, init=False)
    has_limit: Optional[bool] = dataclasses.field(default=None, init=False)
    ts_lst: List[List[NumpyInt64Array]] = dataclasses.field(default_factory=list, init=False)
    dps_lst: List[List[npt.NDArray]] = dataclasses.field(default_factory=list, init=False)
    subtasks: List[SerialFetchSubtask] = dataclasses.field(default_factory=list, init=False)
    _is_done: bool = dataclasses.field(default=False, init=False)

    def __post_init__(self, first_dps_batch):
        self.has_limit = self.query.limit is not None
        # The first batch will handle everything we need for queries with `include_outside_points=True`:
        dps = first_dps_batch.pop("datapoints")
        self.ts_info = first_dps_batch  # Store just the ts info
        if not dps:
            self._is_done = True
            return
        self._store_first_batch(dps)

    @property
    def is_done(self):
        if self.subtasks:
            self._is_done = self._is_done or all(sub.is_done for sub in self.subtasks)
        return self._is_done

    @cached_property
    def n_dps_first_batch(self):
        if self.ts_lst and self.ts_lst[0]:
            return len(self.ts_lst[0][0])
        return 0

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

    def split_into_subtasks(
        self,
        max_workers: int,
        count_agg: Optional[DatapointsFromAPI],
        count_agg_payloads: Optional[Dict],
    ) -> List[SerialFetchSubtask]:
        if self.query.is_raw_query and count_agg is not None:
            if dps := count_agg[0]["datapoints"]:
                return self._split_raw_task_into_subtasks_from_counts(dps, count_agg_payloads)
        # Main mode for string ts -and- fallback for numeric ts missing count aggregate (for any reason)
        # and all queries asking for aggregates:
        return self._split_into_subtasks_uniformly(max_workers)

    @abstractmethod
    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, max_workers: int) -> int:
        ...

    def _split_into_subtasks_uniformly(self, max_workers: int) -> List[SerialFetchSubtask]:
        start, end = self.first_start, self.query.end
        tot_ms = end - start
        n_periods = self._find_number_of_subtasks_uniform_split(tot_ms, max_workers)
        # Find a `delta_ms` thats a multiple of granularity in ms (trivial for raw queries).
        # ...we use `ceil` instead of `round` to make sure we "overshoot" `end`:
        delta_ms = self.offset_next * math.ceil(tot_ms / n_periods / self.offset_next)
        boundaries = [min(end, start + delta_ms * i) for i in range(n_periods + 1)]
        # Make a separate dps bucket for each subtask:
        self.ts_lst.extend([] for _ in range(n_periods))
        self.dps_lst.extend([] for _ in range(n_periods))
        for i, (period_start, period_end) in enumerate(zip(boundaries[:-1], boundaries[1:]), 1):
            limit = self.query.limit - self.n_dps_first_batch if self.has_limit else None
            query = SingleTSQuery(
                **self.query.identifier_dct_sc,
                start=period_start,
                end=period_end,
                aggregates=self.query.aggregates,
                granularity=self.query.granularity,
                limit=limit,
            )
            # Limited queries will be prioritised in chrono. order (to quit as early as possible):
            priority = i - 1 if self.has_limit else 0
            self.subtasks.append(SerialFetchSubtask(query=query, subtask_idx=i, parent=self, priority=priority))
        return self.subtasks


@dataclass(eq=False)
class ConcurrentLimitedMixin(BaseConcurrentTask):
    @property
    def is_done(self):
        if self._is_done:
            return self._is_done
        if self.subtasks:  # No lock needed; this - and storing dps - is only run by the main thread
            # Checking if subtasks are done is not enough; we need to check if the sum of
            # "len of dps takewhile is_done" has reached the limit. Additionally, each subtask might
            # need to fetch a lot of the time subdomain. We want to quit early also when the limit is
            # reached in the first (chronologically) non-finished subtask:
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


@dataclass(eq=False)
class BaseConcurrentRawTask(BaseConcurrentTask):
    offset_next: int = dataclasses.field(default=1, init=False)  # ms
    dp_outside_start: Optional[Tuple[int, Union[float, str]]] = dataclasses.field(default=None, init=False)
    dp_outside_end: Optional[Tuple[int, Union[float, str]]] = dataclasses.field(default=None, init=False)

    def _create_empty_result(self) -> DatapointsArray:
        dtype = np.object_ if self.query.is_string else np.float64
        return DatapointsArray._load(
            {**self.ts_info, "timestamp": np.array([], dtype=np.int64), "value": np.array([], dtype=dtype)}
        )

    def get_result(self) -> DatapointsArray:
        if self.has_limit:
            if self.query.limit == 0:
                return self._create_empty_result()
            self._cap_dps_at_limit()

        if self.query.include_outside_points:
            self._include_outside_points_in_result()
        return DatapointsArray._load(
            {
                **self.ts_info,
                "timestamp": np.hstack(list(chain.from_iterable(self.ts_lst))),
                "value": np.hstack(list(chain.from_iterable(self.dps_lst))),
            }
        )

    def _split_raw_task_into_subtasks_from_counts(self, count_dps, count_agg_payloads):
        count_ts, counts = map(list, zip(*map(DpsUnpackFns.ts_count_tpl, count_dps)))
        if count_ts[-1] < self.query.end:
            # Equal when end is aligned with the dynamically chosen gran of the count agg request
            count_ts.append(self.query.end)

        # Find first count interval, i, we are to start from:
        arr = self.ts_lst[0][0]
        n_dps_fetched, latest = len(arr), arr[-1]
        i = next(i for i, period_end in enumerate(count_ts[1:]) if latest < period_end)

        # How many points are already fetched in the first window?
        n_arr_dps = np.sum(count_ts[i] <= arr)
        counts[i] -= n_arr_dps  # ...subtract this amount!
        count_ts[i] = query_start = self.first_start  # ...and move start

        count_gran_in_ms = granularity_to_ms(count_agg_payloads["granularity"])
        current_count, subqueries = 0, []
        for start, end, count in zip(count_ts[i:-1], count_ts[i + 1 :], counts[i:]):
            if count <= DPS_LIMIT:
                if current_count + count > DPS_LIMIT:
                    subqueries.append((query_start, start, current_count))
                    current_count, query_start = count, start
                else:
                    current_count += count
                continue
            # We have hit a hot-spot of a 'bursty' time series (meaning single count agg interval > DPS_LIMIT).
            # This optimization - subdividing a hot-spot - has amazing perf. benefits (avoids serial fetching):
            n_periods = math.ceil(count / DPS_LIMIT)  # It is important to overestimate the number of points
            sub_count = math.ceil(count / n_periods)  # ...that way we dont get "one extra request" with a few dps
            delta_ms = math.ceil(count_gran_in_ms / n_periods)

            # We split the single period (length: count_gran_in_ms) into n_periods, but make sure that
            # the last of these has its end shifted all the way to `end`; i.e. start of next period, which
            # might be `start + count_gran_in_ms` but can be an arbitrary long period of time:
            sub_periods = [start + delta_ms * i for i in range(n_periods)]
            sub_periods.append(end)
            for sub_start, sub_end in zip(sub_periods[:-1], sub_periods[1:]):
                if current_count + sub_count > DPS_LIMIT:
                    subqueries.append((query_start, sub_end, current_count))
                    current_count = sub_count
                    query_start = sub_end
                else:
                    current_count += sub_count

        # Store last subtask interval:
        if query_start < self.query.end:
            subqueries.append((query_start, self.query.end, current_count))

        # Remove subqueries not needed when given a limit:
        if self.has_limit:
            remaining_limit = self.query.limit - n_dps_fetched
            for i, (period_start, period_end, count) in enumerate(subqueries):
                if (remaining_limit := remaining_limit - count) > 0:
                    continue
                subqueries = subqueries[: i + 1]
                # For safety, we move the end of the last subquery all the way to "actual end":
                subqueries[-1] = (period_start, self.query.end, math.inf)  # Count doesnt matter after this point
                break

        # Make room for subquery results:
        self.ts_lst.extend([] for _ in range(len(subqueries)))
        self.dps_lst.extend([] for _ in range(len(subqueries)))

        # If we trusted the counts, we could use them (but we dont - so we use the full 'remaining dps' for all):
        limit = self.query.limit - n_dps_fetched if self.has_limit else None

        for i, (period_start, period_end, count_est) in enumerate(subqueries, 1):
            query = SingleTSQuery(**self.query.identifier_dct_sc, start=period_start, end=period_end, limit=limit)
            priority = i - 1 if self.has_limit else 0
            self.subtasks.append(SerialFetchSubtask(query=query, subtask_idx=i, parent=self, priority=priority))
        return self.subtasks

    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, max_workers: int) -> int:
        # It makes no sense to split beyond what the max-size of a query allows (for a maximally dense
        # time series), but that is rarely useful as 100k dps is just 1 min 40 sec... we guess an
        # average density of points at 1 dp/sec, giving us split-windows no smaller than ~1 day:
        return min(max_workers, math.ceil((tot_ms / 1000) / self.query.max_query_limit))

    def _cap_dps_at_limit(self):
        # Note 1: Outside points do not count towards given limit (API specs)
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
            self.ts_lst.append([end_ts])
            self.dps_lst.append([end_dp])

    def _unpack_and_store(self, idx, dps):
        # Faster than feeding listcomp to np.array:
        self.ts_lst[idx].append(np.fromiter(map(DpsUnpackFns.ts, dps), dtype=np.int64, count=len(dps)))
        self.dps_lst[idx].append(np.fromiter(map(DpsUnpackFns.raw_dp, dps), dtype=np.object_, count=len(dps)))

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
class ConcurrentUnlimitedRawTask(BaseConcurrentRawTask):
    pass


ParallelUnlimitedRawStringTask = ConcurrentUnlimitedRawTask
ParallelUnlimitedRawNumericTask = ConcurrentUnlimitedRawTask


@dataclass(eq=False)
class ConcurrentLimitedRawTask(ConcurrentLimitedMixin, BaseConcurrentRawTask):
    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, max_workers: int) -> int:
        # We make the guess that the time series has ~1 dp/sec and use this in combination with the given
        # limit to not split into too many queries (highest throughput when each request is close to max limit)
        n_estimate_periods = math.ceil((tot_ms / 1000) / self.query.max_query_limit)
        remaining_limit = self.query.limit - self.n_dps_first_batch
        n_periods = math.ceil(remaining_limit / self.query.max_query_limit)
        # Pick the smallest N from constraints:
        return min(max_workers, n_periods, n_estimate_periods)


ParallelLimitedRawNumericTask = ConcurrentLimitedRawTask
ParallelLimitedRawStringTask = ConcurrentLimitedRawTask


@dataclass(eq=False)
class BaseConcurrentAggTask(BaseConcurrentTask):
    is_count_query: bool = dataclasses.field(default=None, init=False)
    has_non_count_aggs: bool = dataclasses.field(default=None, init=False)
    agg_unpack_fn: Optional[Callable] = dataclasses.field(default=None, init=False)
    dtype_aggs: Optional[np.dtype] = dataclasses.field(default=None, init=False)
    float_aggs: List[str] = dataclasses.field(default=None, init=False)
    count_dps_lst: Optional[List[List[NumpyInt64Array]]] = dataclasses.field(default_factory=list, init=False)

    def __post_init__(self, first_dps_batch):
        self.offset_next = granularity_to_ms(self.query.granularity)
        self._set_aggregate_vars()
        super().__post_init__(first_dps_batch)

    def _set_aggregate_vars(self):
        self.float_aggs = self.query.aggregates[:]
        self.is_count_query = "count" in self.float_aggs
        if self.is_count_query:
            self.count_dps_lst.append([])
            self.float_aggs.remove("count")  # Only aggregate that is integer, handle separately

        self.has_non_count_aggs = bool(self.float_aggs)
        if self.has_non_count_aggs:
            self.agg_unpack_fn = DpsUnpackFns.custom_from_aggregates(self.float_aggs)
            if len(self.float_aggs) > 1:
                self.dtype_aggs = np.dtype((np.float64, len(self.float_aggs)))
            else:
                self.dtype_aggs = np.dtype(np.float64)  # (.., 1) is deprecated

    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, max_workers: int) -> int:
        n_max_dps = tot_ms // self.offset_next  # evenly divides
        return min(max_workers, math.ceil(n_max_dps / DPS_LIMIT_AGG))

    def split_into_subtasks(self, *args, **kwargs) -> List[SerialFetchSubtask]:
        subtasks = super().split_into_subtasks(*args, **kwargs)
        if self.is_count_query:
            self.count_dps_lst.extend([] for _ in subtasks)
        return subtasks

    def _create_empty_result(self) -> DatapointsArray:
        arr_dct = {}
        if self.is_count_query:
            arr_dct["count"] = np.array([], dtype=np.int64)
        if self.has_non_count_aggs:
            arr_dct.update({agg: np.array([], dtype=np.float64) for agg in self.float_aggs})
        return DatapointsArray._load({**self.ts_info, "timestamp": np.array([], dtype=np.int64), **arr_dct})

    def get_result(self) -> DatapointsArray:
        if self.has_limit:
            if self.query.limit == 0:
                return self._create_empty_result()
            self._cap_dps_at_limit()

        arr_dct = {"timestamp": np.hstack(list(chain.from_iterable(self.ts_lst)))}
        if self.is_count_query:
            arr_dct["count"] = np.hstack(list(chain.from_iterable(self.count_dps_lst)))
        if self.has_non_count_aggs:
            arr_lst = np.hsplit(np.vstack(list(chain.from_iterable(self.dps_lst))), len(self.float_aggs))
            arr_dct.update(dict(zip(self.float_aggs, map(np.squeeze, arr_lst))))
        return DatapointsArray._load({**self.ts_info, **arr_dct})

    def _cap_dps_at_limit(self):
        count, data_lsts = 0, [self.ts_lst]
        if self.is_count_query:
            data_lsts.append(self.count_dps_lst)
        if self.has_non_count_aggs:
            data_lsts.append(self.dps_lst)

        for i, sublist in enumerate(self.ts_lst):
            for j, arr in enumerate(sublist):
                if count + len(arr) < self.query.limit:
                    count += len(arr)
                    continue
                end = self.query.limit - count
                for lst in data_lsts:
                    lst[i][j] = lst[i][j][:end]
                    lst[i] = lst[i][: j + 1]
                    del lst[i + 1 :]  # inplace shorten
                return

    def _unpack_and_store(self, idx, dps):
        n = len(dps)
        self.ts_lst[idx].append(np.fromiter(map(DpsUnpackFns.ts, dps), dtype=np.int64, count=n))
        if self.is_count_query:
            self.count_dps_lst[idx].append(np.fromiter(map(DpsUnpackFns.count, dps), dtype=np.int64, count=n))
        if self.has_non_count_aggs:
            try:  # Fast method uses multi-key unpacking:
                arr = np.fromiter(map(self.agg_unpack_fn, dps), dtype=self.dtype_aggs, count=n)
            except KeyError:  # An aggregate is missing, fallback to slower `dict.get(agg)`.
                # This can happen when certain aggregates are undefined, e.g. `interpolate` at first interval.
                arr = np.array([tuple(map(dp.get, self.float_aggs)) for dp in dps], dtype=np.float64)
            self.dps_lst[idx].append(arr.reshape(n, len(self.float_aggs)))


@dataclass(eq=False)
class ParallelUnlimitedAggNumericTask(BaseConcurrentAggTask):
    pass


@dataclass(eq=False)
class ParallelLimitedAggNumericTask(ConcurrentLimitedMixin, BaseConcurrentAggTask):
    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, max_workers: int) -> int:
        remaining_limit = self.query.limit - self.n_dps_first_batch
        n_max_dps = min(remaining_limit, tot_ms // self.offset_next)
        return min(max_workers, math.ceil(n_max_dps / DPS_LIMIT_AGG))
