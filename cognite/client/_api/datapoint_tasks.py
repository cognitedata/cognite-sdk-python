from __future__ import annotations

import dataclasses
import itertools
import math
import operator as op
from abc import ABC
from dataclasses import InitVar, dataclass
from pprint import pprint
from typing import TYPE_CHECKING, List, Optional, Tuple

import numpy as np

from cognite.client._api._type_defs import DatapointsFromAPI, NumpyInt64Array, NumpyObjArray
from cognite.client.data_classes import DatapointsArray
from cognite.client.data_classes.datapoints import SingleTSQuery

if TYPE_CHECKING:
    pprint("type checkin'")  # TODO: Remove.


class DpsUnpackFns:
    ts = op.itemgetter("timestamp")
    raw_dp = op.itemgetter("value")
    ts_dp_tpl = op.itemgetter("timestamp", "value")


def dps_task_type_selector(query):
    # Note: We could add "include-outside-points" to our condition, but since
    # the "initial query" already includes this, when True, we don't.
    condition = (query.is_string, query.is_raw_query, query.limit is None)
    return {  # O pattern matching, where art thou?
        # String tasks:
        (True, True, True): UnlimitedStringTask,
        # (True, True, False): LimitedStringTask,
        # # Raw numeric tasks:
        # (False, True, True): UnlimitedRawNumericTask,
        # (False, True, False): LimitedRawNumericTask,
        # # Agg. numeric tasks:
        # (False, False, True): UnlimitedAggNumericTask,
        # (False, False, False): LimitedAggNumericTask,
    }[condition]


# WORKAROUND: Python does not allow us to easily define abstract dataclasses
# QUESTION  : Do we need it? ;P
class AbstractDpsTask(ABC):
    ...
    # @abstractmethod
    # def get_result(self):
    #     ...


@dataclass(eq=False)  # __hash__ cant be inherited (for safety), so we add eq=False for all
class BaseDpsTask(AbstractDpsTask):
    query: SingleTSQuery


@dataclass(eq=False)
class RawSerialSubtask(BaseDpsTask):
    # Just fetches serially until complete, nice and simple. Stores data in parent
    subtask_idx: int
    parent: BaseDpsTask
    is_done: bool = False
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
        self.parent.unpack_and_store(self.subtask_idx, dps)
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
class UnlimitedStringTask:
    query: SingleTSQuery
    first_dps_batch: InitVar[DatapointsFromAPI]
    first_start: Optional[int] = None
    offset_next: int = 1  # ms
    ts_info: DatapointsFromAPI = dataclasses.field(default=None, init=False)
    ts_lst: List[List[NumpyInt64Array]] = dataclasses.field(default_factory=list, init=False)
    dps_lst: List[List[NumpyObjArray]] = dataclasses.field(default_factory=list, init=False)
    subtasks: List[RawSerialSubtask] = dataclasses.field(default_factory=list, init=False)
    _is_done: bool = dataclasses.field(default=False, init=False)
    dp_outside_start: Optional[Tuple[int, str]] = dataclasses.field(default=None, init=False)
    dp_outside_end: Optional[Tuple[int, str]] = dataclasses.field(default=None, init=False)

    def __post_init__(self, first_dps_batch):
        # The first batch will handle everything we need for queries with `include_outside_points=True`:
        dps = first_dps_batch.pop("datapoints")
        self.ts_info = first_dps_batch  # Store just the ts info
        self.store_first_batch(dps)

    def get_result(self) -> DatapointsArray:
        if self.query.include_outside_points:
            if self.dp_outside_start:
                start_ts, start_dp = self.dp_outside_start
                self.ts_lst.insert(0, [start_ts])
                self.dps_lst.insert(0, [start_dp])
            if self.dp_outside_end:
                end_ts, end_dp = self.dp_outside_end
                self.dp_outside_end
                self.ts_lst.append([end_ts])
                self.dps_lst.append([end_dp])
        return DatapointsArray._load(
            {
                **self.ts_info,
                "timestamp": np.hstack(list(itertools.chain.from_iterable(self.ts_lst))),
                "value": np.hstack(list(itertools.chain.from_iterable(self.dps_lst))),
            }
        )

    def split_task_into_subtasks(self, max_workers) -> List[RawSerialSubtask]:
        # This is were we decide the initial splitting of the time domain (start to end) to tasks:
        # 1. Estimate the max number of dps needed to fetch
        # 2. Create subtasks with this as parent, up to max `max_workers`
        #
        # It makes no sense to split beyond what the max-size of a query allows (for a maximum dense time series),
        # but that is rarely useful as 100k dps is just 1 min 40 sec... we guess an average density of
        # points at 1 dp/sec, giving us split-windows no smaller than ~1 day:
        start, end = self.first_start, self.query.end
        tot_ms = end - start
        n_periods = min(max_workers, max(1, math.ceil((tot_ms / 1000) / self.query.max_query_limit)))
        delta_ms = math.ceil(tot_ms / n_periods)  # Ceil makes sure we "overshoot" end
        boundaries = [min(end, start + delta_ms * i) for i in range(n_periods + 1)]
        # Make a separate dps bucket for each subtask:
        self.ts_lst.extend([[] for _ in range(n_periods)])
        self.dps_lst.extend([[] for _ in range(n_periods)])
        for i, (period_start, period_end) in enumerate(zip(boundaries[:-1], boundaries[1:]), 1):
            subtask = RawSerialSubtask(
                query=SingleTSQuery(**self.query.identifier_dct_sc, start=period_start, end=period_end),
                subtask_idx=i,
                parent=self,
            )
            self.subtasks.append(subtask)
        return self.subtasks

    @property
    def is_done(self):
        if self.subtasks:
            self._is_done = self._is_done or all(sub.is_done for sub in self.subtasks)
        return self._is_done

    def store_first_batch(self, dps):
        if not dps:
            self._is_done = True
            return

        if self.query.include_outside_points:
            self.handle_outside_points(dps)
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
        self.unpack_and_store(0, dps)

    def unpack_and_store(self, subtask_idx, dps):
        # Faster than feeding listcomp to np.array:
        self.ts_lst[subtask_idx].append(np.fromiter(map(DpsUnpackFns.ts, dps), dtype=np.int64, count=len(dps)))
        self.dps_lst[subtask_idx].append(np.fromiter(map(DpsUnpackFns.raw_dp, dps), dtype=np.object_, count=len(dps)))

    def handle_outside_points(self, dps):
        first_dp = dps[0]
        if first_dp["timestamp"] < self.query.start:
            # We got a dp before `start`, this should not impact our count towards `limit`:
            self.dp_outside_start = DpsUnpackFns.ts_dp_tpl(dps.pop(0))  # Slow pop :(
        if dps:
            last_dp = dps[-1]
            if last_dp["timestamp"] >= self.query.end:  # >= because `end` is exclusive
                self.dp_outside_end = DpsUnpackFns.ts_dp_tpl(dps.pop())  # Fast pop :)


@dataclass(eq=False)
class LimitedStringTask:
    query: SingleTSQuery
    first_dps_batch: InitVar[DatapointsFromAPI]

    def __post_init__(self, first_dps_batch):
        # TODO: Handle outside points, if any
        pass


@dataclass(eq=False)
class UnlimitedRawNumericTask:
    query: SingleTSQuery
    first_dps_batch: InitVar[DatapointsFromAPI]

    def __post_init__(self, first_dps_batch):
        # TODO: Handle outside points, if any
        pass


@dataclass(eq=False)
class LimitedRawNumericTask:
    query: SingleTSQuery
    first_dps_batch: InitVar[DatapointsFromAPI]

    def __post_init__(self, first_dps_batch):
        # TODO: Handle outside points, if any
        pass


@dataclass(eq=False)
class UnlimitedAggNumericTask:
    query: SingleTSQuery
    first_dps_batch: InitVar[DatapointsFromAPI]

    def __post_init__(self, first_dps_batch):
        pass


@dataclass(eq=False)
class LimitedAggNumericTask:
    query: SingleTSQuery
    first_dps_batch: InitVar[DatapointsFromAPI]

    def __post_init__(self, first_dps_batch):
        pass
