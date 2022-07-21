from __future__ import annotations

import dataclasses
import operator as op
from dataclasses import InitVar, dataclass
from pprint import pprint
from typing import TYPE_CHECKING, List, Optional, Tuple

import numpy as np

from cognite.client._api._type_defs import DatapointsFromAPI, NumpyInt64Array, NumpyObjArray
from cognite.client.data_classes.datapoints import SingleTSQuery

if TYPE_CHECKING:
    pprint("type checkin'")  # TODO: Remove.


def dps_task_type_selector(query):
    # Note: We could add "include-outside-points" to our condition, but since
    # the "initial query" already includes this, when True, we don't.
    condition = (query.is_string, query.is_raw_query, query.limit is None)
    return {  # O pattern matching, where art thou?
        # String tasks:
        (True, True, True): UnlimitedStringTask,
        (True, True, False): LimitedStringTask,
        # Raw numeric tasks:
        (False, True, True): UnlimitedRawNumericTask,
        (False, True, False): LimitedRawNumericTask,
        # Agg. numeric tasks:
        (False, False, True): UnlimitedAggNumericTask,
        (False, False, False): LimitedAggNumericTask,
    }[condition]


@dataclass(eq=False)  # __hash__ cant be inherited for safety, so we add eq=False for all
class UnlimitedStringTask:
    query: SingleTSQuery
    first_dps_batch: InitVar[DatapointsFromAPI]
    ts_info: DatapointsFromAPI = dataclasses.field(default=None, init=False)
    ts_lst: List[NumpyInt64Array] = dataclasses.field(default_factory=list, init=False)
    dps_lst: List[NumpyObjArray] = dataclasses.field(default_factory=list, init=False)
    is_done: bool = dataclasses.field(default=False, init=False)
    dp_outside_start: Optional[Tuple[int, str]] = dataclasses.field(default=None, init=False)
    dp_outside_end: Optional[Tuple[int, str]] = dataclasses.field(default=None, init=False)

    def __post_init__(self, first_dps_batch):
        dps = first_dps_batch.pop("datapoints")
        self.ts_info = first_dps_batch  # Store just the ts info
        self.store_first_batch(dps)

    def get_result(self) -> Tuple[DatapointsFromAPI, NumpyInt64Array, NumpyObjArray]:
        if self.query.include_outside_points:
            start_ts, start_dp = self.dp_outside_start or [], []
            end_ts, end_dp = self.dp_outside_end or [], []
            self.ts_lst = [start_ts, *self.ts_lst, end_ts]
            self.dps_lst = [start_dp, *self.dps_lst, end_dp]
        return {**self.ts_info, "timestamp": np.concatenate(self.ts_lst), "value": np.concatenate(self.dps_lst)}

    def store_first_batch(self, dps):
        if not dps:
            self.is_done = True
            return

        if self.query.include_outside_points:
            first_dp = dps[0]
            if first_dp["timestamp"] < self.query.start:
                # We got a dp before `start`, this should not impact our count towards `limit`:
                self.dp_outside_start = self.unpack_fn(dps.pop(0))  # Slow pop :(
            if dps:
                last_dp = dps[-1]
                if last_dp["timestamp"] >= self.query.end:  # >= because `end` is exclusive
                    self.dp_outside_end = self.unpack_fn(dps.pop())  # Fast pop :)
            if not dps:
                self.is_done = True
                return

        self._unpack_and_store(dps)
        self.is_done = True  # TODO: REMOVE

    def _unpack_and_store(self, dps):
        # Faster than feeding listcomp to np.array:
        self.ts_lst.append(np.fromiter(map(op.itemgetter("timestamp"), dps), dtype=np.int64, count=len(dps)))
        self.dps_lst.append(np.fromiter(map(op.itemgetter("value"), dps), dtype=np.object_, count=len(dps)))


# @dataclass(eq=False)
# class LimitedStringTask:
#     query: SingleTSQuery
#     first_dps_batch: InitVar[DatapointsFromAPI]
#
#     def __post_init__(self, first_dps_batch):
#         # TODO: Handle outside points, if any
#         pass

LimitedStringTask = UnlimitedStringTask


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
