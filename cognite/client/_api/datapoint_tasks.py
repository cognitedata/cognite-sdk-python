import math
import numbers
import operator as op
import warnings
from abc import abstractmethod
from itertools import chain
from threading import Lock
from typing import Any, Callable, Dict, Generic, List, MutableSequence, Optional, Sequence, Tuple, TypeVar, Union, cast

from google.protobuf.message import Message
from sortedcontainers import SortedDict, SortedList

from cognite.client.data_classes.datapoints import NUMPY_IS_AVAILABLE, Datapoints, DatapointsArray
from cognite.client.utils._auxiliary import (
    convert_all_keys_to_snake_case,
    import_legacy_protobuf,
    is_unlimited,
    to_camel_case,
    to_snake_case,
)
from cognite.client.utils._identifier import Identifier
from cognite.client.utils._time import (
    align_start_and_end_for_granularity,
    granularity_to_ms,
    split_time_range,
    time_ago_to_ms,
    timestamp_to_ms,
)

if not import_legacy_protobuf():
    from cognite.client._proto.data_points_pb2 import AggregateDatapoint, NumericDatapoint, StringDatapoint
else:
    from cognite.client._proto_legacy.data_points_pb2 import AggregateDatapoint, NumericDatapoint, StringDatapoint
if NUMPY_IS_AVAILABLE:
    import numpy as np

    from cognite.client.data_classes.datapoints import NumpyFloat64Array, NumpyInt64Array, NumpyObjArray


DPS_LIMIT_AGG = 10000
DPS_LIMIT = 100000
DatapointsAgg = MutableSequence[AggregateDatapoint]
DatapointsNum = MutableSequence[NumericDatapoint]
DatapointsStr = MutableSequence[StringDatapoint]
DatapointsAny = Union[(DatapointsAgg, DatapointsNum, DatapointsStr)]
DatapointsRaw = Union[(DatapointsNum, DatapointsStr)]
RawDatapointValue = Union[(float, str)]
DatapointsId = Union[(None, int, Dict[(str, Any)], Sequence[Union[(int, Dict[(str, Any)])]])]
DatapointsExternalId = Union[(None, str, Dict[(str, Any)], Sequence[Union[(str, Dict[(str, Any)])]])]


class _DatapointsQuery:
    def __init__(
        self,
        start=None,
        end=None,
        id=None,
        external_id=None,
        aggregates=None,
        granularity=None,
        limit=None,
        include_outside_points=False,
        ignore_unknown_ids=False,
    ):
        self.start = start
        self.end = end
        self.id = id
        self.external_id = external_id
        self.aggregates = aggregates
        self.granularity = granularity
        self.limit = limit
        self.include_outside_points = include_outside_points
        self.ignore_unknown_ids = ignore_unknown_ids

    @property
    def is_single_identifier(self):
        return (isinstance(self.id, (dict, numbers.Integral)) and (self.external_id is None)) or (
            isinstance(self.external_id, (dict, str)) and (self.id is None)
        )


class _SingleTSQueryValidator:
    def __init__(self, user_query):
        self.user_query = user_query
        self.defaults = dict(
            start=user_query.start,
            end=user_query.end,
            limit=user_query.limit,
            aggregates=user_query.aggregates,
            granularity=user_query.granularity,
            include_outside_points=user_query.include_outside_points,
            ignore_unknown_ids=user_query.ignore_unknown_ids,
        )
        self.__time_now = timestamp_to_ms("now")

    def _ts_to_ms_frozen_now(self, ts, default):
        if ts is None:
            return default
        elif isinstance(ts, str):
            return self.__time_now - time_ago_to_ms(ts)
        else:
            return timestamp_to_ms(ts)

    def validate_and_create_single_queries(self):
        queries = []
        if self.user_query.id is not None:
            id_queries = self._validate_multiple_id(self.user_query.id)
            queries.extend(id_queries)
        if self.user_query.external_id is not None:
            xid_queries = self._validate_multiple_xid(self.user_query.external_id)
            queries.extend(xid_queries)
        if queries:
            return queries
        raise ValueError("Pass at least one time series `id` or `external_id`!")

    def _validate_multiple_id(self, id):
        return self._validate_id_or_xid(id, "id", numbers.Integral)

    def _validate_multiple_xid(self, external_id):
        return self._validate_id_or_xid(external_id, "external_id", str)

    def _validate_id_or_xid(self, id_or_xid, arg_name, exp_type):
        id_or_xid_seq: Sequence[Union[(int, str, Dict[(str, Any)])]]
        if isinstance(id_or_xid, (dict, exp_type)):
            id_or_xid_seq = [cast(Union[(int, str, Dict[(str, Any)])], id_or_xid)]
        elif isinstance(id_or_xid, Sequence):
            id_or_xid_seq = id_or_xid
        else:
            self._raise_on_wrong_ts_identifier_type(id_or_xid, arg_name, exp_type)
        queries = []
        for ts in id_or_xid_seq:
            if isinstance(ts, exp_type):
                ts_dct = {**self.defaults, arg_name: ts}
                queries.append(self._validate_and_create_query(ts_dct))
            elif isinstance(ts, dict):
                ts_validated = self._validate_user_supplied_dict_keys(ts, arg_name)
                identifier = ts_validated[arg_name]
                if not isinstance(identifier, exp_type):
                    self._raise_on_wrong_ts_identifier_type(identifier, arg_name, exp_type)
                ts_dct = {**self.defaults, **ts_validated}
                queries.append(self._validate_and_create_query(ts_dct))
            else:
                self._raise_on_wrong_ts_identifier_type(ts, arg_name, exp_type)
        return queries

    @staticmethod
    def _raise_on_wrong_ts_identifier_type(id_or_xid, arg_name, exp_type):
        raise TypeError(
            f"Got unsupported type {type(id_or_xid)}, as, or part of argument `{arg_name}`. Expected one of {exp_type}, {dict} or a (mixed) list of these, but got `{id_or_xid}`."
        )

    @staticmethod
    def _validate_user_supplied_dict_keys(dct, arg_name):
        if arg_name not in dct:
            arg_name_cc = to_camel_case(arg_name)
            if arg_name_cc not in dct:
                raise KeyError(f"Missing required key `{arg_name}` in dict: {dct}.")
            dct = dct.copy()
            dct[arg_name] = dct.pop(arg_name_cc)
        opt_dct_keys = {
            "start",
            "end",
            "aggregates",
            "granularity",
            "limit",
            "include_outside_points",
            "ignore_unknown_ids",
        }
        bad_keys = (set(dct) - opt_dct_keys) - {arg_name}
        if not bad_keys:
            return dct
        raise KeyError(
            f"Dict provided by argument `{arg_name}` included key(s) not understood: {sorted(bad_keys)}. Required key: `{arg_name}`. Optional: {list(opt_dct_keys)}."
        )

    def _validate_and_create_query(self, dct):
        limit = self._verify_limit(dct["limit"])
        (granularity, aggregates) = (dct["granularity"], dct["aggregates"])
        if not ((granularity is None) or isinstance(granularity, str)):
            raise TypeError(f"Expected `granularity` to be of type `str` or None, not {type(granularity)}")
        elif not ((aggregates is None) or isinstance(aggregates, (str, list))):
            raise TypeError(f"Expected `aggregates` to be of type `str`, `list[str]` or None, not {type(aggregates)}")
        elif aggregates is None:
            if granularity is None:
                raw_query = self._convert_parameters(dct, limit, is_raw=True)
                if limit is None:
                    return _SingleTSQueryRawUnlimited(**raw_query)
                return _SingleTSQueryRawLimited(**raw_query)
            raise ValueError("When passing `granularity`, argument `aggregates` is also required.")
        elif isinstance(aggregates, list) and (len(aggregates) == 0):
            raise ValueError("Empty list of `aggregates` passed, expected at least one!")
        elif isinstance(aggregates, list) and (len(aggregates) != len(set(map(to_snake_case, aggregates)))):
            raise ValueError("List of `aggregates` may not contain duplicates")
        elif granularity is None:
            raise ValueError("When passing `aggregates`, argument `granularity` is also required.")
        elif dct["include_outside_points"] is True:
            raise ValueError("'Include outside points' is not supported for aggregates.")
        agg_query = self._convert_parameters(dct, limit, is_raw=False)
        if limit is None:
            return _SingleTSQueryAggUnlimited(**agg_query)
        return _SingleTSQueryAggLimited(**agg_query)

    def _convert_parameters(self, dct, limit, is_raw):
        identifier = Identifier.of_either(
            cast(Optional[int], dct.get("id")), cast(Optional[str], dct.get("external_id"))
        )
        (start, end) = self._verify_time_range(dct["start"], dct["end"], dct["granularity"], is_raw, identifier)
        converted = {
            "identifier": identifier,
            "start": start,
            "end": end,
            "limit": limit,
            "ignore_unknown_ids": dct["ignore_unknown_ids"],
        }
        if is_raw:
            converted["include_outside_points"] = dct["include_outside_points"]
        else:
            aggs = dct["aggregates"]
            if isinstance(aggs, str):
                aggs = [aggs]
            converted["aggregates"] = aggs
            converted["granularity"] = dct["granularity"]
        return converted

    def _verify_limit(self, limit):
        if is_unlimited(limit):
            return None
        elif isinstance(limit, numbers.Integral) and (limit >= 0):
            try:
                return int(limit)
            except Exception:
                raise TypeError(f"Unable to convert given limit={limit} to integer")
        raise TypeError(
            f"Parameter `limit` must be a non-negative integer -OR- one of [None, -1, inf] to indicate an unlimited query. Got: {limit} with type: {type(limit)}"
        )

    def _verify_time_range(self, start, end, granularity, is_raw, identifier):
        start = self._ts_to_ms_frozen_now(start, default=0)
        end = self._ts_to_ms_frozen_now(end, default=self.__time_now)
        if end <= start:
            raise ValueError(
                f"Invalid time range, end={end} must be later than start={start} (from query: {identifier.as_dict(camel_case=False)})"
            )
        if not is_raw:
            (start, end) = align_start_and_end_for_granularity(start, end, cast(str, granularity))
        return (start, end)


class _SingleTSQueryBase:
    def __init__(
        self,
        *,
        identifier: Identifier,
        start: int,
        end: int,
        max_query_limit: int,
        limit: Optional[int],
        include_outside_points: bool,
        ignore_unknown_ids: bool,
    ):
        self.identifier = identifier
        self.start = start
        self.end = end
        self.max_query_limit = max_query_limit
        self.limit = limit
        self.include_outside_points = include_outside_points
        self.ignore_unknown_ids = ignore_unknown_ids
        self.granularity: Optional[str] = None
        self._is_missing: Optional[bool] = None
        if self.include_outside_points and (self.limit is not None):
            warnings.warn(
                "When using `include_outside_points=True` with a finite `limit` you may get a large gap between the last 'inside datapoint' and the 'after/outside' datapoint. Note also that the up-to-two outside points come in addition to your given `limit`; asking for 5 datapoints might yield 5, 6 or 7. It's a feature, not a bug ;)",
                UserWarning,
            )

    @property
    def capped_limit(self):
        if self.limit is None:
            return self.max_query_limit
        return min(self.limit, self.max_query_limit)

    def override_max_query_limit(self, new_limit):
        assert isinstance(new_limit, int)
        self.max_query_limit = new_limit

    @property
    @abstractmethod
    def is_raw_query(self):
        ...

    @property
    @abstractmethod
    def ts_task_type(self):
        ...

    @abstractmethod
    def to_payload(self):
        raise NotImplementedError

    @property
    def is_missing(self):
        if self._is_missing is None:
            raise RuntimeError("Before making API-calls the `is_missing` status is unknown")
        return self._is_missing

    @is_missing.setter
    def is_missing(self, value):
        assert isinstance(value, bool)
        self._is_missing = value


class _SingleTSQueryRaw(_SingleTSQueryBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs, max_query_limit=DPS_LIMIT)
        self.aggregates = self.aggregates_cc = None
        self.granularity = None

    @property
    def is_raw_query(self):
        return True

    def to_payload(self):
        return {
            **self.identifier.as_dict(),
            "start": self.start,
            "end": self.end,
            "limit": self.capped_limit,
            "includeOutsidePoints": self.include_outside_points,
        }


class _SingleTSQueryRawLimited(_SingleTSQueryRaw):
    def __init__(self, *, limit: int, **kwargs):
        super().__init__(limit=limit, **kwargs)
        assert isinstance(limit, int)

    @property
    def ts_task_type(self):
        return ParallelLimitedRawTask


class _SingleTSQueryRawUnlimited(_SingleTSQueryRaw):
    def __init__(self, *, limit: None, **kwargs):
        super().__init__(limit=limit, **kwargs)

    @property
    def ts_task_type(self):
        return ParallelUnlimitedRawTask


class _SingleTSQueryAgg(_SingleTSQueryBase):
    def __init__(self, *, aggregates: List[str], granularity: str, **kwargs):
        agg_query_settings = dict(include_outside_points=False, max_query_limit=DPS_LIMIT_AGG)
        super().__init__(**kwargs, **agg_query_settings)
        self.aggregates = aggregates
        self.granularity = granularity

    @property
    def is_raw_query(self):
        return False

    def aggregates_cc(self):
        return list(map(to_camel_case, self.aggregates))

    def to_payload(self):
        return {
            **self.identifier.as_dict(),
            "start": self.start,
            "end": self.end,
            "aggregates": self.aggregates_cc,
            "granularity": self.granularity,
            "limit": self.capped_limit,
            "includeOutsidePoints": self.include_outside_points,
        }


class _SingleTSQueryAggLimited(_SingleTSQueryAgg):
    def __init__(self, *, limit: int, **kwargs):
        super().__init__(limit=limit, **kwargs)
        assert isinstance(limit, int)

    @property
    def ts_task_type(self):
        return ParallelLimitedAggTask


class _SingleTSQueryAggUnlimited(_SingleTSQueryAgg):
    def __init__(self, *, limit: None, **kwargs):
        super().__init__(limit=limit, **kwargs)

    @property
    def ts_task_type(self):
        return ParallelUnlimitedAggTask


class DpsUnpackFns:
    ts: Callable[([Message], int)] = op.attrgetter("timestamp")
    raw_dp: Callable[([Message], RawDatapointValue)] = op.attrgetter("value")
    ts_dp_tpl: Callable[([Message], Tuple[(int, RawDatapointValue)])] = op.attrgetter("timestamp", "value")
    count: Callable[([Message], int)] = op.attrgetter("count")

    @staticmethod
    def custom_from_aggregates(lst):
        return op.attrgetter(*lst)


T = TypeVar("T")
FIRST_IDX = (0,)


class DefaultSortedDict(SortedDict, Generic[T]):
    def __init__(self, default_factory, **kw):
        self.default_factory = default_factory
        super().__init__(**kw)

    def __missing__(self, key):
        self[key] = self.default_factory()
        return self[key]


def create_dps_container():
    return DefaultSortedDict(list)


def create_subtask_lst():
    return SortedList(key=op.attrgetter("subtask_idx"))


def ensure_int(val, change_nan_to=0):
    if math.isnan(val):
        return change_nan_to
    return int(val)


def get_datapoints_from_proto(res):
    dp_type = res.WhichOneof("datapointType")
    if dp_type is not None:
        return getattr(res, dp_type).datapoints
    return cast(MutableSequence[Any], [])


def get_ts_info_from_proto(res):
    return {
        "id": res.id,
        "external_id": res.externalId,
        "is_string": res.isString,
        "is_step": res.isStep,
        "unit": res.unit,
    }


def create_array_from_dps_container(container):
    return np.hstack(list(chain.from_iterable(container.values())))


def create_aggregates_arrays_from_dps_container(container, n_aggs):
    all_aggs_arr = np.vstack(list(chain.from_iterable(container.values())))
    return list(map(np.ravel, np.hsplit(all_aggs_arr, n_aggs)))


def create_list_from_dps_container(container):
    return list(chain.from_iterable(chain.from_iterable(container.values())))


def create_aggregates_list_from_dps_container(container):
    concatenated = chain.from_iterable(chain.from_iterable(container.values()))
    return map(list, zip(*concatenated))


class BaseDpsFetchSubtask:
    def __init__(self, start, end, identifier, parent, priority, max_query_limit, is_raw_query):
        self.start = start
        self.end = end
        self.identifier = identifier
        self.parent = parent
        self.priority = priority
        self.is_raw_query = is_raw_query
        self.max_query_limit = max_query_limit
        self.is_done = False

    def get_remaining_limit(self):
        return self.parent.get_remaining_limit(self)

    @abstractmethod
    def get_next_payload(self):
        ...

    @abstractmethod
    def store_partial_result(self, res):
        ...


class OutsideDpsFetchSubtask(BaseDpsFetchSubtask):
    def __init__(self, **kwargs):
        outside_dps_settings = dict(priority=0, is_raw_query=True, max_query_limit=0)
        super().__init__(**kwargs, **outside_dps_settings)

    def get_next_payload(self):
        if self.is_done:
            return None
        return self._create_payload_item()

    def _create_payload_item(self):
        return {
            **self.identifier.as_dict(),
            "start": self.start,
            "end": self.end,
            "limit": 0,
            "includeOutsidePoints": True,
        }

    def store_partial_result(self, res):
        dps = get_datapoints_from_proto(res)
        if dps:
            self.parent._extract_outside_points(cast(DatapointsRaw, dps))
        self.is_done = True
        return None


class SerialFetchSubtask(BaseDpsFetchSubtask):
    def __init__(
        self,
        *,
        limit: Optional[int],
        aggregates: Optional[List[str]],
        granularity: Optional[str],
        subtask_idx: Tuple[(float, ...)],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.limit = limit
        self.aggregates = aggregates
        self.granularity = granularity
        self.subtask_idx = subtask_idx
        self.n_dps_fetched = 0
        self.agg_kwargs = {}
        self.next_start = self.start
        if not self.is_raw_query:
            self.agg_kwargs = {"aggregates": self.aggregates, "granularity": self.granularity}

    def get_next_payload(self):
        if self.is_done:
            return None
        remaining = self.get_remaining_limit()
        if (self.parent.ts_info is not None) and (remaining == 0):
            (self.is_done, ts_task) = (True, self.parent)
            with self.parent.lock:
                _ = ts_task.is_done
                i_start = 1 + ts_task.subtasks.index(self)
                for task in ts_task.subtasks[i_start:]:
                    task.is_done = True
            return None
        return self._create_payload_item(math.inf if (remaining is None) else remaining)

    def _create_payload_item(self, remaining_limit):
        return {
            **self.identifier.as_dict(),
            "start": self.next_start,
            "end": self.end,
            "limit": min(remaining_limit, self.max_query_limit),
            **self.agg_kwargs,
        }

    def store_partial_result(self, res):
        if self.parent.ts_info is None:
            self.parent._store_ts_info(res)
        dps = get_datapoints_from_proto(res)
        if not dps:
            self.is_done = True
            return None
        (n, last_ts) = (len(dps), dps[(-1)].timestamp)
        self.parent._unpack_and_store(self.subtask_idx, dps)
        self._update_state_for_next_payload(last_ts, n)
        if self._is_task_done(n, res.nextCursor):
            self.is_done = True
        return None

    def _update_state_for_next_payload(self, last_ts, n):
        self.next_start = last_ts + self.parent.offset_next
        self.n_dps_fetched += n

    def _is_task_done(self, n, next_cursor):
        return (not next_cursor) or (n < self.max_query_limit) or (self.next_start == self.end)


class SplittingFetchSubtask(SerialFetchSubtask):
    def __init__(self, *, max_splitting_factor: int = 10, **kwargs):
        super().__init__(**kwargs)
        self.max_splitting_factor = max_splitting_factor
        self.split_subidx: int = 0

    def store_partial_result(self, res):
        self.prev_start = self.next_start
        super().store_partial_result(res)
        if not self.is_done:
            last_ts = get_datapoints_from_proto(res)[(-1)].timestamp
            return self._split_self_into_new_subtasks_if_needed(last_ts)
        return None

    def _create_subtasks_idxs(self, n_new_tasks):
        end = self.split_subidx
        self.split_subidx -= n_new_tasks
        (yield from ((*self.subtask_idx, i) for i in range(self.split_subidx, end)))

    def _split_self_into_new_subtasks_if_needed(self, last_ts):
        start = self.prev_start
        tot_ms = self.end - start
        part_ms = last_ts - start
        ratio_retrieved = part_ms / tot_ms
        n_new_pct = math.floor(1 / ratio_retrieved)
        n_new_lim = math.inf
        remaining_limit = self.get_remaining_limit()
        if remaining_limit is not None:
            n_new_lim = math.ceil(remaining_limit / self.max_query_limit)
        n_new_tasks = min(cast(int, n_new_lim), n_new_pct, (self.max_splitting_factor + 1))
        if n_new_tasks <= 1:
            return None
        boundaries = split_time_range(last_ts, self.end, n_new_tasks, self.parent.offset_next)
        self.end = boundaries[1]
        static_params = {
            "parent": self.parent,
            "priority": self.priority,
            "identifier": self.identifier,
            "aggregates": self.aggregates,
            "granularity": self.granularity,
            "max_query_limit": self.max_query_limit,
            "is_raw_query": self.is_raw_query,
        }
        split_idxs = self._create_subtasks_idxs(n_new_tasks)
        new_subtasks = [
            SplittingFetchSubtask(start=start, end=end, limit=remaining_limit, subtask_idx=idx, **static_params)
            for (start, end, idx) in zip(boundaries[1:(-1)], boundaries[2:], split_idxs)
        ]
        self.parent.subtasks.update(new_subtasks)
        return new_subtasks


class BaseConcurrentTask:
    def __init__(self, query, eager_mode, use_numpy, first_dps_batch=None, first_limit=None):
        self.query = query
        self.eager_mode = eager_mode
        self.use_numpy = use_numpy
        self.ts_info: Optional[Dict] = None
        self.ts_data = create_dps_container()
        self.dps_data = create_dps_container()
        self.subtasks = create_subtask_lst()
        self.subtask_outside_points: Optional[OutsideDpsFetchSubtask] = None
        self.raw_dtype: Optional[type] = None
        self._is_done = False
        self.lock = Lock()
        self.has_limit = self.query.limit is not None
        if not self.eager_mode:
            assert (first_limit is not None) and (first_dps_batch is not None)
            dps = get_datapoints_from_proto(first_dps_batch)
            self._store_ts_info(first_dps_batch)
            if not dps:
                self._is_done = True
                return None
            self._store_first_batch(dps, first_limit)

    @property
    def ts_info_dct(self):
        assert self.ts_info is not None
        return self.ts_info

    @property
    def start_ts_first_batch(self):
        ts = self.ts_data[FIRST_IDX][0][0]
        return ts.item() if self.use_numpy else ts

    @property
    def end_ts_first_batch(self):
        ts = self.ts_data[FIRST_IDX][0][(-1)]
        return ts.item() if self.use_numpy else ts

    @property
    def n_dps_first_batch(self):
        if self.eager_mode:
            return 0
        return len(self.ts_data[FIRST_IDX][0])

    @property
    def is_done(self):
        if self.ts_info is None:
            return False
        elif self._is_done:
            return True
        elif self.subtask_outside_points and (not self.subtask_outside_points.is_done):
            return False
        elif self.subtasks:
            self._is_done = all(task.is_done for task in self.subtasks)
        return self._is_done

    @is_done.setter
    def is_done(self, value):
        self._is_done = value

    @property
    @abstractmethod
    def offset_next(self):
        ...

    @abstractmethod
    def get_result(self):
        ...

    @abstractmethod
    def _unpack_and_store(self, idx, dps):
        ...

    @abstractmethod
    def _extract_outside_points(self, dps):
        ...

    @abstractmethod
    def _find_number_of_subtasks_uniform_split(self, tot_ms, n_workers_per_queries):
        ...

    def split_into_subtasks(self, max_workers, n_tot_queries):
        if self.is_done:
            return []
        n_workers_per_queries = max(1, round(max_workers / n_tot_queries))
        subtasks: List[BaseDpsFetchSubtask] = self._create_uniformly_split_subtasks(n_workers_per_queries)
        self.subtasks.update(subtasks)
        if self.eager_mode and self.query.include_outside_points:
            self.subtask_outside_points = OutsideDpsFetchSubtask(
                start=self.query.start, end=self.query.end, identifier=self.query.identifier, parent=self
            )
            subtasks.append(self.subtask_outside_points)
        return subtasks

    def _create_uniformly_split_subtasks(self, n_workers_per_queries):
        start = self.query.start if self.eager_mode else self.first_start
        end = self.query.end
        tot_ms = end - start
        n_periods = self._find_number_of_subtasks_uniform_split(tot_ms, n_workers_per_queries)
        boundaries = split_time_range(start, end, n_periods, self.offset_next)
        limit = (self.query.limit - self.n_dps_first_batch) if self.has_limit else None
        return [
            SplittingFetchSubtask(
                start=start,
                end=end,
                limit=limit,
                subtask_idx=(i,),
                parent=self,
                priority=((i - 1) if self.has_limit else 0),
                identifier=self.query.identifier,
                aggregates=self.query.aggregates_cc,
                granularity=self.query.granularity,
                max_query_limit=self.query.max_query_limit,
                is_raw_query=self.query.is_raw_query,
            )
            for (i, (start, end)) in enumerate(zip(boundaries[:(-1)], boundaries[1:]), 1)
        ]

    def _decide_dtype_from_is_string(self, is_string):
        return np.object_ if is_string else np.float64

    def _store_ts_info(self, res):
        self.ts_info = get_ts_info_from_proto(res)
        self.ts_info["granularity"] = self.query.granularity
        if self.use_numpy:
            self.raw_dtype = self._decide_dtype_from_is_string(res.isString)

    def _store_first_batch(self, dps, first_limit):
        self.first_start = dps[(-1)].timestamp + self.offset_next
        self._unpack_and_store(FIRST_IDX, dps)
        if self.first_start == self.query.end:
            self._is_done = True
        elif self.has_limit and (len(dps) <= self.query.limit <= first_limit):
            self._is_done = True
        elif len(dps) < first_limit:
            self._is_done = True

    def get_remaining_limit(self, subtask):
        if not self.has_limit:
            return None
        remaining = cast(int, self.query.limit)
        with self.lock:
            for task in self.subtasks:
                remaining = remaining - task.n_dps_fetched
                if (task is subtask) or (remaining <= 0):
                    break
        return max(0, remaining)


class ConcurrentLimitedMixin(BaseConcurrentTask):
    @property
    def is_done(self):
        if self.ts_info is None:
            return False
        elif self._is_done:
            return True
        elif self.subtask_outside_points and (not self.subtask_outside_points.is_done):
            return False
        elif self.subtasks:
            i_first_in_progress = True
            n_dps_to_fetch = cast(int, self.query.limit) - self.n_dps_first_batch
            for (i, task) in enumerate(self.subtasks):
                if not (task.is_done or i_first_in_progress):
                    break
                if i_first_in_progress:
                    i_first_in_progress = False
                n_dps_to_fetch -= task.n_dps_fetched
                if n_dps_to_fetch == 0:
                    self._is_done = True
                    for task in self.subtasks[(i + 1) :]:
                        task.is_done = True
                    break
                if not i_first_in_progress:
                    break
            else:
                self._is_done = True
        return self._is_done

    @is_done.setter
    def is_done(self, value):
        self._is_done = value


class BaseConcurrentRawTask(BaseConcurrentTask):
    def __init__(self, **kwargs):
        self.dp_outside_start: Optional[Tuple[(int, RawDatapointValue)]] = None
        self.dp_outside_end: Optional[Tuple[(int, RawDatapointValue)]] = None
        super().__init__(**kwargs)

    @property
    def offset_next(self):
        return 1

    def _create_empty_result(self):
        if not self.use_numpy:
            return Datapoints(**self.ts_info_dct, timestamp=[], value=[])
        return DatapointsArray._load(
            {**self.ts_info_dct, "timestamp": np.array([], dtype=np.int64), "value": np.array([], dtype=self.raw_dtype)}
        )

    def _was_any_data_fetched(self):
        return any((self.ts_data, self.dp_outside_start, self.dp_outside_end))

    def get_result(self):
        if not self._was_any_data_fetched():
            return self._create_empty_result()
        if self.has_limit:
            self._cap_dps_at_limit()
        if self.query.include_outside_points:
            self._include_outside_points_in_result()
        if self.use_numpy:
            return DatapointsArray._load(
                {
                    **self.ts_info_dct,
                    "timestamp": create_array_from_dps_container(self.ts_data),
                    "value": create_array_from_dps_container(self.dps_data),
                }
            )
        return Datapoints(
            **self.ts_info_dct,
            timestamp=create_list_from_dps_container(self.ts_data),
            value=create_list_from_dps_container(self.dps_data),
        )

    def _find_number_of_subtasks_uniform_split(self, tot_ms, n_workers_per_queries):
        return min(n_workers_per_queries, math.ceil((tot_ms / 1000) / self.query.max_query_limit))

    def _cap_dps_at_limit(self):
        count = 0
        for (i, (subtask_idx, sublist)) in enumerate(self.ts_data.items()):
            for (j, seq) in enumerate(sublist):
                if (count + len(seq)) < self.query.limit:
                    count += len(seq)
                    continue
                end = self.query.limit - count
                self.ts_data[subtask_idx][j] = seq[:end]
                self.dps_data[subtask_idx][j] = self.dps_data[subtask_idx][j][:end]
                self.ts_data[subtask_idx] = self.ts_data[subtask_idx][: (j + 1)]
                self.dps_data[subtask_idx] = self.dps_data[subtask_idx][: (j + 1)]
                (new_ts, new_dps) = (create_dps_container(), create_dps_container())
                new_ts.update(self.ts_data.items()[: (i + 1)])
                new_dps.update(self.dps_data.items()[: (i + 1)])
                (self.ts_data, self.dps_data) = (new_ts, new_dps)
                return None

    def _include_outside_points_in_result(self):
        for (dp, idx) in zip((self.dp_outside_start, self.dp_outside_end), ((-math.inf), math.inf)):
            if dp:
                ts: Union[(List[int], NumpyInt64Array)] = [dp[0]]
                value: Union[(List[Union[(float, str)]], NumpyFloat64Array, NumpyObjArray)] = [dp[1]]
                if self.use_numpy:
                    ts = np.array(ts, dtype=np.int64)
                    value = np.array(value, dtype=self.raw_dtype)
                self.ts_data[(idx,)].append(ts)
                self.dps_data[(idx,)].append(value)

    def _unpack_and_store(self, idx, dps):
        if self.use_numpy:
            self.ts_data[idx].append(np.fromiter(map(DpsUnpackFns.ts, dps), dtype=np.int64, count=len(dps)))
            self.dps_data[idx].append(np.fromiter(map(DpsUnpackFns.raw_dp, dps), dtype=self.raw_dtype, count=len(dps)))
        else:
            self.ts_data[idx].append(list(map(DpsUnpackFns.ts, dps)))
            self.dps_data[idx].append(list(map(DpsUnpackFns.raw_dp, dps)))

    def _store_first_batch(self, dps, first_limit):
        if self.query.is_raw_query and self.query.include_outside_points:
            self._extract_outside_points(cast(DatapointsRaw, dps))
            if not dps:
                self._is_done = True
                return None
        super()._store_first_batch(dps, first_limit)

    def _extract_outside_points(self, dps):
        if dps[0].timestamp < self.query.start:
            self.dp_outside_start = DpsUnpackFns.ts_dp_tpl(dps.pop(0))
        if dps and (dps[(-1)].timestamp >= self.query.end):
            self.dp_outside_end = DpsUnpackFns.ts_dp_tpl(dps.pop(-1))


class ParallelUnlimitedRawTask(BaseConcurrentRawTask):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        assert isinstance(self.query, _SingleTSQueryRawUnlimited)


class ParallelLimitedRawTask(ConcurrentLimitedMixin, BaseConcurrentRawTask):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        assert isinstance(self.query, _SingleTSQueryRawLimited)

    def _find_number_of_subtasks_uniform_split(self, tot_ms, n_workers_per_queries):
        n_estimate_periods = math.ceil((tot_ms / 1000) / self.query.max_query_limit)
        remaining_limit = self.query.limit - self.n_dps_first_batch
        n_periods = max(1, math.ceil(remaining_limit / self.query.max_query_limit))
        return min(n_workers_per_queries, n_periods, n_estimate_periods)


class BaseConcurrentAggTask(BaseConcurrentTask):
    def __init__(self, *, query: _SingleTSQueryAgg, use_numpy: bool, **kwargs):
        aggregates_cc = query.aggregates_cc
        self._set_aggregate_vars(aggregates_cc, use_numpy)
        super().__init__(query=query, use_numpy=use_numpy, **kwargs)

    def offset_next(self):
        return granularity_to_ms(self.query.granularity)

    def _set_aggregate_vars(self, aggregates_cc, use_numpy):
        self.float_aggs = aggregates_cc[:]
        self.is_count_query = "count" in self.float_aggs
        if self.is_count_query:
            self.count_data = create_dps_container()
            self.float_aggs.remove("count")
        self.has_non_count_aggs = bool(self.float_aggs)
        if self.has_non_count_aggs:
            self.agg_unpack_fn = DpsUnpackFns.custom_from_aggregates(self.float_aggs)
            (self.first_non_count_agg, *others) = self.float_aggs
            self.single_non_count_agg = not others
            if use_numpy:
                if self.single_non_count_agg:
                    self.dtype_aggs: np.dtype[Any] = np.dtype(np.float64)
                else:
                    self.dtype_aggs = np.dtype((np.float64, len(self.float_aggs)))

    def _find_number_of_subtasks_uniform_split(self, tot_ms, n_workers_per_queries):
        n_max_dps = tot_ms // self.offset_next
        return min(n_workers_per_queries, math.ceil(n_max_dps / self.query.max_query_limit))

    def _create_empty_result(self):
        if self.use_numpy:
            arr_dct = {"timestamp": np.array([], dtype=np.int64)}
            if self.is_count_query:
                arr_dct["count"] = np.array([], dtype=np.int64)
            if self.has_non_count_aggs:
                arr_dct.update({agg: np.array([], dtype=np.float64) for agg in self.float_aggs})
            return DatapointsArray._load({**self.ts_info_dct, **arr_dct})
        lst_dct: Dict[(str, List)] = {"timestamp": []}
        if self.is_count_query:
            lst_dct["count"] = []
        if self.has_non_count_aggs:
            lst_dct.update({agg: [] for agg in self.float_aggs})
        return Datapoints(**self.ts_info_dct, **convert_all_keys_to_snake_case(lst_dct))

    def get_result(self):
        if (not self.ts_data) or (self.query.limit == 0):
            return self._create_empty_result()
        if self.has_limit:
            self._cap_dps_at_limit()
        if self.use_numpy:
            arr_dct = {"timestamp": create_array_from_dps_container(self.ts_data)}
            if self.is_count_query:
                arr_dct["count"] = create_array_from_dps_container(self.count_data)
            if self.has_non_count_aggs:
                arr_lst = create_aggregates_arrays_from_dps_container(self.dps_data, len(self.float_aggs))
                arr_dct.update(dict(zip(self.float_aggs, arr_lst)))
            return DatapointsArray._load({**self.ts_info_dct, **arr_dct})
        lst_dct = {"timestamp": create_list_from_dps_container(self.ts_data)}
        if self.is_count_query:
            lst_dct["count"] = create_list_from_dps_container(self.count_data)
        if self.has_non_count_aggs:
            if self.single_non_count_agg:
                lst_dct[self.first_non_count_agg] = create_list_from_dps_container(self.dps_data)
            else:
                aggs_iter = create_aggregates_list_from_dps_container(self.dps_data)
                lst_dct.update(dict(zip(self.float_aggs, aggs_iter)))
        return Datapoints(**self.ts_info_dct, **convert_all_keys_to_snake_case(lst_dct))

    def _cap_dps_at_limit(self):
        (count, to_update) = (0, ["ts_data"])
        if self.is_count_query:
            to_update.append("count_data")
        if self.has_non_count_aggs:
            to_update.append("dps_data")
        for (i, (subtask_idx, sublist)) in enumerate(self.ts_data.items()):
            for (j, arr) in enumerate(sublist):
                if (count + len(arr)) < self.query.limit:
                    count += len(arr)
                    continue
                end = self.query.limit - count
                for attr in to_update:
                    data = getattr(self, attr)
                    data[subtask_idx][j] = data[subtask_idx][j][:end]
                    data[subtask_idx] = data[subtask_idx][: (j + 1)]
                    setattr(self, attr, dict(data.items()[: (i + 1)]))
                return None

    def _unpack_and_store(self, idx, dps):
        if self.use_numpy:
            self._unpack_and_store_numpy(idx, dps)
        else:
            self._unpack_and_store_basic(idx, dps)

    def _unpack_and_store_numpy(self, idx, dps):
        n = len(dps)
        self.ts_data[idx].append(np.fromiter(map(DpsUnpackFns.ts, dps), dtype=np.int64, count=n))
        if self.is_count_query:
            arr = np.fromiter(map(DpsUnpackFns.count, dps), dtype=np.float64, count=n)
            arr = np.nan_to_num(arr, copy=False, nan=0.0, posinf=np.inf, neginf=(-np.inf)).astype(np.int64)
            self.count_data[idx].append(arr)
        if self.has_non_count_aggs:
            try:
                arr = np.fromiter(map(self.agg_unpack_fn, dps), dtype=self.dtype_aggs, count=n)
            except AttributeError:
                arr = np.array(
                    [tuple(getattr(dp, agg, math.nan) for agg in self.float_aggs) for dp in dps], dtype=np.float64
                )
            self.dps_data[idx].append(arr.reshape(n, len(self.float_aggs)))

    def _unpack_and_store_basic(self, idx, dps):
        self.ts_data[idx].append(list(map(DpsUnpackFns.ts, dps)))
        if self.is_count_query:
            self.count_data[idx].append(list(map(ensure_int, (getattr(dp, "count") for dp in dps))))
        if self.has_non_count_aggs:
            try:
                lst = list(map(self.agg_unpack_fn, dps))
            except AttributeError:
                if self.single_non_count_agg:
                    lst = [getattr(dp, self.first_non_count_agg, None) for dp in dps]
                else:
                    lst = [tuple(getattr(dp, agg, None) for agg in self.float_aggs) for dp in dps]
            self.dps_data[idx].append(lst)


class ParallelUnlimitedAggTask(BaseConcurrentAggTask):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        assert isinstance(self.query, _SingleTSQueryAggUnlimited)


class ParallelLimitedAggTask(ConcurrentLimitedMixin, BaseConcurrentAggTask):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        assert isinstance(self.query, _SingleTSQueryAggLimited)

    def _find_number_of_subtasks_uniform_split(self, tot_ms, n_workers_per_queries):
        remaining_limit = self.query.limit - self.n_dps_first_batch
        n_max_dps = min(remaining_limit, (tot_ms // self.offset_next))
        return max(1, min(n_workers_per_queries, math.ceil(n_max_dps / self.query.max_query_limit)))
