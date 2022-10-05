from __future__ import annotations

import math
import numbers
import operator as op
import warnings
from abc import abstractmethod
from datetime import datetime
from functools import cached_property
from itertools import chain
from threading import Lock
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Hashable,
    Iterable,
    Iterator,
    List,
    Literal,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
)

from sortedcontainers import SortedDict, SortedList  # type: ignore [import]

from cognite.client._api.datapoint_constants import (
    DPS_LIMIT,
    DPS_LIMIT_AGG,
    NUMPY_IS_AVAILABLE,
    CustomDatapoints,
    CustomDatapointsQuery,
    DatapointsExternalIdTypes,
    DatapointsFromAPI,
    DatapointsIdTypes,
    DatapointsQueryExternalId,
    DatapointsQueryId,
    DatapointsTypes,
)
from cognite.client.data_classes.datapoints import Datapoints, DatapointsArray, DatapointsQuery
from cognite.client.utils._auxiliary import convert_all_keys_to_snake_case, to_camel_case
from cognite.client.utils._identifier import Identifier
from cognite.client.utils._time import (
    align_start_and_end_for_granularity,
    granularity_to_ms,
    split_time_range,
    timestamp_to_ms,
)

if NUMPY_IS_AVAILABLE:
    import numpy as np


if TYPE_CHECKING:
    import numpy.typing as npt


T = TypeVar("T")


class _SingleTSQueryValidator:
    def __init__(self, user_query: DatapointsQuery) -> None:
        self.user_query = user_query
        self.defaults: CustomDatapointsQuery = dict(
            start=user_query.start,
            end=user_query.end,
            limit=user_query.limit,
            aggregates=user_query.aggregates,
            granularity=user_query.granularity,
            include_outside_points=user_query.include_outside_points,
            ignore_unknown_ids=user_query.ignore_unknown_ids,
        )

    def validate_and_create_single_queries(self) -> List[_SingleTSQueryBase]:
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

    def _validate_multiple_id(self, id: DatapointsIdTypes) -> List[_SingleTSQueryBase]:
        return self._validate_id_or_xid(id, "id", numbers.Integral, is_external_id=False)

    def _validate_multiple_xid(self, external_id: DatapointsExternalIdTypes) -> List[_SingleTSQueryBase]:
        return self._validate_id_or_xid(external_id, "external_id", str, is_external_id=True)

    def _validate_id_or_xid(
        self,
        id_or_xid: Union[DatapointsIdTypes, DatapointsExternalIdTypes],
        arg_name: str,
        exp_type: type,
        is_external_id: bool,
    ) -> List[_SingleTSQueryBase]:

        if isinstance(id_or_xid, (exp_type, dict)):
            # Lazy - we postpone evaluation:
            id_or_xid = [id_or_xid]  # type: ignore [assignment]

        if not isinstance(id_or_xid, Sequence):
            # We use Sequence because we require an ordering of elements
            self._raise_on_wrong_ts_identifier_type(id_or_xid, arg_name, exp_type)

        queries = []
        for ts in id_or_xid:
            if isinstance(ts, exp_type):
                ts_dct = {**self.defaults, arg_name: ts}
                queries.append(self._validate_and_create_query(ts_dct))  # type: ignore [arg-type]

            elif isinstance(ts, dict):
                ts_validated = self._validate_ts_query_dict_keys(ts, arg_name, exp_type)
                # We merge 'defaults' and given ts-dict, ts-dict takes precedence:
                ts_dct = {**self.defaults, **ts_validated}
                queries.append(self._validate_and_create_query(ts_dct))  # type: ignore [arg-type]
            else:  # pragma: no cover
                self._raise_on_wrong_ts_identifier_type(ts, arg_name, exp_type)
        return queries

    @staticmethod
    def _raise_on_wrong_ts_identifier_type(
        id_or_xid: Union[DatapointsIdTypes, DatapointsExternalIdTypes],
        arg_name: str,
        exp_type: type,
    ) -> NoReturn:
        raise TypeError(
            f"Got unsupported type {type(id_or_xid)}, as, or part of argument `{arg_name}`. Expected one of "
            f"{exp_type}, {dict} or a (mixed) list of these, but got `{id_or_xid}`."
        )

    @staticmethod
    def _validate_ts_query_dict_keys(
        dct: Dict[str, Any], arg_name: str, exp_type: type
    ) -> Union[DatapointsQueryId, DatapointsQueryExternalId]:
        if arg_name not in dct:
            if (arg_name_cc := to_camel_case(arg_name)) not in dct:
                raise KeyError(f"Missing required key `{arg_name}` in dict: {dct}.")
            # For backwards compatibility we accept identifiers in camel case: (Make copy to avoid side effects
            # for user's input). Also means we need to return it.
            dct[arg_name] = (dct := dct.copy()).pop(arg_name_cc)

        ts_identifier = dct[arg_name]
        if not isinstance(ts_identifier, exp_type):
            _SingleTSQueryValidator._raise_on_wrong_ts_identifier_type(ts_identifier, arg_name, exp_type)

        opt_dct_keys = {
            "start",
            "end",
            "aggregates",
            "granularity",
            "limit",
            "include_outside_points",
            "ignore_unknown_ids",
        }
        bad_keys = set(dct) - opt_dct_keys - {arg_name}
        if not bad_keys:
            return dct  # type: ignore [return-value]
        raise KeyError(
            f"Dict provided by argument `{arg_name}` included key(s) not understood: {sorted(bad_keys)}. "
            f"Required key: `{arg_name}`. Optional: {list(opt_dct_keys)}."
        )

    def _validate_and_create_query(
        self, dct: Union[DatapointsQueryId, DatapointsQueryExternalId]
    ) -> _SingleTSQueryBase:
        limit = self._verify_limit(dct["limit"])
        granularity, aggregates = dct["granularity"], dct["aggregates"]

        if not (granularity is None or isinstance(granularity, str)):
            raise TypeError(f"Expected `granularity` to be of type `str` or None, not {type(granularity)}")

        elif not (aggregates is None or isinstance(aggregates, list)):
            raise TypeError(f"Expected `aggregates` to be of type `list[str]` or None, not {type(aggregates)}")

        elif aggregates is None:
            if granularity is None:
                # Request is for raw datapoints:
                raw_query = self._convert_parameters(dct, limit, is_raw=True)
                if limit is None:
                    return _SingleTSQueryRawUnlimited(**raw_query)
                return _SingleTSQueryRawLimited(**raw_query)
            raise ValueError("When passing `granularity`, argument `aggregates` is also required.")

        # Aggregates must be a list at this point:
        elif len(aggregates) == 0:
            raise ValueError("Empty list of `aggregates` passed, expected at least one!")

        elif granularity is None:
            raise ValueError("When passing `aggregates`, argument `granularity` is also required.")

        elif dct["include_outside_points"] is True:
            raise ValueError("'Include outside points' is not supported for aggregates.")
        # Request is for one or more aggregates:
        agg_query = self._convert_parameters(dct, limit, is_raw=False)
        if limit is None:
            return _SingleTSQueryAggUnlimited(**agg_query)
        return _SingleTSQueryAggLimited(**agg_query)

    def _convert_parameters(
        self,
        dct: Union[DatapointsQueryId, DatapointsQueryExternalId],
        limit: Optional[int],
        is_raw: bool,
    ) -> Dict[str, Any]:
        identifier = Identifier.of_either(dct.get("id"), dct.get("external_id"))  # type: ignore [arg-type]
        start, end = self._verify_time_range(dct["start"], dct["end"], dct["granularity"], is_raw, identifier)
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
            converted["aggregates"] = dct["aggregates"]
            converted["granularity"] = dct["granularity"]
        return converted

    def _verify_limit(self, limit: Optional[int]) -> Optional[int]:
        if limit in {None, -1, math.inf}:
            return None
        elif isinstance(limit, numbers.Integral) and limit >= 0:  # limit=0 is accepted by the API
            try:
                # We don't want weird stuff like numpy dtypes etc:
                return int(limit)
            except Exception:  # pragma no cover
                raise TypeError(f"Unable to convert given {limit=} to integer")
        raise TypeError(
            "Parameter `limit` must be a non-negative integer -OR- one of [None, -1, inf] to "
            f"indicate an unlimited query. Got: {limit} with type: {type(limit)}"
        )

    def _verify_time_range(
        self,
        start: Union[int, str, datetime, None],
        end: Union[int, str, datetime, None],
        granularity: Optional[str],
        is_raw: bool,
        identifier: Identifier,
    ) -> Tuple[int, int]:
        if start is None:
            start = 0  # 1970-01-01
        else:
            start = timestamp_to_ms(start)
        if end is None:
            end = "now"
        end = timestamp_to_ms(end)

        if end <= start:
            raise ValueError(
                f"Invalid time range, {end=} must be later than {start=} "
                f"(from query: {identifier.as_dict(camel_case=False)})"
            )
        if not is_raw:  # API rounds aggregate query timestamps in a very particular fashion
            start, end = align_start_and_end_for_granularity(start, end, cast(str, granularity))
        return start, end


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
    ) -> None:
        self.identifier = identifier
        self.start = start
        self.end = end
        self.max_query_limit = max_query_limit
        self.limit = limit
        self.include_outside_points = include_outside_points
        self.ignore_unknown_ids = ignore_unknown_ids

        self._is_missing: Optional[bool] = None
        self._is_string: Optional[bool] = None

        if self.include_outside_points and self.limit is not None:
            warnings.warn(
                "When using `include_outside_points=True` with a finite `limit` you may get a large gap "
                "between the last 'inside datapoint' and the 'after/outside' datapoint. Note also that the "
                "up-to-two outside points come in addition to your given `limit`; asking for 5 datapoints might "
                "yield 5, 6 or 7. It's a feature, not a bug ;)",
                UserWarning,
            )

    @property
    def capped_limit(self) -> int:
        if self.limit is None:
            return self.max_query_limit
        return min(self.limit, self.max_query_limit)

    def override_max_query_limit(self, new_limit: int) -> None:
        assert isinstance(new_limit, int)
        self.max_query_limit = new_limit

    @property
    @abstractmethod
    def is_raw_query(self) -> bool:
        ...

    @property
    @abstractmethod
    def ts_task_type(self) -> type[BaseConcurrentTask]:
        ...

    @property
    def is_missing(self) -> bool:
        if self._is_missing is None:
            raise RuntimeError("Before making API-calls the `is_missing` status is unknown")
        return self._is_missing

    @is_missing.setter
    def is_missing(self, value: bool) -> None:
        assert isinstance(value, bool)
        self._is_missing = value

    @property
    def is_string(self) -> bool:
        if self._is_string is None:
            raise RuntimeError(
                "For queries asking for raw datapoints, the `is_string` status is unknown before "
                "any API-calls have been made"
            )
        return self._is_string

    @is_string.setter
    def is_string(self, value: bool) -> None:
        assert isinstance(value, bool)
        self._is_string = value


class _SingleTSQueryRaw(_SingleTSQueryBase):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs, max_query_limit=DPS_LIMIT)
        self.aggregates = self.aggregates_cc = None
        self.granularity = None

    @property
    def is_raw_query(self) -> bool:
        return True

    def to_payload(self) -> Dict[str, Any]:
        return {
            **self.identifier.as_dict(),
            "start": self.start,
            "end": self.end,
            "limit": self.capped_limit,
            "includeOutsidePoints": self.include_outside_points,
        }


class _SingleTSQueryRawLimited(_SingleTSQueryRaw):
    def __init__(self, *, limit: int, **kwargs: Any) -> None:
        super().__init__(limit=limit, **kwargs)
        assert isinstance(limit, int)

    @property
    def ts_task_type(self) -> type[ParallelLimitedRawTask]:
        return ParallelLimitedRawTask


class _SingleTSQueryRawUnlimited(_SingleTSQueryRaw):
    def __init__(self, *, limit: None, **kwargs: Any) -> None:
        super().__init__(limit=limit, **kwargs)

    @property
    def ts_task_type(self) -> type[ParallelUnlimitedRawTask]:
        return ParallelUnlimitedRawTask


class _SingleTSQueryAgg(_SingleTSQueryBase):
    def __init__(self, *, aggregates: List[str], granularity: str, **kwargs: Any) -> None:
        agg_query_settings = dict(include_outside_points=False, max_query_limit=DPS_LIMIT_AGG)
        super().__init__(**kwargs, **agg_query_settings)  # type: ignore [arg-type]
        self.aggregates = aggregates
        self.granularity = granularity

    @property
    def is_raw_query(self) -> bool:
        return False

    @cached_property
    def aggregates_cc(self) -> List[str]:
        return list(map(to_camel_case, self.aggregates))

    def to_payload(self) -> Dict[str, Any]:
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
    def __init__(self, *, limit: int, **kwargs: Any) -> None:
        super().__init__(limit=limit, **kwargs)
        assert isinstance(limit, int)

    @property
    def ts_task_type(self) -> type[ParallelLimitedAggTask]:
        return ParallelLimitedAggTask


class _SingleTSQueryAggUnlimited(_SingleTSQueryAgg):
    def __init__(self, *, limit: None, **kwargs: Any) -> None:
        super().__init__(limit=limit, **kwargs)

    @property
    def ts_task_type(self) -> type[ParallelUnlimitedAggTask]:
        return ParallelUnlimitedAggTask


class DpsUnpackFns:
    ts: Callable[[Dict], int] = op.itemgetter("timestamp")
    raw_dp: Callable[[Dict], DatapointsTypes] = op.itemgetter("value")
    ts_dp_tpl: Callable[[Dict], Tuple[int, DatapointsTypes]] = op.itemgetter("timestamp", "value")
    ts_count_tpl: Callable[[Dict], Tuple[int, int]] = op.itemgetter("timestamp", "count")
    count: Callable[[Dict], int] = op.itemgetter("count")

    @staticmethod
    def custom_from_aggregates(
        lst: List[str],
    ) -> Callable[[List[Dict[str, DatapointsTypes]]], Tuple[DatapointsTypes, ...]]:
        return op.itemgetter(*lst)


class DefaultSortedDict(SortedDict):
    def __init__(self, default_factory: Callable[[], T], /, **kw: Any):
        self.default_factory = default_factory
        super().__init__(**kw)

    def __missing__(self, key: Hashable) -> T:
        self[key] = self.default_factory()
        return self[key]


def create_dps_container() -> DefaultSortedDict:
    """Initialises a new sorted container for datapoints storage"""
    return DefaultSortedDict(list)


def create_subtask_lst() -> SortedList:
    """Initialises a new sorted list for subtasks"""
    return SortedList(key=op.attrgetter("subtask_idx"))


def create_array_from_dps_container(container: DefaultSortedDict) -> npt.NDArray:
    return np.hstack(list(chain.from_iterable(container.values())))


def create_aggregates_arrays_from_dps_container(container: DefaultSortedDict, n_aggs: int) -> List[npt.NDArray]:
    all_aggs_arr = np.vstack(list(chain.from_iterable(container.values())))
    return list(map(np.ravel, np.hsplit(all_aggs_arr, n_aggs)))


def create_list_from_dps_container(container: DefaultSortedDict) -> List:
    return list(chain.from_iterable(chain.from_iterable(container.values())))


def create_aggregates_list_from_dps_container(container: DefaultSortedDict) -> Iterator[List[List]]:
    concatenated = chain.from_iterable(chain.from_iterable(container.values()))
    return map(list, zip(*concatenated))  # rows to columns


class BaseDpsFetchSubtask:
    def __init__(
        self,
        start: int,
        end: int,
        identifier: Identifier,
        parent: BaseConcurrentTask,
        priority: int,
        max_query_limit: int,
        n_dps_left: float,
        is_raw_query: bool,
    ) -> None:
        self.start = start
        self.end = end
        self.identifier = identifier
        self.parent = parent
        self.priority = priority
        self.is_raw_query = is_raw_query
        self.max_query_limit = max_query_limit
        self.n_dps_left = n_dps_left

        self.is_done = False

    @abstractmethod
    def get_next_payload(self) -> Optional[Dict[str, Any]]:
        ...

    @abstractmethod
    def store_partial_result(self, res: DatapointsFromAPI) -> Optional[List[SplittingFetchSubtask]]:
        ...


class OutsideDpsFetchSubtask(BaseDpsFetchSubtask):
    """Fetches outside points and stores in parent"""

    def __init__(self, **kwargs: Any) -> None:
        outside_dps_settings = dict(priority=0, is_raw_query=True, max_query_limit=0, n_dps_left=0)
        super().__init__(**kwargs, **outside_dps_settings)  # type: ignore [arg-type]

    def get_next_payload(self) -> Optional[Dict[str, Any]]:
        if self.is_done:
            return None
        return self._create_payload_item()

    def _create_payload_item(self) -> Dict[str, Any]:
        return {
            **self.identifier.as_dict(),
            "start": self.start,
            "end": self.end,
            "limit": 0,  # Not a bug; it just returns the outside points
            "includeOutsidePoints": True,
        }

    def store_partial_result(self, res: DatapointsFromAPI) -> None:
        if dps := res["datapoints"]:
            self.parent._extract_outside_points(dps)
        self.is_done = True


class SerialFetchSubtask(BaseDpsFetchSubtask):
    """Fetches datapoints serially until complete, nice and simple. Stores data in parent"""

    def __init__(
        self,
        *,
        limit: Optional[int],
        aggregates: Optional[List[str]],
        granularity: Optional[str],
        subtask_idx: Tuple[float, ...],
        **kwargs: Any,
    ) -> None:
        n_dps_left = math.inf if limit is None else limit
        super().__init__(**kwargs, n_dps_left=n_dps_left)
        self.limit = limit
        self.aggregates = aggregates
        self.granularity = granularity
        self.subtask_idx = subtask_idx
        self.n_dps_fetched = 0
        self.agg_kwargs = {}

        self.next_start = self.start
        if not self.is_raw_query:
            self.agg_kwargs = {"aggregates": self.aggregates, "granularity": self.granularity}

    def get_next_payload(self) -> Optional[Dict[str, Any]]:
        if self.is_done:
            return None
        remaining = self.parent.remaining_limit(self)
        if self.parent.ts_info is not None and remaining == 0:
            # Since last time this task fetched points, earlier tasks have already fetched >= limit dps.
            # (If ts_info isn't known, it means we still have to send out a request, happens when given limit=0)
            self.is_done, ts_task = True, self.parent
            with self.parent.lock:  # Keep sorted list `subtasks` from being mutated
                _ = ts_task.is_done  # Trigger a check of parent task
                # Update all subsequent subtasks to "is done":
                i_start = 1 + ts_task.subtasks.index(self)
                for task in ts_task.subtasks[i_start:]:
                    task.is_done = True
            return None
        return self._create_payload_item(math.inf if remaining is None else remaining)

    def _create_payload_item(self, remaining_limit: float) -> Dict[str, Any]:
        return {
            **self.identifier.as_dict(),
            "start": self.next_start,
            "end": self.end,
            "limit": min(remaining_limit, self.n_dps_left, self.max_query_limit),
            **self.agg_kwargs,
        }

    def store_partial_result(self, res: DatapointsFromAPI) -> None:
        if self.parent.ts_info is None:
            # In eager mode, first task to complete gets the honor to store ts info:
            self.parent._store_ts_info(res)

        if not (dps := res["datapoints"]):
            self.is_done = True
            return None

        n, last_ts = len(dps), cast(int, dps[-1]["timestamp"])
        self.parent._unpack_and_store(self.subtask_idx, dps)
        self._update_state_for_next_payload(last_ts, n)
        if self._is_task_done(n):
            self.is_done = True

    def _update_state_for_next_payload(self, last_ts: int, n: int) -> None:
        self.next_start = last_ts + self.parent.offset_next  # Move `start` to prepare for next query
        self.n_dps_left -= n
        self.n_dps_fetched += n  # Used to quit limited queries asap

    def _is_task_done(self, n: int) -> bool:
        return self.n_dps_left == 0 or n < self.max_query_limit or self.next_start == self.end


class SplittingFetchSubtask(SerialFetchSubtask):
    """Fetches data serially, but splits its time domain ("divide and conquer") based on the density
    of returned datapoints. Stores data in parent"""

    def __init__(self, *, max_splitting_factor: int = 10, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.max_splitting_factor = max_splitting_factor
        self.split_subidx: int = 0  # Actual value doesnt matter (any int will do)

    def store_partial_result(self, res: DatapointsFromAPI) -> Optional[List[SplittingFetchSubtask]]:  # type: ignore [override]
        self.prev_start = self.next_start
        super().store_partial_result(res)
        if not self.is_done:
            last_ts = res["datapoints"][-1]["timestamp"]
            return self._split_self_into_new_subtasks_if_needed(cast(int, last_ts))
        return None

    def _create_subtasks_idxs(self, n_new_tasks: int) -> Iterable[Tuple[float, ...]]:
        """Since this task may decide to split itself multiple times, we count backwards to keep order
        (we rely on tuple sorting logic). Example using `self.subtask_idx=(4,)`:
        - First split into e.g. 3 parts: (4,-3), (4,-2), (4,-1)
        - Next, split into 2: (4, -5) and (4, -4). These now sort before the first split."""
        end = self.split_subidx
        self.split_subidx -= n_new_tasks
        yield from ((*self.subtask_idx, i) for i in range(self.split_subidx, end))

    def _split_self_into_new_subtasks_if_needed(self, last_ts: int) -> Optional[List[SplittingFetchSubtask]]:
        # How many new tasks because of % of time range was fetched?
        tot_ms = self.end - (start := self.prev_start)
        part_ms = last_ts - start
        ratio_retrieved = part_ms / tot_ms
        n_new_pct = math.floor(1 / ratio_retrieved)
        # How many new tasks because of limit left (if limit)?
        n_new_lim = math.inf
        if (remaining_limit := self.parent.remaining_limit(self)) is not None:
            n_new_lim = math.ceil(remaining_limit / self.max_query_limit)
        # We pick strictest criterion:
        n_new_tasks = min(cast(int, n_new_lim), n_new_pct, self.max_splitting_factor + 1)  # +1 for "self next"
        if n_new_tasks <= 1:  # No point in splitting; no faster than this task just continuing
            return None
        # Find a `delta_ms` thats a multiple of granularity in ms (trivial for raw queries):
        boundaries = split_time_range(last_ts, self.end, n_new_tasks, self.parent.offset_next)
        self.end = boundaries[1]  # We shift end of 'self' backwards
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
            SplittingFetchSubtask(
                start=start, end=end, limit=remaining_limit, subtask_idx=idx, **static_params  # type: ignore [arg-type]
            )
            for start, end, idx in zip(boundaries[1:-1], boundaries[2:], split_idxs)
        ]
        self.parent.subtasks.update(new_subtasks)
        return new_subtasks


class BaseConcurrentTask:
    def __init__(
        self,
        query: Any,  # subclasses assert correct type
        eager_mode: bool,
        use_numpy: bool,
        first_dps_batch: Optional[DatapointsFromAPI] = None,
        first_limit: Optional[int] = None,
    ) -> None:
        self.query = query
        self.eager_mode = eager_mode
        self.use_numpy = use_numpy
        self.ts_info = None
        self.ts_data = create_dps_container()
        self.dps_data = create_dps_container()
        self.subtasks = create_subtask_lst()
        self.subtask_outside_points: Optional[OutsideDpsFetchSubtask] = None
        self.raw_dtype: Optional[type] = None
        self._is_done = False
        self.lock = Lock()

        self.has_limit = self.query.limit is not None
        # When running large queries (i.e. not "eager"), all time series have a first batch fetched before
        # further subtasks are created. This gives us e.g. outside points (if asked for) and ts info:
        if not self.eager_mode:
            assert first_limit is not None and first_dps_batch is not None  # mypy...
            dps = first_dps_batch.pop("datapoints")  # type: ignore [misc]
            self.ts_info = first_dps_batch  # Store just the ts info
            self.raw_dtype = self._decide_dtype_from_is_string(first_dps_batch["isString"])
            if not dps:
                self._is_done = True
                return None
            self._store_first_batch(dps, first_limit)

    @property
    def n_dps_first_batch(self) -> int:
        if self.eager_mode:
            return 0
        return len(self.ts_data[(0,)][0])

    @property
    def is_done(self) -> bool:
        if self.ts_info is None:
            return False
        elif self._is_done:
            return True
        elif self.subtask_outside_points and not self.subtask_outside_points.is_done:
            return False
        elif self.subtasks:
            self._is_done = all(task.is_done for task in self.subtasks)
        return self._is_done

    @is_done.setter
    def is_done(self, value: bool) -> None:
        self._is_done = value

    @property
    @abstractmethod
    def offset_next(self) -> int:
        ...

    @abstractmethod
    def get_result(self) -> Union[Datapoints, DatapointsArray]:
        ...

    @abstractmethod
    def _unpack_and_store(self, idx: Tuple[float, ...], dps: List[Dict[str, DatapointsTypes]]) -> None:
        ...

    @abstractmethod
    def _extract_outside_points(self, dps: List[Dict[str, DatapointsTypes]]) -> None:
        ...

    @abstractmethod
    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, n_workers_per_queries: int) -> int:
        ...

    def split_into_subtasks(self, max_workers: int, n_tot_queries: int) -> List[SplittingFetchSubtask]:
        # Given e.g. a single time series, we want to put all our workers to work by splitting into lots of pieces!
        # As the number grows - or we start combining multiple into the same query - we want to split less:
        # we hold back to not create too many subtasks:
        if self.is_done:
            return []
        n_workers_per_queries = max(1, round(max_workers / n_tot_queries))
        subtasks = self._create_uniformly_split_subtasks(n_workers_per_queries)
        self.subtasks.update(subtasks)
        if self.eager_mode and self.query.include_outside_points:
            # In eager mode we do not get the "first dps batch" to extract outside points from:
            self.subtask_outside_points = OutsideDpsFetchSubtask(
                start=self.query.start,
                end=self.query.end,
                identifier=self.query.identifier,
                parent=self,
            )
            # Append the outside subtask to returned subtasks so that it will be queued:
            subtasks.append(self.subtask_outside_points)  # type: ignore [arg-type]
        return subtasks

    def _create_uniformly_split_subtasks(self, n_workers_per_queries: int) -> List[SplittingFetchSubtask]:
        start = self.query.start if self.eager_mode else self.first_start
        tot_ms = (end := self.query.end) - start
        n_periods = self._find_number_of_subtasks_uniform_split(tot_ms, n_workers_per_queries)
        boundaries = split_time_range(start, end, n_periods, self.offset_next)
        limit = self.query.limit - self.n_dps_first_batch if self.has_limit else None
        return [
            SplittingFetchSubtask(
                start=start,
                end=end,
                limit=limit,
                subtask_idx=(i,),
                parent=self,
                priority=i - 1 if self.has_limit else 0,  # Prioritise in chrono. order
                identifier=self.query.identifier,
                aggregates=self.query.aggregates_cc,
                granularity=self.query.granularity,
                max_query_limit=self.query.max_query_limit,
                is_raw_query=self.query.is_raw_query,
            )
            for i, (start, end) in enumerate(zip(boundaries[:-1], boundaries[1:]), 1)
        ]

    def _decide_dtype_from_is_string(self, is_string: bool) -> type:
        return np.object_ if is_string else np.float64

    def _store_ts_info(self, res: DatapointsFromAPI) -> None:
        self.ts_info = {k: v for k, v in res.items() if k != "datapoints"}  # type: ignore [assignment]
        if self.use_numpy:
            self.raw_dtype = self._decide_dtype_from_is_string(res["isString"])

    def _store_first_batch(self, dps: List[Dict[str, DatapointsTypes]], first_limit: int) -> None:
        # Set `start` for the first subtask:
        self.first_start = cast(int, dps[-1]["timestamp"]) + self.offset_next
        self._unpack_and_store((0,), dps)

        # Are we done after first batch?
        if self.first_start == self.query.end:
            self._is_done = True
        elif self.has_limit and len(dps) <= self.query.limit <= first_limit:
            self._is_done = True
        elif len(dps) < first_limit:
            self._is_done = True

    def remaining_limit(self, subtask: BaseDpsFetchSubtask) -> Optional[int]:
        if not self.has_limit:
            return None
        # For limited queries: if the sum of fetched points of earlier tasks have already hit/surpassed
        # `limit`, we know for sure we can cancel later/future tasks:
        remaining = cast(int, self.query.limit)
        with self.lock:  # Keep sorted list `subtasks` from being mutated
            for task in self.subtasks:
                # Sum up to - but not including - given subtask:
                if task is subtask or (remaining := remaining - task.n_dps_fetched) <= 0:
                    break
        return max(0, remaining)


class ConcurrentLimitedMixin(BaseConcurrentTask):
    @property
    def is_done(self) -> bool:
        if self.ts_info is None:
            return False
        elif self._is_done:
            return True
        elif self.subtask_outside_points and not self.subtask_outside_points.is_done:
            return False
        elif self.subtasks:
            # Checking if subtasks are done is not enough; we need to check if the sum of
            # "len of dps takewhile is_done" has reached the limit. Additionally, each subtask might
            # need to fetch a lot of the time subdomain. We want to quit early also when the limit is
            # reached in the first (chronologically) non-finished subtask:
            i_first_in_progress = True
            n_dps_to_fetch = cast(int, self.query.limit) - self.n_dps_first_batch
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
    def is_done(self, value: bool) -> None:  # Kill switch
        self._is_done = value


class BaseConcurrentRawTask(BaseConcurrentTask):
    def __init__(self, **kwargs: Any) -> None:
        self.dp_outside_start: Optional[Tuple[int, DatapointsTypes]] = None
        self.dp_outside_end: Optional[Tuple[int, DatapointsTypes]] = None
        super().__init__(**kwargs)

    @property
    def offset_next(self) -> int:
        return 1  # 1 ms

    def _create_empty_result(self) -> Union[Datapoints, DatapointsArray]:
        if not self.use_numpy:
            return Datapoints(**convert_all_keys_to_snake_case(self.ts_info), timestamp=[], value=[])
        return DatapointsArray._load(
            {
                **cast(dict, self.ts_info),
                "timestamp": np.array([], dtype=np.int64),
                "value": np.array([], dtype=self.raw_dtype),
            }
        )

    def _no_data_fetched(self) -> bool:
        return not any((self.ts_data, self.dp_outside_start, self.dp_outside_end))

    def get_result(self) -> Union[Datapoints, DatapointsArray]:
        if self._no_data_fetched():
            return self._create_empty_result()
        if self.has_limit:
            self._cap_dps_at_limit()
        if self.query.include_outside_points:
            self._include_outside_points_in_result()
        if self.use_numpy:
            return DatapointsArray._load(
                {
                    **cast(dict, self.ts_info),
                    "timestamp": create_array_from_dps_container(self.ts_data),
                    "value": create_array_from_dps_container(self.dps_data),
                }
            )
        return Datapoints(
            **convert_all_keys_to_snake_case(self.ts_info),
            timestamp=create_list_from_dps_container(self.ts_data),
            value=create_list_from_dps_container(self.dps_data),
        )

    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, n_workers_per_queries: int) -> int:
        # It makes no sense to split beyond what the max-size of a query allows (for a maximally dense
        # time series), but that is rarely useful as 100k dps is just 1 min 40 sec... we guess an
        # average density of points at 1 dp/sec, giving us split-windows no smaller than ~1 day:
        return min(n_workers_per_queries, math.ceil((tot_ms / 1000) / self.query.max_query_limit))

    def _cap_dps_at_limit(self) -> None:
        # Note 1: Outside points do not count towards given limit (API spec)
        # Note 2: Lock not needed; called after pool is shut down
        count = 0
        for i, (subtask_idx, sublist) in enumerate(self.ts_data.items()):
            for j, seq in enumerate(sublist):
                if count + len(seq) < self.query.limit:
                    count += len(seq)
                    continue
                end = self.query.limit - count
                self.ts_data[subtask_idx][j] = seq[:end]
                self.dps_data[subtask_idx][j] = self.dps_data[subtask_idx][j][:end]
                # Chop off later arrays (or lists) in same sublist (if any):
                self.ts_data[subtask_idx] = self.ts_data[subtask_idx][: j + 1]
                self.dps_data[subtask_idx] = self.dps_data[subtask_idx][: j + 1]
                # Remove later sublists (if any). We keep using DefaultSortedDicts due to the possibility of
                # having to insert/add 'outside points' later:
                (new_ts := create_dps_container()).update(self.ts_data.items()[: i + 1])  # type: ignore [index]
                (new_dps := create_dps_container()).update(self.dps_data.items()[: i + 1])  # type: ignore [index]
                self.ts_data, self.dps_data = new_ts, new_dps
                return None

    def _include_outside_points_in_result(self) -> None:
        for point, idx in zip((self.dp_outside_start, self.dp_outside_end), (-math.inf, math.inf)):
            if point:
                ts, dp = [point[0]], [point[1]]
                if self.use_numpy:
                    ts = np.array(ts, dtype=np.int64)
                    dp = np.array(dp, dtype=self.raw_dtype)
                self.ts_data[(idx,)].append(ts)
                self.dps_data[(idx,)].append(dp)

    def _unpack_and_store(self, idx: Tuple[float, ...], dps: List[Dict[str, DatapointsTypes]]) -> None:
        if self.use_numpy:  # Faster than feeding listcomp to np.array:
            self.ts_data[idx].append(np.fromiter(map(DpsUnpackFns.ts, dps), dtype=np.int64, count=len(dps)))  # type: ignore [arg-type]
            self.dps_data[idx].append(np.fromiter(map(DpsUnpackFns.raw_dp, dps), dtype=self.raw_dtype, count=len(dps)))  # type: ignore [arg-type]
        else:
            self.ts_data[idx].append(list(map(DpsUnpackFns.ts, dps)))
            self.dps_data[idx].append(list(map(DpsUnpackFns.raw_dp, dps)))

    def _store_first_batch(self, dps: List[Dict[str, DatapointsTypes]], first_limit: int) -> None:
        if self.query.is_raw_query and self.query.include_outside_points:
            self._extract_outside_points(dps)
            if not dps:  # We might have only gotten outside points
                self._is_done = True
                return None
        super()._store_first_batch(dps, first_limit)

    def _extract_outside_points(self, dps: List[Dict[str, DatapointsTypes]]) -> None:
        first_ts = cast(int, dps[0]["timestamp"])
        if first_ts < self.query.start:
            # We got a dp before `start`, this should not impact our count towards `limit`:
            self.dp_outside_start = DpsUnpackFns.ts_dp_tpl(dps.pop(0))  # Slow pop :(
        if dps:
            last_ts = cast(int, dps[-1]["timestamp"])
            if last_ts >= self.query.end:  # >= because `end` is exclusive
                self.dp_outside_end = DpsUnpackFns.ts_dp_tpl(dps.pop())  # Fast pop :)


class ParallelUnlimitedRawTask(BaseConcurrentRawTask):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # This entire method just to tell mypy:
        assert isinstance(self.query, _SingleTSQueryRawUnlimited)


class ParallelLimitedRawTask(ConcurrentLimitedMixin, BaseConcurrentRawTask):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # This entire method just to tell mypy:
        assert isinstance(self.query, _SingleTSQueryRawLimited)

    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, n_workers_per_queries: int) -> int:
        # We make the guess that the time series has ~1 dp/sec and use this in combination with the given
        # limit to not split into too many queries (highest throughput when each request is close to max limit)
        n_estimate_periods = math.ceil((tot_ms / 1000) / self.query.max_query_limit)
        remaining_limit = self.query.limit - self.n_dps_first_batch
        n_periods = max(1, math.ceil(remaining_limit / self.query.max_query_limit))
        # Pick the smallest N from constraints:
        return min(n_workers_per_queries, n_periods, n_estimate_periods)


class BaseConcurrentAggTask(BaseConcurrentTask):
    def __init__(self, *, query: _SingleTSQueryAgg, use_numpy: bool, **kwargs: Any) -> None:
        aggregates_cc = query.aggregates_cc
        self._set_aggregate_vars(aggregates_cc, use_numpy)
        super().__init__(query=query, use_numpy=use_numpy, **kwargs)

    @cached_property
    def offset_next(self) -> int:
        return granularity_to_ms(self.query.granularity)

    def _set_aggregate_vars(self, aggregates_cc: List[str], use_numpy: bool) -> None:
        self.float_aggs = aggregates_cc[:]
        self.is_count_query = "count" in self.float_aggs
        if self.is_count_query:
            self.count_data = create_dps_container()
            self.float_aggs.remove("count")  # Only aggregate that is integer, handle separately

        self.has_non_count_aggs = bool(self.float_aggs)
        if self.has_non_count_aggs:
            self.agg_unpack_fn = DpsUnpackFns.custom_from_aggregates(self.float_aggs)

            self.first_non_count_agg, *others = self.float_aggs
            self.single_non_count_agg = not others

            if use_numpy:
                if self.single_non_count_agg:
                    self.dtype_aggs = np.dtype(np.float64)  # type: ignore [assignment]
                else:  # (.., 1) is deprecated for some reason
                    self.dtype_aggs = np.dtype((np.float64, len(self.float_aggs)))

    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, n_workers_per_queries: int) -> int:
        n_max_dps = tot_ms // self.offset_next  # evenly divides
        return min(n_workers_per_queries, math.ceil(n_max_dps / self.query.max_query_limit))

    def _create_empty_result(self) -> Union[Datapoints, DatapointsArray]:
        if self.use_numpy:
            arr_dct = {"timestamp": np.array([], dtype=np.int64)}
            if self.is_count_query:
                arr_dct["count"] = np.array([], dtype=np.int64)
            if self.has_non_count_aggs:
                arr_dct.update({agg: np.array([], dtype=np.float64) for agg in self.float_aggs})
            return DatapointsArray._load({**cast(dict, self.ts_info), **arr_dct})

        lst_dct = {"timestamp": []}
        if self.is_count_query:
            lst_dct["count"] = []
        if self.has_non_count_aggs:
            lst_dct.update({agg: [] for agg in self.float_aggs})
        return Datapoints(**convert_all_keys_to_snake_case({**cast(dict, self.ts_info), **lst_dct}))

    def get_result(self) -> Union[Datapoints, DatapointsArray]:
        if not self.ts_data or self.query.limit == 0:
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
            return DatapointsArray._load({**cast(dict, self.ts_info), **arr_dct})

        lst_dct = {"timestamp": create_list_from_dps_container(self.ts_data)}
        if self.is_count_query:
            lst_dct["count"] = create_list_from_dps_container(self.count_data)
        if self.has_non_count_aggs:
            if self.single_non_count_agg:
                lst_dct[self.first_non_count_agg] = create_list_from_dps_container(self.dps_data)
            else:
                aggs_iter = create_aggregates_list_from_dps_container(self.dps_data)
                lst_dct.update(dict(zip(self.float_aggs, aggs_iter)))
        return Datapoints(**convert_all_keys_to_snake_case({**cast(dict, self.ts_info), **lst_dct}))

    def _cap_dps_at_limit(self) -> None:
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
                return None

    def _unpack_and_store(self, idx: Tuple[float, ...], dps: List[Dict[str, DatapointsTypes]]) -> None:
        if self.use_numpy:
            self._unpack_and_store_numpy(idx, dps)
        else:
            self._unpack_and_store_basic(idx, dps)

    def _unpack_and_store_numpy(self, idx: Tuple[float, ...], dps: List[Dict[str, DatapointsTypes]]) -> None:
        n = len(dps)
        self.ts_data[idx].append(np.fromiter(map(DpsUnpackFns.ts, dps), dtype=np.int64, count=n))  # type: ignore [arg-type]

        if self.is_count_query:
            try:
                arr = np.fromiter(map(DpsUnpackFns.count, dps), dtype=np.int64, count=n)  # type: ignore [arg-type]
            except KeyError:
                # An interval with no datapoints (hence count does not exist) has data from another aggregate... probably
                # (step_)interpolation. Since the resulting agg. arrays share timestamp, we would have to cast count to float in
                # order to store the missing values as NaNs... We don't want that, so we fill with zeros to keep correct dtype:
                arr = np.array([dp.get("count", 0) for dp in dps], dtype=np.int64)
            self.count_data[idx].append(arr)

        if self.has_non_count_aggs:
            try:  # Fast method uses multi-key unpacking:
                arr = np.fromiter(map(self.agg_unpack_fn, dps), dtype=self.dtype_aggs, count=n)  # type: ignore [arg-type]
            except KeyError:  # An aggregate is missing, fallback to slower `dict.get(agg)`.
                # This can happen when certain aggs. are undefined, e.g. `interpolate` at first interval if rounded down
                arr = np.array([tuple(map(dp.get, self.float_aggs)) for dp in dps], dtype=np.float64)
            self.dps_data[idx].append(arr.reshape(n, len(self.float_aggs)))

    def _unpack_and_store_basic(self, idx: Tuple[float, ...], dps: List[Dict[str, DatapointsTypes]]) -> None:
        self.ts_data[idx].append(list(map(DpsUnpackFns.ts, dps)))

        if self.is_count_query:
            self.count_data[idx].append([dp.get("count", 0) for dp in dps])

        if self.has_non_count_aggs:
            try:
                lst = list(map(self.agg_unpack_fn, dps))
            except KeyError:
                if self.single_non_count_agg:
                    lst = [dp.get(self.first_non_count_agg) for dp in dps]
                else:
                    lst = [tuple(map(dp.get, self.float_aggs)) for dp in dps]
            self.dps_data[idx].append(lst)


class ParallelUnlimitedAggTask(BaseConcurrentAggTask):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # This entire method just to tell mypy:
        assert isinstance(self.query, _SingleTSQueryAggUnlimited)


class ParallelLimitedAggTask(ConcurrentLimitedMixin, BaseConcurrentAggTask):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # This entire method just to tell mypy:
        assert isinstance(self.query, _SingleTSQueryAggLimited)

    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, n_workers_per_queries: int) -> int:
        remaining_limit = self.query.limit - self.n_dps_first_batch
        n_max_dps = min(remaining_limit, tot_ms // self.offset_next)
        return max(1, min(n_workers_per_queries, math.ceil(n_max_dps / self.query.max_query_limit)))
