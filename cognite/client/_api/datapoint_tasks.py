from __future__ import annotations

import datetime
import math
import operator as op
import warnings
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from functools import cached_property
from itertools import chain, pairwise
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NoReturn,
    TypeAlias,
    TypeVar,
    cast,
)

from google.protobuf.internal.containers import RepeatedCompositeFieldContainer

from cognite.client._constants import NUMPY_IS_AVAILABLE
from cognite.client._proto.data_point_list_response_pb2 import DataPointListItem
from cognite.client._proto.data_points_pb2 import (
    AggregateDatapoint,
    NumericDatapoint,
    StringDatapoint,
)
from cognite.client.data_classes.data_modeling.ids import NodeId
from cognite.client.data_classes.datapoints import (
    _INT_AGGREGATES,
    Aggregate,
    Datapoints,
    DatapointsArray,
    DatapointsQuery,
    _DatapointsPayloadItem,
)
from cognite.client.utils._auxiliary import exactly_one_is_not_none, is_unlimited
from cognite.client.utils._text import convert_all_keys_to_snake_case, to_snake_case
from cognite.client.utils._time import (
    ZoneInfo,
    align_start_and_end_for_granularity,
    convert_timezone_to_str,
    granularity_to_ms,
    parse_str_timezone,
    split_granularity_into_quantity_and_normalized_unit,
    split_time_range,
    time_ago_to_ms,
    timestamp_to_ms,
)
from cognite.client.utils.useful_types import SequenceNotStr

if NUMPY_IS_AVAILABLE:
    import numpy as np

if TYPE_CHECKING:
    import numpy.typing as npt

    from cognite.client.data_classes.datapoints import NumpyFloat64Array, NumpyInt64Array, NumpyObjArray


_T = TypeVar("_T")
FIRST_IDX = (0,)

AggregateDatapoints = RepeatedCompositeFieldContainer[AggregateDatapoint]
NumericDatapoints = RepeatedCompositeFieldContainer[NumericDatapoint]
StringDatapoints = RepeatedCompositeFieldContainer[StringDatapoint]

DatapointAny = AggregateDatapoint | NumericDatapoint | StringDatapoint
DatapointsAny = AggregateDatapoints | NumericDatapoints | StringDatapoints

DatapointRaw = NumericDatapoint | StringDatapoint
DatapointsRaw = NumericDatapoints | StringDatapoints

RawDatapointValue = float | str
DatapointsId = int | DatapointsQuery | Sequence[int | DatapointsQuery]
DatapointsExternalId = str | DatapointsQuery | SequenceNotStr[str | DatapointsQuery]
DatapointsInstanceId = NodeId | DatapointsQuery | Sequence[NodeId | DatapointsQuery]


@dataclass
class _FullDatapointsQuery:
    """
    Internal representation of a full user request for datapoints, meaning any number of time series
    requested, previously public (before v5).
    """

    start: int | str | datetime.datetime | None = None
    end: int | str | datetime.datetime | None = None
    id: DatapointsId | None = None
    external_id: DatapointsExternalId | None = None
    instance_id: DatapointsInstanceId | None = None
    aggregates: Aggregate | str | list[Aggregate | str] | None = None
    granularity: str | None = None
    timezone: str | datetime.timezone | ZoneInfo | None = None
    target_unit: str | None = None
    target_unit_system: str | None = None
    limit: int | None = None
    include_outside_points: bool = False
    ignore_unknown_ids: bool = False
    include_status: bool = False
    ignore_bad_datapoints: bool = True
    treat_uncertain_as_bad: bool = True

    @property
    def is_single_identifier(self) -> bool:
        # Exactly one of the identifiers given...
        if exactly_one_is_not_none(self.external_id, self.id, self.instance_id):
            # ...and that one is not a sequence:
            return not (
                isinstance(self.id, Sequence)
                or isinstance(self.external_id, SequenceNotStr)
                or isinstance(self.instance_id, Sequence)
            )
        return False

    @cached_property
    def top_level_defaults(self) -> dict[str, Any]:
        return dict(
            start=self.start,
            end=self.end,
            limit=self.limit,
            aggregates=self.aggregates,
            granularity=self.granularity,
            timezone=self.timezone,
            target_unit=self.target_unit,
            target_unit_system=self.target_unit_system,
            include_outside_points=self.include_outside_points,
            ignore_unknown_ids=self.ignore_unknown_ids,
            include_status=self.include_status,
            ignore_bad_datapoints=self.ignore_bad_datapoints,
            treat_uncertain_as_bad=self.treat_uncertain_as_bad,
        )

    def parse_into_queries(self) -> list[DatapointsQuery]:
        queries = []
        if (id_ := self.id) is not None:
            queries.extend(self._parse(id_, arg_name="id", exp_type=int))
        if (xid := self.external_id) is not None:
            queries.extend(self._parse(xid, arg_name="external_id", exp_type=str))
        if (iid := self.instance_id) is not None:
            queries.extend(self._parse(iid, arg_name="instance_id", exp_type=NodeId))
        if queries:
            return queries
        raise ValueError("Pass at least one time series `id`, `external_id` or `instance_id`!")

    def _parse(
        self,
        identifier: DatapointsId | DatapointsExternalId | DatapointsInstanceId,
        arg_name: Literal["id", "external_id", "instance_id"],
        exp_type: type,
    ) -> list[DatapointsQuery]:
        user_queries: SequenceNotStr[int | str | NodeId | DatapointsQuery]
        if isinstance(identifier, (dict, DatapointsQuery, exp_type)):
            # Lazy - we postpone evaluation:
            user_queries = [identifier]

        elif isinstance(identifier, SequenceNotStr):
            # We use Sequence because we require an ordering of elements
            user_queries = identifier
        else:
            self._raise_on_wrong_ts_identifier_type(identifier, arg_name, exp_type)

        parsed_queries = []
        for query in user_queries:
            # We merge 'defaults' and the given user query; the query takes precedence:
            if isinstance(query, exp_type):
                id_dct = {arg_name: query}
                query = DatapointsQuery(**self.top_level_defaults, **id_dct)  # type: ignore [misc, arg-type]
            elif isinstance(query, dict):
                query = DatapointsQuery.from_dict({**self.top_level_defaults, **query}, id_type=arg_name)

            elif isinstance(query, DatapointsQuery):
                if query.identifier.name() != arg_name:
                    raise ValueError(f"DatapointsQuery passed by {arg_name} is missing required field {arg_name!r}")
                query = DatapointsQuery.from_dict({**self.top_level_defaults, **query.dump()}, id_type=arg_name)
            else:
                self._raise_on_wrong_ts_identifier_type(query, arg_name, exp_type)

            parsed_queries.append(query)
        return parsed_queries

    @staticmethod
    def _raise_on_wrong_ts_identifier_type(
        identifier: object,  # This fn is only called when gotten the wrong type
        arg_name: str,
        exp_type: type,
    ) -> NoReturn:
        raise TypeError(
            f"Got unsupported type {type(identifier)}, as, or part of argument `{arg_name}`. Expected one of "
            f"{exp_type}, {DatapointsQuery}, or a (mixed) list of these, but got `{identifier}`."
        )

    def validate(self, queries: list[DatapointsQuery], dps_limit_raw: int, dps_limit_agg: int) -> list[DatapointsQuery]:
        # We want all start/end = "now" (and those using the same relative time specifiers, like "4d-ago")
        # queries to get the same time domain to fetch. This also -guarantees- that we correctly raise
        # exception 'end not after start' if both are set to the same value.
        frozen_time_now = timestamp_to_ms("now")

        # NOTE: The order of verification checks must be kept due to dependencies:
        for query in queries:
            query.limit = self._verify_and_convert_limit(query.limit)
            query.is_raw_query = self._verify_options_and_categorize_query(query)
            query.original_timezone, query.timezone = self._verify_and_convert_timezone(
                query.timezone, query.is_raw_query
            )
            query.granularity, query.is_calendar_query = self._verify_and_convert_granularity(query.granularity)
            query.start, query.end = self._verify_time_range(query, frozen_time_now)
            if query.is_raw_query:
                query.max_query_limit = dps_limit_raw
            else:
                query.max_query_limit = dps_limit_agg
                if isinstance(query.aggregates, str):
                    query.aggregates = [query.aggregates]
        return queries

    @staticmethod
    def _verify_options_and_categorize_query(query: DatapointsQuery) -> bool:
        target_unit, target_unit_system = query.target_unit, query.target_unit_system
        if target_unit is not None and target_unit_system is not None:
            raise ValueError("You must use either 'target_unit' or 'target_unit_system', not both.")

        granularity, aggregates = query.granularity, query.aggregates
        if not (granularity is None or isinstance(granularity, str)):
            raise TypeError(f"Expected `granularity` to be of type `str` or None, not {type(granularity)}")
        elif not (aggregates is None or isinstance(aggregates, (str, list))):
            raise TypeError(f"Expected `aggregates` to be of type `str`, `list[str]` or None, not {type(aggregates)}")
        elif aggregates is None:
            if granularity is None:
                if query.include_outside_points and query.limit is not None:
                    warnings.warn(
                        "When using `include_outside_points=True` with a finite `limit` you may get a large gap "
                        "between the last 'inside datapoint' and the 'after/outside' datapoint. Note also that the "
                        "up-to-two outside points come in addition to your given `limit`; asking for 5 datapoints might "
                        "yield 5, 6 or 7. It's a feature, not a bug ;)",
                        UserWarning,
                    )
                return True
            raise ValueError("When passing `granularity`, argument `aggregates` is also required.")

        elif isinstance(aggregates, list) and len(aggregates) == 0:
            raise ValueError("Empty list of `aggregates` passed, expected at least one!")
        elif isinstance(aggregates, list) and len(aggregates) != len(set(map(to_snake_case, aggregates))):
            raise ValueError("List of `aggregates` may not contain duplicates")
        elif granularity is None:
            raise ValueError("When passing `aggregates`, argument `granularity` is also required.")
        elif query.include_outside_points is True:
            raise ValueError("'Include outside points' is not supported for aggregates.")
        elif query.include_status is True:
            raise ValueError("'Include status' is not supported for aggregates.")
        return False

    @staticmethod
    def _verify_and_convert_timezone(
        tz: str | datetime.timezone | ZoneInfo | None, is_raw_query: bool
    ) -> tuple[datetime.timezone | ZoneInfo | None, str | None]:
        if tz is None:
            return None, None
        elif isinstance(tz, str):
            tz = parse_str_timezone(tz)  # There...
        try:
            api_tz = convert_timezone_to_str(tz)  # ...and back again
        except TypeError:
            raise ValueError(
                f"'timezone' not understood, expected one of: [None, str, datetime.timezone, ZoneInfo], got {type(tz)}"
            )
        if is_raw_query:
            # Timezone will only be used for display purposes (or when converting to pandas), so we fetch
            # like it doesn't exist (concurrently). The API only supports using timezone with agg. queries.
            return tz, None
        return tz, api_tz

    @staticmethod
    def _verify_and_convert_granularity(granularity: str | None) -> tuple[str | None, bool]:
        if granularity is None:
            return None, False
        quantity, unit = split_granularity_into_quantity_and_normalized_unit(granularity)
        return f"{quantity}{unit}", unit == "mo"

    @staticmethod
    def _verify_and_convert_limit(limit: int | None) -> int | None:
        if is_unlimited(limit):
            return None
        elif isinstance(limit, int) and limit >= 0:  # limit=0 is accepted by the API
            try:
                # We don't want weird stuff like numpy dtypes etc:
                return int(limit)
            except Exception:  # pragma no cover
                raise TypeError(f"Unable to convert given {limit=} to integer")
        raise TypeError(
            "Parameter `limit` must be a non-negative integer -OR- one of [None, -1, inf] to "
            f"indicate an unlimited query. Got: {limit} with type: {type(limit)}"
        )

    @staticmethod
    def _verify_time_range(
        query: DatapointsQuery,
        frozen_time_now: int,
    ) -> tuple[int, int]:
        start = _FullDatapointsQuery._ts_to_ms_frozen_now(query.start, frozen_time_now, default=0)  # 1970-01-01
        end = _FullDatapointsQuery._ts_to_ms_frozen_now(query.end, frozen_time_now, default=frozen_time_now)
        if end <= start:
            raise ValueError(
                f"Invalid time range, {end=} {f'({query.end!r}) ' if end != query.end else ''}"
                f"must be later than {start=} {f'({query.start!r}) ' if start != query.start else ''}"
                f"(from query: {query.identifier.as_dict(camel_case=False)})"
            )
        # We align start and end so that we can efficiently parallelize aggregate dps fetching. Queries
        # using timezone or a calendar granularity (month) are left untouched (and thus fetch serially):
        if not (query.is_raw_query or query.use_cursors):
            # API rounds aggregate query timestamps in a very particular fashion:
            start, end = align_start_and_end_for_granularity(start, end, cast(str, query.granularity))
        return start, end

    @staticmethod
    def _ts_to_ms_frozen_now(ts: int | str | datetime.datetime | None, frozen_time_now: int, default: int) -> int:
        # Time 'now' is frozen for all queries in a single call from the user, leading to identical
        # results e.g. "4d-ago" and "now"
        if ts is None:
            return default
        elif isinstance(ts, str):
            return frozen_time_now - time_ago_to_ms(ts)
        else:
            return timestamp_to_ms(ts)


class DpsUnpackFns:
    ts: Callable[[DatapointAny], int] = op.attrgetter("timestamp")
    raw_dp: Callable[[DatapointRaw], RawDatapointValue] = op.attrgetter("value")

    @staticmethod
    def custom_from_aggregates(lst: list[str]) -> Callable[[AggregateDatapoint], tuple[float, ...]]:
        return op.attrgetter(*lst)

    # Status is a nested object in the response (code+symbol). Since most dps is expected to be good
    # (code 0), status is not returned for these:
    status_code: Callable[[DatapointRaw], int] = op.attrgetter("status.code")  # Gives 0 by default when missing

    @staticmethod
    def status_symbol(dp: DatapointRaw) -> str:
        return dp.status.symbol or "Good"  # Gives empty str when missing, so we set 'Good' manually

    # When datapoints with bad status codes are not ignored, value may be missing:
    @staticmethod
    def nullable_raw_dp(dp: DatapointRaw) -> float | str:
        # We pretend like float is always returned to not break every dps annot. in the entire SDK..
        return dp.value if not dp.nullValue else None  # type: ignore [return-value]

    # --------------- #
    # Above are functions that operate on single elements
    # Below are functions that operate on containers
    # --------------- #
    @staticmethod
    def extract_timestamps(dps: DatapointsAny) -> list[int]:
        return list(map(DpsUnpackFns.ts, dps))

    @staticmethod
    def extract_timestamps_numpy(dps: DatapointsAny) -> npt.NDArray[np.int64]:
        return np.fromiter(map(DpsUnpackFns.ts, dps), dtype=np.int64, count=len(dps))

    @staticmethod
    def extract_raw_dps(dps: DatapointsRaw) -> list[float | str]:  # Actually: exclusively either one
        return list(map(DpsUnpackFns.raw_dp, dps))

    @staticmethod
    def extract_raw_dps_numpy(dps: DatapointsRaw, dtype: type[np.float64] | type[np.object_]) -> npt.NDArray[Any]:
        return np.fromiter(map(DpsUnpackFns.raw_dp, dps), dtype=dtype, count=len(dps))

    @staticmethod
    def extract_nullable_raw_dps(dps: DatapointsRaw) -> list[float | str]:  # actually list of [... | None]
        return list(map(DpsUnpackFns.nullable_raw_dp, dps))

    @staticmethod
    def extract_nullable_raw_dps_numpy(
        dps: DatapointsRaw, dtype: type[np.float64] | type[np.object_]
    ) -> tuple[npt.NDArray[Any], list[int]]:
        # This is a very hot loop, thus we make some ugly optimizations:
        values = [None] * len(dps)
        missing: list[int] = []
        add_missing = missing.append
        for i, dp in enumerate(map(DpsUnpackFns.nullable_raw_dp, dps)):
            # we use list because of its significantly lower overhead than numpy on single element access:
            values[i] = dp  # type: ignore [call-overload]
            if dp is None:
                add_missing(i)
        arr = np.array(values, dtype=dtype)
        return arr, missing

    @staticmethod
    def extract_status_code(dps: DatapointsRaw) -> list[int]:
        return list(map(DpsUnpackFns.status_code, dps))

    @staticmethod
    def extract_status_code_numpy(dps: DatapointsRaw) -> npt.NDArray[np.uint32]:
        return np.fromiter(map(DpsUnpackFns.status_code, dps), dtype=np.uint32, count=len(dps))

    @staticmethod
    def extract_status_symbol(dps: DatapointsRaw) -> list[str]:
        return list(map(DpsUnpackFns.status_symbol, dps))

    @staticmethod
    def extract_status_symbol_numpy(dps: DatapointsRaw) -> npt.NDArray[np.object_]:
        return np.fromiter(map(DpsUnpackFns.status_symbol, dps), dtype=np.object_, count=len(dps))

    @staticmethod
    def extract_aggregates(
        dps: AggregateDatapoints,
        aggregates: list[str],
        unpack_fn: Callable[[AggregateDatapoint], tuple[float, ...]],
    ) -> list:
        try:
            # Fast method uses multi-key unpacking:
            return list(map(unpack_fn, dps))
        except AttributeError:
            # An aggregate is missing, fallback to slower `getattr`:
            if len(aggregates) == 1:
                return [getattr(dp, aggregates[0], None) for dp in dps]
            else:
                return [tuple(getattr(dp, agg, None) for agg in aggregates) for dp in dps]

    @staticmethod
    def extract_aggregates_numpy(
        dps: AggregateDatapoints,
        aggregates: list[str],
        unpack_fn: Callable[[AggregateDatapoint], tuple[float, ...]],
        dtype: np.dtype[Any],
    ) -> npt.NDArray[np.float64]:
        try:
            # Fast method uses multi-key unpacking:
            return np.fromiter(map(unpack_fn, dps), dtype=dtype, count=len(dps))
        except AttributeError:
            # An aggregate is missing, fallback to slower `getattr`:
            return np.array([tuple(getattr(dp, agg, math.nan) for agg in aggregates) for dp in dps], dtype=np.float64)


def ensure_int(val: float, change_nan_to: int = 0) -> int:
    if math.isnan(val):
        return change_nan_to
    return int(val)


def decide_numpy_dtype_from_is_string(is_string: bool) -> type:
    return np.object_ if is_string else np.float64


def get_datapoints_from_proto(res: DataPointListItem) -> DatapointsAny:
    if (dp_type := res.WhichOneof("datapointType")) is not None:
        return getattr(res, dp_type).datapoints
    return cast(DatapointsAny, [])


def get_ts_info_from_proto(res: DataPointListItem) -> dict[str, int | str | bool | NodeId | None]:
    # Note: When 'unit_external_id' is returned, regular 'unit' is ditched
    if res.instanceId and res.instanceId.space:  # res.instanceId evaluates to True even when empty :eyes:
        instance_id = NodeId(res.instanceId.space, res.instanceId.externalId)
    else:
        instance_id = None
    return {
        "id": res.id,
        "external_id": res.externalId,
        "is_string": res.isString,
        "is_step": res.isStep,
        "unit": res.unit,
        "unit_external_id": res.unitExternalId,
        "instance_id": instance_id,
    }


_DataContainer: TypeAlias = defaultdict[tuple[float, ...], list]


def datapoints_in_order(container: _DataContainer) -> Iterator[list]:
    return chain.from_iterable(container[k] for k in sorted(container))


def create_array_from_dps_container(container: _DataContainer) -> npt.NDArray:
    return np.hstack(list(datapoints_in_order(container)))


def create_aggregates_arrays_from_dps_container(container: _DataContainer, n_aggs: int) -> list[npt.NDArray]:
    all_aggs_arr = np.vstack(list(datapoints_in_order(container)))
    return list(map(np.ravel, np.hsplit(all_aggs_arr, n_aggs)))


def create_list_from_dps_container(container: _DataContainer) -> list:
    return list(chain.from_iterable(datapoints_in_order(container)))


def create_aggregates_list_from_dps_container(container: _DataContainer) -> Iterator[list[list]]:
    concatenated = chain.from_iterable(datapoints_in_order(container))
    return map(list, zip(*concatenated))  # rows to columns


class BaseDpsFetchSubtask:
    def __init__(self, start: int, end: int, parent: BaseTaskOrchestrator) -> None:
        self.start = start
        self.end = end
        self.parent = parent
        self.max_query_limit = (query := parent.query).max_query_limit
        self.is_done = False
        self.n_dps_fetched = 0

        self.static_kwargs = query.identifier.as_dict()
        if not query.is_raw_query:
            self.static_kwargs.update(aggregates=query.aggs_camel_case, granularity=query.granularity)

        if query.target_unit is not None:
            self.static_kwargs["targetUnit"] = query.target_unit
        elif query.target_unit_system is not None:
            self.static_kwargs["targetUnitSystem"] = query.target_unit_system

        if query.ignore_bad_datapoints is False:
            self.static_kwargs["ignoreBadDataPoints"] = query.ignore_bad_datapoints
        if query.treat_uncertain_as_bad is False:
            self.static_kwargs["treatUncertainAsBad"] = query.treat_uncertain_as_bad

        if query.include_status is True:
            self.static_kwargs["includeStatus"] = query.include_status

        if query.timezone:
            self.static_kwargs["timeZone"] = query.timezone

    @abstractmethod
    def get_next_payload_item(self) -> _DatapointsPayloadItem: ...

    @abstractmethod
    def store_partial_result(self, res: DataPointListItem) -> list[SplittingFetchSubtask] | None: ...


class OutsideDpsFetchSubtask(BaseDpsFetchSubtask):
    """Fetches outside points and stores in parent"""

    def get_next_payload_item(self) -> _DatapointsPayloadItem:
        return _DatapointsPayloadItem(
            start=self.start,
            end=self.end,
            limit=0,  # Not a bug; it just returns the outside points
            includeOutsidePoints=True,
            **self.static_kwargs,  # type: ignore [typeddict-item]
        )

    def store_partial_result(self, res: DataPointListItem) -> None:
        # `Oneof` field `datapointType` can be either `numericDatapoints` or `stringDatapoints`
        # (or `aggregateDatapoints`, but not here of course):
        if dps := get_datapoints_from_proto(res):
            assert isinstance(self.parent, BaseRawTaskOrchestrator)
            self.parent._extract_outside_points(cast(DatapointsRaw, dps))
        self.is_done = True


class SerialFetchSubtask(BaseDpsFetchSubtask):
    """Fetches datapoints serially until complete, nice and simple. Stores data in parent"""

    def __init__(self, *, subtask_idx: tuple[float, ...], first_cursor: str | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.subtask_idx = subtask_idx
        self.next_start = self.start
        self.next_cursor = first_cursor
        self.uses_cursor = self.parent.query.use_cursors

    def get_next_payload_item(self) -> _DatapointsPayloadItem:
        remaining = self.parent.get_remaining_limit()
        return _DatapointsPayloadItem(
            start=self.next_start,
            end=self.end,
            limit=min(self.max_query_limit, remaining),
            cursor=self.next_cursor,
            **self.static_kwargs,  # type: ignore [typeddict-item]
        )

    def store_partial_result(self, res: DataPointListItem) -> list[SplittingFetchSubtask] | None:
        if not self.parent.ts_info:
            # In eager mode, first task to complete gets the honor to store ts info:
            self.parent._store_ts_info(res)

        if not (dps := get_datapoints_from_proto(res)):
            self.is_done = True
            return None

        n, last_ts = len(dps), dps[-1].timestamp
        self.parent._unpack_and_store(self.subtask_idx, dps)
        self._update_state_for_next_payload(res, last_ts, n)
        if self._is_task_done(n):
            self.is_done = True
        return None

    def _update_state_for_next_payload(self, res: DataPointListItem, last_ts: int, n: int) -> None:
        self.next_cursor = res.nextCursor
        if not self.uses_cursor:
            self.next_start = last_ts + self.parent.offset_next  # Move `start` to prepare for next query
        self.n_dps_fetched += n  # Used to quit limited queries asap

    def _is_task_done(self, n: int) -> bool:
        return (
            not self.next_cursor
            or n < self.max_query_limit
            or not self.uses_cursor and self.next_start == self.end
        )  # fmt: skip


class SplittingFetchSubtask(SerialFetchSubtask):
    """Fetches data serially, but splits its time domain ("divide and conquer") based on the density
    of returned datapoints. Stores data in parent.
    """

    def __init__(self, *, max_splitting_factor: int = 10, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.max_splitting_factor = max_splitting_factor
        self.split_subidx: int = 0  # Actual value doesn't matter (any int will do)

    def store_partial_result(self, res: DataPointListItem) -> list[SplittingFetchSubtask] | None:
        self.prev_start = self.next_start
        super().store_partial_result(res)
        if not self.is_done:
            last_ts = get_datapoints_from_proto(res)[-1].timestamp
            return self._split_self_into_new_subtasks_if_needed(last_ts)
        return None

    def _create_subtasks_idxs(self, n_new_tasks: int) -> Iterator[tuple[float, ...]]:
        # Since this task may decide to split itself multiple times, we count backwards to keep order
        # (we rely on tuple sorting logic). Example using `self.subtask_idx=(4,)`:
        # - First split into e.g. 3 parts: (4,-3), (4,-2), (4,-1)
        # - Next, split into 2: (4, -5) and (4, -4). These now sort before the first split.
        end = self.split_subidx
        self.split_subidx -= n_new_tasks
        yield from ((*self.subtask_idx, i) for i in range(self.split_subidx, end))

    def _split_self_into_new_subtasks_if_needed(self, last_ts: int) -> list[SplittingFetchSubtask] | None:
        # How many new tasks because of % of time range was fetched?
        tot_ms = self.end - (start := self.prev_start)
        ratio_retrieved = (last_ts - start) / tot_ms
        n_new_pct = math.floor(1 / ratio_retrieved)
        # We pick strictest criterion:
        n_new_tasks = min(n_new_pct, self.max_splitting_factor + 1)  # +1 for "self next"
        if n_new_tasks <= 1:  # No point in splitting; no faster than this task just continuing
            return None
        # Find a `delta_ms` that's a multiple of granularity in ms (trivial for raw queries):
        boundaries = split_time_range(last_ts, self.end, n_new_tasks, self.parent.offset_next)
        self.end = boundaries[1]  # We shift end of 'self' backwards
        split_idxs = self._create_subtasks_idxs(n_new_tasks)
        new_subtasks = [
            SplittingFetchSubtask(start=start, end=end, subtask_idx=idx, parent=self.parent)
            for start, end, idx in zip(boundaries[1:-1], boundaries[2:], split_idxs)
        ]
        self.parent.subtasks.extend(new_subtasks)
        return new_subtasks


def get_task_orchestrator(query: DatapointsQuery) -> type[BaseTaskOrchestrator]:
    if query.is_raw_query:
        if query.limit is None:
            return ConcurrentUnlimitedRawTaskOrchestrator
        return SerialLimitedRawTaskOrchestrator
    else:
        if query.limit is not None or query.use_cursors:
            return SerialLimitedAggTaskOrchestrator
        return ConcurrentUnlimitedAggTaskOrchestrator


class BaseTaskOrchestrator(ABC):
    def __init__(
        self,
        query: DatapointsQuery,
        eager_mode: bool,
        use_numpy: bool,
        first_dps_batch: DataPointListItem | None = None,
        first_limit: int | None = None,
    ) -> None:
        self.query = query
        self.eager_mode = eager_mode
        self.use_numpy = use_numpy
        self.ts_info: dict[str, Any] = {}
        self.subtask_outside_points: OutsideDpsFetchSubtask | None = None
        self.raw_dtype_numpy: type[np.object_] | type[np.float64] | None = None
        self._is_done = False
        self._final_result: Datapoints | DatapointsArray | None = None

        self.ts_data: _DataContainer = defaultdict(list)
        self.dps_data: _DataContainer = defaultdict(list)
        self.subtasks: list[BaseDpsFetchSubtask] = []

        if query.is_raw_query:
            if query.include_status:
                self.status_code: _DataContainer = defaultdict(list)
                self.status_symbol: _DataContainer = defaultdict(list)
            if use_numpy and not query.ignore_bad_datapoints:
                self.null_timestamps: set[int] = set()

        # When running large queries (i.e. not "eager"), all time series have a first batch fetched before
        # further subtasks are created. This gives us e.g. outside points for free (if asked for) and ts info:
        if eager_mode:
            self.first_cursor = None
        else:
            assert first_dps_batch is not None and first_limit is not None
            self.first_cursor = first_dps_batch.nextCursor
            self._extract_first_dps_batch(first_dps_batch, first_limit)

    @property
    def is_done(self) -> bool:
        if not self.ts_info:
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
    def start_ts_first_batch(self) -> int:
        ts = self.ts_data[FIRST_IDX][0][0]
        return ts.item() if self.use_numpy else ts

    @property
    def end_ts_first_batch(self) -> int:
        ts = self.ts_data[FIRST_IDX][0][-1]
        return ts.item() if self.use_numpy else ts

    @property
    def n_dps_first_batch(self) -> int:
        if self.eager_mode:
            return 0
        return len(self.ts_data[FIRST_IDX][0])

    def _extract_first_dps_batch(self, first_dps_batch: DataPointListItem, first_limit: int) -> None:
        dps = get_datapoints_from_proto(first_dps_batch)
        self._store_ts_info(first_dps_batch)
        if not dps:
            self._is_done = True
            return
        self._store_first_batch(dps, first_limit)

    def _store_ts_info(self, res: DataPointListItem) -> None:
        self.ts_info.update(get_ts_info_from_proto(res))
        self.ts_info["timezone"] = self.query.original_timezone
        self.ts_info["granularity"] = self.query.original_granularity  # show '1quarter', not '3mo'
        if self.use_numpy:
            self.raw_dtype_numpy = decide_numpy_dtype_from_is_string(res.isString)

    def _store_first_batch(self, dps: DatapointsAny, first_limit: int) -> None:
        # Set `start` for the first subtask; since we have a cursor, this is only (really)
        # needed for time domain splitting:
        self.first_start = dps[-1].timestamp
        if not self.query.use_cursors:
            self.first_start += self.offset_next
        self._unpack_and_store(FIRST_IDX, dps)

        # Are we done after first batch?
        if len(dps) < first_limit:
            self._is_done = True
        elif not self.first_cursor and not self.query.include_outside_points:
            self._is_done = True  # no cursor when including outside...
        elif not self.query.use_cursors and self.first_start == self.query.end:
            self._is_done = True
        elif self.query.limit is not None and len(dps) <= self.query.limit <= first_limit:  # TODO: len == limit??
            self._is_done = True

    def _clear_data_containers(self) -> None:
        # Help gc clear out temporary containers:
        del self.query, self.ts_data, self.dps_data
        del self.subtasks, self.subtask_outside_points

    def finalize_datapoints(self) -> None:
        if self._final_result is None:
            self._final_result = self.get_result()
            self._clear_data_containers()

    def get_result(self) -> Datapoints | DatapointsArray:
        if self._final_result is not None:
            return self._final_result
        return self._get_result()

    def _maybe_queue_outside_dps_subtask(self, subtasks: list[BaseDpsFetchSubtask]) -> None:
        if self.eager_mode and self.query.is_raw_query and self.query.include_outside_points:
            # In eager mode we do not get the "first dps batch" to extract outside points from:
            self.subtask_outside_points = OutsideDpsFetchSubtask(
                start=self.query.start_ms,
                end=self.query.end_ms,
                parent=self,
            )
            # Append the outside subtask to returned subtasks so that it will be queued:
            subtasks.append(self.subtask_outside_points)

    @abstractmethod
    def get_remaining_limit(self) -> float:  # What I really want: 'Literal[math.inf]'
        ...

    @abstractmethod
    def _get_result(self) -> Datapoints | DatapointsArray: ...

    @abstractmethod
    def split_into_subtasks(self, max_workers: int, n_tot_queries: int) -> list[BaseDpsFetchSubtask]: ...

    @property
    @abstractmethod
    def offset_next(self) -> int: ...

    @abstractmethod
    def _unpack_and_store(self, idx: tuple[float, ...], dps: DatapointsAny) -> None: ...


class SerialTaskOrchestratorMixin(BaseTaskOrchestrator):
    def get_remaining_limit(self) -> float:
        assert len(self.subtasks) == 1
        if self.query.limit is None:
            return math.inf
        return self.query.limit - self.n_dps_first_batch - self.subtasks[0].n_dps_fetched

    def split_into_subtasks(self, max_workers: int, n_tot_queries: int) -> list[BaseDpsFetchSubtask]:
        # For serial fetching, a single task suffice
        start = self.query.start if self.eager_mode else self.first_start
        subtasks: list[BaseDpsFetchSubtask] = [
            SerialFetchSubtask(
                start=start, end=self.query.end, parent=self, subtask_idx=FIRST_IDX, first_cursor=self.first_cursor
            )
        ]
        self.subtasks.extend(subtasks)
        self._maybe_queue_outside_dps_subtask(subtasks)
        return subtasks


class BaseRawTaskOrchestrator(BaseTaskOrchestrator):
    def __init__(self, **kwargs: Any) -> None:
        self.dp_outside_start: tuple[int, RawDatapointValue] | None = None
        self.dp_outside_end: tuple[int, RawDatapointValue] | None = None
        super().__init__(**kwargs)

        self.dp_outside_status_code_start: int | None = None
        self.dp_outside_status_code_end: int | None = None
        self.dp_outside_status_symbol_start: str | None = None
        self.dp_outside_status_symbol_end: str | None = None

    @property
    def offset_next(self) -> Literal[1]:
        return 1  # millisecond

    def _create_empty_result(self) -> Datapoints | DatapointsArray:
        status_cols: dict[str, Any] = {}
        if not self.use_numpy:
            if self.query.include_status:
                status_cols.update(status_code=[], status_symbol=[])
            return Datapoints(**self.ts_info, timestamp=[], value=[], **status_cols)

        if self.query.include_status:
            status_cols.update(status_code=np.array([], dtype=np.int32), status_symbol=np.array([], dtype=np.object_))
        return DatapointsArray._load_from_arrays(
            {
                **self.ts_info,
                "timestamp": np.array([], dtype=np.int64),
                "value": np.array([], dtype=self.raw_dtype_numpy),
                **status_cols,
            }
        )

    def _was_any_data_fetched(self) -> bool:
        return any((self.ts_data, self.dp_outside_start, self.dp_outside_end))

    def _get_result(self) -> Datapoints | DatapointsArray:
        if not self._was_any_data_fetched():
            return self._create_empty_result()
        if self.query.include_outside_points:
            self._include_outside_points_in_result()

        status_columns: dict[str, Any] = {}
        if self.use_numpy:
            if self.query.include_status:
                status_columns.update(
                    status_code=create_array_from_dps_container(self.status_code),
                    status_symbol=create_array_from_dps_container(self.status_symbol),
                )
            if not self.query.ignore_bad_datapoints:
                status_columns["null_timestamps"] = self.null_timestamps
            return DatapointsArray._load_from_arrays(
                {
                    **self.ts_info,
                    "timestamp": create_array_from_dps_container(self.ts_data),
                    "value": create_array_from_dps_container(self.dps_data),
                    **status_columns,
                }
            )
        if self.query.include_status:
            status_columns.update(
                status_code=create_list_from_dps_container(self.status_code),
                status_symbol=create_list_from_dps_container(self.status_symbol),
            )
        return Datapoints(
            **self.ts_info,
            timestamp=create_list_from_dps_container(self.ts_data),
            value=create_list_from_dps_container(self.dps_data),
            **status_columns,
        )

    def _include_outside_points_in_result(self) -> None:
        for dp, status_code, status_symbol, idx in zip(
            (self.dp_outside_start, self.dp_outside_end),
            ([self.dp_outside_status_code_start], [self.dp_outside_status_code_end]),
            ([self.dp_outside_status_symbol_start], [self.dp_outside_status_symbol_end]),
            (-math.inf, math.inf),
        ):
            if not dp:
                continue
            ts: list[int] | NumpyInt64Array = [dp[0]]
            value: list[float | str] | NumpyFloat64Array | NumpyObjArray = [dp[1]]
            if self.use_numpy:
                ts = np.array(ts, dtype=np.int64)
                value = np.array(value, dtype=self.raw_dtype_numpy)
                if dp[1] is None:  # Only None if self.query.ignore_bad_datapoints=False
                    self.null_timestamps.add(dp[0])
            self.ts_data[(idx,)].append(ts)
            self.dps_data[(idx,)].append(value)

            if self.query.include_status:
                if self.use_numpy:
                    status_code = np.array(status_code, dtype=np.uint32)  # type: ignore [assignment]
                    status_symbol = np.array(status_symbol, dtype=np.object_)  # type: ignore [assignment]
                self.status_code[(idx,)].append(status_code)
                self.status_symbol[(idx,)].append(status_symbol)

    def _unpack_and_store(self, idx: tuple[float, ...], dps: DatapointsRaw) -> None:  # type: ignore [override]
        if self.use_numpy:
            self.ts_data[idx].append(DpsUnpackFns.extract_timestamps_numpy(dps))
            assert self.raw_dtype_numpy is not None
            if self.query.ignore_bad_datapoints:
                self.dps_data[idx].append(DpsUnpackFns.extract_raw_dps_numpy(dps, self.raw_dtype_numpy))
            else:
                # After this step, missing values (represented with None) will become NaNs and thus become
                # indistinguishable from any NaNs that was returned! We need to store these timestamps in a property
                # to allow our users to inspect them - but maybe even more important, allow the SDK to accurately
                # use the DatapointsArray to replicate datapoints (exactly).
                arr, missing_idxs = DpsUnpackFns.extract_nullable_raw_dps_numpy(dps, self.raw_dtype_numpy)
                self.dps_data[idx].append(arr)
                if missing_idxs:
                    self.null_timestamps.update(self.ts_data[idx][-1][missing_idxs].tolist())
            if self.query.include_status:
                self.status_code[idx].append(DpsUnpackFns.extract_status_code_numpy(dps))
                self.status_symbol[idx].append(DpsUnpackFns.extract_status_symbol_numpy(dps))

        else:
            self.ts_data[idx].append(DpsUnpackFns.extract_timestamps(dps))
            if self.query.ignore_bad_datapoints:
                self.dps_data[idx].append(DpsUnpackFns.extract_raw_dps(dps))
            else:
                self.dps_data[idx].append(DpsUnpackFns.extract_nullable_raw_dps(dps))
            if self.query.include_status:
                self.status_code[idx].append(DpsUnpackFns.extract_status_code(dps))
                self.status_symbol[idx].append(DpsUnpackFns.extract_status_symbol(dps))

    def _store_first_batch(self, dps: DatapointsAny, first_limit: int) -> None:
        if self.query.include_outside_points:
            self._extract_outside_points(cast(DatapointsRaw, dps))
            if not dps:  # We might have only gotten outside points
                self._is_done = True
                return
        super()._store_first_batch(dps, first_limit)

    def _extract_outside_points(self, dps: DatapointsRaw) -> None:
        first, last = None, None
        if dps[0].timestamp < self.query.start_ms:
            # We got a dp before `start`, this (and 'after') should not impact our count towards `limit`,
            # so we pop to remove it from dps:
            first = dps.pop(0)
            if not self.query.ignore_bad_datapoints:
                self.dp_outside_start = DpsUnpackFns.ts(first), DpsUnpackFns.nullable_raw_dp(first)
            else:
                self.dp_outside_start = DpsUnpackFns.ts(first), DpsUnpackFns.raw_dp(first)

        if dps and dps[-1].timestamp >= self.query.end_ms:  # >= because `end` is exclusive
            last = dps.pop(-1)
            if not self.query.ignore_bad_datapoints:
                self.dp_outside_end = DpsUnpackFns.ts(last), DpsUnpackFns.nullable_raw_dp(last)
            else:
                self.dp_outside_end = DpsUnpackFns.ts(last), DpsUnpackFns.raw_dp(last)

        if self.query.include_status:
            if first is not None:
                self.dp_outside_status_code_start = DpsUnpackFns.status_code(first)
                self.dp_outside_status_symbol_start = DpsUnpackFns.status_symbol(first)
            if last is not None:
                self.dp_outside_status_code_end = DpsUnpackFns.status_code(last)
                self.dp_outside_status_symbol_end = DpsUnpackFns.status_symbol(last)


class SerialLimitedRawTaskOrchestrator(BaseRawTaskOrchestrator, SerialTaskOrchestratorMixin): ...


class ConcurrentTaskOrchestratorMixin(BaseTaskOrchestrator):
    @abstractmethod
    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, n_workers_per_queries: int) -> int: ...

    def get_remaining_limit(self) -> float:
        return math.inf

    def split_into_subtasks(self, max_workers: int, n_tot_queries: int) -> list[BaseDpsFetchSubtask]:
        # Given e.g. a single time series, we want to put all our workers to work by splitting into lots of pieces!
        # As the number grows - or we start combining multiple into the same query - we want to split less:
        # we hold back to not create too many subtasks:
        n_workers_per_queries = max(1, round(max_workers / n_tot_queries))
        subtasks: list[BaseDpsFetchSubtask] = self._create_uniformly_split_subtasks(n_workers_per_queries)
        self.subtasks.extend(subtasks)
        self._maybe_queue_outside_dps_subtask(subtasks)
        return subtasks

    def _create_uniformly_split_subtasks(self, n_workers_per_queries: int) -> list[BaseDpsFetchSubtask]:
        start = self.query.start_ms if self.eager_mode else self.first_start
        tot_ms = (end := self.query.end_ms) - start
        n_periods = self._find_number_of_subtasks_uniform_split(tot_ms, n_workers_per_queries)
        boundaries = split_time_range(start, end, n_periods, self.offset_next)
        return [
            SplittingFetchSubtask(start=start, end=end, subtask_idx=(i,), parent=self)
            for i, (start, end) in enumerate(pairwise(boundaries), 1)
        ]


class ConcurrentUnlimitedRawTaskOrchestrator(BaseRawTaskOrchestrator, ConcurrentTaskOrchestratorMixin):
    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, n_workers_per_queries: int) -> int:
        # It makes no sense to split beyond what the max-size of a query allows (for a maximally dense
        # time series), but that is rarely useful as 100k dps is just 1 min 40 sec... we guess an
        # average density of points at 1 dp/sec, giving us split-windows no smaller than ~1 day:
        return min(n_workers_per_queries, math.ceil((tot_ms / 1000) / self.query.max_query_limit))


class BaseAggTaskOrchestrator(BaseTaskOrchestrator):
    def __init__(self, *, query: DatapointsQuery, use_numpy: bool, **kwargs: Any) -> None:
        self._set_aggregate_vars(query.aggs_camel_case, use_numpy)
        super().__init__(query=query, use_numpy=use_numpy, **kwargs)

    @cached_property
    def offset_next(self) -> int:
        return granularity_to_ms(cast(str, self.query.granularity))

    def _set_aggregate_vars(self, aggs_camel_case: list[str], use_numpy: bool) -> None:
        # Developer note here: If you ask for datapoints to be returned in JSON, you get `count` as an integer.
        # Nice. However, when using protobuf, you get `double` xD ...so when this code was pivoted from
        # JSON -> protobuf, the special handling of `count` was kept in the hopes that one day protobuf
        # would yield the correct type...!
        self.all_aggregates = aggs_camel_case
        self.int_aggs = _INT_AGGREGATES.intersection(aggs_camel_case)
        self.float_aggs = set(aggs_camel_case).difference(self.int_aggs)

        self.agg_unpack_fn = DpsUnpackFns.custom_from_aggregates(self.all_aggregates)
        self.first_agg, *others = self.all_aggregates
        self.single_agg = not others

        if use_numpy:
            if self.single_agg:
                self.dtype_aggs: np.dtype[Any] = np.dtype(np.float64)
            else:  # (.., 1) is deprecated for some reason
                # We are storing all (2 -> 16) aggregates in one block of memory:
                self.dtype_aggs = np.dtype((np.float64, len(self.all_aggregates)))

    def _create_empty_result(self) -> Datapoints | DatapointsArray:
        if self.use_numpy:
            arr_dct = {"timestamp": np.array([], dtype=np.int64)}
            if self.float_aggs:
                arr_dct.update({agg: np.array([], dtype=np.float64) for agg in self.float_aggs})
            if self.int_aggs:
                arr_dct.update({agg: np.array([], dtype=np.int64) for agg in self.int_aggs})
            return DatapointsArray._load_from_arrays({**self.ts_info, **arr_dct})

        lst_dct: dict[str, list] = {agg: [] for agg in self.all_aggregates}
        return Datapoints(timestamp=[], **self.ts_info, **convert_all_keys_to_snake_case(lst_dct))

    def _get_result(self) -> Datapoints | DatapointsArray:
        if not self.ts_data or self.query.limit == 0:
            return self._create_empty_result()

        if self.use_numpy:
            arr_dct = {"timestamp": create_array_from_dps_container(self.ts_data)}
            arr_lst = create_aggregates_arrays_from_dps_container(self.dps_data, len(self.all_aggregates))
            arr_dct.update(dict(zip(self.all_aggregates, arr_lst)))
            for agg in self.int_aggs:
                # Need to do an extra NaN-aware int-conversion because protobuf (as opposed to json) returns double...
                # If an interval with no datapoints (i.e. count does not exist) has data from another aggregate (probably
                # (step_)interpolation), count returns nan... which we need float to represent... which we do not want.
                # Thus we convert any NaNs to 0 (which for count - and duration - makes perfect sense):
                arr_dct[agg] = np.nan_to_num(arr_dct[agg], copy=False, nan=0.0, posinf=np.inf, neginf=-np.inf).astype(
                    np.int64
                )
            return DatapointsArray._load_from_arrays({**self.ts_info, **arr_dct})

        lst_dct = {"timestamp": create_list_from_dps_container(self.ts_data)}
        if self.single_agg:
            lst_dct[self.first_agg] = create_list_from_dps_container(self.dps_data)
        else:
            aggs_iter = create_aggregates_list_from_dps_container(self.dps_data)
            lst_dct.update(dict(zip(self.all_aggregates, aggs_iter)))
        for agg in self.int_aggs:
            # Need to do an extra NaN-aware int-conversion because protobuf (as opposed to json) returns double:
            lst_dct[agg] = list(map(ensure_int, lst_dct[agg]))
        return Datapoints(**self.ts_info, **convert_all_keys_to_snake_case(lst_dct))

    def _unpack_and_store(self, idx: tuple[float, ...], dps: AggregateDatapoints) -> None:  # type: ignore [override]
        if self.use_numpy:
            self._unpack_and_store_numpy(idx, dps)
        else:
            self._unpack_and_store_basic(idx, dps)

    def _unpack_and_store_numpy(self, idx: tuple[float, ...], dps: AggregateDatapoints) -> None:
        arr = DpsUnpackFns.extract_aggregates_numpy(dps, self.all_aggregates, self.agg_unpack_fn, self.dtype_aggs)
        self.ts_data[idx].append(DpsUnpackFns.extract_timestamps_numpy(dps))
        self.dps_data[idx].append(arr.reshape(len(dps), len(self.all_aggregates)))

    def _unpack_and_store_basic(self, idx: tuple[float, ...], dps: AggregateDatapoints) -> None:
        lst: list[Any] = DpsUnpackFns.extract_aggregates(dps, self.all_aggregates, self.agg_unpack_fn)
        self.ts_data[idx].append(DpsUnpackFns.extract_timestamps(dps))
        self.dps_data[idx].append(lst)


class SerialLimitedAggTaskOrchestrator(BaseAggTaskOrchestrator, SerialTaskOrchestratorMixin): ...


class ConcurrentUnlimitedAggTaskOrchestrator(BaseAggTaskOrchestrator, ConcurrentTaskOrchestratorMixin):
    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, n_workers_per_queries: int) -> int:
        n_max_dps = tot_ms // self.offset_next  # evenly divides
        return min(n_workers_per_queries, math.ceil(n_max_dps / self.query.max_query_limit))
