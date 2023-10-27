from __future__ import annotations

import math
import numbers
import operator as op
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    Generic,
    Hashable,
    Iterator,
    Literal,
    MutableSequence,
    NoReturn,
    Optional,
    Sequence,
    TypedDict,
    TypeVar,
    Union,
    cast,
)

from google.protobuf.message import Message
from sortedcontainers import SortedDict, SortedList
from typing_extensions import NotRequired

from cognite.client.data_classes.datapoints import NUMPY_IS_AVAILABLE, Aggregate, Datapoints, DatapointsArray
from cognite.client.utils._auxiliary import exactly_one_is_not_none, is_unlimited
from cognite.client.utils._identifier import Identifier
from cognite.client.utils._importing import import_legacy_protobuf
from cognite.client.utils._text import convert_all_keys_to_snake_case, to_camel_case, to_snake_case
from cognite.client.utils._time import (
    align_start_and_end_for_granularity,
    granularity_to_ms,
    split_time_range,
    time_ago_to_ms,
    timestamp_to_ms,
)

if not import_legacy_protobuf():
    from cognite.client._proto.data_point_list_response_pb2 import DataPointListItem
    from cognite.client._proto.data_points_pb2 import AggregateDatapoint, NumericDatapoint, StringDatapoint
else:
    from cognite.client._proto_legacy.data_point_list_response_pb2 import DataPointListItem  # type: ignore [assignment]
    from cognite.client._proto_legacy.data_points_pb2 import (  # type: ignore [assignment]
        AggregateDatapoint,
        NumericDatapoint,
        StringDatapoint,
    )

if NUMPY_IS_AVAILABLE:
    import numpy as np

if TYPE_CHECKING:
    import numpy.typing as npt

    from cognite.client.data_classes.datapoints import NumpyFloat64Array, NumpyInt64Array, NumpyObjArray


_T = TypeVar("_T")
FIRST_IDX = (0,)

DatapointsAgg = MutableSequence[AggregateDatapoint]
DatapointsNum = MutableSequence[NumericDatapoint]
DatapointsStr = MutableSequence[StringDatapoint]

DatapointsAny = Union[DatapointsAgg, DatapointsNum, DatapointsStr]
DatapointsRaw = Union[DatapointsNum, DatapointsStr]

RawDatapointValue = Union[float, str]
DatapointsId = Union[None, int, Dict[str, Any], Sequence[Union[int, Dict[str, Any]]]]
DatapointsExternalId = Union[None, str, Dict[str, Any], Sequence[Union[str, Dict[str, Any]]]]


class CustomDatapointsQuery(TypedDict, total=False):
    # No field required
    start: int | str | datetime | None
    end: int | str | datetime | None
    aggregates: list[str] | None
    granularity: str | None
    target_unit: str | None
    target_unit_system: str | None
    limit: int | None
    include_outside_points: bool | None
    ignore_unknown_ids: bool | None


class DatapointsQueryId(CustomDatapointsQuery):
    id: int  # required field


class DatapointsQueryExternalId(CustomDatapointsQuery):
    external_id: str  # required field


class DatapointsPayloadItem(TypedDict, total=False):
    # No field required
    start: int
    end: int
    aggregates: list[str] | None
    granularity: str | None
    targetUnit: str | None
    targetUnitSystem: str | None
    limit: int
    includeOutsidePoints: bool


class DatapointsPayload(DatapointsPayloadItem):
    items: list[DatapointsPayloadItem]
    ignoreUnknownIds: NotRequired[bool]


@dataclass
class _DatapointsQuery:
    """Internal representation of a user request for datapoints, previously public (before v5)"""

    start: int | str | datetime | None = None
    end: int | str | datetime | None = None
    id: DatapointsId | None = None
    external_id: DatapointsExternalId | None = None
    aggregates: Aggregate | str | list[Aggregate | str] | None = None
    granularity: str | None = None
    target_unit: str | None = None
    target_unit_system: str | None = None
    limit: int | None = None
    include_outside_points: bool = False
    ignore_unknown_ids: bool = False

    @property
    def is_single_identifier(self) -> bool:
        # No lists given and exactly one of id/xid was given:
        return (
            isinstance(self.id, (dict, numbers.Integral))
            and self.external_id is None
            or isinstance(self.external_id, (dict, str))
            and self.id is None
        )


class _SingleTSQueryValidator:
    OPTIONAL_DICT_KEYS: ClassVar[frozenset[str]] = frozenset(
        {
            "start",
            "end",
            "aggregates",
            "granularity",
            "target_unit",
            "target_unit_system",
            "limit",
            "include_outside_points",
            "ignore_unknown_ids",
        }
    )

    def __init__(self, user_query: _DatapointsQuery, *, dps_limit_raw: int, dps_limit_agg: int) -> None:
        self.user_query = user_query
        self.dps_limit_raw = dps_limit_raw
        self.dps_limit_agg = dps_limit_agg
        self.defaults: dict[str, Any] = dict(
            start=user_query.start,
            end=user_query.end,
            limit=user_query.limit,
            aggregates=user_query.aggregates,
            granularity=user_query.granularity,
            target_unit=user_query.target_unit,
            target_unit_system=user_query.target_unit_system,
            include_outside_points=user_query.include_outside_points,
            ignore_unknown_ids=user_query.ignore_unknown_ids,
        )
        self._user_query_is_valid = False
        self._user_query_requires_beta_api_subversion = False

        # We want all start/end = "now" (and those using the same relative time specifiers, like "4d-ago")
        # queries to get the same time domain to fetch. This also -guarantees- that we correctly raise
        # exception 'end not after start' if both are set to the same value.
        self.__time_now = timestamp_to_ms("now")

    def beta_api_subversion_is_needed(self) -> bool:
        if self._user_query_is_valid:
            return self._user_query_requires_beta_api_subversion
        raise RuntimeError("user query is invalid or validation has not run yet")

    def validate_and_create_single_queries(self) -> list[_SingleTSQueryBase]:
        queries = []
        if self.user_query.id is not None:
            id_queries = self._validate_multiple_id(self.user_query.id)
            queries.extend(id_queries)
        if self.user_query.external_id is not None:
            xid_queries = self._validate_multiple_xid(self.user_query.external_id)
            queries.extend(xid_queries)
        if queries:
            self._user_query_is_valid = True
            return queries
        raise ValueError("Pass at least one time series `id` or `external_id`!")

    def _ts_to_ms_frozen_now(self, ts: int | str | datetime | None, default: int) -> int:
        # Time 'now' is frozen for all queries in a single call from the user, leading to identical
        # results e.g. "4d-ago" and "now"
        if ts is None:
            return default
        elif isinstance(ts, str):
            return self.__time_now - time_ago_to_ms(ts)
        else:
            return timestamp_to_ms(ts)

    def _validate_multiple_id(self, id: DatapointsId) -> list[_SingleTSQueryBase]:
        return self._validate_id_or_xid(id, "id", numbers.Integral)

    def _validate_multiple_xid(self, external_id: DatapointsExternalId) -> list[_SingleTSQueryBase]:
        return self._validate_id_or_xid(external_id, "external_id", str)

    def _validate_id_or_xid(
        self, id_or_xid: DatapointsId | DatapointsExternalId, arg_name: str, exp_type: type
    ) -> list[_SingleTSQueryBase]:
        id_or_xid_seq: Sequence[int | str | dict[str, Any]]
        if isinstance(id_or_xid, (dict, exp_type)):
            # Lazy - we postpone evaluation:
            id_or_xid_seq = [cast(Union[int, str, Dict[str, Any]], id_or_xid)]
        elif isinstance(id_or_xid, Sequence):
            # We use Sequence because we require an ordering of elements
            id_or_xid_seq = id_or_xid
        else:
            self._raise_on_wrong_ts_identifier_type(id_or_xid, arg_name, exp_type)

        queries = []
        for ts in id_or_xid_seq:
            if isinstance(ts, exp_type):
                ts_dct = {**self.defaults, arg_name: ts}
                queries.append(
                    self._validate_and_create_query(cast(Union[DatapointsQueryId, DatapointsQueryExternalId], ts_dct))
                )

            elif isinstance(ts, dict):
                ts_validated = self._validate_user_supplied_dict_keys(ts, arg_name)
                if not isinstance(identifier := ts_validated[arg_name], exp_type):
                    self._raise_on_wrong_ts_identifier_type(identifier, arg_name, exp_type)
                # We merge 'defaults' and given ts-dict, ts-dict takes precedence:
                ts_dct = {**self.defaults, **ts_validated}
                queries.append(
                    self._validate_and_create_query(cast(Union[DatapointsQueryId, DatapointsQueryExternalId], ts_dct))
                )

            else:  # pragma: no cover
                self._raise_on_wrong_ts_identifier_type(ts, arg_name, exp_type)
        return queries

    @staticmethod
    def _raise_on_wrong_ts_identifier_type(
        id_or_xid: object,  # This fn is only called when gotten the wrong type
        arg_name: str,
        exp_type: type,
    ) -> NoReturn:
        raise TypeError(
            f"Got unsupported type {type(id_or_xid)}, as, or part of argument `{arg_name}`. Expected one of "
            f"{exp_type}, {dict} or a (mixed) list of these, but got `{id_or_xid}`."
        )

    @classmethod
    def _validate_user_supplied_dict_keys(cls, dct: dict[str, Any], arg_name: str) -> dict[str, Any]:
        if arg_name not in dct:
            if (arg_name_cc := to_camel_case(arg_name)) not in dct:
                raise KeyError(f"Missing required key `{arg_name}` in dict: {dct}.")
            # For backwards compatibility we accept identifiers in camel case: (Make copy to avoid side effects
            # for user's input). Also means we need to return it.
            dct[arg_name] = (dct := dct.copy()).pop(arg_name_cc)

        if bad_keys := set(dct) - cls.OPTIONAL_DICT_KEYS - {arg_name}:
            raise KeyError(
                f"Dict provided by argument `{arg_name}` included key(s) not understood: {sorted(bad_keys)}. "
                f"Required key: `{arg_name}`. Optional: {list(cls.OPTIONAL_DICT_KEYS)}."
            )
        return dct

    def _validate_and_create_query(self, dct: DatapointsQueryId | DatapointsQueryExternalId) -> _SingleTSQueryBase:
        limit = self._verify_limit(dct["limit"])

        target_unit, target_unit_system = dct["target_unit"], dct["target_unit_system"]
        if exactly_one_is_not_none(target_unit, target_unit_system):
            self._user_query_requires_beta_api_subversion = True
        elif target_unit is not None and target_unit_system is not None:
            raise ValueError("You must use either 'target_unit' or 'target_unit_system', not both.")

        granularity, aggregates = dct["granularity"], dct["aggregates"]
        if not (granularity is None or isinstance(granularity, str)):
            raise TypeError(f"Expected `granularity` to be of type `str` or None, not {type(granularity)}")

        elif not (aggregates is None or isinstance(aggregates, (str, list))):
            raise TypeError(f"Expected `aggregates` to be of type `str`, `list[str]` or None, not {type(aggregates)}")

        elif aggregates is None:
            if granularity is None:
                # Request is for raw datapoints:
                raw_query = self._convert_parameters(dct, limit, is_raw=True)
                if limit is None:
                    return _SingleTSQueryRawUnlimited(**raw_query, max_query_limit=self.dps_limit_raw)
                return _SingleTSQueryRawLimited(**raw_query, max_query_limit=self.dps_limit_raw)
            raise ValueError("When passing `granularity`, argument `aggregates` is also required.")

        elif isinstance(aggregates, list) and len(aggregates) == 0:
            raise ValueError("Empty list of `aggregates` passed, expected at least one!")

        elif isinstance(aggregates, list) and len(aggregates) != len(set(map(to_snake_case, aggregates))):
            raise ValueError("List of `aggregates` may not contain duplicates")

        elif granularity is None:
            raise ValueError("When passing `aggregates`, argument `granularity` is also required.")

        elif dct["include_outside_points"] is True:
            raise ValueError("'Include outside points' is not supported for aggregates.")
        # Request is for one or more aggregates:
        agg_query = self._convert_parameters(dct, limit, is_raw=False)
        if limit is None:
            return _SingleTSQueryAggUnlimited(**agg_query, max_query_limit=self.dps_limit_agg)
        return _SingleTSQueryAggLimited(**agg_query, max_query_limit=self.dps_limit_agg)

    def _convert_parameters(
        self,
        dct: DatapointsQueryId | DatapointsQueryExternalId,
        limit: int | None,
        is_raw: bool,
    ) -> dict[str, Any]:
        identifier = Identifier.of_either(
            cast(Optional[int], dct.get("id")), cast(Optional[str], dct.get("external_id"))
        )
        start, end = self._verify_time_range(dct["start"], dct["end"], dct["granularity"], is_raw, identifier)
        converted = {
            "identifier": identifier,
            "start": start,
            "end": end,
            "target_unit": dct["target_unit"],
            "target_unit_system": dct["target_unit_system"],
            "limit": limit,
            "ignore_unknown_ids": dct["ignore_unknown_ids"],
        }
        if is_raw:
            converted["include_outside_points"] = dct["include_outside_points"]
        else:
            if isinstance(aggs := dct["aggregates"], str):
                aggs = [aggs]
            converted["aggregates"] = aggs
            converted["granularity"] = dct["granularity"]
        return converted

    def _verify_limit(self, limit: int | None) -> int | None:
        if is_unlimited(limit):
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
        start: int | str | datetime | None,
        end: int | str | datetime | None,
        granularity: str | None,
        is_raw: bool,
        identifier: Identifier,
    ) -> tuple[int, int]:
        start = self._ts_to_ms_frozen_now(start, default=0)  # 1970-01-01
        end = self._ts_to_ms_frozen_now(end, default=self.__time_now)

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
        limit: int | None,
        target_unit: str | None,
        target_unit_system: str | None,
        include_outside_points: bool,
        ignore_unknown_ids: bool,
    ) -> None:
        self.identifier = identifier
        self.start = start
        self.end = end
        self.max_query_limit = max_query_limit
        self.limit = limit
        self.target_unit = target_unit
        self.target_unit_system = target_unit_system
        self.include_outside_points = include_outside_points
        self.ignore_unknown_ids = ignore_unknown_ids

        self.granularity: str | None = None
        self._is_missing: bool | None = None

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
    def task_orchestrator(self) -> type[BaseTaskOrchestrator]:
        ...

    @abstractmethod
    def to_payload_item(self) -> DatapointsPayloadItem:
        raise NotImplementedError

    @property
    def is_missing(self) -> bool:
        if self._is_missing is None:
            raise RuntimeError("Before making API-calls the `is_missing` status is unknown")
        return self._is_missing

    @is_missing.setter
    def is_missing(self, value: bool) -> None:
        assert isinstance(value, bool)
        self._is_missing = value


class _SingleTSQueryRaw(_SingleTSQueryBase):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.aggregates = self.aggs_camel_case = None
        self.granularity = None

    @property
    def is_raw_query(self) -> Literal[True]:
        return True

    def to_payload_item(self) -> DatapointsPayloadItem:
        payload = DatapointsPayloadItem(
            **self.identifier.as_dict(),  # type: ignore [typeddict-item]
            start=self.start,
            end=self.end,
            limit=self.capped_limit,
            includeOutsidePoints=self.include_outside_points,
        )
        if self.target_unit is not None:
            payload["targetUnit"] = self.target_unit
        elif self.target_unit_system is not None:
            payload["targetUnitSystem"] = self.target_unit_system
        return payload


class _SingleTSQueryRawLimited(_SingleTSQueryRaw):
    def __init__(self, *, limit: int, **kwargs: Any) -> None:
        super().__init__(limit=limit, **kwargs)
        assert isinstance(limit, int)

    @property
    def task_orchestrator(self) -> type[SerialLimitedRawTaskOrchestrator]:
        return SerialLimitedRawTaskOrchestrator


class _SingleTSQueryRawUnlimited(_SingleTSQueryRaw):
    def __init__(self, *, limit: None, **kwargs: Any) -> None:
        super().__init__(limit=limit, **kwargs)

    @property
    def task_orchestrator(self) -> type[ConcurrentUnlimitedRawTaskOrchestrator]:
        return ConcurrentUnlimitedRawTaskOrchestrator


class _SingleTSQueryAgg(_SingleTSQueryBase):
    def __init__(self, *, aggregates: list[str], granularity: str, **kwargs: Any) -> None:
        super().__init__(**kwargs, include_outside_points=False)
        self.aggregates = aggregates
        self.granularity = granularity

    @property
    def is_raw_query(self) -> Literal[False]:
        return False

    @cached_property
    def aggs_camel_case(self) -> list[str]:
        return list(map(to_camel_case, self.aggregates))

    def to_payload_item(self) -> DatapointsPayloadItem:
        payload = DatapointsPayloadItem(
            **self.identifier.as_dict(),  # type: ignore [typeddict-item]
            start=self.start,
            end=self.end,
            aggregates=self.aggs_camel_case,
            granularity=self.granularity,
            limit=self.capped_limit,
            includeOutsidePoints=self.include_outside_points,
        )
        if self.target_unit is not None:
            payload["targetUnit"] = self.target_unit
        elif self.target_unit_system is not None:
            payload["targetUnitSystem"] = self.target_unit_system
        return payload


class _SingleTSQueryAggLimited(_SingleTSQueryAgg):
    def __init__(self, *, limit: int, **kwargs: Any) -> None:
        super().__init__(limit=limit, **kwargs)
        assert isinstance(limit, int)

    @property
    def task_orchestrator(self) -> type[SerialLimitedAggTaskOrchestrator]:
        return SerialLimitedAggTaskOrchestrator


class _SingleTSQueryAggUnlimited(_SingleTSQueryAgg):
    def __init__(self, *, limit: None, **kwargs: Any) -> None:
        super().__init__(limit=limit, **kwargs)

    @property
    def task_orchestrator(self) -> type[ConcurrentUnlimitedAggTaskOrchestrator]:
        return ConcurrentUnlimitedAggTaskOrchestrator


class DpsUnpackFns:
    ts: Callable[[Message], int] = op.attrgetter("timestamp")
    raw_dp: Callable[[Message], RawDatapointValue] = op.attrgetter("value")
    ts_dp_tpl: Callable[[Message], tuple[int, RawDatapointValue]] = op.attrgetter("timestamp", "value")
    count: Callable[[Message], int] = op.attrgetter("count")

    @staticmethod
    def custom_from_aggregates(lst: list[str]) -> Callable[[DatapointsAgg], tuple[float, ...]]:
        return op.attrgetter(*lst)


class DefaultSortedDict(SortedDict, Generic[_T]):
    def __init__(self, default_factory: Callable[[], _T], /, **kw: Any) -> None:
        self.default_factory = default_factory
        super().__init__(**kw)

    def __missing__(self, key: Hashable) -> _T:
        self[key] = self.default_factory()
        return self[key]


def create_dps_container() -> DefaultSortedDict:
    """Initialises a new sorted container for datapoints storage"""
    return DefaultSortedDict(list)


def create_subtask_lst() -> SortedList:
    """Initialises a new sorted list for subtasks"""
    return SortedList(key=op.attrgetter("subtask_idx"))


def ensure_int(val: float, change_nan_to: int = 0) -> int:
    if math.isnan(val):
        return change_nan_to
    return int(val)


def decide_numpy_dtype_from_is_string(is_string: bool) -> type:
    return np.object_ if is_string else np.float64


def get_datapoints_from_proto(res: DataPointListItem) -> DatapointsAny:
    if (dp_type := res.WhichOneof("datapointType")) is not None:
        return getattr(res, dp_type).datapoints
    return cast(MutableSequence[Any], [])


def get_ts_info_from_proto(res: DataPointListItem) -> dict[str, int | str | bool]:
    # Note: When 'unit_external_id' is returned, regular 'unit' is ditched
    return {
        "id": res.id,
        "external_id": res.externalId,
        "is_string": res.isString,
        "is_step": res.isStep,
        "unit": res.unit,
        "unit_external_id": res.unitExternalId,
    }


def create_array_from_dps_container(container: DefaultSortedDict) -> npt.NDArray:
    return np.hstack(list(chain.from_iterable(container.values())))


def create_aggregates_arrays_from_dps_container(container: DefaultSortedDict, n_aggs: int) -> list[npt.NDArray]:
    all_aggs_arr = np.vstack(list(chain.from_iterable(container.values())))
    return list(map(np.ravel, np.hsplit(all_aggs_arr, n_aggs)))


def create_list_from_dps_container(container: DefaultSortedDict) -> list:
    return list(chain.from_iterable(chain.from_iterable(container.values())))


def create_aggregates_list_from_dps_container(container: DefaultSortedDict) -> Iterator[list[list]]:
    concatenated = chain.from_iterable(chain.from_iterable(container.values()))
    return map(list, zip(*concatenated))  # rows to columns


class BaseDpsFetchSubtask:
    def __init__(
        self,
        start: int,
        end: int,
        identifier: Identifier,
        parent: BaseTaskOrchestrator,
        max_query_limit: int,
        is_raw_query: bool,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
    ) -> None:
        self.start = start
        self.end = end
        self.identifier = identifier
        self.parent = parent
        self.is_raw_query = is_raw_query
        self.max_query_limit = max_query_limit
        self.is_done = False

        self.static_kwargs = identifier.as_dict()
        if target_unit is not None:
            self.static_kwargs["targetUnit"] = target_unit
        elif target_unit_system is not None:
            self.static_kwargs["targetUnitSystem"] = target_unit_system

    @abstractmethod
    def get_next_payload_item(self) -> DatapointsPayloadItem:
        ...

    @abstractmethod
    def store_partial_result(self, res: DataPointListItem) -> list[SplittingFetchSubtask] | None:
        ...


T_BaseDpsFetchSubtask = TypeVar("T_BaseDpsFetchSubtask", bound=BaseDpsFetchSubtask)


class OutsideDpsFetchSubtask(BaseDpsFetchSubtask):
    """Fetches outside points and stores in parent"""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs, is_raw_query=True, max_query_limit=0)

    def get_next_payload_item(self) -> DatapointsPayloadItem:
        return DatapointsPayloadItem(
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

    def __init__(
        self,
        *,
        aggregates: list[str] | None,
        granularity: str | None,
        subtask_idx: tuple[float, ...],
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.aggregates = aggregates
        self.granularity = granularity
        self.subtask_idx = subtask_idx
        self.n_dps_fetched = 0
        self.next_start = self.start

        if not self.is_raw_query:
            self.static_kwargs.update(aggregates=aggregates, granularity=granularity)

    def get_next_payload_item(self) -> DatapointsPayloadItem:
        remaining = self.parent.get_remaining_limit()
        return DatapointsPayloadItem(
            start=self.next_start,
            end=self.end,
            limit=min(self.max_query_limit, remaining),
            **self.static_kwargs,  # type: ignore [typeddict-item]
        )

    def store_partial_result(self, res: DataPointListItem) -> list[SplittingFetchSubtask] | None:
        if self.parent.ts_info is None:
            # In eager mode, first task to complete gets the honor to store ts info:
            self.parent._store_ts_info(res)

        if not (dps := get_datapoints_from_proto(res)):
            self.is_done = True
            return None

        n, last_ts = len(dps), dps[-1].timestamp
        self.parent._unpack_and_store(self.subtask_idx, dps)
        self._update_state_for_next_payload(last_ts, n)
        if self._is_task_done(n, res.nextCursor):
            self.is_done = True
        return None

    def _update_state_for_next_payload(self, last_ts: int, n: int) -> None:
        self.next_start = last_ts + self.parent.offset_next  # Move `start` to prepare for next query
        self.n_dps_fetched += n  # Used to quit limited queries asap

    def _is_task_done(self, n: int, next_cursor: str) -> bool:
        return not next_cursor or n < self.max_query_limit or self.next_start == self.end


class SplittingFetchSubtask(SerialFetchSubtask):
    """Fetches data serially, but splits its time domain ("divide and conquer") based on the density
    of returned datapoints. Stores data in parent.
    """

    def __init__(self, *, max_splitting_factor: int = 10, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.max_splitting_factor = max_splitting_factor
        self.split_subidx: int = 0  # Actual value doesn't matter (any int will do)

    @property
    def _static_params(self) -> dict[str, Any]:
        return dict(
            parent=self.parent,
            identifier=self.identifier,
            aggregates=self.aggregates,
            granularity=self.granularity,
            max_query_limit=self.max_query_limit,
            is_raw_query=self.is_raw_query,
        )

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
            SplittingFetchSubtask(start=start, end=end, subtask_idx=idx, **self._static_params)
            for start, end, idx in zip(boundaries[1:-1], boundaries[2:], split_idxs)
        ]
        self.parent.subtasks.update(new_subtasks)
        return new_subtasks


class BaseTaskOrchestrator(ABC):
    def __init__(
        self,
        query: Any,  # subclasses assert correct type
        eager_mode: bool,
        use_numpy: bool,
        first_dps_batch: DataPointListItem | None = None,
        first_limit: int | None = None,
    ) -> None:
        self.query = query
        self.eager_mode = eager_mode
        self.use_numpy = use_numpy
        self.ts_info: dict | None = None
        self.subtask_outside_points: OutsideDpsFetchSubtask | None = None
        self.raw_dtype: type | None = None
        self.has_limit = self.query.limit is not None
        self._is_done = False
        self._final_result: Datapoints | DatapointsArray | None = None

        # Only concurrent fetchers really need auto-sorted containers. To keep indexing simple,
        # we also use them for serial fetchers (nice for outside points as well):
        self.ts_data = create_dps_container()
        self.dps_data = create_dps_container()
        self.subtasks = create_subtask_lst()

        # When running large queries (i.e. not "eager"), all time series have a first batch fetched before
        # further subtasks are created. This gives us e.g. outside points for free (if asked for) and ts info:
        if not self.eager_mode:
            assert first_dps_batch is not None and first_limit is not None
            self._extract_first_dps_batch(first_dps_batch, first_limit)

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
    def ts_info_dct(self) -> dict[str, Any]:
        # This is mostly for mypy to avoid 'cast' x 10000, but also a nice check to make sure
        # we have the required ts info before returning a result dps object.
        assert self.ts_info is not None
        return self.ts_info

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
        self.ts_info = get_ts_info_from_proto(res)
        self.ts_info["granularity"] = self.query.granularity
        if self.use_numpy:
            self.raw_dtype = decide_numpy_dtype_from_is_string(res.isString)

    def _store_first_batch(self, dps: DatapointsAny, first_limit: int) -> None:
        # Set `start` for the first subtask:
        self.first_start = dps[-1].timestamp + self.offset_next
        self._unpack_and_store(FIRST_IDX, dps)

        # Are we done after first batch?
        if self.first_start == self.query.end or len(dps) < first_limit:
            self._is_done = True
        elif self.has_limit and len(dps) <= self.query.limit <= first_limit:
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
                start=self.query.start,
                end=self.query.end,
                identifier=self.query.identifier,
                parent=self,
            )
            # Append the outside subtask to returned subtasks so that it will be queued:
            subtasks.append(self.subtask_outside_points)

    @abstractmethod
    def get_remaining_limit(self) -> float:  # What I really want: 'Literal[math.inf]'
        ...

    @abstractmethod
    def _get_result(self) -> Datapoints | DatapointsArray:
        ...

    @abstractmethod
    def split_into_subtasks(self, max_workers: int, n_tot_queries: int) -> list[BaseDpsFetchSubtask]:
        ...

    @property
    @abstractmethod
    def offset_next(self) -> int:
        ...

    @abstractmethod
    def _unpack_and_store(self, idx: tuple[float, ...], dps: DatapointsAny) -> None:
        ...


class SerialTaskOrchestratorMixin(BaseTaskOrchestrator):
    def get_remaining_limit(self) -> int:
        assert len(self.subtasks) == 1
        return self.query.limit - self.subtasks[0].n_dps_fetched

    def split_into_subtasks(self, max_workers: int, n_tot_queries: int) -> list[BaseDpsFetchSubtask]:
        # For serial fetching, a single task suffice
        start = self.query.start if self.eager_mode else self.first_start
        subtasks: list[BaseDpsFetchSubtask] = [
            SerialFetchSubtask(
                start=start,
                end=self.query.end,
                identifier=self.query.identifier,
                parent=self,
                max_query_limit=self.query.max_query_limit,
                is_raw_query=self.query.is_raw_query,
                target_unit=self.query.target_unit,
                target_unit_system=self.query.target_unit_system,
                aggregates=self.query.aggs_camel_case,
                granularity=self.query.granularity,
                subtask_idx=FIRST_IDX,
            )
        ]
        self.subtasks.update(subtasks)
        self._maybe_queue_outside_dps_subtask(subtasks)
        return subtasks


class BaseRawTaskOrchestrator(BaseTaskOrchestrator):
    def __init__(self, **kwargs: Any) -> None:
        self.dp_outside_start: tuple[int, RawDatapointValue] | None = None
        self.dp_outside_end: tuple[int, RawDatapointValue] | None = None
        super().__init__(**kwargs)

    @property
    def offset_next(self) -> Literal[1]:
        return 1  # millisecond

    def _create_empty_result(self) -> Datapoints | DatapointsArray:
        if not self.use_numpy:
            return Datapoints(**self.ts_info_dct, timestamp=[], value=[])
        return DatapointsArray._load(
            {
                **self.ts_info_dct,
                "timestamp": np.array([], dtype=np.int64),
                "value": np.array([], dtype=self.raw_dtype),
            }
        )

    def _was_any_data_fetched(self) -> bool:
        return any((self.ts_data, self.dp_outside_start, self.dp_outside_end))

    def _get_result(self) -> Datapoints | DatapointsArray:
        if not self._was_any_data_fetched():
            return self._create_empty_result()
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

    def _include_outside_points_in_result(self) -> None:
        for dp, idx in zip((self.dp_outside_start, self.dp_outside_end), (-math.inf, math.inf)):
            if not dp:
                continue
            ts: list[int] | NumpyInt64Array = [dp[0]]
            value: list[float | str] | NumpyFloat64Array | NumpyObjArray = [dp[1]]
            if self.use_numpy:
                ts = np.array(ts, dtype=np.int64)
                value = np.array(value, dtype=self.raw_dtype)
            self.ts_data[(idx,)].append(ts)
            self.dps_data[(idx,)].append(value)

    def _unpack_and_store(self, idx: tuple[float, ...], dps: DatapointsRaw) -> None:  # type: ignore [override]
        if self.use_numpy:  # Faster than feeding listcomp to np.array:
            self.ts_data[idx].append(np.fromiter(map(DpsUnpackFns.ts, dps), dtype=np.int64, count=len(dps)))
            self.dps_data[idx].append(np.fromiter(map(DpsUnpackFns.raw_dp, dps), dtype=self.raw_dtype, count=len(dps)))
        else:
            self.ts_data[idx].append(list(map(DpsUnpackFns.ts, dps)))
            self.dps_data[idx].append(list(map(DpsUnpackFns.raw_dp, dps)))

    def _store_first_batch(self, dps: DatapointsAny, first_limit: int) -> None:
        if self.query.include_outside_points:
            self._extract_outside_points(cast(DatapointsRaw, dps))
            if not dps:  # We might have only gotten outside points
                self._is_done = True
                return
        super()._store_first_batch(dps, first_limit)

    def _extract_outside_points(self, dps: DatapointsRaw) -> None:
        if dps[0].timestamp < self.query.start:
            # We got a dp before `start`, this (and 'after') should not impact our count towards `limit`,
            # so we pop to remove it from dps:
            self.dp_outside_start = DpsUnpackFns.ts_dp_tpl(dps.pop(0))

        if dps and dps[-1].timestamp >= self.query.end:  # >= because `end` is exclusive
            self.dp_outside_end = DpsUnpackFns.ts_dp_tpl(dps.pop(-1))


class SerialLimitedRawTaskOrchestrator(BaseRawTaskOrchestrator, SerialTaskOrchestratorMixin):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        assert isinstance(self.query, _SingleTSQueryRawLimited)


class ConcurrentTaskOrchestratorMixin(BaseTaskOrchestrator):
    @abstractmethod
    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, n_workers_per_queries: int) -> int:
        ...

    def get_remaining_limit(self) -> float:
        return math.inf

    def split_into_subtasks(self, max_workers: int, n_tot_queries: int) -> list[BaseDpsFetchSubtask]:
        # Given e.g. a single time series, we want to put all our workers to work by splitting into lots of pieces!
        # As the number grows - or we start combining multiple into the same query - we want to split less:
        # we hold back to not create too many subtasks:
        n_workers_per_queries = max(1, round(max_workers / n_tot_queries))
        subtasks: list[BaseDpsFetchSubtask] = self._create_uniformly_split_subtasks(n_workers_per_queries)
        self.subtasks.update(subtasks)
        self._maybe_queue_outside_dps_subtask(subtasks)
        return subtasks

    def _create_uniformly_split_subtasks(self, n_workers_per_queries: int) -> list[BaseDpsFetchSubtask]:
        start = self.query.start if self.eager_mode else self.first_start
        tot_ms = (end := self.query.end) - start
        n_periods = self._find_number_of_subtasks_uniform_split(tot_ms, n_workers_per_queries)
        boundaries = split_time_range(start, end, n_periods, self.offset_next)
        return [
            SplittingFetchSubtask(
                start=start,
                end=end,
                subtask_idx=(i,),
                parent=self,
                identifier=self.query.identifier,
                aggregates=self.query.aggs_camel_case,
                granularity=self.query.granularity,
                target_unit=self.query.target_unit,
                target_unit_system=self.query.target_unit_system,
                max_query_limit=self.query.max_query_limit,
                is_raw_query=self.query.is_raw_query,
            )
            for i, (start, end) in enumerate(zip(boundaries[:-1], boundaries[1:]), 1)
        ]


class ConcurrentUnlimitedRawTaskOrchestrator(BaseRawTaskOrchestrator, ConcurrentTaskOrchestratorMixin):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        assert isinstance(self.query, _SingleTSQueryRawUnlimited)

    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, n_workers_per_queries: int) -> int:
        # It makes no sense to split beyond what the max-size of a query allows (for a maximally dense
        # time series), but that is rarely useful as 100k dps is just 1 min 40 sec... we guess an
        # average density of points at 1 dp/sec, giving us split-windows no smaller than ~1 day:
        return min(n_workers_per_queries, math.ceil((tot_ms / 1000) / self.query.max_query_limit))


class BaseAggTaskOrchestrator(BaseTaskOrchestrator):
    def __init__(self, *, query: Any, use_numpy: bool, **kwargs: Any) -> None:
        self._set_aggregate_vars(query.aggs_camel_case, use_numpy)
        super().__init__(query=query, use_numpy=use_numpy, **kwargs)

    @cached_property
    def offset_next(self) -> int:
        return granularity_to_ms(self.query.granularity)

    def _set_aggregate_vars(self, aggs_camel_case: list[str], use_numpy: bool) -> None:
        # Developer note here: If you ask for datapoints to be returned in JSON, you get `count` as an integer.
        # Nice. However, when using protobuf, you get `double` xD ...so when this code was pivoted from
        # JSON -> protobuf, the special handling of `count` was kept in the hopes that one day protobuf
        # would yield the correct type...!
        self.float_aggs = aggs_camel_case[:]
        self.is_count_query = "count" in self.float_aggs
        if self.is_count_query:
            self.count_data = create_dps_container()
            self.float_aggs.remove("count")  # Only aggregate that is (supposed to be) integer, handle separately

        self.has_non_count_aggs = bool(self.float_aggs)
        if not self.has_non_count_aggs:
            return

        self.agg_unpack_fn = DpsUnpackFns.custom_from_aggregates(self.float_aggs)
        self.first_non_count_agg, *others = self.float_aggs
        self.single_non_count_agg = not others

        if use_numpy:
            if self.single_non_count_agg:
                self.dtype_aggs: np.dtype[Any] = np.dtype(np.float64)
            else:  # (.., 1) is deprecated for some reason
                self.dtype_aggs = np.dtype((np.float64, len(self.float_aggs)))

    def _clear_data_containers(self) -> None:
        super()._clear_data_containers()
        if self.is_count_query:
            del self.count_data

    def _create_empty_result(self) -> Datapoints | DatapointsArray:
        if self.use_numpy:
            arr_dct = {"timestamp": np.array([], dtype=np.int64)}
            if self.is_count_query:
                arr_dct["count"] = np.array([], dtype=np.int64)
            if self.has_non_count_aggs:
                arr_dct.update({agg: np.array([], dtype=np.float64) for agg in self.float_aggs})
            return DatapointsArray._load({**self.ts_info_dct, **arr_dct})

        lst_dct: dict[str, list] = {"timestamp": []}
        if self.is_count_query:
            lst_dct["count"] = []
        if self.has_non_count_aggs:
            lst_dct.update({agg: [] for agg in self.float_aggs})
        return Datapoints(**self.ts_info_dct, **convert_all_keys_to_snake_case(lst_dct))

    def _get_result(self) -> Datapoints | DatapointsArray:
        if not self.ts_data or self.query.limit == 0:
            return self._create_empty_result()

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

    def _unpack_and_store(self, idx: tuple[float, ...], dps: DatapointsAgg) -> None:  # type: ignore [override]
        if self.use_numpy:
            self._unpack_and_store_numpy(idx, dps)
        else:
            self._unpack_and_store_basic(idx, dps)

    def _unpack_and_store_numpy(self, idx: tuple[float, ...], dps: DatapointsAgg) -> None:
        n = len(dps)
        self.ts_data[idx].append(np.fromiter(map(DpsUnpackFns.ts, dps), dtype=np.int64, count=n))

        if self.is_count_query:
            # If an interval with no datapoints (i.e. count does not exist) has data from another aggregate (probably
            # (step_)interpolation), count returns nan... which we need float to represent... which we do not want.
            # Thus we convert any NaNs to 0 (which for count makes perfect sense):
            arr = np.fromiter(map(DpsUnpackFns.count, dps), dtype=np.float64, count=n)
            arr = np.nan_to_num(arr, copy=False, nan=0.0, posinf=np.inf, neginf=-np.inf).astype(np.int64)
            self.count_data[idx].append(arr)

        if self.has_non_count_aggs:
            try:  # Fast method uses multi-key unpacking:
                arr = np.fromiter(map(self.agg_unpack_fn, dps), dtype=self.dtype_aggs, count=n)  # type: ignore [arg-type]
            except AttributeError:  # An aggregate is missing, fallback to slower `getattr`:
                arr = np.array(
                    [tuple(getattr(dp, agg, math.nan) for agg in self.float_aggs) for dp in dps],
                    dtype=np.float64,
                )
            self.dps_data[idx].append(arr.reshape(n, len(self.float_aggs)))

    def _unpack_and_store_basic(self, idx: tuple[float, ...], dps: DatapointsAgg) -> None:
        self.ts_data[idx].append(list(map(DpsUnpackFns.ts, dps)))

        if self.is_count_query:
            # Need to do an extra NaN-aware int-conversion because protobuf (as opposed to json) returns double:
            self.count_data[idx].append(list(map(ensure_int, (getattr(dp, "count") for dp in dps))))

        if self.has_non_count_aggs:
            try:
                lst: list[Any] = list(map(self.agg_unpack_fn, dps))  # type: ignore [arg-type]
            except AttributeError:
                if self.single_non_count_agg:
                    lst = [getattr(dp, self.first_non_count_agg, None) for dp in dps]
                else:
                    lst = [tuple(getattr(dp, agg, None) for agg in self.float_aggs) for dp in dps]
            self.dps_data[idx].append(lst)


class SerialLimitedAggTaskOrchestrator(BaseAggTaskOrchestrator, SerialTaskOrchestratorMixin):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        assert isinstance(self.query, _SingleTSQueryAggLimited)


class ConcurrentUnlimitedAggTaskOrchestrator(BaseAggTaskOrchestrator, ConcurrentTaskOrchestratorMixin):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        assert isinstance(self.query, _SingleTSQueryAggUnlimited)

    def _find_number_of_subtasks_uniform_split(self, tot_ms: int, n_workers_per_queries: int) -> int:
        n_max_dps = tot_ms // self.offset_next  # evenly divides
        return min(n_workers_per_queries, math.ceil(n_max_dps / self.query.max_query_limit))
