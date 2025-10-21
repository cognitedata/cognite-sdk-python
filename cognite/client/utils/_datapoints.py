from __future__ import annotations

import math
import operator as op
from collections import defaultdict
from collections.abc import Callable, Iterator, Sequence
from itertools import chain
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, cast

from google.protobuf.internal.containers import RepeatedCompositeFieldContainer

from cognite.client._constants import NUMPY_IS_AVAILABLE
from cognite.client._proto.data_point_list_response_pb2 import DataPointListItem
from cognite.client._proto.data_points_pb2 import (
    AggregateDatapoint,
    NumericDatapoint,
    StringDatapoint,
)
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.datapoints import (
    DatapointsQuery,
    MaxDatapoint,
    MaxDatapointWithStatus,
    MaxOrMinDatapoint,
    MinDatapoint,
    MinDatapointWithStatus,
)
from cognite.client.utils.useful_types import SequenceNotStr

if NUMPY_IS_AVAILABLE:
    import numpy as np

if TYPE_CHECKING:
    import numpy.typing as npt

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

    # minDatapoint and maxDatapoint are also objects in the response. The proto lookups doesn't fail,
    # so we must be very careful to only attach status codes if requested.
    @staticmethod
    def min_datapoint(agg_dp: AggregateDatapoint) -> MinDatapoint:
        dp = agg_dp.minDatapoint
        return MinDatapoint(dp.timestamp, dp.value)

    @staticmethod
    def max_datapoint(agg_dp: AggregateDatapoint) -> MaxDatapoint:
        dp = agg_dp.maxDatapoint
        return MaxDatapoint(dp.timestamp, dp.value)

    @staticmethod
    def min_datapoint_with_status(agg_dp: AggregateDatapoint) -> MinDatapointWithStatus:
        dp = agg_dp.minDatapoint
        return MinDatapointWithStatus(
            dp.timestamp, dp.value, DpsUnpackFns.status_code(dp), DpsUnpackFns.status_symbol(dp)
        )

    @staticmethod
    def max_datapoint_with_status(agg_dp: AggregateDatapoint) -> MaxDatapointWithStatus:
        dp = agg_dp.maxDatapoint
        return MaxDatapointWithStatus(
            dp.timestamp, dp.value, DpsUnpackFns.status_code(dp), DpsUnpackFns.status_symbol(dp)
        )

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
    def extract_numeric_aggregates_numpy(
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

    @staticmethod
    def extract_fn_min_or_max_dp(
        aggregate: Literal["minDatapoint", "maxDatapoint"], include_status: bool
    ) -> Callable[[AggregateDatapoint], MaxOrMinDatapoint]:
        match aggregate, include_status:
            case "minDatapoint", False:
                return DpsUnpackFns.min_datapoint
            case "maxDatapoint", False:
                return DpsUnpackFns.max_datapoint
            case "minDatapoint", True:
                return DpsUnpackFns.min_datapoint_with_status
            case "maxDatapoint", True:
                return DpsUnpackFns.max_datapoint_with_status
            case _:
                raise ValueError(f"Unsupported {aggregate=} and/or {include_status=}")


def ensure_int(val: float, change_nan_to: int = 0) -> int:
    if math.isnan(val):
        return change_nan_to
    return int(val)


def ensure_int_numpy(arr: npt.NDArray[np.float64]) -> npt.NDArray[np.int64]:
    return np.nan_to_num(arr, copy=False, nan=0.0, posinf=np.inf, neginf=-np.inf).astype(np.int64)


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


def create_object_array_from_container(container: _DataContainer) -> npt.NDArray[np.object_]:
    return np.array(create_list_from_dps_container(container), dtype=np.object_)


def create_aggregates_arrays_from_dps_container(container: _DataContainer, n_aggs: int) -> list[npt.NDArray]:
    all_aggs_arr = np.vstack(list(datapoints_in_order(container)))
    return list(map(np.ravel, np.hsplit(all_aggs_arr, n_aggs)))


def create_list_from_dps_container(container: _DataContainer) -> list:
    return list(chain.from_iterable(datapoints_in_order(container)))


def create_aggregates_list_from_dps_container(container: _DataContainer) -> Iterator[list[list]]:
    concatenated = chain.from_iterable(datapoints_in_order(container))
    return map(list, zip(*concatenated))  # rows to columns
