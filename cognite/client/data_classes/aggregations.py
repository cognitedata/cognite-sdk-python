from __future__ import annotations

from abc import ABC, abstractmethod
from collections import UserList
from collections.abc import Collection, Iterator, MutableSequence, Sequence
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    SupportsIndex,
    TypeAlias,
    TypeVar,
    cast,
    final,
    overload,
)

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteObject, CogniteResourceList, UnknownCogniteObject
from cognite.client.data_classes.labels import Label
from cognite.client.utils._text import convert_all_keys_recursive

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class CountAggregate(CogniteObject):
    """
    [DEPRECATED] This represents the result of a count aggregation.

    Args:
        count (int): The number of items matching the aggregation.
    """

    def __init__(self, count: int) -> None:
        self.count = count

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> CountAggregate:
        return cls(count=resource["count"])


@dataclass
class Aggregation(CogniteObject, ABC):
    _aggregation_name: ClassVar[str]

    property: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Aggregation:
        if "avg" in resource:
            return Avg(property=resource["avg"]["property"])
        elif "count" in resource:
            return Count(property=resource["count"]["property"])
        elif "max" in resource:
            return Max(property=resource["max"]["property"])
        elif "min" in resource:
            return Min(property=resource["min"]["property"])
        elif "sum" in resource:
            return Sum(property=resource["sum"]["property"])
        elif "histogram" in resource:
            return Histogram(property=resource["histogram"]["property"], interval=resource["histogram"]["interval"])
        return cast(Aggregation, UnknownCogniteObject(resource))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = {self._aggregation_name: {"property": self.property}}
        if camel_case:
            output = convert_all_keys_recursive(output)
        return output


@dataclass
class MetricAggregation(Aggregation, ABC): ...


@final
@dataclass
class Avg(MetricAggregation):
    _aggregation_name = "avg"


@final
@dataclass
class Count(MetricAggregation):
    _aggregation_name = "count"


@final
@dataclass
class Max(MetricAggregation):
    _aggregation_name = "max"


@final
@dataclass
class Min(MetricAggregation):
    _aggregation_name = "min"


@final
@dataclass
class Sum(MetricAggregation):
    _aggregation_name = "sum"


@final
@dataclass
class Histogram(Aggregation):
    _aggregation_name = "histogram"

    interval: float

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output[self._aggregation_name]["interval"] = self.interval
        return output


@dataclass
class AggregatedValue(CogniteObject, ABC):
    _aggregate: ClassVar[str] = field(init=False)

    property: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        if "aggregate" not in resource:
            raise ValueError("Missing aggregate, this is required")
        aggregate = resource["aggregate"]

        if aggregate == "avg":
            deserialized: Any = AvgValue(property=resource["property"], value=resource["value"])
        elif aggregate == "count":
            deserialized = CountValue(property=resource["property"], value=resource["value"])
        elif aggregate == "max":
            deserialized = MaxValue(property=resource["property"], value=resource["value"])
        elif aggregate == "min":
            deserialized = MinValue(property=resource["property"], value=resource["value"])
        elif aggregate == "sum":
            deserialized = SumValue(property=resource["property"], value=resource["value"])
        elif aggregate == "histogram":
            deserialized = HistogramValue(
                property=resource["property"], interval=resource["interval"], buckets=resource["buckets"]
            )
        else:
            deserialized = UnknownCogniteObject(resource)
        return cast(Self, deserialized)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = {"aggregate": self._aggregate, "property": self.property}
        if camel_case:
            output = convert_all_keys_recursive(output)
        return output


@dataclass
class AggregatedNumberedValue(AggregatedValue, ABC):
    _aggregate: ClassVar[str] = "number"

    value: float

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["value"] = self.value
        return output


@final
@dataclass
class AvgValue(AggregatedNumberedValue):
    _aggregate: ClassVar[str] = "avg"


@final
@dataclass
class CountValue(AggregatedNumberedValue):
    _aggregate: ClassVar[str] = "count"
    value: int


@final
@dataclass
class MaxValue(AggregatedNumberedValue):
    _aggregate: ClassVar[str] = "max"


@final
@dataclass
class MinValue(AggregatedNumberedValue):
    _aggregate: ClassVar[str] = "min"


@final
@dataclass
class SumValue(AggregatedNumberedValue):
    _aggregate: ClassVar[str] = "sum"


@dataclass
class Bucket:
    start: float
    count: int

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"start": self.start, "count": self.count}


class Buckets(UserList, MutableSequence[Bucket]):
    def __init__(self, items: Collection[dict | Bucket]) -> None:
        buckets = [
            Bucket(start=bucket["start"], count=bucket["count"]) if isinstance(bucket, dict) else bucket
            for bucket in items
        ]
        super().__init__(buckets)

    def dump(self, camel_case: bool = True) -> list[dict[str, Any]]:
        return [bucket.dump(camel_case) for bucket in self.data]

    @property
    def starts(self) -> list[float]:
        return [bucket.start for bucket in self.data]

    @property
    def counts(self) -> list[int]:
        return [bucket.count for bucket in self.data]

    # The following methods are needed for proper type hinting
    def pop(self, i: int = -1) -> Bucket:
        return super().pop(i)

    def __iter__(self) -> Iterator[Bucket]:
        return super().__iter__()

    @overload
    def __getitem__(self, item: SupportsIndex) -> Bucket: ...

    @overload
    def __getitem__(self, item: slice) -> Buckets: ...

    def __getitem__(self, item: SupportsIndex | slice) -> Bucket | Buckets:
        value = self.data[item]
        if isinstance(item, slice):
            return type(self)(value)
        return cast(Bucket, value)


@final
@dataclass
class HistogramValue(AggregatedValue):
    _aggregate: ClassVar[str] = "histogram"
    interval: float
    buckets: Buckets

    def __post_init__(self) -> None:
        if isinstance(self.buckets, Collection):
            self.buckets = Buckets(self.buckets)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["interval"] = self.interval
        output["buckets"] = self.buckets.dump(camel_case)
        return output


FilterValue: TypeAlias = str | float | bool | Label


class AggregationFilter(ABC):
    _filter_name: ClassVar[str]

    def dump(self) -> dict[str, Any]:
        return {self._filter_name: self._filter_body()}

    @abstractmethod
    def _filter_body(self) -> list | dict:
        raise NotImplementedError


class CompoundFilter(AggregationFilter):
    _filter_name = "compound"

    def __init__(self, *filters: AggregationFilter):
        self._filters = filters

    def _filter_body(self) -> list | dict:
        return [filter_.dump() for filter_ in self._filters]


def _dump_value(value: FilterValue) -> dict[str, str] | str | bool | int | float:
    if isinstance(value, Label) and value.external_id is not None:
        return {"externalId": value.external_id}
    elif isinstance(value, Label):
        raise ValueError("Label must have external_id set")
    return value


class FilterWithValue(AggregationFilter):
    _filter_name = "valueFilter"

    def __init__(self, value: FilterValue):
        self._value = value

    def _filter_body(self) -> dict:
        return {"value": _dump_value(self._value)}


class FilterWithValueList(AggregationFilter):
    _filter_name = "valueListFilter"

    def __init__(self, values: Sequence[FilterValue]):
        self._values = values

    def _filter_body(self) -> dict[str, Any]:
        return {"values": [_dump_value(value) for value in self._values]}


@final
class And(CompoundFilter):
    _filter_name = "and"


@final
class Or(CompoundFilter):
    _filter_name = "or"


@final
class Not(CompoundFilter):
    _filter_name = "not"

    def __init__(self, filter: AggregationFilter):
        super().__init__(filter)

    def _filter_body(self) -> dict:
        return self._filters[0].dump()


@final
class Prefix(FilterWithValue):
    _filter_name = "prefix"


@final
class In(FilterWithValueList):
    _filter_name = "in"


@final
class Range(AggregationFilter):
    _filter_name = "range"

    def __init__(
        self,
        gt: str | int | float | None = None,
        gte: str | int | float | None = None,
        lt: str | int | float | None = None,
        lte: str | int | float | None = None,
    ):
        self._gt = gt
        self._gte = gte
        self._lt = lt
        self._lte = lte

    def _filter_body(self) -> dict[str, Any]:
        body: dict[str, str | int | float] = {}
        if self._gt is not None:
            body["gt"] = self._gt
        if self._gte is not None:
            body["gte"] = self._gte
        if self._lt is not None:
            body["lt"] = self._lt
        if self._lte is not None:
            body["lte"] = self._lte
        return body


@dataclass
class UniqueResult(CogniteObject):
    count: int
    values: list[str | int | float | Label]

    @property
    def value(self) -> str | int | float | Label:
        return self.values[0]

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> UniqueResult:
        return cls(
            count=resource["count"],
            values=resource["values"],
        )


T_UniqueResult = TypeVar("T_UniqueResult", bound="UniqueResult")


class UniqueResultList(CogniteResourceList):
    _RESOURCE = UniqueResult

    @property
    def unique(self) -> list[str | int | float | Label]:
        return [item.value for item in self]
