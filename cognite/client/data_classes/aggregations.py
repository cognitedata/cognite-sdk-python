from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, ClassVar, Type, TypeVar, Union, cast, final

from typing_extensions import TypeAlias

from cognite.client.data_classes import Label
from cognite.client.utils._auxiliary import rename_and_exclude_keys
from cognite.client.utils._text import convert_all_keys_recursive, convert_all_keys_to_snake_case


@dataclass
class Aggregation(ABC):
    _aggregation_name: ClassVar[str]

    property: str

    @classmethod
    def load(cls, aggregation: dict[str, Any]) -> Aggregation:
        (aggregation_name,) = aggregation
        body = convert_all_keys_to_snake_case(aggregation[aggregation_name])
        if aggregation_name == "avg":
            return Avg(**body)
        elif aggregation_name == "count":
            return Count(**body)
        elif aggregation_name == "max":
            return Max(**body)
        elif aggregation_name == "min":
            return Min(**body)
        elif aggregation_name == "sum":
            return Sum(**body)
        elif aggregation_name == "histogram":
            return Histogram(**body)
        raise ValueError(f"Unknown aggregation: {aggregation_name}")

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = {self._aggregation_name: {"property": self.property}}
        if camel_case:
            output = convert_all_keys_recursive(output)
        return output


@dataclass
class MetricAggregation(Aggregation):
    ...


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

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = super().dump(camel_case)
        output[self._aggregation_name]["interval"] = self.interval
        return output


T_AggregatedValue = TypeVar("T_AggregatedValue", bound="AggregatedValue")


@dataclass
class AggregatedValue(ABC):
    _aggregate: ClassVar[str] = field(init=False)

    property: str

    @classmethod
    def load(cls: Type[T_AggregatedValue], aggregated_value: dict[str, Any]) -> T_AggregatedValue:
        if "aggregate" not in aggregated_value:
            raise ValueError("Missing aggregate, this is required")
        aggregate = aggregated_value["aggregate"]
        aggregated_value = rename_and_exclude_keys(aggregated_value, exclude={"aggregate"})
        body = convert_all_keys_to_snake_case(aggregated_value)

        if aggregate == "avg":
            deserialized: AggregatedValue = AvgValue(**body)
        elif aggregate == "count":
            deserialized = CountValue(**body)
        elif aggregate == "max":
            deserialized = MaxValue(**body)
        elif aggregate == "min":
            deserialized = MinValue(**body)
        elif aggregate == "sum":
            deserialized = SumValue(**body)
        elif aggregate == "histogram":
            deserialized = HistogramValue(**body)
        else:
            raise ValueError(f"Unknown aggregation: {aggregate}")
        return cast(T_AggregatedValue, deserialized)

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = {"aggregate": self._aggregate, "property": self.property}
        if camel_case:
            output = convert_all_keys_recursive(output)
        return output


@dataclass
class AggregatedNumberedValue(AggregatedValue):
    _aggregate: ClassVar[str] = "number"

    value: float

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
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

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return {"start": self.start, "count": self.count}


@final
@dataclass
class HistogramValue(AggregatedValue):
    _aggregate: ClassVar[str] = "histogram"
    interval: float
    buckets: list[Bucket]

    def __post_init__(self) -> None:
        self.buckets = [Bucket(**bucket) if isinstance(bucket, dict) else bucket for bucket in self.buckets]

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["interval"] = self.interval
        output["buckets"] = [bucket.dump(camel_case) for bucket in self.buckets]
        return output


Value: TypeAlias = Union[str, float, bool, Label]


class AggregationFilter(ABC):
    _filter_name: ClassVar[str]

    def dump(self) -> dict[str, Any]:
        return {self._filter_name: self._filter_body()}

    @classmethod
    def load(cls, filter_: dict[str, Any]) -> AggregationFilter:
        ...

    @abstractmethod
    def _filter_body(self) -> list | dict:
        raise NotImplementedError


class CompoundFilter(AggregationFilter):
    _filter_name = "compound"

    def __init__(self, *filters: AggregationFilter):
        self._filters = filters

    def _filter_body(self) -> list | dict:
        return [filter_.dump() for filter_ in self._filters]


class FilterWithValue(AggregationFilter):
    _filter_name = "valueFilter"

    def __init__(self, value: Value):
        self._value = value

    def _filter_body(self) -> dict:
        return {"value": self._value}


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
