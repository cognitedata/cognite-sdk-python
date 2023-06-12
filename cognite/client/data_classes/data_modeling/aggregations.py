from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field
from typing import Any, ClassVar, final

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


@final
@dataclass
class Avg(Aggregation):
    _aggregation_name = "avg"


@final
@dataclass
class Count(Aggregation):
    _aggregation_name = "count"


@final
@dataclass
class Max(Aggregation):
    _aggregation_name = "max"


@final
@dataclass
class Min(Aggregation):
    _aggregation_name = "min"


@final
@dataclass
class Sum(Aggregation):
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


@dataclass
class AggregatedValue(ABC):
    _aggregate: ClassVar[str] = field(init=False)

    property: str

    @classmethod
    def load(cls, aggregated_value: dict[str, Any]) -> AggregatedValue:
        if "aggregate" not in aggregated_value:
            raise ValueError("Missing aggregate, this is required")
        aggregate = aggregated_value["aggregate"]
        aggregated_value = rename_and_exclude_keys(aggregated_value, exclude={"aggregate"})
        body = convert_all_keys_to_snake_case(aggregated_value)
        if aggregate == "avg":
            return AvgValue(**body)
        elif aggregate == "count":
            return CountValue(**body)
        elif aggregate == "max":
            return MaxValue(**body)
        elif aggregate == "min":
            return MinValue(**body)
        elif aggregate == "sum":
            return SumValue(**body)
        elif aggregate == "histogram":
            return HistogramValue(**body)
        raise ValueError(f"Unknown aggregation: {aggregate}")

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
