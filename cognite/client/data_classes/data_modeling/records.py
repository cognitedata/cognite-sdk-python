from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar, Literal

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    WriteableCogniteResource,
)
from cognite.client.data_classes.data_modeling.ids import ContainerId
from cognite.client.data_classes.data_modeling.instances import TypeInformation
from cognite.client.data_classes.filters import Filter
from cognite.client.utils._identifier import IdentifierSequenceCore, RecordId

__all__ = [
    "Avg",
    "Count",
    "FilterAggregateResult",
    "Filters",
    "Max",
    "MetricAggregateResult",
    "Min",
    "MovingFunction",
    "MovingFunctionAggregateResult",
    "NumberHistogram",
    "NumberHistogramAggregateResult",
    "RecordContainerId",
    "RecordId",
    "RecordIdSequence",
    "RecordSource",
    "RecordWrite",
    "RecordWriteList",
    "RecordsAggregate",
    "RecordsAggregateResult",
    "RecordsAggregation",
    "RecordsBucket",
    "Sum",
    "TimeHistogram",
    "TimeHistogramAggregateResult",
    "UniqueValues",
    "UniqueValuesAggregateResult",
]


def _dump_aggregate_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _dump_aggregate_value(val) for key, val in value.items()}
    if isinstance(value, list | tuple):
        return [_dump_aggregate_value(item) for item in value]
    dump = getattr(value, "dump", None)
    if callable(dump):
        return _dump_aggregate_value(dump())
    return value


class RecordsAggregate(CogniteResource):
    """Base class for typed Records aggregate request builders."""

    _aggregate_name: ClassVar[str]

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        raise NotImplementedError(f"{cls.__name__} is a request builder and cannot be loaded from API responses")

    def _dump_body(self) -> dict[str, Any]:
        raise NotImplementedError

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {self._aggregate_name: _dump_aggregate_value(self._dump_body())}


class _PropertyAggregate(RecordsAggregate):
    def __init__(self, property: Sequence[str]) -> None:
        self.property = list(property)

    def _dump_body(self) -> dict[str, Any]:
        return {"property": self.property}


class Avg(_PropertyAggregate):
    """Average aggregate over a container property."""

    _aggregate_name = "avg"


class Count(RecordsAggregate):
    """Count records, or non-null values when ``property`` is provided."""

    _aggregate_name = "count"

    def __init__(self, property: Sequence[str] | None = None) -> None:
        self.property = list(property) if property is not None else None

    def _dump_body(self) -> dict[str, Any]:
        return {"property": self.property} if self.property is not None else {}


class Min(_PropertyAggregate):
    """Minimum aggregate over a property."""

    _aggregate_name = "min"


class Max(_PropertyAggregate):
    """Maximum aggregate over a property."""

    _aggregate_name = "max"


class Sum(_PropertyAggregate):
    """Sum aggregate over a container property."""

    _aggregate_name = "sum"


class _NestedAggregate(RecordsAggregate):
    def __init__(self, aggregates: Mapping[str, Any] | None = None) -> None:
        self.aggregates = aggregates

    def _add_aggregates(self, body: dict[str, Any]) -> dict[str, Any]:
        if self.aggregates is not None:
            body["aggregates"] = self.aggregates
        return body


class UniqueValues(_NestedAggregate):
    """Bucket records by unique property values."""

    _aggregate_name = "uniqueValues"

    def __init__(self, property: Sequence[str], aggregates: Mapping[str, Any] | None = None, size: int | None = None):
        super().__init__(aggregates)
        self.property = list(property)
        self.size = size

    def _dump_body(self) -> dict[str, Any]:
        body: dict[str, Any] = {"property": self.property}
        self._add_aggregates(body)
        if self.size is not None:
            body["size"] = self.size
        return body


class NumberHistogram(_NestedAggregate):
    """Bucket numeric property values into fixed-width intervals."""

    _aggregate_name = "numberHistogram"

    def __init__(
        self,
        property: Sequence[str],
        interval: float,
        aggregates: Mapping[str, Any] | None = None,
        hard_bounds: Mapping[str, float] | None = None,
    ) -> None:
        super().__init__(aggregates)
        self.property = list(property)
        self.interval = interval
        self.hard_bounds = hard_bounds

    def _dump_body(self) -> dict[str, Any]:
        body: dict[str, Any] = {"property": self.property, "interval": self.interval}
        self._add_aggregates(body)
        if self.hard_bounds is not None:
            body["hardBounds"] = self.hard_bounds
        return body


class TimeHistogram(_NestedAggregate):
    """Bucket timestamp values into calendar or fixed time intervals."""

    _aggregate_name = "timeHistogram"

    def __init__(
        self,
        property: Sequence[str],
        *,
        calendar_interval: str | None = None,
        fixed_interval: str | None = None,
        aggregates: Mapping[str, Any] | None = None,
        hard_bounds: Mapping[str, str] | None = None,
    ) -> None:
        if (calendar_interval is None) == (fixed_interval is None):
            raise ValueError("Exactly one of calendar_interval or fixed_interval must be specified")
        super().__init__(aggregates)
        self.property = list(property)
        self.calendar_interval = calendar_interval
        self.fixed_interval = fixed_interval
        self.hard_bounds = hard_bounds

    def _dump_body(self) -> dict[str, Any]:
        body: dict[str, Any] = {"property": self.property}
        self._add_aggregates(body)
        if self.calendar_interval is not None:
            body["calendarInterval"] = self.calendar_interval
        if self.fixed_interval is not None:
            body["fixedInterval"] = self.fixed_interval
        if self.hard_bounds is not None:
            body["hardBounds"] = self.hard_bounds
        return body


class Filters(_NestedAggregate):
    """Bucket records by a list of filter expressions."""

    _aggregate_name = "filters"

    def __init__(
        self,
        filters: Sequence[Filter | dict[str, Any]],
        aggregates: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(aggregates)
        self.filters = filters

    def _dump_body(self) -> dict[str, Any]:
        body: dict[str, Any] = {
            "filters": [filter.dump() if isinstance(filter, Filter) else filter for filter in self.filters]
        }
        return self._add_aggregates(body)


class MovingFunction(RecordsAggregate):
    """Pipeline aggregate over a parent histogram bucket series."""

    _aggregate_name = "movingFunction"

    def __init__(
        self,
        buckets_path: str,
        window: int,
        function: Literal[
            "MovingFunctions.max",
            "MovingFunctions.min",
            "MovingFunctions.sum",
            "MovingFunctions.unweightedAvg",
            "MovingFunctions.linearWeightedAvg",
        ],
    ) -> None:
        self.buckets_path = buckets_path
        self.window = window
        self.function = function

    def _dump_body(self) -> dict[str, Any]:
        return {"bucketsPath": self.buckets_path, "window": self.window, "function": self.function}


class RecordIdSequence(IdentifierSequenceCore[RecordId]):
    @classmethod
    def load(cls, items: RecordId | Sequence[RecordId]) -> RecordIdSequence:
        if isinstance(items, RecordId):
            return cls([items], is_singleton=True)
        return cls(list(items), is_singleton=False)


@dataclass(frozen=True)
class RecordContainerId(ContainerId):
    """Container reference used as a source in a record write.

    Args:
        space (str): Space that contains the container.
        external_id (str): External ID of the container.
    """


class RecordSource(CogniteResource):
    """Container source with property values for a record write.

    Args:
        source (RecordContainerId): Reference to the container.
        properties (dict[str, Any]): The data to write to the source container.
    """

    def __init__(self, source: RecordContainerId, properties: dict[str, Any]) -> None:
        self.source = source
        self.properties = properties

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            source=RecordContainerId.load(resource["source"]),
            properties=resource["properties"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "source": self.source.dump(camel_case=camel_case),
            "properties": self.properties,
        }


class RecordWrite(WriteableCogniteResource["RecordWrite"]):
    """Write representation of a record, used for ingest and upsert.

    This is the write version of :class:`Record`.

    Args:
        space (str): Space the record belongs to.
        external_id (str): External ID of the record (1-256 chars, no null bytes).
        sources (list[RecordSource]): Container property values to write (1-100 sources).
    """

    def __init__(self, space: str, external_id: str, sources: list[RecordSource]) -> None:
        self.space = space
        self.external_id = external_id
        self.sources = sources

    def as_id(self) -> RecordId:
        return RecordId(space=self.space, external_id=self.external_id)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            sources=[RecordSource._load(s) for s in resource.get("sources", [])],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "space": self.space,
            "externalId" if camel_case else "external_id": self.external_id,
            "sources": [s.dump(camel_case=camel_case) for s in self.sources],
        }

    def as_write(self) -> RecordWrite:
        return self


class RecordWriteList(CogniteResourceList[RecordWrite]):
    """A list of :class:`RecordWrite` objects."""

    _RESOURCE = RecordWrite

    def as_ids(self) -> list[RecordId]:
        return [v.as_id() for v in self]


class RecordsAggregateResult(CogniteResource):
    """Base class for typed Records aggregate results."""

    _raw_result: dict[str, Any]

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> RecordsAggregateResult:
        for aggregate in ("avg", "count", "min", "max", "sum"):
            if aggregate in resource:
                return MetricAggregateResult(aggregate=aggregate, value=resource[aggregate], raw_result=resource)
        if "fnValue" in resource:
            return MovingFunctionAggregateResult(fn_value=resource["fnValue"], raw_result=resource)
        for result_cls in (
            UniqueValuesAggregateResult,
            NumberHistogramAggregateResult,
            TimeHistogramAggregateResult,
            FilterAggregateResult,
        ):
            if result_cls._buckets_key in resource:
                return result_cls._load(resource)
        return cls(resource)

    def __init__(self, raw_result: dict[str, Any]) -> None:
        self._raw_result = raw_result

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return self._raw_result


class MetricAggregateResult(RecordsAggregateResult):
    """Metric aggregate result such as ``avg``, ``count``, ``min``, ``max``, or ``sum``."""

    def __init__(self, aggregate: str, value: Any, raw_result: dict[str, Any] | None = None) -> None:
        super().__init__(raw_result or {aggregate: value})
        self.aggregate = aggregate
        self.value = value


class MovingFunctionAggregateResult(RecordsAggregateResult):
    """Pipeline moving function result."""

    def __init__(self, fn_value: float, raw_result: dict[str, Any] | None = None) -> None:
        super().__init__(raw_result or {"fnValue": fn_value})
        self.fn_value = fn_value


class RecordsBucket(CogniteResource):
    """Bucket result from a Records bucket aggregate."""

    def __init__(self, raw_bucket: dict[str, Any]) -> None:
        self._raw_bucket = raw_bucket
        self.count = raw_bucket["count"]
        self.value = raw_bucket.get("value")
        self.interval_start = raw_bucket.get("intervalStart")
        self.aggregates = raw_bucket.get("aggregates", {})
        self.results = {
            aggregate_id: RecordsAggregateResult._load(result)
            for aggregate_id, result in self.aggregates.items()
            if isinstance(result, dict)
        }

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(resource)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return self._raw_bucket


class _BucketAggregateResult(RecordsAggregateResult):
    _buckets_key: ClassVar[str]

    def __init__(self, buckets: Sequence[RecordsBucket], raw_result: dict[str, Any] | None = None) -> None:
        self.buckets = list(buckets)
        super().__init__(raw_result or {self._buckets_key: [bucket.dump() for bucket in self.buckets]})

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            buckets=[RecordsBucket._load(bucket) for bucket in resource.get(cls._buckets_key, [])],
            raw_result=resource,
        )


class UniqueValuesAggregateResult(_BucketAggregateResult):
    """Result from a ``uniqueValues`` aggregate."""

    _buckets_key = "uniqueValueBuckets"


class NumberHistogramAggregateResult(_BucketAggregateResult):
    """Result from a ``numberHistogram`` aggregate."""

    _buckets_key = "numberHistogramBuckets"


class TimeHistogramAggregateResult(_BucketAggregateResult):
    """Result from a ``timeHistogram`` aggregate."""

    _buckets_key = "timeHistogramBuckets"


class FilterAggregateResult(_BucketAggregateResult):
    """Result from a ``filters`` aggregate."""

    _buckets_key = "filterBuckets"


class RecordsAggregation(CogniteResource):
    """Aggregate results returned from the Records aggregate endpoint.

    Args:
        aggregates (dict[str, Any]): Aggregate results keyed by the client-defined aggregate IDs.
        typing (TypeInformation | None): Optional property typing metadata.
    """

    def __init__(self, aggregates: dict[str, Any], typing: TypeInformation | None = None) -> None:
        self.aggregates = aggregates
        self.results = {
            aggregate_id: RecordsAggregateResult._load(result)
            for aggregate_id, result in aggregates.items()
            if isinstance(result, dict)
        }
        self.typing = typing

    def __getitem__(self, aggregate_id: str) -> RecordsAggregateResult:
        return self.results[aggregate_id]

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            aggregates=resource["aggregates"],
            typing=TypeInformation._load(resource["typing"]) if "typing" in resource else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"aggregates": _dump_aggregate_value(self.aggregates)}
        if self.typing is not None:
            output["typing"] = self.typing.dump(camel_case=camel_case)
        return output
