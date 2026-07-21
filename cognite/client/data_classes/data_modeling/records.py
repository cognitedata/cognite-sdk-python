from __future__ import annotations

from abc import abstractmethod
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar, Literal, cast

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    UnknownCogniteResource,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling.data_types import UnitReference, UnitSystemReference
from cognite.client.data_classes.data_modeling.ids import ContainerId
from cognite.client.data_classes.data_modeling.instances import TypeInformation
from cognite.client.data_classes.filters import Filter
from cognite.client.utils._identifier import IdentifierSequenceCore, RecordId
from cognite.client.utils._text import convert_all_keys_to_snake_case, to_snake_case

__all__ = [
    "Avg",
    "Count",
    "Filters",
    "Max",
    "Min",
    "MovingFunction",
    "MovingFunctions",
    "NumberHistogram",
    "Record",
    "RecordContainerId",
    "RecordId",
    "RecordIdSequence",
    "RecordList",
    "RecordSource",
    "RecordSourceSelector",
    "RecordTargetUnit",
    "RecordTargetUnits",
    "RecordWrite",
    "RecordWriteList",
    "RecordsAggregate",
    "RecordsAggregateResult",
    "RecordsAggregation",
    "RecordsBucket",
    "RecordsFilterAggregateResult",
    "RecordsMetricAggregateResult",
    "RecordsMovingFunctionAggregateResult",
    "RecordsNumberHistogramAggregateResult",
    "RecordsTimeHistogramAggregateResult",
    "RecordsUniqueValuesAggregateResult",
    "RecordsUnknownAggregateResult",
    "Sum",
    "SyncRecord",
    "SyncRecordList",
    "TimeHistogram",
    "TimeRange",
    "UniqueValues",
]


def _dump_aggregate_value(value: Any) -> Any:
    match value:
        case Mapping() as m:
            return {key: _dump_aggregate_value(val) for key, val in m.items()}
        case [*items]:
            return [_dump_aggregate_value(item) for item in items]
        case RecordsAggregate() as agg:
            return _dump_aggregate_value(agg.dump())
        case _:
            return value


def _dump_aggregate_results(
    aggregates: dict[str, Any],
    results: dict[str, RecordsAggregateResult],
    camel_case: bool,
) -> dict[str, Any]:
    """Dump a map of aggregate results keyed by client-defined aggregate IDs.

    The IDs are chosen by the caller and left untouched; only each result's own payload honors
    ``camel_case``. Entries without a parsed result (e.g. non-dict values) are passed through.
    """
    return {
        aggregate_id: (
            results[aggregate_id].dump(camel_case=camel_case)
            if aggregate_id in results
            else _dump_aggregate_value(value)
        )
        for aggregate_id, value in aggregates.items()
    }


class RecordsAggregate(CogniteResource):
    """Base class for typed Records aggregate request builders.

    Aggregates are request bodies: they serialize via :meth:`dump` and can be loaded back from that
    same representation via :meth:`load`, so an aggregate spec round-trips through a config file.
    """

    _aggregate_name: ClassVar[str]

    @abstractmethod
    def _dump_body(self) -> dict[str, Any]:
        raise NotImplementedError

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {self._aggregate_name: _dump_aggregate_value(self._dump_body())}

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> RecordsAggregate:
        # A dumped aggregate has a single top-level key naming the aggregate type; dispatch on it.
        # Nested `aggregates` are kept as their dumped form (dicts), which dump() handles verbatim.
        name = next(iter(resource))
        body = resource[name]
        if name == "avg":
            return Avg(body["property"])
        if name == "min":
            return Min(body["property"])
        if name == "max":
            return Max(body["property"])
        if name == "sum":
            return Sum(body["property"])
        if name == "count":
            return Count(body.get("property"))
        if name == "uniqueValues":
            return UniqueValues(body["property"], aggregates=body.get("aggregates"), size=body.get("size"))
        if name == "numberHistogram":
            return NumberHistogram(
                body["property"],
                interval=body["interval"],
                aggregates=body.get("aggregates"),
                hard_bounds=body.get("hardBounds"),
            )
        if name == "timeHistogram":
            return TimeHistogram(
                body["property"],
                calendar_interval=body.get("calendarInterval"),
                fixed_interval=body.get("fixedInterval"),
                aggregates=body.get("aggregates"),
                hard_bounds=body.get("hardBounds"),
            )
        if name == "filters":
            return Filters(filters=body["filters"], aggregates=body.get("aggregates"))
        if name == "movingFunction":
            return MovingFunction(buckets_path=body["bucketsPath"], window=body["window"], function=body["function"])
        return cast(RecordsAggregate, UnknownCogniteResource(resource))


class _PropertyAggregate(RecordsAggregate):
    def __init__(self, property: list[str] | tuple[str, ...]) -> None:
        self.property = list(property)

    def _dump_body(self) -> dict[str, Any]:
        return {"property": self.property}


class Avg(_PropertyAggregate):
    """Average aggregate over a container property."""

    _aggregate_name = "avg"


class Count(RecordsAggregate):
    """Count records, or non-null values when ``property`` is provided."""

    _aggregate_name = "count"

    def __init__(self, property: list[str] | tuple[str, ...] | None = None) -> None:
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

    def __init__(
        self,
        property: list[str] | tuple[str, ...],
        aggregates: Mapping[str, Any] | None = None,
        size: int | None = None,
    ):
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
        property: list[str] | tuple[str, ...],
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
        property: list[str] | tuple[str, ...],
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


class MovingFunctions(str, Enum):
    """Pipeline functions available to :class:`MovingFunction`."""

    MAX = "MovingFunctions.max"
    MIN = "MovingFunctions.min"
    SUM = "MovingFunctions.sum"
    UNWEIGHTED_AVG = "MovingFunctions.unweightedAvg"
    LINEAR_WEIGHTED_AVG = "MovingFunctions.linearWeightedAvg"


class MovingFunction(RecordsAggregate):
    """Pipeline aggregate over a parent histogram bucket series."""

    _aggregate_name = "movingFunction"

    def __init__(
        self,
        buckets_path: str,
        window: int,
        function: MovingFunctions
        | Literal[
            "MovingFunctions.max",
            "MovingFunctions.min",
            "MovingFunctions.sum",
            "MovingFunctions.unweightedAvg",
            "MovingFunctions.linearWeightedAvg",
        ],
    ) -> None:
        self.buckets_path = buckets_path
        self.window = window
        self.function = MovingFunctions(function)

    def _dump_body(self) -> dict[str, Any]:
        return {"bucketsPath": self.buckets_path, "window": self.window, "function": self.function.value}


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
            "properties": deepcopy(self.properties),
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

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> RecordsAggregateResult:
        # Dispatcher: each aggregate result carries exactly one top-level key that selects the
        # concrete result type. Each subclass implements its own _load returning Self.
        assert len(resource) == 1, f"expected exactly one aggregate result key, got {sorted(resource)}"
        key = next(iter(resource))
        if key in _METRIC_AGGREGATE_KEYS:
            return RecordsMetricAggregateResult._load(resource)
        if key == "fnValue":
            return RecordsMovingFunctionAggregateResult._load(resource)
        if (result_cls := _BUCKET_RESULT_BY_KEY.get(key)) is not None:
            return result_cls._load(resource)
        return RecordsUnknownAggregateResult._load(resource)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        raise NotImplementedError


class RecordsMetricAggregateResult(RecordsAggregateResult):
    """Metric aggregate result such as ``avg``, ``count``, ``min``, ``max``, or ``sum``."""

    def __init__(self, aggregate: str, value: Any) -> None:
        self.aggregate = aggregate
        self.value = value

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        key = next(iter(resource))
        return cls(aggregate=key, value=resource[key])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {self.aggregate: self.value}


class RecordsMovingFunctionAggregateResult(RecordsAggregateResult):
    def __init__(self, fn_value: float) -> None:
        self.fn_value = fn_value

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(fn_value=resource["fnValue"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"fnValue" if camel_case else "fn_value": self.fn_value}


class RecordsUnknownAggregateResult(RecordsAggregateResult):
    """Fallback for aggregate result shapes the SDK does not model yet.

    Preserves the raw payload verbatim so nothing is lost, snake-casing the API keys on request.
    """

    def __init__(self, raw_result: dict[str, Any]) -> None:
        self._raw_result = raw_result

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(resource)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        if camel_case:
            return dict(self._raw_result)
        return convert_all_keys_to_snake_case(self._raw_result)


class RecordsBucket(CogniteResource):
    def __init__(
        self,
        count: int,
        value: Any = None,
        interval_start: float | str | None = None,
        aggregates: dict[str, Any] | None = None,
    ) -> None:
        self.count = count
        self.value = value
        self.interval_start = interval_start
        self.aggregates = aggregates or {}
        self.results = {
            aggregate_id: RecordsAggregateResult._load(result)
            for aggregate_id, result in self.aggregates.items()
            if isinstance(result, dict)
        }

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            count=resource["count"],
            value=resource.get("value"),
            interval_start=resource.get("intervalStart"),
            aggregates=resource.get("aggregates"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"count": self.count}
        if self.value is not None:
            output["value"] = self.value
        if self.interval_start is not None:
            output["intervalStart" if camel_case else "interval_start"] = self.interval_start
        if self.aggregates:
            output["aggregates"] = _dump_aggregate_results(self.aggregates, self.results, camel_case)
        return output


class _RecordsBucketAggregateResult(RecordsAggregateResult):
    _buckets_key: ClassVar[str]

    def __init__(self, buckets: Sequence[RecordsBucket]) -> None:
        self._buckets = list(buckets)

    @property
    def buckets(self) -> list[RecordsBucket]:
        return list(self._buckets)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(buckets=[RecordsBucket._load(bucket) for bucket in resource.get(cls._buckets_key, [])])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        key = self._buckets_key if camel_case else to_snake_case(self._buckets_key)
        return {key: [bucket.dump(camel_case=camel_case) for bucket in self._buckets]}


class RecordsUniqueValuesAggregateResult(_RecordsBucketAggregateResult):
    _buckets_key = "uniqueValueBuckets"


class RecordsNumberHistogramAggregateResult(_RecordsBucketAggregateResult):
    _buckets_key = "numberHistogramBuckets"


class RecordsTimeHistogramAggregateResult(_RecordsBucketAggregateResult):
    _buckets_key = "timeHistogramBuckets"


class RecordsFilterAggregateResult(_RecordsBucketAggregateResult):
    _buckets_key = "filterBuckets"


_METRIC_AGGREGATE_KEYS: frozenset[str] = frozenset({"avg", "count", "min", "max", "sum"})

_BUCKET_RESULT_BY_KEY: dict[str, type[_RecordsBucketAggregateResult]] = {
    result_cls._buckets_key: result_cls
    for result_cls in (
        RecordsUniqueValuesAggregateResult,
        RecordsNumberHistogramAggregateResult,
        RecordsTimeHistogramAggregateResult,
        RecordsFilterAggregateResult,
    )
}


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
        output: dict[str, Any] = {"aggregates": _dump_aggregate_results(self.aggregates, self.results, camel_case)}
        if self.typing is not None:
            output["typing"] = self.typing.dump(camel_case=camel_case)
        return output


class Record(WriteableCogniteResource["RecordWrite"]):
    """A record returned from the stream records API.

    This is the read version of :class:`RecordWrite`.

    Args:
        space (str): Space the record belongs to.
        external_id (str): External ID of the record.
        created_time (int): Creation time in milliseconds since epoch.
        last_updated_time (int): Last updated time in milliseconds since epoch.
        properties (dict[str, dict[str, dict[str, Any]]] | None): Property values keyed by
            ``{space: {container_external_id: {property_id: value}}}``.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        created_time: int,
        last_updated_time: int,
        properties: dict[str, dict[str, dict[str, Any]]] | None = None,
    ) -> None:
        self.space = space
        self.external_id = external_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.properties = properties

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            properties=resource.get("properties"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "space": self.space,
            "externalId" if camel_case else "external_id": self.external_id,
            "createdTime" if camel_case else "created_time": self.created_time,
            "lastUpdatedTime" if camel_case else "last_updated_time": self.last_updated_time,
        }
        if self.properties is not None:
            output["properties"] = self.properties
        return output

    def as_id(self) -> RecordId:
        return RecordId(space=self.space, external_id=self.external_id)

    def as_write(self) -> RecordWrite:
        """Reconstruct the :class:`RecordWrite` by grouping read properties back into sources."""
        sources = [
            RecordSource(
                source=RecordContainerId(space=space, external_id=container),
                properties=dict(props),
            )
            for space, containers in (self.properties or {}).items()
            for container, props in containers.items()
        ]
        return RecordWrite(space=self.space, external_id=self.external_id, sources=sources)


class RecordList(WriteableCogniteResourceList[RecordWrite, Record]):
    """A list of :class:`Record` objects.

    Args:
        resources (Sequence[Record]): The records.
        typing (TypeInformation | None): Property type information, present when the request
            was made with ``include_typing=True``.
    """

    _RESOURCE = Record

    def __init__(self, resources: Sequence[Record], typing: TypeInformation | None = None) -> None:
        super().__init__(resources)
        self.typing = typing

    def as_ids(self) -> list[RecordId]:
        return [record.as_id() for record in self]

    def as_write(self) -> RecordWriteList:
        return RecordWriteList([record.as_write() for record in self])

    @classmethod
    def _load_raw_api_response(cls, responses: list[dict[str, Any]]) -> Self:
        typing = next((TypeInformation._load(resp["typing"]) for resp in responses if "typing" in resp), None)
        resources = [cls._RESOURCE._load(item) for response in responses for item in response.get("items", [])]
        return cls(resources, typing)


class TimeRange(CogniteResource):
    """A time range filter on ``lastUpdatedTime``.

    Bounds are either milliseconds since the Unix epoch (int) or an ISO-8601 string. At least a
    lower bound (``gte`` or ``gt``) is required for immutable streams; specifying two lower or two
    upper bounds is not allowed.

    Args:
        gte (int | str | None): Greater than or equal to.
        gt (int | str | None): Greater than.
        lte (int | str | None): Less than or equal to.
        lt (int | str | None): Less than.
    """

    def __init__(
        self,
        gte: int | str | None = None,
        gt: int | str | None = None,
        lte: int | str | None = None,
        lt: int | str | None = None,
    ) -> None:
        self.gte = gte
        self.gt = gt
        self.lte = lte
        self.lt = lt

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(gte=resource.get("gte"), gt=resource.get("gt"), lte=resource.get("lte"), lt=resource.get("lt"))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            key: value
            for key, value in {"gte": self.gte, "gt": self.gt, "lte": self.lte, "lt": self.lt}.items()
            if value is not None
        }


class RecordSourceSelector(CogniteResource):
    """Selects which container properties to return for a record.

    Args:
        source (RecordContainerId): The container to select properties from.
        properties (list[str]): Property identifiers to return; use ``["*"]`` to return all.
    """

    def __init__(self, source: RecordContainerId, properties: list[str]) -> None:
        self.source = source
        self.properties = properties

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(source=RecordContainerId.load(resource["source"]), properties=resource["properties"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"source": self.source.dump(camel_case=camel_case), "properties": self.properties}


class RecordTargetUnit(CogniteResource):
    """A target unit conversion for one Records container property.

    Args:
        property (list[str]): Fully qualified container property path:
            ``[space, container_external_id, property_id]``.
        unit (UnitReference | UnitSystemReference): Target unit or target unit system.
    """

    def __init__(self, property: list[str], unit: UnitReference | UnitSystemReference) -> None:
        self.property = property
        self.unit = unit

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            property=resource["property"],
            unit=UnitReference.load(resource["unit"])
            if "externalId" in resource["unit"]
            else UnitSystemReference.load(resource["unit"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"property": self.property, "unit": self.unit.dump(camel_case=camel_case)}


class RecordTargetUnits(CogniteResource):
    """Target unit conversions for a Records filter, sync, or aggregate request.

    Args:
        properties (list[RecordTargetUnit] | None): Property-specific target unit conversions.
        unit_system_name (str | None): Convert all convertible properties to a target unit system.
    """

    def __init__(self, properties: list[RecordTargetUnit] | None = None, unit_system_name: str | None = None) -> None:
        self.properties = properties
        self.unit_system_name = unit_system_name

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        if "properties" in resource:
            return cls(properties=[RecordTargetUnit._load(item) for item in resource["properties"]])
        if "unitSystemName" in resource:
            return cls(unit_system_name=resource["unitSystemName"])
        return cls()

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        if self.unit_system_name is not None:
            return {"unitSystemName" if camel_case else "unit_system_name": self.unit_system_name}
        if self.properties is not None:
            return {
                "properties": [target_unit.dump(camel_case=camel_case) for target_unit in self.properties],
            }
        return {}


class SyncRecord(Record):
    """A record returned by the sync endpoint, annotated with a change status.

    For ``status="deleted"`` tombstones (mutable streams), :attr:`properties` is ``None``.

    Args:
        space (str): Space the record belongs to.
        external_id (str): External ID of the record.
        created_time (int): Creation time in milliseconds since epoch.
        last_updated_time (int): Last updated time in milliseconds since epoch.
        status (Literal['created', 'updated', 'deleted']): The record's change status.
        properties (dict[str, dict[str, dict[str, Any]]] | None): Property values (absent for
            deleted tombstones).
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        created_time: int,
        last_updated_time: int,
        status: Literal["created", "updated", "deleted"],
        properties: dict[str, dict[str, dict[str, Any]]] | None = None,
    ) -> None:
        super().__init__(space, external_id, created_time, last_updated_time, properties)
        self.status = status

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            status=resource["status"],
            properties=resource.get("properties"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["status"] = self.status
        if self.properties is not None:
            output["properties"] = deepcopy(self.properties)
        return output


class SyncRecordList(CogniteResourceList[SyncRecord]):
    """A page of :class:`SyncRecord` objects from the sync endpoint.

    Args:
        resources (Sequence[SyncRecord]): The records in this page.
        cursor (str | None): Cursor to pass as ``cursor`` to the next ``sync_resume`` call to resume
            from this position.
        has_next (bool): Whether more changes are available beyond this page.
        typing (TypeInformation | None): Property type information, present when the request was
            made with ``include_typing=True``.
    """

    _RESOURCE = SyncRecord

    def __init__(
        self,
        resources: Sequence[SyncRecord],
        cursor: str | None = None,
        has_next: bool = False,
        typing: TypeInformation | None = None,
    ) -> None:
        super().__init__(resources)
        self.cursor = cursor
        self.has_next = has_next
        self.typing = typing

    @classmethod
    def _load_raw_api_response(cls, responses: list[dict[str, Any]]) -> Self:
        last_response = responses[-1]
        typing = next(
            (TypeInformation._load(response["typing"]) for response in responses if "typing" in response), None
        )
        return cls(
            [SyncRecord._load(item) for response in responses for item in response["items"]],
            cursor=last_response["nextCursor"],
            has_next=last_response["hasNext"],
            typing=typing,
        )
