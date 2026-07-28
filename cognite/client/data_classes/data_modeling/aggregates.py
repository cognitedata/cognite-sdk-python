"""Typed aggregate request builders shared by the data modeling aggregate endpoints.

The builders here describe wire shapes that are endpoint-independent, so they are deliberately not
named after any single endpoint: Records uses them today, and the upcoming data modeling aggregate
endpoint - which is modelled on the Records one - will reuse them.

Nothing in this module is exported through ``cognite.client.data_classes.data_modeling``. That
namespace already re-exports the legacy :mod:`cognite.client.data_classes.aggregations` module, which
is scheduled for deprecation but still defines its own ``Average``, ``Count``, ``Min``, ``Max`` and
``Sum`` for ``instances.aggregate``. Keeping this module reachable only by its own path is what lets
the two families share those names without either shadowing the other. Import it as::

    from cognite.client.data_classes.data_modeling import aggregates as aggs

The two families are independent: :class:`Aggregate` is unrelated to the legacy ``Aggregation``, and
because both use the same wire keys (``avg``, ``count``, ...), :meth:`Aggregate.load` only ever
returns members of this family.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Mapping, Sequence
from enum import Enum
from typing import Any, ClassVar, Literal

from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.filters import Filter
from cognite.client.utils.useful_types import SequenceNotStr


def _dump_aggregate_value(value: Any) -> Any:
    match value:
        case Mapping() as m:
            return {key: _dump_aggregate_value(val) for key, val in m.items()}
        case [*items]:
            return [_dump_aggregate_value(item) for item in items]
        case Aggregate() as agg:
            return _dump_aggregate_value(agg.dump())
        case _:
            return value


class Aggregate(CogniteResource):
    """Base class for typed aggregate request builders.

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
    def _load(cls, resource: dict[str, Any]) -> Aggregate:
        # A dumped aggregate has a single top-level key naming the aggregate type; dispatch on it.
        # Nested `aggregates` are kept as their dumped form (dicts), which dump() handles verbatim.
        name = next(iter(resource))
        body = resource[name]
        if name == "avg":
            return Average(body["property"])
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
        return UnknownAggregate(resource)


class _PropertyAggregate(Aggregate):
    def __init__(self, property: SequenceNotStr[str]) -> None:
        self.property = list(property)

    def _dump_body(self) -> dict[str, Any]:
        return {"property": self.property}


class Average(_PropertyAggregate):
    """Average aggregate over a container property."""

    _aggregate_name = "avg"


class Count(Aggregate):
    """Count matched items, or non-null values when ``property`` is provided."""

    _aggregate_name = "count"

    def __init__(self, property: SequenceNotStr[str] | None = None) -> None:
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


class _NestedAggregate(Aggregate):
    def __init__(self, aggregates: Mapping[str, Aggregate | dict[str, Any]] | None = None) -> None:
        self.aggregates = aggregates

    def _add_aggregates(self, body: dict[str, Any]) -> dict[str, Any]:
        if self.aggregates is not None:
            body["aggregates"] = self.aggregates
        return body


class UniqueValues(_NestedAggregate):
    """Bucket matched items by unique property values."""

    _aggregate_name = "uniqueValues"

    def __init__(
        self,
        property: SequenceNotStr[str],
        aggregates: Mapping[str, Aggregate | dict[str, Any]] | None = None,
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
        property: SequenceNotStr[str],
        interval: float,
        aggregates: Mapping[str, Aggregate | dict[str, Any]] | None = None,
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
        property: SequenceNotStr[str],
        *,
        calendar_interval: str | None = None,
        fixed_interval: str | None = None,
        aggregates: Mapping[str, Aggregate | dict[str, Any]] | None = None,
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
    """Bucket matched items by a list of filter expressions."""

    _aggregate_name = "filters"

    def __init__(
        self,
        filters: Sequence[Filter | dict[str, Any]],
        aggregates: Mapping[str, Aggregate | dict[str, Any]] | None = None,
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


class MovingFunction(Aggregate):
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


class UnknownAggregate(Aggregate):
    """Fallback for aggregate request shapes this SDK version does not model yet.

    Preserves the raw request body verbatim so an unknown or newer aggregate type still round-trips
    through :meth:`dump`/:meth:`load` instead of failing. The request builders' :meth:`dump` is
    always camelCase, so the payload is returned as-is regardless of ``camel_case``.
    """

    def __init__(self, raw: dict[str, Any]) -> None:
        self._raw = raw

    def _dump_body(self) -> dict[str, Any]:  # never called: dump is overridden
        raise NotImplementedError

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return dict(self._raw)
