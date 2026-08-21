"""Typed aggregate request builders and results shared by the data modeling aggregate endpoints.

The classes here describe wire shapes that are endpoint-independent, so they are deliberately not
named after any single endpoint: Records uses them today, and the upcoming data modeling aggregate
endpoint - which is modelled on the Records one - will reuse them. :class:`Aggregate` and its
subclasses are the request side; :class:`Result` and its subclasses are what the API returns.

Nothing in this module is exported through ``cognite.client.data_classes.data_modeling``. That
namespace already re-exports :mod:`cognite.client.data_classes.aggregations`, which defines its own
``Average``, ``Count``, ``Min``, ``Max`` and ``Sum`` for ``instances.aggregate``. Keeping this module
reachable only by its own path is what lets the two families share those names without either
shadowing the other. Import it as::

    from cognite.client.data_classes.data_modeling import aggregates as aggs

The two families are independent: :class:`Aggregate` is unrelated to the legacy ``Aggregation``, and
because both use the same wire keys (``avg``, ``count``, ...), :meth:`Aggregate.load` only ever
returns members of this family.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Mapping, Sequence
from enum import Enum
from typing import Any, ClassVar

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.data_modeling._validation import validate_property_path
from cognite.client.data_classes.filters import Filter
from cognite.client.utils._text import convert_all_keys_to_snake_case, to_snake_case
from cognite.client.utils.useful_types import SequenceNotStr

_AGGREGATES_HINT = (
    "Aggregates are keyed by the IDs you pick for them, e.g. "
    '{"avg_temperature": Average(["my_space", "my_container", "temperature"])}.'
)


def _validate_aggregates(
    aggregates: Mapping[str, Aggregate | dict[str, Any]] | None, argument: str = "aggregates"
) -> Mapping[str, Aggregate | dict[str, Any]] | None:
    """Validate a mapping of client-defined aggregate IDs to aggregate specs.

    A raw dict value is the dumped form of a bucket aggregate, e.g. ``{"uniqueValues": {"property":
    [...], "aggregates": {...}}}``, so its own nested ``aggregates`` (if any) is validated too.
    """
    if aggregates is None:
        return None
    if not isinstance(aggregates, Mapping):
        raise TypeError(f"{argument!r} must be a mapping, not {type(aggregates).__name__}. {_AGGREGATES_HINT}")
    for aggregate_id, aggregate in aggregates.items():
        if not isinstance(aggregate_id, str):
            raise TypeError(f"{argument!r} keys must be strings, not {type(aggregate_id).__name__}. {_AGGREGATES_HINT}")
        if not isinstance(aggregate, (Aggregate, Mapping)):
            raise TypeError(
                f"{argument!r}[{aggregate_id!r}] must be an Aggregate or dict, "
                f"not {type(aggregate).__name__}. {_AGGREGATES_HINT}"
            )
        if isinstance(aggregate, Mapping):
            for body in aggregate.values():
                if isinstance(body, Mapping) and "aggregates" in body:
                    _validate_aggregates(body["aggregates"], f"{argument}[{aggregate_id!r}]['aggregates']")
    return aggregates


def _validate_filters(
    filters: Filter | Mapping[str, Any] | Sequence[Filter | Mapping[str, Any]], argument: str = "filters"
) -> Sequence[Filter | Mapping[str, Any]]:
    """Validate the filters a Filters aggregate buckets matched items by, one bucket per filter.

    As with most sequence arguments in the SDK, a single filter is accepted and wrapped into a
    one-item list rather than rejected, so ``Filters(filters=filters.MatchAll())`` works too.
    """
    if isinstance(filters, (Filter, Mapping)):
        filters = [filters]
    hint = "Each filter creates one bucket."
    if isinstance(filters, str) or not isinstance(filters, Sequence):
        raise TypeError(
            f"{argument!r} must be a Filter, dict, or sequence of those, not {type(filters).__name__}. {hint}"
        )
    if invalid := [filter for filter in filters if not isinstance(filter, (Filter, Mapping))]:
        raise TypeError(
            f"{argument!r} must be a sequence of Filter or dict, but got an entry "
            f"of type {type(invalid[0]).__name__}. {hint}"
        )
    return filters


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
            return MovingFunction(
                buckets_path=body["bucketsPath"],
                window=body["window"],
                function=MovingFunctions(body["function"]),
            )
        return UnknownAggregate(resource)


class Average(Aggregate):
    """Average aggregate over a container property."""

    _aggregate_name = "avg"

    def __init__(self, property: SequenceNotStr[str]) -> None:
        self.property = validate_property_path(property)

    def _dump_body(self) -> dict[str, Any]:
        return {"property": self.property}


class Count(Aggregate):
    """Count matched items, or non-null values when ``property`` is provided."""

    _aggregate_name = "count"

    def __init__(self, property: SequenceNotStr[str] | None = None) -> None:
        self.property = validate_property_path(property) if property is not None else None

    def _dump_body(self) -> dict[str, Any]:
        return {"property": self.property} if self.property is not None else {}


class Min(Aggregate):
    """Minimum aggregate over a property."""

    _aggregate_name = "min"

    def __init__(self, property: SequenceNotStr[str]) -> None:
        self.property = validate_property_path(property)

    def _dump_body(self) -> dict[str, Any]:
        return {"property": self.property}


class Max(Aggregate):
    """Maximum aggregate over a property."""

    _aggregate_name = "max"

    def __init__(self, property: SequenceNotStr[str]) -> None:
        self.property = validate_property_path(property)

    def _dump_body(self) -> dict[str, Any]:
        return {"property": self.property}


class Sum(Aggregate):
    """Sum aggregate over a container property."""

    _aggregate_name = "sum"

    def __init__(self, property: SequenceNotStr[str]) -> None:
        self.property = validate_property_path(property)

    def _dump_body(self) -> dict[str, Any]:
        return {"property": self.property}


class UniqueValues(Aggregate):
    """Bucket matched items by unique property values."""

    _aggregate_name = "uniqueValues"

    def __init__(
        self,
        property: SequenceNotStr[str],
        aggregates: Mapping[str, Aggregate | dict[str, Any]] | None = None,
        size: int | None = None,
    ):
        self.property = validate_property_path(property)
        self.aggregates = _validate_aggregates(aggregates)
        self.size = size

    def _dump_body(self) -> dict[str, Any]:
        body: dict[str, Any] = {"property": self.property}
        if self.aggregates is not None:
            body["aggregates"] = self.aggregates
        if self.size is not None:
            body["size"] = self.size
        return body


class NumberHistogram(Aggregate):
    """Bucket numeric property values into fixed-width intervals."""

    _aggregate_name = "numberHistogram"

    def __init__(
        self,
        property: SequenceNotStr[str],
        interval: float,
        aggregates: Mapping[str, Aggregate | dict[str, Any]] | None = None,
        hard_bounds: Mapping[str, float] | None = None,
    ) -> None:
        self.property = validate_property_path(property)
        self.interval = interval
        self.aggregates = _validate_aggregates(aggregates)
        self.hard_bounds = hard_bounds

    def _dump_body(self) -> dict[str, Any]:
        body: dict[str, Any] = {"property": self.property, "interval": self.interval}
        if self.aggregates is not None:
            body["aggregates"] = self.aggregates
        if self.hard_bounds is not None:
            body["hardBounds"] = self.hard_bounds
        return body


class TimeHistogram(Aggregate):
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
        self.property = validate_property_path(property)
        self.calendar_interval = calendar_interval
        self.fixed_interval = fixed_interval
        self.aggregates = _validate_aggregates(aggregates)
        self.hard_bounds = hard_bounds

    def _dump_body(self) -> dict[str, Any]:
        body: dict[str, Any] = {"property": self.property}
        if self.aggregates is not None:
            body["aggregates"] = self.aggregates
        if self.calendar_interval is not None:
            body["calendarInterval"] = self.calendar_interval
        if self.fixed_interval is not None:
            body["fixedInterval"] = self.fixed_interval
        if self.hard_bounds is not None:
            body["hardBounds"] = self.hard_bounds
        return body


class Filters(Aggregate):
    """Bucket matched items by a list of filter expressions."""

    _aggregate_name = "filters"

    def __init__(
        self,
        filters: Filter | Mapping[str, Any] | Sequence[Filter | Mapping[str, Any]],
        aggregates: Mapping[str, Aggregate | dict[str, Any]] | None = None,
    ) -> None:
        self.filters = _validate_filters(filters)
        self.aggregates = _validate_aggregates(aggregates)

    def _dump_body(self) -> dict[str, Any]:
        body: dict[str, Any] = {
            "filters": [filter.dump() if isinstance(filter, Filter) else filter for filter in self.filters]
        }
        if self.aggregates is not None:
            body["aggregates"] = self.aggregates
        return body


class MovingFunctions(str, Enum):
    """How a :class:`MovingFunction` reduces each window of buckets to a single number.

    ``MAX``, ``MIN`` and ``SUM`` take that statistic over the window. ``UNWEIGHTED_AVG`` averages the
    window; ``LINEAR_WEIGHTED_AVG`` averages it too, but weights the most recent buckets highest, so it
    follows a trend more closely at the cost of being noisier.

    The wire values carry a ``MovingFunctions.`` prefix, but the unprefixed suffix (e.g. ``"sum"``) is
    also accepted, so ``MovingFunctions("sum") is MovingFunctions.SUM``.
    """

    MAX = "MovingFunctions.max"
    MIN = "MovingFunctions.min"
    SUM = "MovingFunctions.sum"
    UNWEIGHTED_AVG = "MovingFunctions.unweightedAvg"
    LINEAR_WEIGHTED_AVG = "MovingFunctions.linearWeightedAvg"

    @classmethod
    def _missing_(cls, value: object) -> MovingFunctions | None:
        if isinstance(value, str) and not value.startswith(f"{cls.__name__}."):
            for member in cls:
                if member.value == f"{cls.__name__}.{value}":
                    return member
        return None


class MovingFunction(Aggregate):
    """Smooth a histogram's buckets by re-aggregating them over a sliding window.

    Nested inside a :class:`NumberHistogram` or :class:`TimeHistogram`, it reads one of that histogram's
    own aggregates bucket by bucket and reduces every ``window`` consecutive buckets to one value - a
    7-day moving average of a daily count, say - so a trend stays readable through the noise in
    individual buckets. ``buckets_path`` names the sibling aggregate to read, or ``"_count"`` for the
    buckets' own item counts.
    """

    _aggregate_name = "movingFunction"

    def __init__(self, buckets_path: str, window: int, function: MovingFunctions) -> None:
        self.buckets_path = buckets_path
        self.window = window
        # Coerced rather than stored as given: MovingFunctions is a str enum, so an untyped caller
        # passing the raw wire string gets a ValueError here instead of an AttributeError in dump().
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


def _load_results(resource: dict[str, Any] | None) -> dict[str, Result]:
    """Load a map of aggregate results keyed by the client-defined aggregate IDs.

    The IDs are chosen by the caller and left untouched; only each result's own payload is parsed.
    """
    return {aggregate_id: Result._load(result) for aggregate_id, result in (resource or {}).items()}


def _dump_results(results: dict[str, Result], camel_case: bool) -> dict[str, Any]:
    """Dump a map of aggregate results keyed by the client-defined aggregate IDs."""
    return {aggregate_id: result.dump(camel_case=camel_case) for aggregate_id, result in results.items()}


class Result(CogniteResource):
    """Base class for typed aggregate results.

    One result type corresponds to each request builder above; :meth:`load` dispatches on the wire
    key the API returns.
    """

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Result:
        # Dispatcher: each aggregate result carries exactly one top-level key that selects the
        # concrete result type. Each subclass implements its own _load returning Self.
        assert len(resource) == 1, f"expected exactly one aggregate result key, got {sorted(resource)}"
        key = next(iter(resource))
        if key in _METRIC_RESULT_KEYS:
            return MetricResult._load(resource)
        if key == "fnValue":
            return MovingFunctionResult._load(resource)
        if (result_cls := _BUCKET_RESULT_BY_KEY.get(key)) is not None:
            return result_cls._load(resource)
        return UnknownResult._load(resource)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        raise NotImplementedError


class MetricResult(Result):
    """Result of a metric aggregate (:class:`Average`, :class:`Count`, :class:`Min`, :class:`Max`, :class:`Sum`)."""

    def __init__(self, aggregate: str, value: Any) -> None:
        self.aggregate = aggregate
        self.value = value

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        key = next(iter(resource))
        return cls(aggregate=key, value=resource[key])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {self.aggregate: self.value}


class MovingFunctionResult(Result):
    """Result of a :class:`MovingFunction` aggregate."""

    def __init__(self, fn_value: float) -> None:
        self.fn_value = fn_value

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(fn_value=resource["fnValue"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"fnValue" if camel_case else "fn_value": self.fn_value}


class UnknownResult(Result):
    """Fallback for aggregate result shapes this SDK version does not model yet.

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


class Bucket(CogniteResource):
    """One bucket of a :class:`BucketResult`.

    Args:
        count (int): Number of matched items in this bucket.
        value (Any): The unique property value this bucket represents, for :class:`UniqueValues`.
        interval_start (float | str | None): Lower bound of this bucket, for the histograms.
        aggregates (dict[str, Result] | None): Results of the aggregates nested under this bucket,
            keyed by the client-defined aggregate IDs.
    """

    def __init__(
        self,
        count: int,
        value: Any = None,
        interval_start: float | str | None = None,
        aggregates: dict[str, Result] | None = None,
    ) -> None:
        self.count = count
        self.value = value
        self.interval_start = interval_start
        self.aggregates = aggregates or {}

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            count=resource["count"],
            value=resource.get("value"),
            interval_start=resource.get("intervalStart"),
            aggregates=_load_results(resource.get("aggregates")),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"count": self.count}
        if self.value is not None:
            output["value"] = self.value
        if self.interval_start is not None:
            output["intervalStart" if camel_case else "interval_start"] = self.interval_start
        if self.aggregates:
            output["aggregates"] = _dump_results(self.aggregates, camel_case)
        return output


class BucketResult(Result):
    """Result of an aggregate that buckets the matched items.

    Args:
        buckets (Sequence[Bucket]): The buckets the aggregate produced.
    """

    _buckets_key: ClassVar[str]

    def __init__(self, buckets: Sequence[Bucket]) -> None:
        self._buckets = list(buckets)

    @property
    def buckets(self) -> list[Bucket]:
        return list(self._buckets)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(buckets=[Bucket._load(bucket) for bucket in resource.get(cls._buckets_key, [])])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        key = self._buckets_key if camel_case else to_snake_case(self._buckets_key)
        return {key: [bucket.dump(camel_case=camel_case) for bucket in self._buckets]}


class UniqueValuesResult(BucketResult):
    """Result of a :class:`UniqueValues` aggregate."""

    _buckets_key = "uniqueValueBuckets"


class NumberHistogramResult(BucketResult):
    """Result of a :class:`NumberHistogram` aggregate."""

    _buckets_key = "numberHistogramBuckets"


class TimeHistogramResult(BucketResult):
    """Result of a :class:`TimeHistogram` aggregate."""

    _buckets_key = "timeHistogramBuckets"


class FiltersResult(BucketResult):
    """Result of a :class:`Filters` aggregate."""

    _buckets_key = "filterBuckets"


_METRIC_RESULT_KEYS: frozenset[str] = frozenset({"avg", "count", "min", "max", "sum"})

_BUCKET_RESULT_BY_KEY: dict[str, type[BucketResult]] = {
    result_cls._buckets_key: result_cls
    for result_cls in (
        UniqueValuesResult,
        NumberHistogramResult,
        TimeHistogramResult,
        FiltersResult,
    )
}
